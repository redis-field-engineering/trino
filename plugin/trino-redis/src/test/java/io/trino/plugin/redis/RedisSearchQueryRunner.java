/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.redis.util.RedisSearchServer;
import io.trino.plugin.redis.util.RedisTestUtils;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class RedisSearchQueryRunner
{
    private RedisSearchQueryRunner() {}

    private static final Logger log = Logger.get(RedisSearchQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createRedisSearchQueryRunner(
            RedisSearchServer redisSearchServer,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            installRedisPlugin(redisSearchServer, queryRunner, connectorProperties);

            TestingTrinoClient trinoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTable(redisSearchServer, trinoClient, table);
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, redisSearchServer);
            throw e;
        }
    }

    private static void loadTpchTable(RedisSearchServer redisSearchServer, TestingTrinoClient trinoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        RedisTestUtils.loadTpchTable(
                redisSearchServer,
                trinoClient,
                redisTableName(table),
                new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)));
        log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String redisTableName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + ":" + table.getTableName().toLowerCase(ENGLISH);
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("redis")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRedisSearchQueryRunner(
                new RedisSearchServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());

        Logger log = Logger.get(RedisSearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
