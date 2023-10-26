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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.testing.Closeables;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class RediSearchQueryRunner
{
    private static final Logger LOG = Logger.get(RediSearchQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    private RediSearchQueryRunner()
    {
    }

    public static DistributedQueryRunner createRediSearchQueryRunner(RediSearchServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createRediSearchQueryRunner(server, ImmutableList.copyOf(tables), ImmutableMap.of(), ImmutableMap.of());
    }

    public static DistributedQueryRunner createRediSearchQueryRunner(RediSearchServer server,
            Iterable<TpchTable<?>> tables, Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties)
                    throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).setExtraProperties(extraProperties).build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            installRediSearchPlugin(server, queryRunner, extraConnectorProperties);

            TestingTrinoClient trinoClient = queryRunner.getClient();

            LOG.info("Loading data...");

            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTable(server, trinoClient, table);
            }
            LOG.info("Loading complete in %s s", Duration.ofNanos(System.nanoTime() - startTime).toSeconds());
            return queryRunner;
        }
        catch (Throwable e) {
            Closeables.closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static <T extends Throwable> T closeAllSuppress(T rootCause, AutoCloseable... closeables)
    {
        requireNonNull(rootCause, "rootCause is null");
        if (closeables == null) {
            return rootCause;
        }
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            }
            catch (Throwable e) {
                // Self-suppression not permitted
                if (rootCause != e) {
                    rootCause.addSuppressed(e);
                }
            }
        }
        return rootCause;
    }

    private static void installRediSearchPlugin(RediSearchServer server, QueryRunner queryRunner, Map<String, String> connectorProperties)
    {
        queryRunner.installPlugin(new RedisPlugin());
        // note: additional copy via ImmutableList so that if fails on nulls
        connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
        connectorProperties.putIfAbsent("redis.nodes", server.getHostAndPort().toString());
        connectorProperties.putIfAbsent("redis.default-search-limit", "100000");
        connectorProperties.putIfAbsent("redis.default-schema", "default");
        queryRunner.createCatalog("redis", "redis", connectorProperties);
    }

    private static void loadTpchTable(RediSearchServer server, TestingTrinoClient trinoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        LOG.info("Running import for %s", table.getTableName());
        try (RediSearchLoader loader = new RediSearchLoader(server.getClient(),
                table.getTableName().toLowerCase(ENGLISH), trinoClient.getServer(), trinoClient.getDefaultSession())) {
            loader.execute(format("SELECT * from %s",
                    new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
        }
        LOG.info("Imported %s in %s s", table.getTableName(), Duration.ofNanos(System.nanoTime() - start).toSeconds());
    }

    public static Session createSession()
    {
        return testSessionBuilder().setCatalog("redis").setSchema(TPCH_SCHEMA).build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createRediSearchQueryRunner(new RediSearchServer(), TpchTable.getTables(),
                ImmutableMap.of("http-server.http.port", "8080"), ImmutableMap.of());

        Logger log = Logger.get(RediSearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
