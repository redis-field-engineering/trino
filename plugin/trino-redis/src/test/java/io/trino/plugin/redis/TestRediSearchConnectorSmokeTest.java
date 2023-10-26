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

import com.google.common.base.Throwables;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.util.RedisModulesUtils;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;

public class TestRediSearchConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final Logger log = Logger.get(TestRediSearchConnectorSmokeTest.class);
    private static final String EMPTY_INDEX = "emptyidx";
    private static final String JSON_INDEX = "jsontest";
    private static final String JSON_PREFIX = "json:";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        RediSearchServer redisearch = closeAfterClass(new RediSearchServer());
        redisearch.getConnection().sync().flushall();
        try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
                .connection(redisearch.getClient())) {
            Beers.populateIndex(connection);
            RedisModulesCommands<String, String> sync = connection.sync();
            CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix(EMPTY_INDEX + ":").build();
            sync.ftCreate(EMPTY_INDEX, options, Field.tag("field1").build());
            sync.ftCreate(JSON_INDEX, CreateOptions.<String, String>builder().on(DataType.JSON).prefix(JSON_PREFIX).build(),
                    Field.tag("$.id").as("id").build(), Field.text("$.message").as("message").build());
            sync.jsonSet(JSON_PREFIX + "1", "$", "{\"id\": \"1\", \"message\": \"this is a test\"}");
            sync.jsonSet(JSON_PREFIX + "2", "$", "{\"id\": \"2\", \"message\": \"this is another test\"}");
        }
        return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, CUSTOMER, NATION, ORDERS, REGION);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_VIEW:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            case SUPPORTS_LIMIT_PUSHDOWN:
                return true;

            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
                return true;

            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_NEGATIVE_DATE:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected void assertQuery(String sql)
    {
        log.info("assertQuery: %s", sql);
        super.assertQuery(sql);
    }

    @Test
    public void testRediSearchFields()
            throws IOException, InterruptedException
    {
        getQueryRunner().execute("select id, last_mod from beers");
        getQueryRunner().execute("select __key from beers");
    }

    @Test
    public void testCountEmptyIndex()
            throws IOException, InterruptedException
    {
        assertQuery("SELECT count(*) FROM " + EMPTY_INDEX, "VALUES 0");
    }

    @Test
    public void testJsonSearch()
            throws IOException
    {
        getQueryRunner().execute("select id, message from " + JSON_INDEX);
    }

    static RuntimeException getTrinoExceptionCause(Throwable e)
    {
        return Throwables.getCausalChain(e).stream().filter(TestRediSearchConnectorSmokeTest::isTrinoException)
                .findFirst().map(RuntimeException.class::cast)
                .orElseThrow(() -> new IllegalArgumentException("Exception does not have TrinoException cause", e));
    }

    private static boolean isTrinoException(Throwable exception)
    {
        requireNonNull(exception, "exception is null");

        if (exception instanceof TrinoException || exception instanceof ParsingException) {
            return true;
        }

        if (exception.getClass().getName().equals("io.trino.client.FailureInfo$FailureException")) {
            try {
                String originalClassName = exception.toString().split(":", 2)[0];
                Class<? extends Throwable> originalClass = Class.forName(originalClassName).asSubclass(Throwable.class);
                return TrinoException.class.isAssignableFrom(originalClass)
                        || ParsingException.class.isAssignableFrom(originalClass);
            }
            catch (ClassNotFoundException e) {
                return false;
            }
        }

        return false;
    }

    @Test
    public void testLikePredicate()
    {
        assertQuery("SELECT name, regionkey FROM nation WHERE name LIKE 'EGY%'");
    }

    @Test
    public void testInPredicate()
    {
        assertQuery("SELECT name, regionkey FROM nation WHERE name in ('EGYPT', 'FRANCE')");
    }

    @Test
    public void testInPredicateNumeric()
    {
        assertQuery("SELECT name, regionkey FROM nation WHERE regionKey in (1, 2, 3)");
    }
}
