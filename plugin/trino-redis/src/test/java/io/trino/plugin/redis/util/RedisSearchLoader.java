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
package io.trino.plugin.redis.util;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.operation.Hset;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;
import org.springframework.batch.item.ExecutionContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class RedisSearchLoader
        extends AbstractTestingTrinoClient<Void>
{
    private final AbstractRedisClient client;
    private final String tableName;
    private final AtomicLong count = new AtomicLong();

    public RedisSearchLoader(TestingTrinoServer trinoServer, Session defaultSession, AbstractRedisClient client,
            String tableName)
    {
        super(trinoServer, defaultSession);
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new RedisLoadingSession(session);
    }

    private static class WriteItem
    {
        private final QueryStatusInfo statusInfo;
        private final List<Object> fields;

        private WriteItem(QueryStatusInfo statusInfo, List<Object> fields)
        {
            this.statusInfo = statusInfo;
            this.fields = fields;
        }
    }

    private class RedisLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private OperationItemWriter<String, String, WriteItem> writer;

        private RedisLoadingSession(Session session)
        {
            Hset<String, String, WriteItem> hset = new Hset<>();
            hset.setKeyFunction(fields -> tableName + ":" + count.getAndIncrement());
            hset.setMapFunction(t -> map(t.statusInfo.getColumns(), t.fields));
            writer = new OperationItemWriter<>(client, StringCodec.UTF8, hset);
            writer.open(new ExecutionContext());
        }

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }
            if (data.getData() != null) {
                checkState(types.get() != null, "Data without types received!");
                List<Column> columns = statusInfo.getColumns();
                StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
                if (!connection.sync().ftList().contains(tableName)) {
                    List<Field<String>> schema = new ArrayList<>();
                    for (int i = 0; i < columns.size(); i++) {
                        Type type = types.get().get(i);
                        schema.add(field(columns.get(i).getName(), type));
                    }
                    connection.sync().ftCreate(tableName,
                            CreateOptions.<String, String>builder().prefix(tableName + ":").build(),
                            schema.toArray(Field[]::new));
                }
                List<WriteItem> fieldsList = StreamSupport.stream(data.getData().spliterator(), false)
                        .map(f -> new WriteItem(statusInfo, f)).collect(Collectors.toList());
                writer.write(fieldsList);
            }
        }

        private Field<String> field(String name, Type type)
        {
            if (type instanceof VarcharType) {
                return Field.tag(name).build();
            }
            if (type == BOOLEAN || type == DATE) {
                return Field.tag(name).build();
            }
            if (type == BIGINT || type == INTEGER || type == DOUBLE) {
                return Field.numeric(name).build();
            }
            throw new IllegalArgumentException("Unhandled type: " + type);
        }

        private Map<String, String> map(List<Column> columns, List<Object> fields)
        {
            Map<String, String> map = new LinkedHashMap<>();
            // add values to Hash
            for (int i = 0; i < fields.size(); i++) {
                map.put(columns.get(i).getName(), fields.get(i).toString());
            }
            return map;
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }
    }
}
