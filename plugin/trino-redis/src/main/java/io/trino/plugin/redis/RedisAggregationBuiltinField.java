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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import redis.clients.jedis.search.Schema.FieldType;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;

enum RedisAggregationBuiltinField
{
    KEY("__key", VARCHAR, FieldType.TAG);

    private static final Map<String, RedisAggregationBuiltinField> COLUMNS_BY_NAME = stream(values())
            .collect(toImmutableMap(RedisAggregationBuiltinField::getName, identity()));

    private final String name;
    private final Type type;
    private final FieldType fieldType;

    RedisAggregationBuiltinField(String name, Type type, FieldType fieldType)
    {
        this.name = name;
        this.type = type;
        this.fieldType = fieldType;
    }

    public static Optional<RedisAggregationBuiltinField> of(String name)
    {
        return Optional.ofNullable(COLUMNS_BY_NAME.get(name));
    }

    public static boolean isBuiltinColumn(String name)
    {
        return COLUMNS_BY_NAME.containsKey(name);
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public FieldType getFieldType()
    {
        return fieldType;
    }

    public ColumnMetadata getMetadata()
    {
        return ColumnMetadata.builder().setName(name).setType(type).setHidden(true).build();
    }

    public RedisAggregationColumnHandle getColumnHandle()
    {
        return new RedisAggregationColumnHandle(name, type, fieldType, true, false);
    }

    public static boolean isKeyColumn(String columnName)
    {
        return KEY.name.equals(columnName);
    }
}
