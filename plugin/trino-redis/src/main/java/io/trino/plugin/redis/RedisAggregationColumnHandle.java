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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import redis.clients.jedis.search.Schema.FieldType;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RedisAggregationColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;
    private final FieldType fieldType;
    private final boolean hidden;
    private final boolean supportsPredicates;

    @JsonCreator
    public RedisAggregationColumnHandle(@JsonProperty("name") String name, @JsonProperty("columnType") Type type,
            @JsonProperty("fieldType") FieldType fieldType, @JsonProperty("hidden") boolean hidden,
            @JsonProperty("supportsPredicates") boolean supportsPredicates)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.fieldType = requireNonNull(fieldType, "fieldType is null");
        this.hidden = hidden;
        this.supportsPredicates = supportsPredicates;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty("columnType")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("fieldType")
    public FieldType getFieldType()
    {
        return fieldType;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @JsonProperty
    public boolean isSupportsPredicates()
    {
        return supportsPredicates;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return ColumnMetadata.builder().setName(name).setType(type).setHidden(hidden).build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, fieldType, hidden, supportsPredicates);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RedisAggregationColumnHandle other = (RedisAggregationColumnHandle) obj;
        return Objects.equals(name, other.name) && Objects.equals(type, other.type) && this.fieldType == other.fieldType
                && this.hidden == other.hidden && this.supportsPredicates == other.supportsPredicates;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("name", name).add("type", type).add("fieldType", fieldType)
                .add("hidden", hidden).add("supportsPredicates", supportsPredicates).toString();
    }
}
