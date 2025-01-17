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
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

public class RedisAggregationTerm
{
    private final String term;

    private final Type type;

    @JsonCreator
    public RedisAggregationTerm(@JsonProperty("term") String term, @JsonProperty("type") Type type)
    {
        this.term = term;
        this.type = type;
    }

    @JsonProperty
    public String getTerm()
    {
        return term;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    public static Optional<RedisAggregationTerm> fromColumnHandle(ColumnHandle columnHandle)
    {
        RedisAggregationColumnHandle column = (RedisAggregationColumnHandle) columnHandle;
        return Optional.of(new RedisAggregationTerm(column.getName(), column.getType()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisAggregationTerm that = (RedisAggregationTerm) o;
        return Objects.equals(term, that.term) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(term, type);
    }

    @Override
    public String toString()
    {
        return term;
    }
}
