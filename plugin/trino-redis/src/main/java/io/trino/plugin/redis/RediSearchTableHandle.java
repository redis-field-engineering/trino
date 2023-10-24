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
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RediSearchTableHandle
        implements ConnectorTableHandle
{
    public enum Type
    {
        SEARCH, AGGREGATE
    }

    private final SchemaTableName schemaTableName;
    private final String index;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;
    // for group by fields
    private final List<RediSearchAggregationTerm> aggregationTerms;
    private final List<RediSearchAggregation> aggregations;
    private final Map<String, String> wildcards;
    // UPDATE only
    private final List<RediSearchColumnHandle> updatedColumns;

    public RediSearchTableHandle(SchemaTableName schemaTableName, String index)
    {
        this(schemaTableName, index, TupleDomain.all(), OptionalLong.empty(), Collections.emptyList(),
                Collections.emptyList(), Map.of(), Collections.emptyList());
    }

    @JsonCreator
    public RediSearchTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("index") String index, @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("aggTerms") List<RediSearchAggregationTerm> termAggregations,
            @JsonProperty("aggregates") List<RediSearchAggregation> metricAggregations,
            @JsonProperty("wildcards") Map<String, String> wildcards,
            @JsonProperty("updatedColumns") List<RediSearchColumnHandle> updatedColumns)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.index = requireNonNull(index, "index is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.aggregationTerms = requireNonNull(termAggregations, "aggTerms is null");
        this.aggregations = requireNonNull(metricAggregations, "aggregates is null");
        this.wildcards = requireNonNull(wildcards, "wildcards is null");
        this.updatedColumns = ImmutableList.copyOf(requireNonNull(updatedColumns, "updatedColumns is null"));
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public List<RediSearchAggregationTerm> getTermAggregations()
    {
        return aggregationTerms;
    }

    @JsonProperty
    public List<RediSearchAggregation> getMetricAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public Map<String, String> getWildcards()
    {
        return wildcards;
    }

    @JsonProperty
    public List<RediSearchColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, constraint, limit, updatedColumns);
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
        RediSearchTableHandle other = (RediSearchTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) && Objects.equals(this.index, other.index)
                && Objects.equals(this.constraint, other.constraint) && Objects.equals(this.limit, other.limit)
                && Objects.equals(updatedColumns, other.updatedColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("schemaTableName", schemaTableName).add("index", index)
                .add("limit", limit).add("constraint", constraint).toString();
    }
}
