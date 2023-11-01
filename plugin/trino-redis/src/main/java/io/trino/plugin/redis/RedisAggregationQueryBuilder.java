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

import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.plugin.redis.RedisAggregationOptions.Reducer;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import redis.clients.jedis.search.Schema.FieldType;
import redis.clients.jedis.search.querybuilder.Node;
import redis.clients.jedis.search.querybuilder.QueryBuilders;
import redis.clients.jedis.search.querybuilder.QueryNode;
import redis.clients.jedis.search.querybuilder.Value;
import redis.clients.jedis.search.querybuilder.Values;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class RedisAggregationQueryBuilder
{
    public String buildQuery(TupleDomain<ColumnHandle> tupleDomain)
    {
        return buildQuery(tupleDomain, Map.of());
    }

    public String buildQuery(TupleDomain<ColumnHandle> tupleDomain, Map<String, String> wildcards)
    {
        List<Node> nodes = new ArrayList<>();
        Optional<Map<ColumnHandle, Domain>> domains = tupleDomain.getDomains();
        if (domains.isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
                RedisAggregationColumnHandle column = (RedisAggregationColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                checkArgument(!domain.isNone(), "Unexpected NONE domain for %s", column.getName());
                if (!domain.isAll()) {
                    buildPredicate(column, domain).ifPresent(nodes::add);
                }
            }
        }
        for (Entry<String, String> wildcard : wildcards.entrySet()) {
            nodes.add(QueryBuilders.intersect(wildcard.getKey(), wildcard.getValue()));
        }
        if (nodes.isEmpty()) {
            return "*";
        }
        return QueryBuilders.intersect(nodes.toArray(new Node[0])).toString();
    }

    private static String escapeTag(String value)
    {
        return value.replaceAll("([^a-zA-Z0-9])", "\\\\$1");
    }

    private Optional<Node> buildPredicate(RedisAggregationColumnHandle column, Domain domain)
    {
        String columnName = escapeTag(column.getName());
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
        if (domain.getValues().isNone()) {
            return Optional.empty();
        }
        if (domain.getValues().isAll()) {
            return Optional.empty();
        }
        Set<Object> singleValues = new HashSet<>();
        List<Node> disjuncts = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(translateValue(range.getSingleValue(), column.getType()));
            }
            else {
                List<Value> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    Object translated = translateValue(range.getLowBoundedValue(), column.getType());
                    if (translated instanceof Number) {
                        double doubleValue = ((Number) translated).doubleValue();
                        rangeConjuncts.add(range.isLowInclusive() ? Values.ge(doubleValue) : Values.gt(doubleValue));
                    }
                    else {
                        throw new UnsupportedOperationException(
                                String.format("Range constraint not supported for type %s (column: '%s')",
                                        column.getType(), column.getName()));
                    }
                }
                if (!range.isHighUnbounded()) {
                    Object translated = translateValue(range.getHighBoundedValue(), column.getType());
                    if (translated instanceof Number) {
                        double doubleValue = ((Number) translated).doubleValue();
                        rangeConjuncts.add(range.isHighInclusive() ? Values.le(doubleValue) : Values.lt(doubleValue));
                    }
                    else {
                        throw new UnsupportedOperationException(
                                String.format("Range constraint not supported for type %s (column: '%s')",
                                        column.getType(), column.getName()));
                    }
                }
                // If conjuncts is null, then the range was ALL, which should
                // already have been
                // checked for
                if (!rangeConjuncts.isEmpty()) {
                    disjuncts.add(QueryBuilders.intersect(columnName, rangeConjuncts.toArray(Value[]::new)));
                }
            }
        }
        singleValues(column, singleValues).ifPresent(disjuncts::add);
        return Optional.of(QueryBuilders.union(disjuncts.toArray(Node[]::new)));
    }

    private Optional<QueryNode> singleValues(RedisAggregationColumnHandle column, Set<Object> singleValues)
    {
        if (singleValues.isEmpty()) {
            return Optional.empty();
        }
        if (singleValues.size() == 1) {
            return Optional.of(
                    QueryBuilders.intersect(column.getName(), value(Iterables.getOnlyElement(singleValues), column)));
        }
        if (column.getType() instanceof VarcharType && column.getFieldType() == FieldType.TAG) {
            // Takes care of IN: col IN ('value1', 'value2', ...)
            return Optional
                    .of(QueryBuilders.intersect(column.getName(), Values.tags(singleValues.toArray(String[]::new))));
        }
        Value[] values = singleValues.stream().map(v -> value(v, column)).toArray(Value[]::new);
        return Optional.of(QueryBuilders.union(column.getName(), values));
    }

    private Value value(Object trinoNativeValue, RedisAggregationColumnHandle column)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");
        requireNonNull(column, "column is null");
        Type type = column.getType();
        if (type == DOUBLE) {
            return Values.eq((Double) trinoNativeValue);
        }
        if (type == TINYINT) {
            return Values.eq((long) SignedBytes.checkedCast(((Long) trinoNativeValue)));
        }
        if (type == SMALLINT) {
            return Values.eq((long) Shorts.checkedCast(((Long) trinoNativeValue)));
        }
        if (type == IntegerType.INTEGER) {
            return Values.eq((long) toIntExact(((Long) trinoNativeValue)));
        }
        if (type == BIGINT) {
            return Values.eq((Long) trinoNativeValue);
        }
        if (type instanceof VarcharType) {
            if (column.getFieldType() == FieldType.TAG) {
                return Values.tags(escapeTag((String) trinoNativeValue));
            }
            return Values.value((String) trinoNativeValue);
        }
        throw new UnsupportedOperationException("Type " + type + " not supported");
    }

    private Object translateValue(Object trinoNativeValue, Type type)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");
        requireNonNull(type, "type is null");
        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue),
                "%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(), type);

        if (type == DOUBLE) {
            return trinoNativeValue;
        }
        if (type == TINYINT) {
            return (long) SignedBytes.checkedCast(((Long) trinoNativeValue));
        }

        if (type == SMALLINT) {
            return (long) Shorts.checkedCast(((Long) trinoNativeValue));
        }

        if (type == IntegerType.INTEGER) {
            return (long) toIntExact(((Long) trinoNativeValue));
        }

        if (type == BIGINT) {
            return trinoNativeValue;
        }
        if (type instanceof VarcharType) {
            return ((Slice) trinoNativeValue).toStringUtf8();
        }
        throw new IllegalArgumentException("Unhandled type: " + type);
    }

    public static class GroupBy
    {
        private List<String> fields = new ArrayList<>();
        private List<Reducer> reducers = new ArrayList<>();

        public List<String> getFields()
        {
            return fields;
        }

        public void setFields(List<String> fields)
        {
            this.fields = fields;
        }

        public List<Reducer> getReducers()
        {
            return reducers;
        }

        public void setReducers(List<Reducer> reducers)
        {
            this.reducers = reducers;
        }
    }

    public GroupBy groupBy(RedisAggregationTableHandle table)
    {
        GroupBy groupBy = new GroupBy();
        List<RedisAggregation> aggregates = table.getMetricAggregations();
        if (table.getTermAggregations() != null) {
            groupBy.setFields(table.getTermAggregations().stream().map(RedisAggregationTerm::getTerm)
                    .collect(Collectors.toList()));
        }
        groupBy.setReducers(aggregates.stream().map(this::reducer).collect(Collectors.toList()));
        return groupBy;
    }

    private Reducer reducer(RedisAggregation aggregation)
    {
        Optional<RedisAggregationColumnHandle> column = aggregation.getColumnHandle();
        String field = column.isPresent() ? column.get().getName() : null;
        String alias = aggregation.getAlias();
        Reducer reducer = new Reducer();
        reducer.setField(field);
        reducer.setAlias(alias);
        reducer.setName(aggregation.getFunctionName());
        return reducer;
    }
}
