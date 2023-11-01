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
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import redis.clients.jedis.search.Schema.FieldType;
import redis.clients.jedis.search.querybuilder.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class RedisAggregationMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(RedisAggregationMetadata.class);

    private static final String SYNTHETIC_COLUMN_NAME_PREFIX = "syntheticColumn";
    private static final Set<Integer> REDISEARCH_RESERVED_CHARACTERS = IntStream
            .of('?', '*', '|', '{', '}', '[', ']', '(', ')', '"', '#', '@', '&', '<', '>', '~').boxed()
            .collect(toImmutableSet());

    private final RedisAggregationSession rediSearchSession;
    private final String schemaName;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public RedisAggregationMetadata(RedisAggregationSession rediSearchSession)
    {
        this.rediSearchSession = requireNonNull(rediSearchSession, "rediSearchSession is null");
        this.schemaName = rediSearchSession.getConfig().getDefaultSchema();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return List.of(schemaName);
    }

    @Override
    public RedisAggregationTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");

        if (tableName.getSchemaName().equals(schemaName)) {
            try {
                return rediSearchSession.getTable(tableName).getTableHandle();
            }
            catch (TableNotFoundException e) {
                // ignore and return null
            }
        }
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(session, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String tableName : rediSearchSession.getAllTables()) {
            tableNames.add(new SchemaTableName(schemaName, tableName));
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RedisAggregationTableHandle table = (RedisAggregationTableHandle) tableHandle;
        List<RedisAggregationColumnHandle> columns = rediSearchSession.getTable(table.getSchemaTableName()).getColumns();

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (RedisAggregationColumnHandle columnHandle : columns) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.buildOrThrow();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return List.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((RedisAggregationColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        rediSearchSession.createTable(tableMetadata.getTable(), buildColumnHandles(tableMetadata));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RedisAggregationTableHandle table = (RedisAggregationTableHandle) tableHandle;
        rediSearchSession.dropTable(table.getSchemaTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        rediSearchSession.addColumn(((RedisAggregationTableHandle) tableHandle).getSchemaTableName(), column);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        rediSearchSession.dropColumn(((RedisAggregationTableHandle) tableHandle).getSchemaTableName(),
                ((RedisAggregationColumnHandle) column).getName());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        RedisAggregationTableHandle handle = (RedisAggregationTableHandle) table;
        return new ConnectorTableProperties(handle.getConstraint(), Optional.empty(), Optional.empty(), List.of());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle table, long limit)
    {
        RedisAggregationTableHandle handle = (RedisAggregationTableHandle) table;

        if (limit == 0) {
            return Optional.empty();
        }

        if (handle.getLimit().isPresent() && handle.getLimit().getAsInt() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(new RedisAggregationTableHandle(handle.getSchemaTableName(),
                handle.getIndex(), handle.getConstraint(), OptionalInt.of(toIntExact(limit)), handle.getTermAggregations(),
                handle.getMetricAggregations(), handle.getWildcards(), handle.getUpdatedColumns()), true, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle table, Constraint constraint)
    {
        RedisAggregationTableHandle handle = (RedisAggregationTableHandle) table;

        ConnectorExpression oldExpression = constraint.getExpression();
        Map<String, String> newWildcards = new HashMap<>(handle.getWildcards());
        List<ConnectorExpression> expressions = ConnectorExpressions.extractConjuncts(constraint.getExpression());
        List<ConnectorExpression> notHandledExpressions = new ArrayList<>();
        for (ConnectorExpression expression : expressions) {
            if (expression instanceof Call) {
                Call call = (Call) expression;
                if (isSupportedLikeCall(call)) {
                    List<ConnectorExpression> arguments = call.getArguments();
                    String variableName = ((Variable) arguments.get(0)).getName();
                    RedisAggregationColumnHandle column = (RedisAggregationColumnHandle) constraint.getAssignments()
                            .get(variableName);
                    verifyNotNull(column, "No assignment for %s", variableName);
                    String columnName = column.getName();
                    Object pattern = ((Constant) arguments.get(1)).getValue();
                    Optional<Slice> escape = Optional.empty();
                    if (arguments.size() == 3) {
                        escape = Optional.of((Slice) (((Constant) arguments.get(2)).getValue()));
                    }

                    if (!newWildcards.containsKey(columnName) && pattern instanceof Slice) {
                        String wildcard = likeToWildcard((Slice) pattern, escape);
                        if (column.getFieldType() == FieldType.TAG) {
                            wildcard = Values.tags(wildcard).toString();
                        }
                        newWildcards.put(columnName, wildcard);
                        continue;
                    }
                }
            }
            notHandledExpressions.add(expression);
        }

        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();
        Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains()
                .orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            RedisAggregationColumnHandle column = (RedisAggregationColumnHandle) entry.getKey();

            if (column.isSupportsPredicates() && !newWildcards.containsKey(column.getName())) {
                supported.put(column, entry.getValue());
            }
            else {
                unsupported.put(column, entry.getValue());
            }
        }

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(supported));
        ConnectorExpression newExpression = ConnectorExpressions.and(notHandledExpressions);
        if (oldDomain.equals(newDomain) && oldExpression.equals(newExpression)) {
            return Optional.empty();
        }

        handle = new RedisAggregationTableHandle(handle.getSchemaTableName(), handle.getIndex(), newDomain, handle.getLimit(),
                handle.getTermAggregations(), handle.getMetricAggregations(), newWildcards, handle.getUpdatedColumns());

        return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.withColumnDomains(unsupported),
                newExpression, false));
    }

    protected static boolean isSupportedLikeCall(Call call)
    {
        if (!LIKE_FUNCTION_NAME.equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (arguments.size() < 2 || arguments.size() > 3) {
            return false;
        }

        if (!(arguments.get(0) instanceof Variable) || !(arguments.get(1) instanceof Constant)) {
            return false;
        }

        if (arguments.size() == 3) {
            return arguments.get(2) instanceof Constant;
        }

        return true;
    }

    private static char getEscapeChar(Slice escape)
    {
        String escapeString = escape.toStringUtf8();
        if (escapeString.length() == 1) {
            return escapeString.charAt(0);
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Escape string must be a single character");
    }

    protected static String likeToWildcard(Slice pattern, Optional<Slice> escape)
    {
        Optional<Character> escapeChar = escape.map(RedisAggregationMetadata::getEscapeChar);
        StringBuilder wildcard = new StringBuilder();
        boolean escaped = false;
        int position = 0;
        while (position < pattern.length()) {
            int currentChar = getCodePointAt(pattern, position);
            position += 1;
            checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar.get());
            if (!escaped && escapeChar.isPresent() && currentChar == escapeChar.get()) {
                escaped = true;
            }
            else {
                switch (currentChar) {
                    case '%':
                        wildcard.append(escaped ? "%" : "*");
                        escaped = false;
                        break;
                    case '_':
                        wildcard.append(escaped ? "_" : "?");
                        escaped = false;
                        break;
                    case '\\':
                        wildcard.append("\\\\");
                        break;
                    default:
                        // escape special RediSearch characters
                        if (REDISEARCH_RESERVED_CHARACTERS.contains(currentChar)) {
                            wildcard.append('\\');
                        }

                        wildcard.appendCodePoint(currentChar);
                        escaped = false;
                }
            }
        }

        checkEscape(!escaped);
        return wildcard.toString();
    }

    private static void checkEscape(boolean condition)
    {
        if (!condition) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                    "Escape character must be followed by '%', '_' or the escape character itself");
        }
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session,
            ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        log.info("applyAggregation aggregates=%s groupingSets=%s", aggregates, groupingSets);
        RedisAggregationTableHandle table = (RedisAggregationTableHandle) handle;
        // Global aggregation is represented by [[]]
        verify(!groupingSets.isEmpty(), "No grouping sets provided");
        if (!table.getTermAggregations().isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        ImmutableList.Builder<RedisAggregation> aggregations = ImmutableList.builder();
        ImmutableList.Builder<RedisAggregationTerm> terms = ImmutableList.builder();
        for (int i = 0; i < aggregates.size(); i++) {
            AggregateFunction function = aggregates.get(i);
            String colName = SYNTHETIC_COLUMN_NAME_PREFIX + i;
            Optional<RedisAggregation> aggregation = RedisAggregation.handleAggregation(function, assignments,
                    colName);
            if (aggregation.isEmpty()) {
                return Optional.empty();
            }
            io.trino.spi.type.Type outputType = function.getOutputType();
            RedisAggregationColumnHandle newColumn = new RedisAggregationColumnHandle(colName, outputType,
                    RedisAggregationSession.toFieldType(outputType), false, true);
            projections.add(new Variable(colName, function.getOutputType()));
            resultAssignments.add(new Assignment(colName, newColumn, function.getOutputType()));
            aggregations.add(aggregation.get());
        }
        for (ColumnHandle columnHandle : groupingSets.get(0)) {
            Optional<RedisAggregationTerm> termAggregation = RedisAggregationTerm
                    .fromColumnHandle(columnHandle);
            if (termAggregation.isEmpty()) {
                return Optional.empty();
            }
            terms.add(termAggregation.get());
        }
        ImmutableList<RedisAggregation> aggregationList = aggregations.build();
        if (aggregationList.isEmpty()) {
            return Optional.empty();
        }
        RedisAggregationTableHandle tableHandle = new RedisAggregationTableHandle(table.getSchemaTableName(), table.getIndex(),
                table.getConstraint(), table.getLimit(), terms.build(), aggregationList, table.getWildcards(),
                table.getUpdatedColumns());
        return Optional.of(new AggregationApplicationResult<>(tableHandle, projections.build(),
                resultAssignments.build(), Map.of(), false));
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    private SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return ((RedisAggregationTableHandle) tableHandle).getSchemaTableName();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        RedisAggregationTableHandle tableHandle = rediSearchSession.getTable(tableName).getTableHandle();

        List<ColumnMetadata> columns = ImmutableList
                .copyOf(getColumnHandles(session, tableHandle).values().stream().map(RedisAggregationColumnHandle.class::cast)
                        .map(RedisAggregationColumnHandle::toColumnMetadata).collect(Collectors.toList()));

        return new ConnectorTableMetadata(tableName, columns);
    }

    private List<RedisAggregationColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream().map(m -> new RedisAggregationColumnHandle(m.getName(), m.getType(),
                RedisAggregationSession.toFieldType(m.getType()), m.isHidden(), true)).collect(Collectors.toList());
    }
}
