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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.FTCreateParams;
import redis.clients.jedis.search.Schema.FieldType;
import redis.clients.jedis.search.SearchProtocol.SearchKeyword;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.aggr.AggregationBuilder;
import redis.clients.jedis.search.aggr.AggregationResult;
import redis.clients.jedis.search.aggr.Reducer;
import redis.clients.jedis.search.aggr.Reducers;
import redis.clients.jedis.search.schemafields.GeoField;
import redis.clients.jedis.search.schemafields.NumericField;
import redis.clients.jedis.search.schemafields.SchemaField;
import redis.clients.jedis.search.schemafields.TagField;
import redis.clients.jedis.search.schemafields.TextField;
import redis.clients.jedis.search.schemafields.VectorField;
import redis.clients.jedis.search.schemafields.VectorField.VectorAlgorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class RedisAggregationSession
{
    private static final String FIELD_ATTRIBUTES = "attributes";
    private static final Logger log = Logger.get(RedisAggregationSession.class);

    private final TypeManager typeManager;

    private final RedisConnectorConfig config;

    private final RedisAggregationQueryBuilder queryBuilder = new RedisAggregationQueryBuilder();

    private final JedisPooled jedis;

    private final Cache<SchemaTableName, RedisAggregationTable> tableCache;

    public RedisAggregationSession(TypeManager typeManager, RedisConnectorConfig config, JedisPooled jedisPool)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.config = requireNonNull(config, "config is null");
        this.jedis = jedisPool;
        this.tableCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(config.getTableDescriptionCacheDuration().toMillis(), TimeUnit.MILLISECONDS).build();
    }

    public RedisConnectorConfig getConfig()
    {
        return config;
    }

    public List<HostAddress> getAddresses()
    {
        return new ArrayList<>(config.getNodes());
    }

    private Set<String> listIndexNames()
            throws SchemaNotFoundException
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(jedis.ftList());
        return builder.build();
    }

    /**
     * @param schemaTableName SchemaTableName to load
     * @return RediSearchTable describing the RediSearch index
     * @throws TableNotFoundException if no index by that name was found
     */
    public RedisAggregationTable getTable(SchemaTableName tableName)
            throws TableNotFoundException
    {
        try {
            return tableCache.get(tableName, () -> loadTableSchema(tableName));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e);
        }
    }

    public Set<String> getAllTables()
    {
        return listIndexNames().stream().collect(toSet());
    }

    public void createTable(SchemaTableName schemaTableName, List<RedisAggregationColumnHandle> columns)
    {
        String index = schemaTableName.getTableName();
        if (!jedis.ftList().contains(index)) {
            List<SchemaField> fields = columns.stream().filter(c -> !RedisAggregationBuiltinField.isKeyColumn(c.getName()))
                    .map(this::field).collect(Collectors.toList());
            FTCreateParams params = new FTCreateParams();
            params.addPrefix(index + ":");
            jedis.ftCreate(index, params, fields);
        }
    }

    public void dropTable(SchemaTableName tableName)
    {
        jedis.ftDropIndexDD(toRemoteTableName(tableName.getTableName()));
        tableCache.invalidate(tableName);
    }

    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata columnMetadata)
    {
        String tableName = toRemoteTableName(schemaTableName.getTableName());
        jedis.ftAlter(tableName, field(columnMetadata.getName(), columnMetadata.getType()));
        tableCache.invalidate(schemaTableName);
    }

    private String toRemoteTableName(String tableName)
    {
        verify(tableName.equals(tableName.toLowerCase(ENGLISH)), "tableName not in lower-case: %s", tableName);
        if (!config.isCaseInsensitiveNames()) {
            return tableName;
        }
        for (String remoteTableName : listIndexNames()) {
            if (tableName.equals(remoteTableName.toLowerCase(ENGLISH))) {
                return remoteTableName;
            }
        }
        return tableName;
    }

    public void dropColumn(SchemaTableName schemaTableName, String columnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    /**
     * @param schemaTableName SchemaTableName to load
     * @return RediSearchTable describing the RediSearch index
     * @throws TableNotFoundException if no index by that name was found
     */
    private RedisAggregationTable loadTableSchema(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        String index = toRemoteTableName(schemaTableName.getTableName());
        Optional<RedisAggregationIndexInfo> indexInfoOptional = indexInfo(index);
        if (indexInfoOptional.isEmpty()) {
            throw new TableNotFoundException(schemaTableName, format("Index '%s' not found", index), null);
        }
        RedisAggregationIndexInfo indexInfo = indexInfoOptional.get();
        Set<String> fields = new HashSet<>();
        ImmutableList.Builder<RedisAggregationColumnHandle> columns = ImmutableList.builder();
        for (RedisAggregationBuiltinField builtinfield : RedisAggregationBuiltinField.values()) {
            fields.add(builtinfield.getName());
            columns.add(builtinfield.getColumnHandle());
        }
        for (SchemaField indexedField : indexInfo.getFields()) {
            RedisAggregationColumnHandle column = buildColumnHandle(indexedField);
            fields.add(column.getName());
            columns.add(column);
        }
        SearchResult results = jedis.ftSearch(index, "*");
        for (Document doc : results.getDocuments()) {
            for (Entry<String, Object> property : doc.getProperties()) {
                String propertyName = property.getKey();
                if (fields.contains(propertyName)) {
                    continue;
                }
                columns.add(
                        new RedisAggregationColumnHandle(propertyName, VarcharType.VARCHAR, FieldType.TEXT, false, false));
                fields.add(propertyName);
            }
        }
        RedisAggregationTableHandle tableHandle = new RedisAggregationTableHandle(schemaTableName, index);
        return new RedisAggregationTable(tableHandle, columns.build(), indexInfo);
    }

    private Optional<RedisAggregationIndexInfo> indexInfo(String index)
    {
        try {
            Map<String, Object> map = jedis.ftInfo(index);
            if (map != null && !map.isEmpty()) {
                return Optional.of(indexInfo(map));
            }
        }
        catch (Exception e) {
            // Ignore as index might not exist
        }
        return Optional.empty();
    }

    private RedisAggregationColumnHandle buildColumnHandle(SchemaField field)
    {
        return buildColumnHandle(name(field), fieldType(field), false, true);
    }

    private FieldType fieldType(SchemaField field)
    {
        if (field instanceof GeoField) {
            return FieldType.GEO;
        }
        if (field instanceof NumericField) {
            return FieldType.NUMERIC;
        }
        if (field instanceof TagField) {
            return FieldType.TAG;
        }
        if (field instanceof TextField) {
            return FieldType.TEXT;
        }
        throw new UnsupportedOperationException("Field type not supported for field " + field.getFieldName().getName());
    }

    private String name(SchemaField field)
    {
        String attribute = field.getFieldName().getAttribute();
        if (attribute == null || attribute.isEmpty()) {
            return field.getName();
        }
        return attribute;
    }

    private RedisAggregationColumnHandle buildColumnHandle(String name, FieldType type, boolean hidden,
            boolean supportsPredicates)
    {
        return new RedisAggregationColumnHandle(name, columnType(type), type, hidden, supportsPredicates);
    }

    private Type columnType(FieldType type)
    {
        return columnType(typeSignature(type));
    }

    private Type columnType(TypeSignature typeSignature)
    {
        return typeManager.fromSqlType(typeSignature.toString());
    }

    public RedisAggregationOptions aggregation(RedisAggregationTableHandle table, List<String> columnNames)
    {
        RedisAggregationOptions aggregation = new RedisAggregationOptions();
        aggregation.setIndex(table.getIndex());
        aggregation.setQuery(queryBuilder.buildQuery(table.getConstraint(), table.getWildcards()));
        List<String> loads = new ArrayList<>();
        loads.add(RedisAggregationBuiltinField.KEY.getName());
        loads.addAll(columnNames);
        aggregation.setLoads(loads);
        aggregation.setGroupBy(queryBuilder.groupBy(table));
        aggregation.setLimit(limit(table));
        aggregation.setCursorCount(config.getSearchCursorCount());
        return aggregation;
    }

    private int limit(RedisAggregationTableHandle tableHandle)
    {
        if (tableHandle.getLimit().isPresent()) {
            return tableHandle.getLimit().getAsInt();
        }
        return config.getDefaultSearchLimit();
    }

    public RedisAggregationResult aggregate(RedisAggregationTableHandle table, List<String> columnNames)
    {
        RedisAggregationOptions aggregation = aggregation(table, columnNames);
        log.info("Running %s", aggregation);
        AggregationBuilder builder = new AggregationBuilder(aggregation.getQuery());
        if (aggregation.getCursorCount() > 0) {
            builder.cursor(aggregation.getCursorCount());
        }
        if (aggregation.getLimit() > 0) {
            builder.limit(aggregation.getLimit());
        }
        builder.load(aggregation.getLoads().toArray(new String[0]));
        builder.groupBy(aggregation.getGroupBy().getFields(),
                aggregation.getGroupBy().getReducers().stream().map(this::reducer).toList());
        AggregationResult results = jedis.ftAggregate(aggregation.getIndex(), builder);
        if (results.getResults().isEmpty() && !aggregation.getGroupBy().getFields().isEmpty()) {
            if (!aggregation.getGroupBy().getReducers().isEmpty()) {
                String alias = aggregation.getGroupBy().getReducers().get(0).getAlias();
                if (alias != null) {
                    Map<String, Object> doc = new HashMap<>();
                    doc.put(alias, 0);
                    results.getResults().add(doc);
                }
            }
        }
        return aggregationResult(results);
    }

    private RedisAggregationResult aggregationResult(AggregationResult results)
    {
        RedisAggregationResult aggResult = new RedisAggregationResult();
        aggResult.setCursorId(results.getCursorId());
        aggResult.setResults(results.getResults());
        aggResult.setTotalResults(results.getTotalResults());
        return aggResult;
    }

    private Reducer reducer(io.trino.plugin.redis.RedisAggregationOptions.Reducer reducer)
    {
        String field = reducer.getField();
        String alias = reducer.getAlias();
        switch (reducer.getName()) {
            case RedisAggregation.MAX:
                return Reducers.max(field).as(alias);
            case RedisAggregation.MIN:
                return Reducers.min(field).as(alias);
            case RedisAggregation.SUM:
                return Reducers.sum(field).as(alias);
            case RedisAggregation.AVG:
                return Reducers.avg(field).as(alias);
            case RedisAggregation.COUNT:
                return Reducers.count().as(alias);
            default:
                throw new UnsupportedOperationException("Unsupported reducer: " + reducer.getName());
        }
    }

    public RedisAggregationResult cursorRead(RedisAggregationTableHandle tableHandle, long cursor)
    {
        return aggregationResult(jedis.ftCursorRead(tableHandle.getIndex(), cursor, config.getSearchCursorCount()));
    }

    private SchemaField field(RedisAggregationColumnHandle column)
    {
        return field(column.getName(), column.getType());
    }

    private SchemaField field(String name, Type type)
    {
        FieldType fieldType = toFieldType(type);
        switch (fieldType) {
            case GEO:
                return new GeoField(name);
            case NUMERIC:
                return new NumericField(name);
            case TAG:
                return new TagField(name);
            case TEXT:
                return new TextField(name);
            case VECTOR:
                throw new UnsupportedOperationException("Vector field not supported");
        }
        throw new IllegalArgumentException(String.format("Field type %s not supported", fieldType));
    }

    public static FieldType toFieldType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(BigintType.BIGINT)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(IntegerType.INTEGER)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(TinyintType.TINYINT)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(RealType.REAL)) {
            return FieldType.NUMERIC;
        }
        if (type instanceof DecimalType) {
            return FieldType.NUMERIC;
        }
        if (type instanceof VarcharType) {
            return FieldType.TAG;
        }
        if (type instanceof CharType) {
            return FieldType.TAG;
        }
        if (type.equals(DateType.DATE)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
            return FieldType.NUMERIC;
        }
        if (type.equals(UuidType.UUID)) {
            return FieldType.TAG;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private TypeSignature typeSignature(FieldType type)
    {
        if (type == FieldType.NUMERIC) {
            return doubleType();
        }
        return varcharType();
    }

    private TypeSignature doubleType()
    {
        return DOUBLE.getTypeSignature();
    }

    private TypeSignature varcharType()
    {
        return createUnboundedVarcharType().getTypeSignature();
    }

    public void cursorDelete(RedisAggregationTableHandle tableHandle, long cursor)
    {
        jedis.ftCursorDel(tableHandle.getIndex(), cursor);
    }

    public Long deleteDocs(List<String> docIds)
    {
        return jedis.del(docIds.toArray(String[]::new));
    }

    @SuppressWarnings("unchecked")
    public static RedisAggregationIndexInfo indexInfo(Map<String, Object> map)
    {
        RedisAggregationIndexInfo indexInfo = new RedisAggregationIndexInfo();
        indexInfo.setIndexName((String) map.get("index_name"));
        indexInfo.setFields(fieldsFromAttributes((List<Object>) map.getOrDefault(FIELD_ATTRIBUTES, new ArrayList<>())));
        return indexInfo;
    }

    @SuppressWarnings("unchecked")
    private static List<SchemaField> fieldsFromAttributes(List<Object> list)
    {
        List<SchemaField> fields = new ArrayList<>();
        for (Object object : list) {
            List<Object> attributes = (List<Object>) object;
            SchemaField field = field((String) attributes.get(5), (String) attributes.get(1));
            field.as((String) attributes.get(3));
            fields.add(field);
        }
        return fields;
    }

    private static SchemaField field(String type, String name)
    {
        if (type.equalsIgnoreCase(SearchKeyword.GEO.name())) {
            return new GeoField(name);
        }
        if (type.equalsIgnoreCase(SearchKeyword.NUMERIC.name())) {
            return new NumericField(name);
        }
        if (type.equalsIgnoreCase(SearchKeyword.TAG.name())) {
            return new TagField(name);
        }
        if (type.equalsIgnoreCase(SearchKeyword.TEXT.name())) {
            return new TextField(name);
        }
        if (type.equalsIgnoreCase(SearchKeyword.VECTOR.name())) {
            return new VectorField(name, VectorAlgorithm.FLAT, Collections.emptyMap());
        }
        throw new IllegalArgumentException("Unknown field type: " + type);
    }
}
