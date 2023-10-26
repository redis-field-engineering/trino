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
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.AggregateOperation;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.Group;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.util.RedisModulesUtils;
import io.airlift.log.Logger;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.redis.RediSearchTranslator.Aggregation;
import io.trino.plugin.redis.RediSearchTranslator.Search;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

public class RediSearchSession
{
    private static final Logger log = Logger.get(RediSearchSession.class);

    private final TypeManager typeManager;

    private final RedisConnectorConfig config;

    private final RediSearchTranslator translator;

    private final AbstractRedisClient client;

    private final StatefulRedisModulesConnection<String, String> connection;

    private final Cache<SchemaTableName, RediSearchTable> tableCache;

    public RediSearchSession(TypeManager typeManager, RedisConnectorConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.config = requireNonNull(config, "config is null");
        this.translator = new RediSearchTranslator(config);
        this.client = client(config);
        this.connection = RedisModulesUtils.connection(client);
        this.tableCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(config.getTableDescriptionCacheDuration().toMillis(), TimeUnit.MILLISECONDS).build();
    }

    private AbstractRedisClient client(RedisConnectorConfig config)
    {
        RedisURI redisURI = redisURI(config);
        return RedisModulesClient.create(redisURI);
    }

    private RedisURI redisURI(RedisConnectorConfig config)
    {
        HostAddress hostAddress = config.getNodes().iterator().next();
        RedisURI.Builder uri = RedisURI.builder(RedisURI.create(hostAddress.getHostText(), hostAddress.getPort()));
        if (hasLength(config.getRedisPassword())) {
            if (hasLength(config.getRedisUser())) {
                uri.withAuthentication(config.getRedisUser(), config.getRedisPassword());
            }
            else {
                uri.withPassword(config.getRedisPassword().toCharArray());
            }
        }
        if (config.isInsecure()) {
            uri.withVerifyPeer(false);
        }
        return uri.build();
    }

    private static boolean hasLength(String string)
    {
        return string != null && !string.isEmpty();
    }

    public StatefulRedisModulesConnection<String, String> getConnection()
    {
        return connection;
    }

    public RedisConnectorConfig getConfig()
    {
        return config;
    }

    public void shutdown()
    {
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
    }

    public List<HostAddress> getAddresses()
    {
        return new ArrayList<>(config.getNodes());
    }

    private Set<String> listIndexNames()
            throws SchemaNotFoundException
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(connection.sync().ftList());
        return builder.build();
    }

    /**
     * @param schemaTableName SchemaTableName to load
     * @return RediSearchTable describing the RediSearch index
     * @throws TableNotFoundException if no index by that name was found
     */
    public RediSearchTable getTable(SchemaTableName tableName)
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

    @SuppressWarnings("unchecked")
    public void createTable(SchemaTableName schemaTableName, List<RediSearchColumnHandle> columns)
    {
        String index = schemaTableName.getTableName();
        if (!connection.sync().ftList().contains(index)) {
            List<Field<String>> fields = columns.stream().filter(c -> !RediSearchBuiltinField.isKeyColumn(c.getName()))
                    .map(c -> buildField(c.getName(), c.getType())).collect(Collectors.toList());
            CreateOptions.Builder<String, String> options = CreateOptions.<String, String>builder();
            options.prefix(index + ":");
            connection.sync().ftCreate(index, options.build(), fields.toArray(Field[]::new));
        }
    }

    public void dropTable(SchemaTableName tableName)
    {
        connection.sync().ftDropindexDeleteDocs(toRemoteTableName(tableName.getTableName()));
        tableCache.invalidate(tableName);
    }

    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata columnMetadata)
    {
        String tableName = toRemoteTableName(schemaTableName.getTableName());
        connection.sync().ftAlter(tableName, buildField(columnMetadata.getName(), columnMetadata.getType()));
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
    private RediSearchTable loadTableSchema(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        String index = toRemoteTableName(schemaTableName.getTableName());
        Optional<IndexInfo> indexInfoOptional = indexInfo(index);
        if (indexInfoOptional.isEmpty()) {
            throw new TableNotFoundException(schemaTableName, format("Index '%s' not found", index), null);
        }
        IndexInfo indexInfo = indexInfoOptional.get();
        Set<String> fields = new HashSet<>();
        ImmutableList.Builder<RediSearchColumnHandle> columns = ImmutableList.builder();
        for (RediSearchBuiltinField builtinfield : RediSearchBuiltinField.values()) {
            fields.add(builtinfield.getName());
            columns.add(builtinfield.getColumnHandle());
        }
        for (Field<String> indexedField : indexInfo.getFields()) {
            RediSearchColumnHandle column = buildColumnHandle(indexedField);
            fields.add(column.getName());
            columns.add(column);
        }
        SearchResults<String, String> results = connection.sync().ftSearch(index, "*");
        for (Document<String, String> doc : results) {
            for (String docField : doc.keySet()) {
                if (fields.contains(docField)) {
                    continue;
                }
                columns.add(new RediSearchColumnHandle(docField, VarcharType.VARCHAR, Field.Type.TEXT, false, false));
                fields.add(docField);
            }
        }
        RediSearchTableHandle tableHandle = new RediSearchTableHandle(schemaTableName, index);
        return new RediSearchTable(tableHandle, columns.build(), indexInfo);
    }

    private Optional<IndexInfo> indexInfo(String index)
    {
        try {
            List<Object> indexInfoList = connection.sync().ftInfo(index);
            if (indexInfoList != null) {
                return Optional.of(RedisModulesUtils.indexInfo(indexInfoList));
            }
        }
        catch (Exception e) {
            // Ignore as index might not exist
        }
        return Optional.empty();
    }

    private RediSearchColumnHandle buildColumnHandle(Field<String> field)
    {
        return buildColumnHandle(name(field), field.getType(), false, true);
    }

    private String name(Field<String> field)
    {
        Optional<String> as = field.getAs();
        if (as.isEmpty()) {
            return field.getName();
        }
        return as.get();
    }

    private RediSearchColumnHandle buildColumnHandle(String name, Field.Type type, boolean hidden,
            boolean supportsPredicates)
    {
        return new RediSearchColumnHandle(name, columnType(type), type, hidden, supportsPredicates);
    }

    private Type columnType(Field.Type type)
    {
        return columnType(typeSignature(type));
    }

    private Type columnType(TypeSignature typeSignature)
    {
        return typeManager.fromSqlType(typeSignature.toString());
    }

    public SearchResults<String, String> search(RediSearchTableHandle tableHandle, String[] columns)
    {
        Search search = translator.search(tableHandle, columns);
        log.info("Running %s", search);
        return connection.sync().ftSearch(search.getIndex(), search.getQuery(), search.getOptions());
    }

    public AggregateWithCursorResults<String> aggregate(RediSearchTableHandle table, String[] columnNames)
    {
        Aggregation aggregation = translator.aggregate(table, columnNames);
        log.info("Running %s", aggregation);
        String index = aggregation.getIndex();
        String query = aggregation.getQuery();
        CursorOptions cursor = aggregation.getCursorOptions();
        AggregateOptions<String, String> options = aggregation.getOptions();
        AggregateWithCursorResults<String> results = connection.sync().ftAggregate(index, query, cursor, options);
        List<AggregateOperation<String, String>> groupBys = aggregation.getOptions().getOperations().stream()
                .filter(this::isGroupOperation).collect(Collectors.toList());
        if (results.isEmpty() && !groupBys.isEmpty()) {
            Group groupBy = (Group) groupBys.get(0);
            Optional<String> as = groupBy.getReducers()[0].getAs();
            if (as.isPresent()) {
                Map<String, Object> doc = new HashMap<>();
                doc.put(as.get(), 0);
                results.add(doc);
            }
        }
        return results;
    }

    private boolean isGroupOperation(AggregateOperation<String, String> operation)
    {
        return operation.getType() == AggregateOperation.Type.GROUP;
    }

    public AggregateWithCursorResults<String> cursorRead(RediSearchTableHandle tableHandle, long cursor)
    {
        String index = tableHandle.getIndex();
        if (config.getSearchCursorCount() > 0) {
            return connection.sync().ftCursorRead(index, cursor, config.getSearchCursorCount());
        }
        return connection.sync().ftCursorRead(index, cursor);
    }

    private Field<String> buildField(String columnName, Type columnType)
    {
        Field.Type fieldType = toFieldType(columnType);
        switch (fieldType) {
            case GEO:
                return Field.geo(columnName).build();
            case NUMERIC:
                return Field.numeric(columnName).build();
            case TAG:
                return Field.tag(columnName).build();
            case TEXT:
                return Field.text(columnName).build();
            case VECTOR:
                throw new UnsupportedOperationException("Vector field not supported");
        }
        throw new IllegalArgumentException(String.format("Field type %s not supported", fieldType));
    }

    public static Field.Type toFieldType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(BigintType.BIGINT)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(IntegerType.INTEGER)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(TinyintType.TINYINT)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(RealType.REAL)) {
            return Field.Type.NUMERIC;
        }
        if (type instanceof DecimalType) {
            return Field.Type.NUMERIC;
        }
        if (type instanceof VarcharType) {
            return Field.Type.TAG;
        }
        if (type instanceof CharType) {
            return Field.Type.TAG;
        }
        if (type.equals(DateType.DATE)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
            return Field.Type.NUMERIC;
        }
        if (type.equals(UuidType.UUID)) {
            return Field.Type.TAG;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private TypeSignature typeSignature(Field.Type type)
    {
        if (type == Field.Type.NUMERIC) {
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

    public void cursorDelete(RediSearchTableHandle tableHandle, long cursor)
    {
        connection.sync().ftCursorDelete(tableHandle.getIndex(), cursor);
    }

    public Long deleteDocs(List<String> docIds)
    {
        return connection.sync().del(docIds.toArray(String[]::new));
    }
}
