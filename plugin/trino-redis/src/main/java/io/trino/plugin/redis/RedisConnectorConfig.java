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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.trino.spi.HostAddress;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.io.File;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class RedisConnectorConfig
{
    private static final int REDIS_DEFAULT_PORT = 6379;

    private Set<HostAddress> nodes = ImmutableSet.of();
    private int redisScanCount = 100;
    private int redisMaxKeysPerFetch = 100;
    private int redisDataBaseIndex;
    private char redisKeyDelimiter = ':';
    private String redisUser;
    private String redisPassword;
    private Duration redisConnectTimeout = new Duration(2000, MILLISECONDS);
    private String defaultSchema = "default";
    private Set<String> tableNames = ImmutableSet.of();
    private File tableDescriptionDir = new File("etc/redis/");
    private Duration tableDescriptionCacheDuration = new Duration(5, MINUTES);
    private boolean hideInternalColumns = true;
    private boolean keyPrefixSchemaTable;
    private boolean search;
    private int defaultSearchLimit = 10000;
    private int searchCursorCount = 1000;
    private boolean caseInsensitiveNames;

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("redis.table-description-dir")
    @ConfigDescription("Folder holding the JSON description files for Redis values")
    public RedisConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getTableDescriptionCacheDuration()
    {
        return tableDescriptionCacheDuration;
    }

    @Config("redis.table-description-cache-ttl")
    @ConfigDescription("The cache time for redis table description files")
    public RedisConnectorConfig setTableDescriptionCacheDuration(Duration tableDescriptionCacheDuration)
    {
        this.tableDescriptionCacheDuration = tableDescriptionCacheDuration;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("redis.table-names")
    @ConfigDescription("Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given table")
    public RedisConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("redis.default-schema")
    @ConfigDescription("The schema name to use in the connector")
    public RedisConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("redis.nodes")
    @ConfigDescription("Seed nodes for Redis cluster. At least one must exist")
    public RedisConnectorConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    public int getRedisScanCount()
    {
        return redisScanCount;
    }

    @Config("redis.scan-count")
    @ConfigDescription("Count parameter for Redis scan command")
    public RedisConnectorConfig setRedisScanCount(int redisScanCount)
    {
        this.redisScanCount = redisScanCount;
        return this;
    }

    @Min(1)
    public int getRedisMaxKeysPerFetch()
    {
        return redisMaxKeysPerFetch;
    }

    @Config("redis.max-keys-per-fetch")
    @ConfigDescription("Get values associated with the specified number of keys in the command such as MGET(key...)")
    public RedisConnectorConfig setRedisMaxKeysPerFetch(int redisMaxKeysPerFetch)
    {
        this.redisMaxKeysPerFetch = redisMaxKeysPerFetch;
        return this;
    }

    public int getRedisDataBaseIndex()
    {
        return redisDataBaseIndex;
    }

    @Config("redis.database-index")
    @ConfigDescription("Index of the Redis DB to connect to")
    public RedisConnectorConfig setRedisDataBaseIndex(int redisDataBaseIndex)
    {
        this.redisDataBaseIndex = redisDataBaseIndex;
        return this;
    }

    @MinDuration("1s")
    public Duration getRedisConnectTimeout()
    {
        return redisConnectTimeout;
    }

    @Config("redis.connect-timeout")
    @ConfigDescription("Timeout to connect to Redis")
    public RedisConnectorConfig setRedisConnectTimeout(String redisConnectTimeout)
    {
        this.redisConnectTimeout = Duration.valueOf(redisConnectTimeout);
        return this;
    }

    public char getRedisKeyDelimiter()
    {
        return redisKeyDelimiter;
    }

    @Config("redis.key-delimiter")
    @ConfigDescription("Delimiter for separating schema name and table name in the KEY prefix")
    public RedisConnectorConfig setRedisKeyDelimiter(String redisKeyDelimiter)
    {
        this.redisKeyDelimiter = redisKeyDelimiter.charAt(0);
        return this;
    }

    @Nullable
    public String getRedisUser()
    {
        return redisUser;
    }

    @Config("redis.user")
    @ConfigDescription("Username for a Redis server")
    public RedisConnectorConfig setRedisUser(String redisUser)
    {
        this.redisUser = redisUser;
        return this;
    }

    public String getRedisPassword()
    {
        return redisPassword;
    }

    @Config("redis.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for a password-protected Redis server")
    public RedisConnectorConfig setRedisPassword(String redisPassword)
    {
        this.redisPassword = redisPassword;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("redis.hide-internal-columns")
    @ConfigDescription("Whether internal columns are shown in table metadata or not. Default is no")
    public RedisConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public boolean isKeyPrefixSchemaTable()
    {
        return keyPrefixSchemaTable;
    }

    @Config("redis.key-prefix-schema-table")
    @ConfigDescription("Whether Redis key string follows \"schema:table:*\" format")
    public RedisConnectorConfig setKeyPrefixSchemaTable(boolean keyPrefixSchemaTable)
    {
        this.keyPrefixSchemaTable = keyPrefixSchemaTable;
        return this;
    }

    public boolean isSearch()
    {
        return search;
    }

    @Config("redis.search")
    @ConfigDescription("Whether to use advanced Redis search capabilities. Default is no")
    public RedisConnectorConfig setSearch(boolean search)
    {
        this.search = search;
        return this;
    }

    public int getDefaultSearchLimit()
    {
        return defaultSearchLimit;
    }

    @Config("redis.default-search-limit")
    @ConfigDescription("Default search limit number to use")
    public RedisConnectorConfig setDefaultSearchLimit(int limit)
    {
        this.defaultSearchLimit = limit;
        return this;
    }

    @Min(0)
    public int getSearchCursorCount()
    {
        return searchCursorCount;
    }

    @Config("redis.search-cursor-count")
    @ConfigDescription("Search cursor read size")
    public RedisConnectorConfig setSearchCursorCount(int count)
    {
        this.searchCursorCount = count;
        return this;
    }

    public boolean isCaseInsensitiveNames()
    {
        return caseInsensitiveNames;
    }

    @Config("redis.case-insensitive-names")
    @ConfigDescription("Case-insensitive name-matching for search index")
    public RedisConnectorConfig setCaseInsensitiveNames(boolean enabled)
    {
        this.caseInsensitiveNames = enabled;
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();

        return stream(splitter.split(nodes))
                .map(RedisConnectorConfig::toHostAddress)
                .collect(toImmutableSet());
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(REDIS_DEFAULT_PORT);
    }
}
