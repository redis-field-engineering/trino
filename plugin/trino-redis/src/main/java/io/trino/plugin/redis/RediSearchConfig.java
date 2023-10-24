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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.time.Duration;

public class RediSearchConfig
{
    public static final String DEFAULT_SCHEMA = "default";

    public static final long DEFAULT_LIMIT = 10000;

    public static final long DEFAULT_CURSOR_COUNT = 1000;

    public static final Duration DEFAULT_TABLE_CACHE_EXPIRATION = Duration.ofHours(1);

    public static final Duration DEFAULT_TABLE_CACHE_REFRESH = Duration.ofMinutes(1);

    private String defaultSchema = DEFAULT_SCHEMA;

    private String uri;

    private String username;

    private String password;

    private boolean insecure;

    private boolean cluster;

    private String caCertPath;

    private String keyPath;

    private String certPath;

    private String keyPassword;

    private boolean resp2;

    private boolean caseInsensitiveNames;

    private long defaultLimit = DEFAULT_LIMIT;

    private long cursorCount = DEFAULT_CURSOR_COUNT;

    private long tableCacheExpiration = DEFAULT_TABLE_CACHE_EXPIRATION.toSeconds();

    private long tableCacheRefresh = DEFAULT_TABLE_CACHE_REFRESH.toSeconds();

    @Min(0)
    public long getCursorCount()
    {
        return cursorCount;
    }

    @Config("redisearch.cursor-count")
    public RediSearchConfig setCursorCount(long cursorCount)
    {
        this.cursorCount = cursorCount;
        return this;
    }

    public long getDefaultLimit()
    {
        return defaultLimit;
    }

    @Config("redisearch.default-limit") @ConfigDescription("Default search limit number to use")
    public RediSearchConfig setDefaultLimit(long defaultLimit)
    {
        this.defaultLimit = defaultLimit;
        return this;
    }

    public boolean isCaseInsensitiveNames()
    {
        return caseInsensitiveNames;
    }

    @Config("redisearch.case-insensitive-names") @ConfigDescription("Case-insensitive name-matching")
    public RediSearchConfig setCaseInsensitiveNames(boolean caseInsensitiveNames)
    {
        this.caseInsensitiveNames = caseInsensitiveNames;
        return this;
    }

    public boolean isResp2()
    {
        return resp2;
    }

    @Config("redisearch.resp2") @ConfigDescription("Force Redis protocol version to RESP2")
    public RediSearchConfig setResp2(boolean resp2)
    {
        this.resp2 = resp2;
        return this;
    }

    @Config("redisearch.table-cache-expiration") @ConfigDescription("Duration in seconds since the entry creation after which a table should be automatically removed from the cache.")
    public RediSearchConfig setTableCacheExpiration(long expirationDuration)
    {
        this.tableCacheExpiration = expirationDuration;
        return this;
    }

    public long getTableCacheExpiration()
    {
        return tableCacheExpiration;
    }

    @Config("redisearch.table-cache-refresh") @ConfigDescription("Duration in seconds since the entry creation after which to automatically refresh the table cache.")
    public RediSearchConfig setTableCacheRefresh(long refreshDuration)
    {
        this.tableCacheRefresh = refreshDuration;
        return this;
    }

    public long getTableCacheRefresh()
    {
        return tableCacheRefresh;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("redisearch.default-schema-name") @ConfigDescription("Default schema name to use")
    public RediSearchConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    public @Pattern(message = "Invalid Redis URI. Expected redis:// rediss://", regexp = "^rediss?://.*") String getUri()
    {
        return uri;
    }

    @Config("redisearch.uri") @ConfigDescription("Redis connection URI e.g. 'redis://localhost:6379'") @ConfigSecuritySensitive
    public RediSearchConfig setUri(String uri)
    {
        this.uri = uri;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("redisearch.username") @ConfigDescription("Redis connection username") @ConfigSecuritySensitive
    public RediSearchConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("redisearch.password") @ConfigDescription("Redis connection password") @ConfigSecuritySensitive
    public RediSearchConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public boolean isCluster()
    {
        return cluster;
    }

    @Config("redisearch.cluster") @ConfigDescription("Connect to a Redis Cluster")
    public RediSearchConfig setCluster(boolean cluster)
    {
        this.cluster = cluster;
        return this;
    }

    public boolean isInsecure()
    {
        return insecure;
    }

    @Config("redisearch.insecure") @ConfigDescription("Allow insecure connections (e.g. invalid certificates) to Redis when using SSL")
    public RediSearchConfig setInsecure(boolean insecure)
    {
        this.insecure = insecure;
        return this;
    }

    public String getCaCertPath()
    {
        return caCertPath;
    }

    @Config("redisearch.cacert-path") @ConfigDescription("X.509 CA certificate file to verify with")
    public RediSearchConfig setCaCertPath(String caCertPath)
    {
        this.caCertPath = caCertPath;
        return this;
    }

    public String getKeyPath()
    {
        return keyPath;
    }

    @Config("redisearch.key-path") @ConfigDescription("PKCS#8 private key file to authenticate with (PEM format)")
    public RediSearchConfig setKeyPath(String keyPath)
    {
        this.keyPath = keyPath;
        return this;
    }

    public String getKeyPassword()
    {
        return keyPassword;
    }

    @Config("redisearch.key-password") @ConfigSecuritySensitive @ConfigDescription("Password of the private key file, or null if it's not password-protected")
    public RediSearchConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public String getCertPath()
    {
        return certPath;
    }

    @Config("redisearch.cert-path") @ConfigDescription("X.509 certificate chain file to authenticate with (PEM format)")
    public RediSearchConfig setCertPath(String certPath)
    {
        this.certPath = certPath;
        return this;
    }
}
