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

import com.google.common.net.HostAndPort;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.testcontainers.RedisStackContainer;
import io.lettuce.core.RedisURI;

import java.io.Closeable;

public class RediSearchServer
        implements Closeable
{
    private final RedisStackContainer container = new RedisStackContainer(
            RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG)).withEnv("REDISEARCH_ARGS",
                    "MAXAGGREGATERESULTS -1");

    private final RedisModulesClient client;

    private final StatefulRedisModulesConnection<String, String> connection;

    public RediSearchServer()
    {
        this.container.start();
        RedisURI uri = RedisURI.create(container.getRedisURI());
        this.client = RedisModulesClient.create(uri);
        this.connection = RedisModulesUtils.connection(client);
    }

    public HostAndPort getHostAndPort()
    {
        return HostAndPort.fromParts(container.getHost(), container.getFirstMappedPort());
    }

    public RedisModulesClient getClient()
    {
        return client;
    }

    public StatefulRedisModulesConnection<String, String> getConnection()
    {
        return connection;
    }

    @Override
    public void close()
    {
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
        container.close();
    }
}
