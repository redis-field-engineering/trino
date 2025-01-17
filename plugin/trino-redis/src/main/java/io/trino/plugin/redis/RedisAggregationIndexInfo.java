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

import redis.clients.jedis.search.schemafields.SchemaField;

import java.util.List;

public class RedisAggregationIndexInfo
{
    private String indexName;

    private List<SchemaField> fields;

    public String getIndexName()
    {
        return indexName;
    }

    public void setIndexName(String indexName)
    {
        this.indexName = indexName;
    }

    public List<SchemaField> getFields()
    {
        return fields;
    }

    public void setFields(List<SchemaField> fields)
    {
        this.fields = fields;
    }
}
