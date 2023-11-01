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

import io.trino.plugin.redis.RedisAggregationQueryBuilder.GroupBy;

import java.util.ArrayList;
import java.util.List;

public class RedisAggregationOptions
{
    public static class Reducer
    {
        private String name;
        private String field;
        private String alias;

        public String getAlias()
        {
            return alias;
        }

        public String getField()
        {
            return field;
        }

        public String getName()
        {
            return name;
        }

        public void setAlias(String alias)
        {
            this.alias = alias;
        }

        public void setField(String field)
        {
            this.field = field;
        }

        public void setName(String name)
        {
            this.name = name;
        }
    }

    private String index;
    private String query;
    private List<String> loads = new ArrayList<>();
    private GroupBy groupBy = new GroupBy();
    private int limit;
    private int cursorCount;

    public String getIndex()
    {
        return index;
    }

    public void setIndex(String index)
    {
        this.index = index;
    }

    public String getQuery()
    {
        return query;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public List<String> getLoads()
    {
        return loads;
    }

    public void setLoads(List<String> loads)
    {
        this.loads = loads;
    }

    public GroupBy getGroupBy()
    {
        return groupBy;
    }

    public void setGroupBy(GroupBy groupBy)
    {
        this.groupBy = groupBy;
    }

    public int getLimit()
    {
        return limit;
    }

    public void setLimit(int limit)
    {
        this.limit = limit;
    }

    public int getCursorCount()
    {
        return cursorCount;
    }

    public void setCursorCount(int cursorCount)
    {
        this.cursorCount = cursorCount;
    }
}
