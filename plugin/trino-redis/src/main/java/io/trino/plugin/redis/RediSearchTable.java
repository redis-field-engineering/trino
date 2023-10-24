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
import com.redis.lettucemod.search.IndexInfo;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RediSearchTable
{
    private final RediSearchTableHandle tableHandle;
    private final List<RediSearchColumnHandle> columns;
    private final IndexInfo indexInfo;

    public RediSearchTable(RediSearchTableHandle tableHandle, List<RediSearchColumnHandle> columns, IndexInfo indexInfo)
    {
        this.tableHandle = tableHandle;
        this.columns = ImmutableList.copyOf(columns);
        this.indexInfo = indexInfo;
    }

    public IndexInfo getIndexInfo()
    {
        return indexInfo;
    }

    public RediSearchTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<RediSearchColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        return tableHandle.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RediSearchTable)) {
            return false;
        }
        RediSearchTable that = (RediSearchTable) obj;
        return this.tableHandle.equals(that.tableHandle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("tableHandle", tableHandle).toString();
    }
}
