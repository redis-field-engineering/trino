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

import com.redis.lettucemod.search.AggregateWithCursorResults;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;

public class RediSearchPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(RediSearchPageSource.class);

    private static final int ROWS_PER_REQUEST = 1024;

    private final RediSearchPageSourceResultWriter writer = new RediSearchPageSourceResultWriter();
    private final String[] columnNames;
    private final List<Type> columnTypes;
    private final CursorIterator iterator;
    private Map<String, Object> currentDoc;
    private long count;
    private boolean finished;

    private final PageBuilder pageBuilder;

    public RediSearchPageSource(RediSearchSession session, RediSearchTableHandle table,
            List<RediSearchColumnHandle> columns)
    {
        this.columnNames = columns.stream().map(RediSearchColumnHandle::getName).toArray(String[]::new);
        this.iterator = new CursorIterator(session, table, columnNames);
        this.columnTypes = columns.stream().map(RediSearchColumnHandle::getType).collect(Collectors.toList());
        this.currentDoc = null;
        this.pageBuilder = new PageBuilder(columnTypes);
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0L;
    }

    @Override
    public Page getNextPage()
    {
        verify(pageBuilder.isEmpty());
        count = 0;
        for (int i = 0; i < ROWS_PER_REQUEST; i++) {
            if (!iterator.hasNext()) {
                finished = true;
                break;
            }
            currentDoc = iterator.next();
            count++;

            pageBuilder.declarePosition();
            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                Object value = currentValue(columnNames[column]);
                if (value == null) {
                    output.appendNull();
                }
                else {
                    writer.appendTo(columnTypes.get(column), value.toString(), output);
                }
            }
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private Object currentValue(String columnName)
    {
        if (RediSearchBuiltinField.isKeyColumn(columnName)) {
            return currentDoc.get(RediSearchBuiltinField.KEY.getName());
        }
        return currentDoc.get(columnName);
    }

    @Override
    public void close()
    {
        try {
            iterator.close();
        }
        catch (Exception e) {
            log.error(e, "Could not close cursor iterator");
        }
    }

    private static class CursorIterator
            implements Iterator<Map<String, Object>>, AutoCloseable
    {
        private final RediSearchSession session;
        private final RediSearchTableHandle table;
        private Iterator<Map<String, Object>> iterator;
        private long cursor;

        public CursorIterator(RediSearchSession session, RediSearchTableHandle table, String[] columnNames)
        {
            this.session = session;
            this.table = table;
            read(session.aggregate(table, columnNames));
        }

        private void read(AggregateWithCursorResults<String> results)
        {
            this.iterator = results.iterator();
            this.cursor = results.getCursor();
        }

        @Override
        public boolean hasNext()
        {
            while (!iterator.hasNext()) {
                if (cursor == 0) {
                    return false;
                }
                read(session.cursorRead(table, cursor));
            }
            return true;
        }

        @Override
        public Map<String, Object> next()
        {
            return iterator.next();
        }

        @Override
        public void close()
                throws Exception
        {
            if (cursor == 0) {
                return;
            }
            session.cursorDelete(table, cursor);
        }
    }
}
