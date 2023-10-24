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

import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class RediSearchConnector
        implements Connector
{
    private final RediSearchSession rediSearchSession;
    private final RediSearchSplitManager splitManager;
    private final RediSearchPageSourceProvider pageSourceProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, RediSearchMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public RediSearchConnector(RediSearchSession rediSearchSession, RediSearchSplitManager splitManager,
            RediSearchPageSourceProvider pageSourceProvider)
    {
        this.rediSearchSession = rediSearchSession;
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly,
            boolean autoCommit)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        RediSearchTransactionHandle transaction = new RediSearchTransactionHandle();
        transactions.put(transaction, new RediSearchMetadata(rediSearchSession));
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        RediSearchMetadata metadata = transactions.get(transactionHandle);
        checkTransaction(metadata, transactionHandle);
        return metadata;
    }

    private void checkTransaction(Object object, ConnectorTransactionHandle transactionHandle)
    {
        checkArgument(object != null, "no such transaction: %s", transactionHandle);
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkTransaction(transactions.remove(transaction), transaction);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public void shutdown()
    {
        rediSearchSession.shutdown();
    }
}
