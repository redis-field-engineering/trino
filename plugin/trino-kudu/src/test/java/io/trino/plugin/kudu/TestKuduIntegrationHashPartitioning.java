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
package io.trino.plugin.kudu;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static org.testng.Assert.assertEquals;

public class TestKuduIntegrationHashPartitioning
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createKuduQueryRunner(closeAfterClass(new TestingKuduServer()), "hash");
    }

    @Test
    public void testCreateTableSingleHashPartitionLevel()
    {
        String createTable = "" +
                "CREATE TABLE hashtest1 (\n" +
                "  id INT WITH (primary_key=true,encoding='auto', compression='default'),\n" +
                "  event_time TIMESTAMP WITH (primary_key=true, encoding='plain', compression='lz4'),\n" +
                "  value DOUBLE WITH (primary_key=false, nullable=false, compression='no')\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id', 'event_time'],\n" +
                " partition_by_hash_buckets = 3\n" +
                ")";

        doTestCreateTable("hashtest1", createTable);
    }

    @Test
    public void testCreateTableDoubleHashPartitionLevel()
    {
        String createTable = "" +
                "CREATE TABLE hashtest2 (\n" +
                "  id INT WITH (primary_key=true, encoding='bitshuffle', compression='zlib'),\n" +
                "  event_time TIMESTAMP WITH (primary_key=true, encoding='runlength', compression='snappy'),\n" +
                "  value DOUBLE WITH (nullable=true)\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 3\n," +
                " partition_by_second_hash_columns = ARRAY['event_time'],\n" +
                " partition_by_second_hash_buckets = 3\n" +
                ")";

        doTestCreateTable("hashtest2", createTable);
    }

    private void doTestCreateTable(String tableName, @Language("SQL") String createTable)
    {
        String dropTable = "DROP TABLE IF EXISTS " + tableName;

        assertUpdate(dropTable);
        assertUpdate(createTable);

        String insert = "INSERT INTO " + tableName + " VALUES (1, TIMESTAMP '2001-08-22 03:04:05.321', 2.5)";

        assertUpdate(insert, 1);

        MaterializedResult result = computeActual("SELECT id FROM " + tableName);
        assertEquals(result.getRowCount(), 1);
    }
}
