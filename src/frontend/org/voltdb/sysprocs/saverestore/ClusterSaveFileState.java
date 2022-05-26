/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

public class ClusterSaveFileState
{
    // hack usage of ERROR_CODE to include error messages without changing ClusterSaveFileState API
    // long term future, we can use NT-Procedure to check the path exists
    // on every node without using MP fragments
    public final static int ERROR_CODE = -127;

    public static VoltTable constructEmptySaveFileStateVoltTable()
    {
        ColumnInfo[] result_columns = new ColumnInfo[11];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo("CURRENT_HOST_ID", VoltType.INTEGER);
        result_columns[ii++] = new ColumnInfo("CURRENT_HOSTNAME", VoltType.STRING);
        result_columns[ii++] =
                new ColumnInfo("ORIGINAL_HOST_ID", VoltType.INTEGER);
        result_columns[ii++] =
                new ColumnInfo("ORIGINAL_HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("CLUSTER", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("DATABASE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TXNID", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("IS_REPLICATED", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PARTITION", VoltType.INTEGER);
        result_columns[ii++] = new ColumnInfo("TOTAL_PARTITIONS",VoltType.INTEGER);

        return new VoltTable(result_columns);
    }

    private TableSaveFileState constructTableState(
            VoltTableRow row)
    {
        TableSaveFileState table_state = null;
        String table_name = row.getString("TABLE");
        long txnId = row.getLong("TXNID");
        if (row.getString("IS_REPLICATED").equals("TRUE"))
        {
            table_state = new ReplicatedTableSaveFileState(table_name, txnId);
        }
        else if (row.getString("IS_REPLICATED").equals("FALSE"))
        {
            table_state = new PartitionedTableSaveFileState(table_name, txnId);
        }
        else
        {
            // XXX not reached
            assert(false);
        }
        return table_state;
    }

    public ClusterSaveFileState(VoltTable saveFileState)
        throws IOException
    {
        // Checks cluster/database name consistency between rows.
        ConsistencyChecker checker = new ConsistencyChecker();

        m_tableStateMap = new HashMap<String, TableSaveFileState>();
        long txnId = -1;
        while (saveFileState.advanceRow())
        {
            long originalHostId = saveFileState.getLong("ORIGINAL_HOST_ID");
            // it means we couldn't find snapshot file on this node, ignore it now,
            // @SnapshotRestore will check completion of the snapshot later
            if (originalHostId == ClusterSaveFileState.ERROR_CODE) {
                continue;
            }
            checker.checkRow(saveFileState); // throws if inconsistent
            String table_name = saveFileState.getString("TABLE");

            // Check if the transaction IDs match
            if (txnId == -1)
            {
                txnId = saveFileState.getLong("TXNID");
            }
            else if (txnId != saveFileState.getLong("TXNID"))
            {
                String error = "Table: " + table_name + " has inconsistent" +
                        " transaction ID ";
                throw new IOException(error);
            }

            TableSaveFileState table_state = null;
            if (!(getSavedTableNames().contains(table_name)))
            {
                table_state = constructTableState(saveFileState);
                m_tableStateMap.put(table_name, table_state);
            }
            table_state = getTableState(table_name);
            table_state.addHostData(saveFileState); // throws if inconsistent
        }
        for (TableSaveFileState table_state : m_tableStateMap.values()) {
            if (!table_state.isConsistent()) {
                String error = table_state.getConsistencyResult();
                throw new IOException(error);
            }
        }
    }

    public Set<String> getSavedTableNames()
    {
        return m_tableStateMap.keySet();
    }

    public TableSaveFileState getTableState(String tableName)
    {
        return m_tableStateMap.get(tableName);
    }

    private static class ConsistencyChecker
    {
        private int m_numRows = 0;
        private String m_clusterName = null;
        private String m_databaseName = null;

        private void checkRow(VoltTableRow row) throws IOException
        {
            // Get the comparison cluster/database names from the first row received.
            if (m_numRows++ == 0) {
                m_clusterName = row.getString("CLUSTER");
                m_databaseName = row.getString("DATABASE");
            }
            else if (!row.getString("CLUSTER").equals(m_clusterName))
            {
                String error = "Site: " + row.getLong("CURRENT_HOST_ID") +
                ", Table: " + row.getString("TABLE") + " has an inconsistent " +
                "cluster name: " + row.getString("CLUSTER") + " (previous was: " +
                m_clusterName + ").";
                throw new IOException(error);
            }
            else if (!row.getString("DATABASE").equals(m_databaseName))
            {
                String error = "Site: " + row.getLong("CURRENT_HOST_ID") +
                ", Table: " + row.getString("TABLE") + " has an inconsistent " +
                "database name: " + row.getString("DATABASE") + " (previous was: " +
                m_databaseName + ").";
                throw new IOException(error);
            }
        }
    }

    private Map<String, TableSaveFileState> m_tableStateMap;
}
