/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

public class ClusterSaveFileState
{
    public static VoltTable constructEmptySaveFileStateVoltTable()
    {
        ColumnInfo[] result_columns = new ColumnInfo[8];
        result_columns[0] = new ColumnInfo("CURRENT_HOST_ID", VoltType.INTEGER);
        result_columns[1] =
                new ColumnInfo("ORIGINAL_HOST_ID", VoltType.INTEGER);
        result_columns[2] = new ColumnInfo("CLUSTER", VoltType.STRING);
        result_columns[3] = new ColumnInfo("DATABASE", VoltType.STRING);
        result_columns[4] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[5] = new ColumnInfo("IS_REPLICATED", VoltType.STRING);
        result_columns[6] = new ColumnInfo("PARTITION", VoltType.INTEGER);
        result_columns[7] = new ColumnInfo("TOTAL_PARTITIONS",
                                           VoltType.INTEGER);

        return new VoltTable(result_columns);
    }

    private TableSaveFileState constructTableState(
            VoltTableRow row)
    {
        TableSaveFileState table_state = null;
        String table_name = row.getString("TABLE");
        if (row.getString("IS_REPLICATED").equals("TRUE"))
        {
            table_state = new ReplicatedTableSaveFileState(table_name, m_allowELT);
        }
        else if (row.getString("IS_REPLICATED").equals("FALSE"))
        {
            table_state = new PartitionedTableSaveFileState(table_name, m_allowELT);
        }
        else
        {
            // XXX not reached
            assert(false);
        }
        return table_state;
    }

    public ClusterSaveFileState(VoltTable saveFileState, int allowELT)
        throws IOException
    {
        if (saveFileState.getRowCount() == 0)
        {
            String error = "No savefile state to restore";
            throw new IOException(error);
        }
        VoltTableRow a_row = saveFileState.fetchRow(0);
        m_clusterName = a_row.getString("CLUSTER");
        m_databaseName = a_row.getString("DATABASE");
        m_allowELT = allowELT;


        m_tableStateMap = new HashMap<String, TableSaveFileState>();
        while (saveFileState.advanceRow())
        {
            checkConsistency(saveFileState); // throws if inconsistent
            String table_name = saveFileState.getString("TABLE");
            TableSaveFileState table_state = null;
            if (!(getSavedTableNames().contains(table_name)))
            {
                table_state = constructTableState(saveFileState);
                m_tableStateMap.put(table_name, table_state);
            }
            table_state = getTableState(table_name);
            table_state.addHostData(saveFileState); // throws if inconsistent
        }
        for (TableSaveFileState table_state : m_tableStateMap.values())
        {
            if (!table_state.isConsistent())
            {
                String error = "Table: " + table_state.getTableName() +
                " has some inconsistency in the savefile state";
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

    private void checkConsistency(VoltTableRow row) throws IOException
    {
        if (!row.getString("CLUSTER").equals(m_clusterName))
        {
            String error = "Site: " + row.getLong("CURRENT_HOST_ID") +
            ", Table: " + row.getString("TABLE") + " has an inconsistent " +
            "cluster name: " + row.getString("CLUSTER") + " (previous was: " +
            m_clusterName + ").";
            throw new IOException(error);
        }
        if (!row.getString("DATABASE").equals(m_databaseName))
        {
            String error = "Site: " + row.getLong("CURRENT_HOST_ID") +
            ", Table: " + row.getString("TABLE") + " has an inconsistent " +
            "database name: " + row.getString("DATABASE") + " (previous was: " +
            m_databaseName + ").";
            throw new IOException(error);
        }
    }

    private String m_clusterName;
    private String m_databaseName;
    final private int m_allowELT;
    private Map<String, TableSaveFileState> m_tableStateMap;
}
