/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.planner.parseinfo;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.TupleValueExpression;

/**
 * StmtTableScan caches data related to a given instance of a table within the statement scope
 */
public class StmtTargetTableScan extends StmtTableScan {

    public StmtTargetTableScan(Table table, String tableAlias) {
        super(tableAlias);
        assert (table != null);
        m_table = table;
    }

    @Override
    public TABLE_SCAN_TYPE getScanType() {
        return TABLE_SCAN_TYPE.TARGET_TABLE_SCAN;
    }

    @Override
    public String getTableName() {
        return m_table.getTypeName();
    }

    @Override
    public Table getTargetTable() {
        assert(m_table != null);
        return m_table;
    }

    @Override
    public boolean getIsreplicated() {
        return m_table.getIsreplicated();
    }

    @Override
    public boolean isPartitioningColumn(String columnName) {
        Column partitionColumn = m_table.getPartitioncolumn();
        if (partitionColumn != null) {
            return partitionColumn.getTypeName().equals(columnName);
        } else {
            return false;
        }
    }

    @Override
    public String getPartitionColumnName() {
        String colName = null;
        if (getIsreplicated() == false) {
            Column partitionCol = m_table.getPartitioncolumn();
            // "(partitionCol != null)" tests around an obscure edge case.
            // The table is declared non-replicated yet specifies no partitioning column.
            // This can occur legitimately when views based on partitioned tables neglect to group by the partition column.
            // The interpretation of this edge case is that the table has "randomly distributed data".
            // In such a case, the table is valid for use by MP queries only and can only be joined with replicated tables
            // because it has no recognized partitioning join key.
            if (partitionCol != null) {
                colName = partitionCol.getTypeName(); // Note getTypeName gets the column name -- go figure.
            }
        }
        return colName;
    }

    @Override
    public TupleValueExpression resolveTVEForDB(Database db, TupleValueExpression tve) {
        tve.resolveForDB(db);
        return tve;
    }

    // Catalog table
    private Table m_table = null;
}
