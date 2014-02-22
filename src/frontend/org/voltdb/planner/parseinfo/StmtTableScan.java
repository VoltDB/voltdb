/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PartitioningForStatement;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a table or a sub-query
 * within the statement scope
 */
public abstract class StmtTableScan {

    public static final int NULL_ALIAS_INDEX = -1;

    public enum TABLE_SCAN_TYPE {
        TARGET_TABLE_SCAN,
        TEMP_TABLE_SCAN
    }

    // table alias
    protected String m_tableAlias = null;
    protected List<SchemaColumn> m_scanColumnsList = new ArrayList<>();

    protected StmtTableScan(String tableAlias) {
        m_tableAlias = tableAlias;
    }

    public String getTableAlias() {
        return m_tableAlias;
    }

    public List<SchemaColumn> getScanColumns() {
        return m_scanColumnsList;
    }

    abstract public TABLE_SCAN_TYPE getScanType();

    abstract public String getTableName();

    abstract public boolean getIsReplicated();

    abstract public String getPartitionColumnName();

    abstract public List<Index> getIndexes();

    public void setPartitioning(PartitioningForStatement partitioning) {}

    abstract public String getColumnName(int m_columnIndex);

    abstract public void resolveTVE(TupleValueExpression expr, String columnName);

}
