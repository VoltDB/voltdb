/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a table or a sub-query
 * within the statement scope
 */
public abstract class StmtTableScan {

    public static final int NULL_ALIAS_INDEX = -1;

    // table alias
    protected String m_tableAlias = null;

    // Store a unique list of scan columns.
    protected List<SchemaColumn> m_scanColumnsList = new ArrayList<>();
    protected Set<String> m_scanColumnNameSet = new HashSet<>();

    // Partitioning column info
    protected List<SchemaColumn> m_partitioningColumns = null;

    protected StmtTableScan(String tableAlias) {
        m_tableAlias = tableAlias;
    }

    public String getTableAlias() {
        return m_tableAlias;
    }

    public List<SchemaColumn> getScanColumns() {
        return m_scanColumnsList;
    }

    public List<SchemaColumn> getPartitioningColumns() {
        return m_partitioningColumns;
    }

    abstract public String getTableName();

    abstract public boolean getIsReplicated();

    abstract public List<Index> getIndexes();

    abstract public String getColumnName(int m_columnIndex);


    abstract public void processTVE(TupleValueExpression expr, String columnName);


    public void resolveTVE(TupleValueExpression expr, String columnName) {

        processTVE(expr, columnName);

        if (!m_scanColumnNameSet.contains(columnName)) {
            SchemaColumn scol = new SchemaColumn(getTableName(), m_tableAlias,
                    columnName, columnName, (TupleValueExpression) expr.clone());
            m_scanColumnNameSet.add(columnName);
            m_scanColumnsList.add(scol);
        }
    }
}
