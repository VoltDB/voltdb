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

import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
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

    protected StmtTableScan(String tableAlias, int stmtId) {
        m_tableAlias = tableAlias;
        m_stmtId = stmtId;
    }

    abstract public TABLE_SCAN_TYPE getScanType();

    abstract public String getTableName();

    abstract public boolean getIsreplicated();

    abstract public boolean isPartitioningColumn(String columnName);

    abstract public String getPartitionColumnName();

    abstract public TupleValueExpression resolveTVEForDB(Database db, TupleValueExpression tve);

    public String getTableAlias() {
        return m_tableAlias;
    }

    public int getStatementId() {
        return m_stmtId;
    }

    public Set<SchemaColumn> getScanColumns() {
        return m_scanColumns;
    }

    public Table getTargetTable() {
        return null;
    }

    public TempTable getTempTable() {
        return null;
    }

    public PartitioningForStatement getPartitioning() {
        return null;
    }

    public void setPartitioning(PartitioningForStatement partitioning) {
    }

    // The statement id this table belongs to
    protected int m_stmtId;
    // table alias
    protected String m_tableAlias = null;
    // Store a unique list of the columns actually used by this table instance.
    protected Set<SchemaColumn> m_scanColumns = new  HashSet<SchemaColumn>();
}
