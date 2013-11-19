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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.ParsedSelectStmt.ParsedColInfo;

/**
* Temporary Table derived from a sub-query.
*/
public class TempTable {

    /**
     * Constructor
     * @param tableName The name of the temp table
     * @param tableAlias The alias assigned to this table.
     *                   SQL Grammar mandate it to be not null
     * @param subQuery The subquery behind the table
     */
    public TempTable(String tableName, String tableAlias, AbstractParsedStmt subQuery) {
        assert(tableName != null);
        assert(tableAlias != null);
        assert(subQuery != null);

        m_tableName = tableName;
        m_tableAlias = tableAlias;
        m_subQuery = subQuery;
        if (m_subQuery instanceof ParsedSelectStmt) {
            m_derivedSchema = generateOutputSchema(tableName, tableAlias, ((ParsedSelectStmt) m_subQuery).displayColumns());
        } else if (m_subQuery instanceof ParsedUnionStmt) {
            ParsedUnionStmt unionStmt = (ParsedUnionStmt) m_subQuery;
            assert(!unionStmt.m_children.isEmpty());
            AbstractParsedStmt selectStmt = unionStmt.m_children.get(0);
            // Can we have UNION of UNIONS?
            assert (selectStmt instanceof ParsedSelectStmt);
            m_derivedSchema = generateOutputSchema(tableName, tableAlias, ((ParsedSelectStmt) selectStmt).displayColumns());
        } else {
            m_derivedSchema = null;
        }
        assert(m_derivedSchema != null);
    }

    private List<ParsedColInfo> generateOutputSchema(String tableName, String tableAlias, List<ParsedColInfo> subQuerySchema) {
        List<ParsedColInfo> derivedSchema = new ArrayList<ParsedColInfo>();
        for (ParsedColInfo col : subQuerySchema) {
            ParsedColInfo newCol = col.clone();
            newCol.tableName = tableName;
            newCol.tableAlias = tableAlias;
            // The column name is the column alias of the corresponding column from the subquery
            if (col.alias != null) {
                newCol.columnName = col.alias;
            }
            derivedSchema.add(newCol);
        }
        return derivedSchema;
    }

    public String getTableName() {
        return m_tableName;
    }

    public String getTableAlias() {
        return m_tableAlias;
    }

    public AbstractParsedStmt getSubQuery() {
        return m_subQuery;
    }

    public List<ParsedColInfo> getDerivedSchema() {
        return m_derivedSchema;
    }

    public CompiledPlan getBetsCostPlan() {
        return m_bestCostPlan;
    }

    public void setBetsCostPlan(CompiledPlan costPlan) {
        m_bestCostPlan = costPlan;
    }

    /**
     * The subquery is replicated if all tables from the FROM clause defining this subquery
     * are replicated
     * @return True if the subquery is replicated
     */
    public boolean getIsreplicated() {
        boolean isReplicated = true;
        for (StmtTableScan tableCache : m_subQuery.stmtCache) {
            isReplicated = isReplicated && tableCache.getIsreplicated();
            if (isReplicated == false) {
                break;
            }
        }
        return isReplicated;
    }

    private final String m_tableName;
    private final String m_tableAlias;
    private final AbstractParsedStmt m_subQuery;
    private final List<ParsedColInfo> m_derivedSchema;
    private CompiledPlan m_bestCostPlan = null;
}
