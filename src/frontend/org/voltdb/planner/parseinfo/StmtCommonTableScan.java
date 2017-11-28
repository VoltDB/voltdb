/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltcore.utils.Pair;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.plannodes.SchemaColumn;

public class StmtCommonTableScan extends StmtTableScan {

    private boolean m_isReplicated = false;
    private AbstractParsedStmt m_baseQuery;
    private AbstractParsedStmt m_recursiveQuery;
    private Map<Pair<String, Integer>, Integer> m_outputColumnIndexMap = new HashMap<>();

    public StmtCommonTableScan(String tableAlias, int stmtId) {
        super(tableAlias, stmtId);
    }

    @Override
    public String getTableName() {
        return m_tableAlias;
    }

    @Override
    public boolean getIsReplicated() {
        return m_isReplicated;
    }

    public void setIsReplicated(boolean isReplicated) {
        m_isReplicated = isReplicated;
    }

    @Override
    public List<Index> getIndexes() {
        return noIndexesSupportedOnSubqueryScansOrCommonTables;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getScanColumns().get(columnIndex).getColumnName();
    }

    @Override
    public AbstractExpression processTVE(TupleValueExpression expr, String columnName) {
        Integer idx = m_outputColumnIndexMap.get(new Pair<>(columnName, expr.getDifferentiator()));
        if (idx == null) {
            throw new PlanningErrorException("Mismatched columns " + columnName + " in subquery");
        }
        assert((0 <= idx) && (idx < getScanColumns().size()));
        int idxValue = idx.intValue();
        SchemaColumn schemaCol = getScanColumns().get(idxValue);

        expr.setColumnIndex(idxValue);
        expr.setTypeSizeAndInBytes(schemaCol);
        return expr;
    }

    public final AbstractParsedStmt getBaseQuery() {
        return m_baseQuery;
    }

    public final AbstractParsedStmt getRecursiveQuery() {
        return m_recursiveQuery;
    }

    public final void setBaseQuery(AbstractParsedStmt baseQuery) {
        m_baseQuery = baseQuery;
    }

    public final void setRecursiveQuery(AbstractParsedStmt recursiveQuery) {
        m_recursiveQuery = recursiveQuery;
    }

    public final void addColumn(SchemaColumn col) {
        m_outputColumnIndexMap.put(new Pair<>(col.getColumnName(), col.getDifferentiator()), getScanColumns().size());
        getScanColumns().add(col);
    }

    public final Integer getColumnIndex(String columnName, Integer index) {
        return m_outputColumnIndexMap.get(new Pair<>(columnName, index));
    }
}
