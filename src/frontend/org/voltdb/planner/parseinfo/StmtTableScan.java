/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltcore.utils.Pair;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a table or a sub-query
 * within the statement scope
 */
public abstract class StmtTableScan {

    public static final int NULL_ALIAS_INDEX = -1;

    // The statement id this table belongs to
    protected int m_stmtId = 0;

    // table alias
    protected String m_tableAlias = null;

    // Store a unique list of scan columns.
    private final List<SchemaColumn> m_scanColumnsList = new ArrayList<>();
    private final Set<Pair<String, Integer>> m_scanColumnNameSet = new HashSet<>();

    // Partitioning column info
    protected List<SchemaColumn> m_partitioningColumns = null;

    protected StmtTableScan(String tableAlias, int stmtId) {
        m_tableAlias = tableAlias;
        m_stmtId = stmtId;
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

    public int getStatementId() {
        return m_stmtId;
    }

    abstract public String getColumnName(int columnIndex);

    abstract public AbstractExpression processTVE(TupleValueExpression expr, String columnName);

    public AbstractExpression resolveTVE(TupleValueExpression tve) {
        AbstractExpression resolvedExpr = processTVE(tve, tve.getColumnName());

        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(resolvedExpr);
        for (TupleValueExpression subqTve : tves) {
            resolveLeafTve(subqTve);
        }
        return resolvedExpr;
    }

    private void resolveLeafTve(TupleValueExpression subqTve) {
        String columnName = subqTve.getColumnName();
        subqTve.setOrigStmtId(m_stmtId);
        Pair<String, Integer> setItem =
                Pair.of(columnName, subqTve.getDifferentiator());
        if (m_scanColumnNameSet.add(setItem)) {
            SchemaColumn scol = new SchemaColumn(getTableName(), m_tableAlias,
                    columnName, columnName, subqTve);
            m_scanColumnsList.add(scol);
        }
    }
}
