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
package org.voltdb.planner;

import java.util.HashMap;
import java.util.Map;

import org.voltcore.utils.Pair;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.SchemaColumn;

/**
 * An ephemeral table is one which is not persistent.  These are
 * derived tables, which we call subqueries, and common tables, which
 * are defined by WITH clauses.  This just aggregates some common
 * behavior.
 */
public abstract class StmtEphemeralTableScan extends StmtTableScan {
    // If we have a TVE which is a reference to a column in
    // the this table scan, it will have a differentiator.  It will
    // also have a name, but there may be two columns in the
    // this table scan which have the same name, so that
    // won't uniquely determine the column.  Note that the
    // value from m_outputColumnIndexMap is the index in the
    // result of getScanColumns().  So, getScanColumns() has to
    // return the list of columns we will scan.  The index of
    // TVEs which reference columns in this scan comes from these
    // values.
    protected final Map<Pair<String, Integer>, Integer> m_outputColumnIndexMap = new HashMap<>();

    public StmtEphemeralTableScan(String tableAlias, int stmtId) {
        super(tableAlias, stmtId);
    }

    private StatementPartitioning m_scanPartitioning = null;

    public void setScanPartitioning(StatementPartitioning scanPartitioning) {
        m_scanPartitioning = scanPartitioning;
    }

    public final StatementPartitioning getScanPartitioning() {
        return m_scanPartitioning;
    }

    @Override
    public AbstractExpression processTVE(TupleValueExpression expr, String columnName) {
        assert(expr.getTableAlias().equals(m_tableAlias));
        Integer idx = m_outputColumnIndexMap.get(Pair.of(columnName, expr.getDifferentiator()));
        if (idx == null) {
            throw new PlanningErrorException("Mismatched columns " + columnName + " in common table expression.");
        }
        assert((0 <= idx) && (idx < getScanColumns().size()));
        int idxValue = idx.intValue();
        SchemaColumn schemaCol = getScanColumns().get(idxValue);

        expr.setColumnIndex(idxValue);
        expr.setTypeSizeAndInBytes(schemaCol);
        return expr;
    }

    public abstract boolean canRunInOneFragment();

    public abstract boolean isOrderDeterministic(boolean orderIsDeterministic);

    public abstract String isContentDeterministic(String isContentDeterministic);

    public abstract boolean hasSignificantOffsetOrLimit(boolean hasSignificantOffsetOrLimit);

}
