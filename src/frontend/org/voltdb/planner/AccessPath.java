/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

/**
 * We may may have several ways to access data in tables.  We
 * may use a simple table scan or an index scan.  Index scans may
 * have sort orders.  There are also other data we may want to
 * attach to a particular plan.  This is a convenient place to
 * organize everything associated with accessing tables or indexes.
 */
public class AccessPath {
    Index index = null;
    IndexUseType use = IndexUseType.COVERING_UNIQUE_EQUALITY;
    boolean nestLoopIndexJoin = false;
    boolean requiresSendReceive = false;
    boolean keyIterate = false;
    IndexLookupType lookupType = IndexLookupType.EQ;
    SortDirectionType sortDirection = SortDirectionType.INVALID;
    // The initial expression is needed to adjust (forward) the start of the reverse
    // iteration when it had to initially settle for starting at
    // "greater than a prefix key".
    final List<AbstractExpression> initialExpr = new ArrayList<>();
    final List<AbstractExpression> indexExprs = new ArrayList<>();
    final List<AbstractExpression> endExprs = new ArrayList<>();
    final List<AbstractExpression> otherExprs = new ArrayList<>();
    final List<AbstractExpression> joinExprs = new ArrayList<>();
    final List<AbstractExpression> bindings = new ArrayList<>();
    final List<AbstractExpression> eliminatedPostExprs = new ArrayList<>();
    //
    // If a window function uses the index, then this will be set
    // to the number of the window function which uses this index.
    // If it is set to SubPlanAssembler.STATEMENT_LEVEL_ORDER_BY_INDEX
    // then no window function uses the index, but
    // the window function ordering is compatible with the statement
    // level ordering, and the statement level order does not need
    // an order by node.  If it is set to SubPlanAssembler.NO_INDEX_USE,
    // then nothing uses the index.
    //
    int m_windowFunctionUsesIndex = SubPlanAssembler.NO_INDEX_USE;
    //
    // This is true iff there is a window function which
    // uses an index for order, but the statement level
    // order by can use the same index.  In this case we
    // may not use any sorts at all.
    //
    boolean m_stmtOrderByIsCompatible = false;
    //
    // This is the final expression ordering.  We need this
    // to remember the order an index imposes on a WindowFunction,
    // since the partition by expressions are not ordered among
    // themselves.  Note that this will never be null, but may be empty
    // if there is no index in this access path.
    //
    final List<AbstractExpression> m_finalExpressionOrder = new ArrayList<>();

    @Override
    public String toString() {
        String retval = "";

        retval += "INDEX: " + ((index == null) ? "NULL" : (index.getParent().getTypeName() + "." + index.getTypeName())) + "\n";
        retval += "USE:   " + use.toString() + "\n";
        retval += "FOR:   " + indexPurposeString() + "\n";
        retval += "TYPE:  " + lookupType.toString() + "\n";
        retval += "DIR:   " + sortDirection.toString() + "\n";
        retval += "ITER?: " + String.valueOf(keyIterate) + "\n";
        retval += "NLIJ?: " + String.valueOf(nestLoopIndexJoin) + "\n";

        retval += "IDX EXPRS:\n";
        int i = 0;
        for (AbstractExpression expr : indexExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "END EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : endExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "OTHER EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : otherExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "JOIN EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : joinExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "ELIMINATED POST FILTER EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : eliminatedPostExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        return retval;
    }
    private String indexPurposeString() {
        switch (m_windowFunctionUsesIndex) {
        case SubPlanAssembler.STATEMENT_LEVEL_ORDER_BY_INDEX:
            return "Statement Level Order By";
        case SubPlanAssembler.NO_INDEX_USE:
            return "No Indexing Used";
        default:
            if (0 <= m_windowFunctionUsesIndex) {
                return "Window function plan node " + m_windowFunctionUsesIndex;
            }
        }
        /*
         * This should never happen.
         */
        assert(false);
        return "";
    }
}
