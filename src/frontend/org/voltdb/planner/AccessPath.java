/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

/**
 * We may may have several ways to access data in tables.  We
 * may use a simple table scan or an index scan.
 * Index scans may have sort orders.
 * There are also other data we may want to attach to a particular plan.
 *
 * This is a convenient place to organize everything associated with accessing tables or indexes.
 * (TODO: not really; needs better controlled way to access/udpate).
 */
public class AccessPath {
    Index index = null;
    IndexUseType use = IndexUseType.COVERING_UNIQUE_EQUALITY;
    boolean nestLoopIndexJoin = false;
    boolean keyIterate = false;
    IndexLookupType lookupType = IndexLookupType.EQ;
    SortDirectionType sortDirection = SortDirectionType.INVALID;
    // The initial expression is needed to adjust (forward) the start of the reverse
    // iteration when it had to initially settle for starting at
    // "greater than a prefix key".
    // For the purpose of these expressions, see SubPlanAssembler#getRelevantAccessPathForIndex().
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
    // an order by node.  If it is set to WindowFunctionScoreboard.NO_INDEX_USE,
    // then nothing uses the index.
    //
    int m_windowFunctionUsesIndex = WindowFunctionScoreboard.NO_INDEX_USE;
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

    // Partial Index predicates if any
    final List<AbstractExpression> m_partialIndexPredicate = new ArrayList<>();

    /**
     * For Calcite
     * @param index
     * @param lookupType
     * @param sortDirection
     * @param stmtOrderByIsCompatible
     */
    public AccessPath(
            Index index, IndexLookupType lookupType, SortDirectionType sortDirection, boolean stmtOrderByIsCompatible) {
        this.index = index;
        this.lookupType = lookupType;
        this.sortDirection = sortDirection;
        this.m_stmtOrderByIsCompatible = stmtOrderByIsCompatible;
    }

    public AccessPath() {}

    @Override
    public String toString() {
        final StringBuilder retval = new StringBuilder()
                .append("INDEX: ")
                .append((index == null) ? "NULL" : (index.getParent().getTypeName() + "." + index.getTypeName()))
                .append("\n")
                .append("USE:   ").append(use.toString()).append("\n")
                .append("FOR:   ").append(indexPurposeString()).append("\n")
                .append("TYPE:  ").append(lookupType.toString()).append("\n")
                .append("DIR:   ").append(sortDirection.toString()).append("\n")
                .append("ITER?: ").append(false).append("\n")
                .append("NLIJ?: ").append(false).append("\n")
                .append("IDX EXPRS:\n");
        int i = 0;
        for (AbstractExpression expr : indexExprs)
            retval.append("\t(").append(i++).append(") ").append(expr.toString()).append("\n");

        retval.append("END EXPRS:\n");
        i = 0;
        for (AbstractExpression expr : endExprs)
            retval.append("\t(").append(i++).append(") ").append(expr.toString()).append("\n");

        retval.append("OTHER EXPRS:\n");
        i = 0;
        for (AbstractExpression expr : otherExprs)
            retval.append("\t(").append(i++).append(") ").append(expr.toString()).append("\n");

        retval.append("JOIN EXPRS:\n");
        i = 0;
        for (AbstractExpression expr : joinExprs)
            retval.append("\t(").append(i++).append(") ").append(expr.toString()).append("\n");

        retval.append("ELIMINATED POST FILTER EXPRS:\n");
        i = 0;
        for (AbstractExpression expr : eliminatedPostExprs)
            retval.append("\t(").append(i++).append(") ").append(expr.toString()).append("\n");
        return retval.toString();
    }

    private String indexPurposeString() {
        switch (m_windowFunctionUsesIndex) {
        case WindowFunctionScoreboard.STATEMENT_LEVEL_ORDER_BY_INDEX:
            return "Statement Level Order By";
        case WindowFunctionScoreboard.NO_INDEX_USE:
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

    public void setSortDirection(SortDirectionType sortDirection) {
        this.sortDirection = sortDirection;
    }

    public SortDirectionType getSortDirection() {
        return sortDirection;
    }

    public IndexLookupType getIndexLookupType() {
        return lookupType;
    }

    public List<AbstractExpression> getIndexExpressions() {
        return indexExprs;
    }

    public List<AbstractExpression> getEndExpressions() {
        return endExprs;
    }

    public List<AbstractExpression> getEliminatedPostExpressions() {
        return eliminatedPostExprs;
    }

    public List<AbstractExpression> getOtherExprs() {
        return otherExprs;
    }

    public void setPartialIndexExpression(AbstractExpression partialPredicate) {
        m_partialIndexPredicate.addAll(ExpressionUtil.uncombineAny(partialPredicate));
    }

    public List<AbstractExpression> getPartialIndexExpression() {
        return m_partialIndexPredicate;
    }

    public Index getIndex() {
        return index;
    }
}

