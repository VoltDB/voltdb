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
package org.voltdb.plannodes;

import java.util.List;

import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.WindowFunctionScoreboard;
import org.voltdb.types.SortDirectionType;

/**
 * This is the interface for the class AbstractIndexSortablePlanNode.  See
 * the comment there for more direction.
 */
public class IndexUseForOrderBy {
    // If a window function uses an index, we
    // mark which window function it is here.
    // If this is SubPlanAssembler.STATEMENT_LEVEL_ORDER_BY_INDEX,
    // the statement level order by function uses this index.
    // If this is WindowFunctionScoreboard.NO_INDEX_USE, then nothing
    // uses this index.
    //
    // This will be propagated into a scan plan from the access
    // path and up the outer branch of a join.
    private int m_windowFunctionUsesIndex = WindowFunctionScoreboard.NO_INDEX_USE;
    // If m_windowFunctionUsesIndex is non-negative, so that
    // the index is used to order a window function, but the
    //
    // This will be propagated into a scan plan from the access
    // path and up the outer branch of a join.
    private boolean m_windowFunctionIsCompatibleWithOrderBy = false;
    // If there is an index scan used for ordering,
    // this is the final expression order.  This may
    // be used for a window function or for the statement
    // level order by or both.
    //
    // This will be propagated into a scan plan from the access
    // path and up the outer branch of a join.
    private List<AbstractExpression> m_finalExpressionOrderFromIndexScan;
    // Set the order direction for an index scan.  There
    // is only one of these.
    //
    // This will be propagated into a scan plan from the access
    // path and up the outer branch of a join.
    private SortDirectionType m_sortDirectionFromIndexScan = SortDirectionType.INVALID;

    /**
     * Return the number of the window function which
     * uses an index to provide ordering.  If none uses an
     * index, but there is a statement level order by which
     * uses an index, return SubPlanAssembler.STATEMENT_LEVEL_ORDER_BY_INDEX.
     * If nothing uses an index for ordering, return
     * WindowFunctionScoreboard.NO_INDEX_USE.
     *
     * @return
     */

    /**
     * Set which window function or statement level order by
     * uses the index for ordering.  Only one window function
     * can use an index, and the statement level order by
     * can use the index or not, depending on the expressions
     * in the order by.
     *
     * @param windowFunctionUsesIndex
     */

    /**
     * Is the window function compatible with an index.  If there is
     * a window function and a statement level order by, both need
     * to be compatible with each other for the statement level order
     * by to use the index.  The window function can use the index by
     * itself.
     *
     * @return
     */

    /**
     * Set whether the statement level orderby can use an index.
     *
     * @param value
     */

    /**
     * If there is an index scan used for for this plan we propagate the
     * final expression list here.  This should only happen in join nodes
     * and scan nodes.
     *
     * @return
     */

    /**
     * Set the final expression order from an index scan.  This should only be
     * called for join and scan nodes.
     *
     * @param finalExpressionOrder
     */
    /**
     * Get a sort order from an index scan.  This should only be called from
     * scan and join nodes, as it is not propagated past them.
     *
     * @return
     */
    /**
     * Set a scan order from an index scan.  This should only be
     * called from scan and join nodes.
     *
     * @param sortOrderFromIndexScan
     */
    /**
     * Return the number of the window function which
     * uses an index to provide ordering.  If none uses an
     * index, but there is a statement level order by which
     * uses an index, return SubPlanAssembler.STATEMENT_LEVEL_ORDER_BY_INDEX.
     * If nothing uses an index for ordering, return
     * WindowFunctionScoreboard.NO_INDEX_USE.
     *
     * @return
     */
    public int getWindowFunctionUsesIndex() throws PlanningErrorException {
        return m_windowFunctionUsesIndex;
    }

    /**
     * Set which window function or statement level order by
     * uses the index for ordering.  Only one window function
     * can use an index, and the statement level order by
     * can use the index or not, depending on the expressions
     * in the order by.
     *
     * @param windowFunctionUsesIndex
     */
    public void setWindowFunctionUsesIndex(int windowFunctionUsesIndex) {
        m_windowFunctionUsesIndex = windowFunctionUsesIndex;
    }

    /**
     * Is the window function compatible with an index.  If there is
     * a window function and a statement level order by, both need
     * to be compatible with each other for the statement level order
     * by to use the index.  The window function can use the index by
     * itself.
     *
     * @return
     */
    public boolean isWindowFunctionCompatibleWithOrderBy() {
        return m_windowFunctionIsCompatibleWithOrderBy;
    }

    /**
     * Set whether the statement level orderby can use an index.
     *
     * @param value
     */
    public void setWindowFunctionIsCompatibleWithOrderBy(boolean value) {
        m_windowFunctionIsCompatibleWithOrderBy = value;
    }

    /**
     * If there is an index scan used for for this plan we propagate the
     * final expression list here.  This should only happen in join nodes
     * and scan nodes.
     *
     * @return
     */
    public List<AbstractExpression> getFinalExpressionOrderFromIndexScan() {
        return m_finalExpressionOrderFromIndexScan;
    }

    /**
     * Set the final expression order from an index scan.  This should only be
     * called for join and scan nodes.
     *
     * @param finalExpressionOrder
     */
    public void setFinalExpressionOrderFromIndexScan(List<AbstractExpression> finalExpressionOrder) {
        m_finalExpressionOrderFromIndexScan = finalExpressionOrder;
    }

    /**
     * Get a sort order from an index scan.  This should only be called from
     * scan and join nodes, as it is not propagated past them.
     *
     * @return
     */
    public SortDirectionType getSortOrderFromIndexScan() {
        return m_sortDirectionFromIndexScan;
    }

    /**
     * Set a scan order from an index scan.  This should only be
     * called from scan and join nodes.
     *
     * @param sortOrderFromIndexScan
     */
    public void setSortOrderFromIndexScan(SortDirectionType sortOrderFromIndexScan) {
        m_sortDirectionFromIndexScan = sortOrderFromIndexScan;
    }
}
