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
package org.voltdb.plannodes;

import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.SortDirectionType;

/**
 * This is the interface for the class AbstractIndexSortablePlanNode.  See
 * the comment there for more direction.
 */
public interface IndexUseForOrderBy {
    /**
     * Return the number of the window function which
     * uses an index to provide ordering.  If none uses an
     * index, but there is a statement level order by which
     * uses an index, return SubPlanAssembler.STATEMENT_LEVEL_ORDER_BY_INDEX.
     * If nothing uses an index for ordering, return
     * SubPlanAssember.NO_INDEX_USE.
     *
     * @return
     */
    default public int getWindowFunctionUsesIndex() throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.getWindowFunctionUsesIndex called on "
                                         + getClass().getName() + " object.");
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
    default public void setWindowFunctionUsesIndex(int windowFunctionUsesIndex)  throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.setWindowFunctionUsesIndex called on "
                                         + getClass().getName() + " object.");
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
    default public boolean isWindowFunctionCompatibleWithOrderBy()  throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.isWindowFunctionCompatibleWithOrderBy called on "
                                         + getClass().getName() + " object.");
    }

    /**
     * Set whether the statement level orderby can use an index.
     *
     * @param value
     */
    default public void setWindowFunctionIsCompatibleWithOrderBy(boolean value) throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.setWindowFunctionIsCompatibleWithOrderBy called on "
                                         + getClass().getName() + " object.");
    }

    /**
     * If there is an index scan used for for this plan we propagate the
     * final expression list here.  This should only happen in join nodes
     * and scan nodes.
     *
     * @return
     */
    default public List<AbstractExpression> getFinalExpressionOrderFromIndexScan() throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.getFinalExpressionOrderFromIndexScan called on "
                                         + getClass().getName() + " object.");
    }

    /**
     * Set the final expression order from an index scan.  This should only be
     * called for join and scan nodes.
     *
     * @param finalExpressionOrder
     */
    default public void setFinalExpressionOrderFromIndexScan(List<AbstractExpression> finalExpressionOrder)  throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.setFinalExpressionOrderFromIndexScan called on "
                                         + getClass().getName() + " object.");
    }

    /**
     * Get a sort order from an index scan.  This should only be called from
     * scan and join nodes, as it is not propagated past them.
     *
     * @return
     */
    default public SortDirectionType getSortOrderFromIndexScan()  throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.getSortOrderFromIndexScan called on "
                                         + getClass().getName() + " object.");
    }

    /**
     * Set a scan order from an index scan.  This should only be
     * called from scan and join nodes.
     *
     * @param sortOrderFromIndexScan
     */
    default public void setSortOrderFromIndexScan(SortDirectionType sortOrderFromIndexScan) throws PlanningErrorException {
        throw new PlanningErrorException("Internal Error: IndexUseForOrderBy.setFinalExpressionOrderFromIndexScan called on "
                                         + getClass().getName() + " object.");
    }
}
