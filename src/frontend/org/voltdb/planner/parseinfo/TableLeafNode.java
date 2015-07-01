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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;

public class TableLeafNode extends JoinNode {
    private StmtTargetTableScan m_tableScan;
    /**
     * Construct a table leaf node
     * @param id - node unique id
     * @param table - join table index
     * @param joinExpr - join expression
     * @param whereExpr - filter expression
     * @param id - node id
     */
    public TableLeafNode(int id, AbstractExpression joinExpr, AbstractExpression whereExpr,
            StmtTargetTableScan tableScan) {
        super(id);
        m_joinExpr = joinExpr;
        m_whereExpr = whereExpr;
        m_tableScan = tableScan;
    }

    /**
     * Deep clone
     */
    @Override
    public Object clone() {
        AbstractExpression joinExpr = (m_joinExpr != null) ?
                (AbstractExpression) m_joinExpr.clone() : null;
        AbstractExpression whereExpr = (m_whereExpr != null) ?
                (AbstractExpression) m_whereExpr.clone() : null;
        JoinNode newNode = new TableLeafNode(m_id, joinExpr, whereExpr, m_tableScan);
        return newNode;
    }

    @Override
    public JoinNode cloneWithoutFilters() {
        JoinNode newNode = new TableLeafNode(m_id, null, null, m_tableScan);
        return newNode;
    }

    @Override
    public StmtTableScan getTableScan() { return m_tableScan; }

    @Override public String getTableAlias() { return m_tableScan.getTableAlias(); }

    @Override
    public void analyzeJoinExpressions(List<AbstractExpression> noneList) {
        m_joinInnerList.addAll(ExpressionUtil.uncombineAny(getJoinExpression()));
        m_whereInnerList.addAll(ExpressionUtil.uncombineAny(getWhereExpression()));
    }

    @Override
    protected void collectEquivalenceFilters(HashMap<AbstractExpression,
            Set<AbstractExpression>> equivalenceSet,
            ArrayDeque<JoinNode> joinNodes)
    {
        if ( ! m_whereInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereInnerList,
                                                      equivalenceSet);
        }
        // HSQL sometimes tags single-table filters in inner joins as join clauses
        // rather than where clauses? OR does analyzeJoinExpressions correct for this?
        // If so, these CAN contain constant equivalences that get used as the basis for equivalence
        // conditions that determine partitioning, so process them as where clauses.
        if ( ! m_joinInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_joinInnerList,
                                                      equivalenceSet);
        }
    }
}
