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

package org.voltdb.planner.parseinfo;

import java.util.Deque;
import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.StmtEphemeralTableScan;

/**
 * An object of class SubqueryLeafNode is a leaf in a join expression tree
 * which corresponds to a subquery.
 */
public class SubqueryLeafNode extends JoinNode {

    private final StmtSubqueryScan m_subqueryScan;

    /**
     * Construct a subquery node
     * @param id - node unique id
     * @param table - join table index
     * @param joinExpr - join expression
     * @param whereExpr - filter expression
     * @param id - node id
     */
    public SubqueryLeafNode(int id,
            AbstractExpression joinExpr, AbstractExpression  whereExpr, StmtSubqueryScan scan) {
        super(id);
        m_joinExpr = joinExpr;
        m_whereExpr = whereExpr;
        m_subqueryScan = scan;
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
        JoinNode newNode = new SubqueryLeafNode(m_id, joinExpr, whereExpr, m_subqueryScan);
        return newNode;
    }

    @Override
    public JoinNode cloneWithoutFilters() {
        JoinNode newNode = new SubqueryLeafNode(m_id, null, null, m_subqueryScan);
        return newNode;
    }

    @Override
    public void extractEphemeralTableQueries(List<StmtEphemeralTableScan> scans) {
        scans.add(m_subqueryScan);
    }

    public StmtSubqueryScan getSubqueryScan() { return m_subqueryScan; }

    @Override
    public StmtTableScan getTableScan() { return m_subqueryScan; }

    @Override
    public String getTableAlias() { return m_subqueryScan.getTableAlias(); }

    @Override
    public boolean hasSubqueryScans() {
        //  This is a subquery scan.
        return true;
    }

    @Override
    protected void queueChildren(Deque<JoinNode> joinNodes) {
    }
}
