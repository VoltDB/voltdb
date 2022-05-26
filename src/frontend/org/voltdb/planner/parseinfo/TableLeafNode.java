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
 * An object of class TableLeafNode is a leaf in a join expression tree.  It
 * represents a table reference.
 */
public class TableLeafNode extends JoinNode {
    private StmtTargetTableScan m_tableScan;
    /**
     * Construct a table leaf node
     * @param id - node unique id
     * @param table - join table index
     * @param joinExpr - join expression
     * @param whereExpr - filter expression
     * @param tableScan - table scan of this join node
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
        final AbstractExpression joinExpr = (m_joinExpr != null) ? m_joinExpr.clone() : null;
        final AbstractExpression whereExpr = (m_whereExpr != null) ? m_whereExpr.clone() : null;
        return new TableLeafNode(m_id, joinExpr, whereExpr, m_tableScan);
    }

    @Override
    public JoinNode cloneWithoutFilters() {
        return new TableLeafNode(m_id, null, null, m_tableScan);
    }

    @Override
    public StmtTableScan getTableScan() {
        return m_tableScan;
    }

    @Override
    public String getTableAlias() {
        return m_tableScan.getTableAlias();
    }

    @Override
    public boolean hasSubqueryScans() {
        // No subquery scans here.
        return false;
    }

    @Override
    public void extractEphemeralTableQueries(List<StmtEphemeralTableScan> scans) { }

    @Override
    protected void queueChildren(Deque<JoinNode> joinNodes) {
    }
}
