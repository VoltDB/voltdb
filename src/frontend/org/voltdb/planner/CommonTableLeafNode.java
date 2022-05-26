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

import java.util.Deque;
import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtCommonTableScan;

public class CommonTableLeafNode extends JoinNode {
    StmtCommonTableScan m_commonTableScan;
    /**
     * Construct a subquery node
     * @param id - node unique id
     * @param join
     */
    public CommonTableLeafNode(int id,
                               AbstractExpression joinExpr,
                               AbstractExpression  whereExpr,
                               StmtCommonTableScan scan) {
        super(id);
        m_joinExpr = joinExpr;
        m_whereExpr = whereExpr;
        m_commonTableScan = scan;
    }

    @Override
    public Object clone() {
        AbstractExpression joinExpr = m_joinExpr != null ? m_joinExpr.clone() : null;
        AbstractExpression whereExpr =m_whereExpr != null ? m_whereExpr.clone() : null;
        return new CommonTableLeafNode(m_id, joinExpr, whereExpr, m_commonTableScan);
    }

    @Override
    public StmtCommonTableScan getTableScan() {
        return m_commonTableScan;
    }

    @Override
    public JoinNode cloneWithoutFilters() {
        return new CommonTableLeafNode(m_id, null, null, m_commonTableScan);
    }

    @Override
    public String getTableAlias() {
        return m_commonTableScan.getTableAlias();
    }

    @Override
    public void extractEphemeralTableQueries(List<StmtEphemeralTableScan> scans) {
        scans.add(m_commonTableScan);
    }

    @Override
    public boolean hasSubqueryScans() {
        // No subquery scans here.
        return false;
    }

    @Override
    protected void queueChildren(Deque<JoinNode> joinNodes) {
    }
}

