/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.types.JoinType;

public class BranchNode extends JoinNode {

    /**
     * Construct a join node
     * @param id - node unique id
     * @param joinType - join type
     * @param leftNode - left node
     * @param rightNode - right node
     */
    public BranchNode(int id, JoinType joinType, JoinNode leftNode, JoinNode rightNode) {
        super(id, joinType, NodeType.JOIN);
        m_leftNode = leftNode;
        m_rightNode = rightNode;
    }
    /**
     * Deep clone
     */
    @Override
    public Object clone() {
        assert(m_leftNode != null && m_rightNode != null);
        JoinNode leftNode = (JoinNode) m_leftNode.clone();
        JoinNode rightNode = (JoinNode) m_rightNode.clone();

        BranchNode newNode = new BranchNode(m_id, m_joinType, leftNode, rightNode);

        if (m_joinExpr != null) {
            newNode.m_joinExpr = (AbstractExpression) m_joinExpr.clone();
        }
        if (m_whereExpr != null) {
            newNode.m_whereExpr = (AbstractExpression) m_whereExpr.clone();
        }
        return newNode;
    }

    @Override
    public JoinNode getLeftNode() {
        assert(m_leftNode != null);
        return m_leftNode;
    }

    @Override
    public JoinNode getRightNode() {
        assert(m_rightNode != null);
        return m_rightNode;
    }


}
