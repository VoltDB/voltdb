/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

public class SubqueryLeafNode extends JoinNode{

    /**
     * Construct a subquery node
     * @param id - node unique id
     * @param table - join table index
     * @param joinExpr - join expression
     * @param whereExpr - filter expression
     * @param id - node id
     */
    public SubqueryLeafNode(int id, int tableAliasIdx, AbstractExpression joinExpr, AbstractExpression  whereExpr) {
        super(id, JoinType.INNER, NodeType.SUBQUERY);
        m_tableAliasIndex = tableAliasIdx;
        m_joinExpr = joinExpr;
        m_whereExpr = whereExpr;
        assert(m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX);
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
        JoinNode newNode = new SubqueryLeafNode(m_id, m_tableAliasIndex, joinExpr, whereExpr);
        return newNode;
    }

    @Override
    public int getTableAliasIndex() {
        assert (m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX);
        return m_tableAliasIndex;
    }

}
