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

package org.voltdb.expressions;

import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.types.ExpressionType;

/**
 * Represents a value of evaluation of a single expression
 */
public class ScalarValueExpression extends AbstractValueExpression {

    public ScalarValueExpression() {
        super(ExpressionType.VALUE_SCALAR);
    }

    @Override
    public void finalizeValueTypes() {
        // Nothing to finalize
    }

    @Override
    public boolean equals(Object obj) {
        assert(m_left != null);
        if (! (obj instanceof ScalarValueExpression)) {
            return false;
        } else {
            ScalarValueExpression expr = (ScalarValueExpression) obj;
            return m_left.equals(expr.getLeft());
        }
    }

    @Override
    public int hashCode() {
        return m_left.hashCode();
    }

    @Override
    public String explain(String impliedTableName) {
        assert(m_left != null);
        return m_left.explain(impliedTableName);
    }

    /**
     * Currently ScalarValueExpressions can only have a subquery as children.  Return
     * the StmtSubqueryScan object for our child.
     * @return StmtSubqueryScan object for subquery
     */
    public StmtSubqueryScan getSubqueryScan() {
        SelectSubqueryExpression subqExpr = (SelectSubqueryExpression)m_left;
        return subqExpr.getSubqueryScan();
    }

}
