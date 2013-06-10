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

package org.voltdb.expressions;

import java.util.List;

import org.voltdb.types.ExpressionType;

/**
 *
 */
public class InComparisonExpression extends ComparisonExpression {

    public InComparisonExpression() {
        super(ExpressionType.COMPARE_IN);
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // We need at least one value defined
        //
        if (m_args.isEmpty()) {
            throw new Exception("ERROR: There were no values defined for '" + this + "'");
        }
        //
        // We always need both a left node and a right node
        //
        if (m_left == null) {
            throw new Exception("ERROR: The left node for '" + this + "' is NULL");
        } else if (m_right == null) {
            throw new Exception("ERROR: The right node for '" + this + "' is NULL");
        }

        // right needs to be vector or parameter
        if (!(m_right instanceof VectorValueExpression) && !(m_right instanceof ParameterValueExpression)) {
            throw new Exception("ERROR: The right node for '" + this + "' is not a list or a parameter");
        }
    }

    /**
     * @return the values to be matched by the lhs expression
     */
    public List<AbstractExpression> getValues() {
        return m_args;
    }
    /**
     * @param values the values to be matched by the lhs expression
     */
    public void setValues(List<AbstractExpression> values) {
        m_args = values;
    }
}
