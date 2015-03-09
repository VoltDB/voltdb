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

package org.voltdb.expressions;

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

    @Override
    public void finalizeValueTypes()
    {
        // First, make sure this node and its children have valid types.
        // This ignores the overall element type of the rhs.
        super.finalizeValueTypes();
        // Force the lhs type as the overall element type of the rhs.
        // The element type gets used in the EE to handle overflow/underflow cases.
        m_right.setValueType(m_left.getValueType());
        m_right.setValueSize(m_left.getValueSize());
    }
}
