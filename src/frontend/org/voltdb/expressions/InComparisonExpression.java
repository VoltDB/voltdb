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

import org.voltdb.VoltType;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class InComparisonExpression extends ComparisonExpression {

    public InComparisonExpression() {
        super(ExpressionType.COMPARE_IN);
    }

    public InComparisonExpression(AbstractExpression left, AbstractExpression right) {
        this();
        setLeft(left);
        setRight(right);
        setValueType(VoltType.BOOLEAN);
    }

    @Override
    public void validate() {
        super.validate();
        //
        // Args list is not used by IN.
        //
        if (m_args != null) {
            throw new ValidationError("Args list was not null for '%s'", toString());
        }
        //
        // We always need both a left node and a right node
        //
        if (m_left == null) {
            throw new ValidationError("The left node for '%s' is NULL", toString());
        } else if (m_right == null) {
            throw new ValidationError("The right node for '%s' is NULL", toString());
        }

        // right needs to be vector or parameter
        if (!(m_right instanceof VectorValueExpression) && !(m_right instanceof ParameterValueExpression)) {
            throw new ValidationError("The right node for '%s' is not a list or a parameter", toString());
        }
    }

    /**
     * A "x in (a, b, c)" relation cannot be reversed as "x > y" to "y < x", so
     * we return itself.
     */
    @Override
    public ComparisonExpression reverseOperator() {
       return this;
    }

    @Override
    public void finalizeValueTypes() {
        // First, make sure this node and its children have valid types.
        // This ignores the overall element type of the rhs.
        super.finalizeValueTypes();
        // Force the lhs type as the overall element type of the rhs.
        // The element type gets used in the EE to handle overflow/underflow cases.
        m_right.setValueType(m_left.getValueType());
        m_right.setValueSize(m_left.getValueSize());
    }
}
