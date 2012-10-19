/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.expressions;

import org.voltdb.types.ExpressionType;

/**
 * Base class for Expression types that represent a value.
 * This does nothing, but makes examining expression trees easier in some ways.
 *
 */
public abstract class AbstractValueExpression extends AbstractExpression {

    public AbstractValueExpression() {
        // This is needed for serialization
    }

    public AbstractValueExpression(ExpressionType type) {
        super(type);
    }

    // Disable all the structural equality checking overhead of AbstractExpression.
    // Force AbstractValueExpression derived classes to fend for themselves.
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public void normalizeOperandTypes_recurse()
    {
        // Nothing to do... no operands.
    }

    @Override
    public void finalizeValueTypes()
    {
        // Nothing to do... This is all about pulling types UP from child expressions.
    }

}
