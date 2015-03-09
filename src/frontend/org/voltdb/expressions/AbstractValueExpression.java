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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.types.ExpressionType;

/**
 * Base class for Expression types that represent a value.
 * This does nothing, but makes examining expression trees easier in some ways.
 *
 */
public abstract class AbstractValueExpression extends AbstractExpression {

    // This works on the assumption that it is only used to return final "leaf node" bindingLists that
    // are never updated "in place", but just get their contents dumped into a summary List that was created
    // inline and NOT initialized here.
    private final static List<AbstractExpression> s_reusableImmutableEmptyBinding = new ArrayList<AbstractExpression>();

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

    // Except for ParameterValueExpression, which takes care of itself, binding to value expressions
    // amounts to an equality test. If the values expressions are identical, the binding is trivially
    // possible, indicated by returning an empty list of binding requirements.
    // Otherwise, there is no binding possible, indicated by a null return.
    @Override
    public List<AbstractExpression> bindingToIndexedExpression(AbstractExpression expr) {
        if (equals(expr)) {
            return s_reusableImmutableEmptyBinding;
        }
        return null;
    }

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
