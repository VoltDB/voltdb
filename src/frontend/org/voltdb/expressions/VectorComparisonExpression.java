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

package org.voltdb.expressions;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

/**
 * Expression to compare two vectors or scalars with ALL/ANY quantifiers
 */
public class VectorComparisonExpression extends AbstractExpression {

    ExpressionType m_quantifier = null;;

    public VectorComparisonExpression(ExpressionType type) {
        super(type);
        setValueType(VoltType.BIGINT);
    }

    public VectorComparisonExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        super(type, left, right);
        setValueType(VoltType.BIGINT);
    }

    public VectorComparisonExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    @Override
    public boolean needsRightExpression() {
        return true;
    }

    public void setQuantifier(ExpressionType quantifier) {
        m_quantifier = quantifier;
    }

    public ExpressionType getQuantifier() {
        return m_quantifier;
    }

    public static final Map<ExpressionType,ExpressionType> reverses = new HashMap<ExpressionType, ExpressionType>();
    static {
        reverses.put(ExpressionType.COMPARE_VECTOREQUAL, ExpressionType.COMPARE_VECTOREQUAL);
        reverses.put(ExpressionType.COMPARE_VECTORNOTEQUAL, ExpressionType.COMPARE_VECTORNOTEQUAL);
        reverses.put(ExpressionType.COMPARE_VECTORLESSTHAN, ExpressionType.COMPARE_VECTORGREATERTHAN);
        reverses.put(ExpressionType.COMPARE_VECTORGREATERTHAN, ExpressionType.COMPARE_VECTORLESSTHAN);
        reverses.put(ExpressionType.COMPARE_VECTORLESSTHANOREQUALTO, ExpressionType.COMPARE_VECTORGREATERTHANOREQUALTO);
        reverses.put(ExpressionType.COMPARE_VECTORGREATERTHANOREQUALTO, ExpressionType.COMPARE_VECTORLESSTHANOREQUALTO);
    }

    public VectorComparisonExpression reverseOperator() {
        ExpressionType reverseType = reverses.get(this.m_type);
        // Left and right exprs are reversed on purpose
        return new VectorComparisonExpression(reverseType, m_right, m_left);
    }

    @Override
    public Object clone() {
        AbstractExpression clone = (AbstractExpression) super.clone();
        assert(clone instanceof VectorComparisonExpression);
        ((VectorComparisonExpression) clone).m_quantifier = m_quantifier;
        return clone;
    }

    @Override
    public boolean equals(Object other) {
        boolean equal = super.equals(other);
        if (equal && other instanceof VectorComparisonExpression) {
            equal = ((VectorComparisonExpression) other).m_quantifier == m_quantifier;
        }
        return equal;
    }

    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        //
        // IMPORTANT:
        // We are not handling the case where one of types is NULL. That is because we
        // are only dealing with what the *output* type should be, not what the actual
        // value is at execution time. There will need to be special handling code
        // over on the ExecutionEngine to handle special cases for conjunctions with NULLs
        // Therefore, it is safe to assume that the output is always going to be an
        // integer (for booleans)
        //
        m_valueType = VoltType.BIGINT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        String typeStr = (m_quantifier != null) ? type.symbol() + " " + m_quantifier.symbol() : type.symbol();
        return "(" + m_left.explain(impliedTableName) +
            " " + typeStr +
            m_right.explain(impliedTableName) + ")";
    }

}
