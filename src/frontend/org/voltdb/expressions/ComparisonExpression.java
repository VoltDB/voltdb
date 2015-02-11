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

import java.util.HashMap;
import java.util.Map;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class ComparisonExpression extends AbstractExpression {

    public ComparisonExpression(ExpressionType type) {
        super(type);
        setValueType(VoltType.BIGINT);
    }

    public ComparisonExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        super(type, left, right);
        setValueType(VoltType.BIGINT);
    }

    public ComparisonExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    @Override
    public boolean needsRightExpression() {
        return true;
    }

    public static final Map<ExpressionType,ExpressionType> reverses = new HashMap<ExpressionType, ExpressionType>();
    static {
        reverses.put(ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_EQUAL);
        reverses.put(ExpressionType.COMPARE_NOTEQUAL, ExpressionType.COMPARE_NOTEQUAL);
        reverses.put(ExpressionType.COMPARE_LESSTHAN, ExpressionType.COMPARE_GREATERTHAN);
        reverses.put(ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_LESSTHAN);
        reverses.put(ExpressionType.COMPARE_LESSTHANOREQUALTO, ExpressionType.COMPARE_GREATERTHANOREQUALTO);
        reverses.put(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_LESSTHANOREQUALTO);
    }

    public ComparisonExpression reverseOperator() {
        ExpressionType reverseType = reverses.get(this.m_type);
        // Left and right exprs are reversed on purpose
        return new ComparisonExpression(reverseType, m_right, m_left);
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

    /**
     * Construct the upper or lower bound expression that is implied by a prefix LIKE operator, given its required elements.
     * @param leftExpr - the LIKE operator's (and the result's) lhs expression
     * @param rangeComparator - a GTE or LT operator to indicate lower or upper bound, respectively,
     * @param comparand - a string operand value derived from the LIKE operator's rhs pattern
     * A helper for getGteFilterFromPrefixLike/getLtFilterFromPrefixLike
     **/
    static private ComparisonExpression rangeFilterFromPrefixLike(AbstractExpression leftExpr, ExpressionType rangeComparator, String comparand) {
        ConstantValueExpression cve = new ConstantValueExpression();
        cve.setValueType(VoltType.STRING);
        cve.setValue(comparand);
        cve.setValueSize(comparand.length());
        ComparisonExpression rangeFilter = new ComparisonExpression(rangeComparator, leftExpr, cve);
        return rangeFilter;
    }

    /**
     * Extract a prefix string from a prefix LIKE comparison's rhs pattern,
     * suitable for use as a lower bound constant.
     * Currently assumes the simple case of one final '%' wildcard character.
     * A helper for getGteFilterFromPrefixLike/getLtFilterFromPrefixLike
     **/
    private String extractLikePatternPrefix() {
        assert(getExpressionType() == ExpressionType.COMPARE_LIKE);
        ConstantValueExpression cve;
        if (m_right instanceof ParameterValueExpression) {
            ParameterValueExpression pve = (ParameterValueExpression)m_right;
            cve = pve.getOriginalValue();
        }
        else {
            assert(m_right instanceof ConstantValueExpression);
            cve = (ConstantValueExpression)m_right;
        }
        String pattern = cve.getValue();
        return pattern.substring(0, pattern.length()-1);
    }

    /**
     * Extract a modified prefix string from a prefix LIKE comparison's rhs pattern,
     * suitable for use as an upper bound constant.
     * Currently assumes the simple case of one final '%' wildcard character.
     * A helper for getLtFilterFromPrefixLike
     **/
    private String extractAndIncrementLikePatternPrefix() {
        String starter = extractLikePatternPrefix();
        // Right or wrong, this mimics what HSQL does for the case of " column LIKE prefix-pattern ".
        // It assumes that this last-sorting JAVA UTF-16 character maps to a suitably last-sorting UTF-8 string.
        String ender = starter + "\uffff";
        return ender;
    }

    /// Construct the lower bound comparison filter implied by a prefix LIKE comparison.
    public ComparisonExpression getGteFilterFromPrefixLike() {
        ExpressionType rangeComparator = ExpressionType.COMPARE_GREATERTHANOREQUALTO;
        String comparand = extractLikePatternPrefix();
        return rangeFilterFromPrefixLike(m_left, rangeComparator, comparand);
    }

    /// Construct the upper bound comparison filter implied by a prefix LIKE comparison.
    public ComparisonExpression getLtFilterFromPrefixLike() {
        ExpressionType rangeComparator = ExpressionType.COMPARE_LESSTHAN;
        String comparand = extractAndIncrementLikePatternPrefix();
        return rangeFilterFromPrefixLike(m_left, rangeComparator, comparand);
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        return "(" + m_left.explain(impliedTableName) +
            " " + type.symbol() + " " +
            m_right.explain(impliedTableName) + ")";
    }

}
