/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.planner;

import org.voltdb.types.ExpressionType;
import org.voltdb.expressions.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Recursively compares if two filter expressions match on the view table and select table.
 * Now, checks that expressions perfectly match each other, e.g. "WHERE a > b" and "WHERE b < a"
 * is considered mismatch. A fancier way to do some cumbersome recursive task.
 * It handles equivalence between ConstantValueExpression and ParameterValueExpression.
 *
 * Given filter expression from SELECT stmt's source table, filter expression from view table,
 * (mapping of  SELECT stmt's table column index ==> view table column index is NOT needed here, as
 *  the column are indexed on view's source table),
 * returns whether two filter expressions match with each other
 */
final class FilterMatcher {
    private static final Set<ExpressionType> EXCHANGEABLE_EXPRESSIONS = new HashSet<ExpressionType>() {
        {
            add(ExpressionType.CONJUNCTION_AND);
            add(ExpressionType.CONJUNCTION_OR);
            add(ExpressionType.OPERATOR_PLUS);
            add(ExpressionType.OPERATOR_MULTIPLY);
        }
    };
    private final AbstractExpression m_expr1, m_expr2;
    public FilterMatcher(AbstractExpression e1, AbstractExpression e2) {
        m_expr1 = e1;
        m_expr2 = e2;
    }
    public boolean match() {
        if (m_expr1 == null || m_expr2 == null) {
            return m_expr1 == m_expr2;
        } else if (!exprTypesMatch(m_expr1, m_expr2)) {
            // Exception to the rule:
            // 1. CVE and PVE need to be translated before compared.
            // 2. Comparisons could be reversed, e.g. "a >= b" and "b <= a" are the same relation
            return valueConstantsMatch(m_expr1, m_expr2) || comparisonsMatch(m_expr1, m_expr2);
        } else if (m_expr1 instanceof ConstantValueExpression) {
            return m_expr1.equals(m_expr2);
        } else if (m_expr1 instanceof TupleValueExpression) {
            return tvesMatch((TupleValueExpression) m_expr1, (TupleValueExpression) m_expr2);
        } else if (m_expr1 instanceof VectorValueExpression) {
            return vectorsMatch((VectorValueExpression) m_expr1, (VectorValueExpression) m_expr2) &&
                    childrenMatch(m_expr1, m_expr2);    // no need to compare args (orders/duplication don't matter)
        } else if(EXCHANGEABLE_EXPRESSIONS.contains(m_expr1.getExpressionType())) {
            // For AND/OR, left/right sub-expr are exchangeable
            return (argsMatch(m_expr1, m_expr2) && childrenMatch(m_expr1, m_expr2)) ||
                    ((new FilterMatcher(m_expr1.getLeft(), m_expr2.getRight())).match() &&
                            (new FilterMatcher(m_expr1.getRight(), m_expr2.getLeft())).match());
        } else
            return argsMatch(m_expr1, m_expr2) && childrenMatch(m_expr1, m_expr2);
    }
    private static boolean argsMatch(AbstractExpression e1, AbstractExpression e2) {
        final List<AbstractExpression> args1 = e1.getArgs(), args2 = e2.getArgs();
        return args1 == null || args2 == null ? args1 == args2 :
                args1.size() == args2.size() && IntStream.range(0, args1.size())
                        .mapToObj(index -> (new FilterMatcher(args1.get(index), args2.get(index))).match())
                        .reduce(Boolean::logicalAnd).get();
    }
    private static boolean childrenMatch(AbstractExpression e1, AbstractExpression e2) {
        return (new FilterMatcher(e1.getLeft(), e2.getLeft())).match() &&
                (new FilterMatcher(e1.getRight(), e2.getRight())).match();
    }
    // Check that typeof() both args equal, with additional equivalence of one of
    // ParameterValueExpression type (from SELECT stmt); and the other from ConstantValueExpression
    // type (from VIEW definition).
    private static boolean exprTypesMatch(AbstractExpression e1, AbstractExpression e2) {
        assert(e1.getExpressionType() != null);
        return e1.getExpressionType().equals(e2.getExpressionType());
    }

    private static boolean valueConstantsMatch(AbstractExpression e1, AbstractExpression e2) {
        return (e1 instanceof ParameterValueExpression && e2 instanceof ConstantValueExpression ||
                e1 instanceof ConstantValueExpression && e2 instanceof ParameterValueExpression) &&
                equalsAsCVE(e1, e2);
    }

    private static boolean comparisonsMatch(AbstractExpression e1, AbstractExpression e2) {
        return e1 instanceof ComparisonExpression &&
                e1.getExpressionType().equals(ComparisonExpression.reverses.get(e2.getExpressionType())) &&
                (new FilterMatcher(((ComparisonExpression) e1).reverseOperator(), e2)).match();
    }

    private static boolean equalsAsCVE(AbstractExpression e1, AbstractExpression e2) {
        final ConstantValueExpression ce1 = asCVE(e1), ce2 = asCVE(e2);
        return ce1 == null || ce2 == null ? ce1 == ce2 : ce1.equals(ce2);
    }
    /**
     * Convert a ConstantValueExpression or ParameterValueExpression into a ConstantValueExpression.
     * \pre argument must be either of the two.
     * @param expr expression to be casted
     * @return casted ConstantValueExpression
     */
    private static ConstantValueExpression asCVE(AbstractExpression expr) {
        return expr instanceof ConstantValueExpression ? (ConstantValueExpression) expr :
                ((ParameterValueExpression) expr).getOriginalValue();
    }
    /**
     * Compare two vectors as sets, e.g. "WHERE a in (1, 2, 3)" vs. "WHERE a in (2, 1, 3)"
     */
    private static boolean vectorsMatch(VectorValueExpression e1, VectorValueExpression e2) {
        return e1.getArgs().stream().map(FilterMatcher::asCVE).collect(Collectors.toSet())
                .equals(e2.getArgs().stream().map(FilterMatcher::asCVE).collect(Collectors.toSet()));
    }
    /**
     * Matches two tuple value expressions, using column indexing map that converts SELECT stmt src table column indices
     * into VIEW table column indices.
     *
     * NOTE: TVEs don't have arguments or children.
     * TODO: TVE.column index might not get properly set till end of planning, when query is made across several tables.
     */
    private static boolean tvesMatch(TupleValueExpression sel, TupleValueExpression view) {
        return sel.getColumnIndex() == view.getColumnIndex();
    }
}
