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

package org.voltdb.planner.optimizer;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.types.ExpressionType;
import static org.voltdb.planner.optimizer.NormalizerUtil.*;

import java.util.stream.Collectors;

/**
 * Push down unary minus to the level that cannot be further pushed. e.g.
 * - (a + b * c - power(d, e)) ==> (-a) - (b * c) + power(d, e).
 */
final class UnaryMinusPushDown {
    private final AbstractExpression m_expr;
    UnaryMinusPushDown(AbstractExpression e) {
        if (e == null) {
            m_expr = null;
        } else if (isLiteralConstant(e)) {      // terminal node: CVE/PVE/VVE
            m_expr = negate_of(e);
        } else {
            switch (e.getExpressionType()) {
                case OPERATOR_UNARY_MINUS:      // minus of minus: further eliminate next level
                    m_expr = eliminate(e.getLeft());
                    break;
                case OPERATOR_PLUS:             // a + b ==> -a - b
                    m_expr = new OperatorExpression(ExpressionType.OPERATOR_MINUS,
                            new UnaryMinusPushDown(e.getLeft()).get(), eliminate(e.getRight()));
                    break;
                case OPERATOR_MINUS:            // -(a - b) ==> b - a
                    m_expr = new OperatorExpression(e.getExpressionType(),
                            eliminate(e.getRight()), eliminate(e.getLeft()));
                    break;
                case OPERATOR_MULTIPLY:         // If there is a number constant in multiplication/division, negate that number constant;
                case OPERATOR_DIVIDE:
                    AbstractExpression left = e.getLeft(), right = e.getRight();
                    final boolean leftIsConstant = isLiteralConstant(left), rightIsConstant = isLiteralConstant(right);
                    if (leftIsConstant || rightIsConstant) {  // prefer to negate a constant term
                        if (leftIsConstant) {
                            left = negate_of(left);
                        } else {
                            right = negate_of(right);
                        }
                    } else {    // else prefer to eliminate on the expression that already has unary minus on it; prefer negating left otherwise.
                        if (!right.getExpressionType().equals(ExpressionType.OPERATOR_UNARY_MINUS)) {
                            left = new UnaryMinusPushDown(left).get();
                        } else {
                            right = new UnaryMinusPushDown(right).get();
                        }
                    }
                    m_expr = new OperatorExpression(e.getExpressionType(), eliminate(left), eliminate(right));
                    break;
                default:
                    m_expr = new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS, e, null);
            }
        }
    }
    AbstractExpression get() {
        return m_expr;
    }

    /**
     * Non-destructive operation that eliminates all non-terminal unary minus operators.
     * @param e source expression
     * @return expression without non-terminal unary-minus operators.
     */
    static AbstractExpression eliminate(AbstractExpression e) {
        if (e == null) {
            return null;
        } else if (e.getExpressionType().equals(ExpressionType.OPERATOR_UNARY_MINUS)) {
            return new UnaryMinusPushDown(e.getLeft().clone()).get();
        } else {
            final AbstractExpression copy = e.clone();
            if (copy.getLeft() != null)
                copy.setLeft(eliminate(copy.getLeft()));
            if (copy.getRight() != null)
                copy.setRight(eliminate(copy.getRight()));
            if (copy.getArgs() != null)
                copy.setArgs(copy.getArgs().stream().map(UnaryMinusPushDown::eliminate).collect(Collectors.toList()));
            return copy;
        }
    }
}


