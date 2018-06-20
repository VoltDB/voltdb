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

import org.voltdb.expressions.*;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ExpressionType;

import java.util.stream.Collectors;

/**
 * Push down a NOT (...) expression to the bottom of AND/OR relation. e.g.
 * NOT (a && b) is converted/pushed-down as (NOT a) || (NOT b).
 */
final class NegatePushDown {
    private final AbstractExpression m_expr;
    // Evaluate `NOT expr' to push down "NOT" to the leaves
    private NegatePushDown(AbstractExpression expr) {
        final ExpressionType type = expr.getExpressionType();
        if (type == ExpressionType.OPERATOR_NOT) {
            m_expr = eliminate(expr.getLeft());
        } else if (expr instanceof ConjunctionExpression) {
            assert(type == ExpressionType.CONJUNCTION_AND || type == ExpressionType.CONJUNCTION_OR);
            m_expr = eliminate(new ConjunctionExpression(type == ExpressionType.CONJUNCTION_AND ?
                    ExpressionType.CONJUNCTION_OR : ExpressionType.CONJUNCTION_AND,
                    new NegatePushDown(expr.getLeft()).get(), new NegatePushDown(expr.getRight()).get()));
        } else if (expr instanceof ComparisonExpression && ! (expr instanceof InComparisonExpression)) {
            m_expr = new ComparisonExpression(reverseCmpOperator(type), expr.getLeft(), expr.getRight());
        } else {    // other types of terminal node
            m_expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr, null);
        }
    }
    private static ExpressionType reverseCmpOperator(ExpressionType from) {
        switch (from) {
            case COMPARE_EQUAL:
                return ExpressionType.COMPARE_NOTEQUAL;
            case COMPARE_NOTEQUAL:
                return ExpressionType.COMPARE_EQUAL;
            case COMPARE_GREATERTHAN:
                return ExpressionType.COMPARE_LESSTHANOREQUALTO;
            case COMPARE_GREATERTHANOREQUALTO:
                return ExpressionType.COMPARE_LESSTHAN;
            case COMPARE_LESSTHAN:
                return ExpressionType.COMPARE_GREATERTHANOREQUALTO;
            case COMPARE_LESSTHANOREQUALTO:
                return ExpressionType.COMPARE_GREATERTHAN;
            default:
                throw new PlanningErrorException("Internal error: comparison operator " +
                        from.toString() + " cannot be reversed.");
        }
    }
    private AbstractExpression get() {
        return m_expr;
    }
    static AbstractExpression eliminate(AbstractExpression e) {
        if (e == null) {
            return null;
        } else if (e.getExpressionType() == ExpressionType.OPERATOR_NOT) {
            return new NegatePushDown(e.getLeft().clone()).get();
        } else {
            final AbstractExpression copy = e.clone();
            if (copy.getLeft() != null) {
                copy.setLeft(eliminate(copy.getLeft()));
            }
            if (copy.getRight() != null) {
                copy.setRight(eliminate(copy.getRight()));
            }
            if (copy.getArgs() != null) {
                copy.setArgs(copy.getArgs().stream()
                        .map(NegatePushDown::eliminate).collect(Collectors.toList()));
            }
            return copy;
        }
    }
}
