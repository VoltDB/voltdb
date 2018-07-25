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
import org.voltdb.types.ExpressionType;

import java.util.stream.Collectors;

import static org.voltdb.planner.optimizer.NormalizerUtil.ConjunctionRelation;
import static org.voltdb.planner.optimizer.NormalizerUtil.ArithOpType;
import static org.voltdb.planner.optimizer.NormalizerUtil.isLiteralConstant;

/**
 * Does the chain of all simplification and normalization work on an expression:
 * 1. Push any logic NOT to the bottom of a logic expression
 * 2. Push unary minus (i.e. - expression) to the bottom of an arithmetic expression
 * 3. Flatten logic expression to multi-layered multi-way trees of AND-ing layers and OR-ing layers
 * 4. On each such layer, flatten arithmetic expression to multi-layered multi-way trees of
 *     Plus-Minus layers and Multiplication-Division layers
 * 5. Simplify on each arithmetic layer: numeral constant combination and common term combination/elimination
 * 6. After arithmetic simplification, simplify *integer* intervals on common left expressions
 * 7. Combine back to a new expression
 * 8. Do step 1-7 again, until the new expression is the same as old one.
 *
 * Non-destructive.
 */
public class ExpressionNormalizer {
    private final AbstractExpression m_normalizedExpression;

    /**
     * Simplifies an expression. The result expression might be further simplifiable.
     * @param e original expression
     */
    private ExpressionNormalizer(AbstractExpression e) {
        if (e == null) {
            m_normalizedExpression = null;
        } else if (e instanceof ConstantValueExpression) {
            m_normalizedExpression = e;
        } else if (e instanceof ConjunctionExpression || e instanceof ComparisonExpression ||
                e.getExpressionType().equals(ExpressionType.OPERATOR_NOT)) {    // <that is, the expression is boolean type>
            m_normalizedExpression =
                    LogicExpressionFlattener.apply(
                            new LogicExpressionFlattener(
                                    OpExpressionBalancer.balance(UnaryMinusPushDown.eliminate(NegatePushDown.eliminate(e)))),
                            ExpressionNormalizer::reduce).toExpression();      // When creating a brand-new, transformed expression,
            m_normalizedExpression.setValueType(e.getValueType());             // be sure to restore the original expression's value type,
            m_normalizedExpression.setValueSize(e.getValueSize());             // as none of sub-components used here heeds the value type,
        } else {                                                               // <or the expression might assume any other value type>
            m_normalizedExpression = ArithmeticExpressionFlattener.apply(
                    new ArithmeticExpressionFlattener(UnaryMinusPushDown.eliminate(e), ArithOpType.PlusMinus),
                    ArithmeticSimplifier::of);
            m_normalizedExpression.setValueType(e.getValueType());              // especially in this branch.
            m_normalizedExpression.setValueSize(e.getValueSize());
        }
    }
    private AbstractExpression getNormalized() {
        return m_normalizedExpression;
    }
    private static AbstractExpression createExpression(ArithOpType op, AbstractExpression e) {
        switch (op) {
            case Atom:
                if (e instanceof ComparisonExpression) {
                    return OpExpressionBalancer.balance(e);
                } else {
                    return e;
                }
            default:
                //assert(e.getValueType() == VoltType.BOOLEAN);
                assert(e instanceof ComparisonExpression || e instanceof ConjunctionExpression || e instanceof TupleValueExpression);
                return e instanceof ComparisonExpression && ! (e instanceof InComparisonExpression) ?
                        new ComparisonExpression(e.getExpressionType(),
                                ArithmeticSimplifier.of(op, e.getLeft()),
                                ArithmeticSimplifier.of(op, e.getRight())) :
                        e;
        }
    }
    private static AbstractExpression reduce(AbstractExpression e, ConjunctionRelation rel) {
        if (e instanceof ConstantValueExpression) {
            return e;
        } else {
            return IntegerIntervalCombiner.combine(ArithmeticExpressionFlattener.apply(
                    new ArithmeticExpressionFlattener(e, ArithOpType.PlusMinus),
                    ExpressionNormalizer::createExpression), rel);
        }
    }

    /**
     * Simplify an expression to its simplest form. Nondestructive.
     * @param e original expression
     * @return equivalent expression in its simplest form.
     */
    public static AbstractExpression normalize(AbstractExpression e) {
        if (e == null) {
            return null;
        } else if (e instanceof FunctionExpression) {       // only need to normalize all args for a function expression
            final AbstractExpression expr = e.clone();
            if (e.getArgs() != null) {
                expr.setArgs(e.getArgs().stream()
                        .map(ExpressionNormalizer::normalize)
                        .collect(Collectors.toList()));
            }
            return expr;
        } else {
            AbstractExpression expr = e;
            do {
                e = expr;
                expr = new ExpressionNormalizer(e).getNormalized();
            } while (expr != null && !expr.equals(e));
            return expr;
        }
    }
}
