/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public abstract class ExpressionUtil {

    /**
     * Make a final pass over the abstract expression tree, first specializing
     * the types of certain operands based on the types of their sibling
     * operands and then finalizing types to be specific enough to be
     * acceptable to the EE.
     * @param exp expression tree whose value types may still need refinement.
     */
    public static void finalizeValueTypes(AbstractExpression exp) {
        exp.normalizeOperandTypes_recurse();
        exp.finalizeValueTypes();
    }

    /**
     * Combine a list of source predicate expressions into a single AND-tree
     * expression containing clones of the source predicates.
     * @param colExps
     */
    @SafeVarargs
    public static AbstractExpression cloneAndCombinePredicates(Collection<AbstractExpression>... colExps) {
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        for (Collection<AbstractExpression> exps : colExps) {
            if (exps == null) {
                continue;
            }
            for (AbstractExpression expr : exps) {
                stack.add((AbstractExpression)expr.clone());
            }
        }
        if (stack.isEmpty()) {
            return null;
        }
        return combinePredicateStack(stack);
    }

    /**
     * Combine two predicate arguments with AND into a single predicate
     * @param left
     * @param right
     * @return Both expressions passed in combined by an And conjunction.
     */
    public static AbstractExpression combinePredicates(AbstractExpression left, AbstractExpression right) {
        AbstractExpression retval = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, left, right);
        // Simplify combined expression if possible
        return evaluateExpression(retval);
    }

    /**
     * Combine one or more lists of predicate expressions into a single AND-tree expression.
     * @param colExps
     */
    @SafeVarargs
    public static AbstractExpression combinePredicates(Collection<AbstractExpression>... colExps) {
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        for (Collection<AbstractExpression> exps : colExps) {
            if (exps != null) {
                stack.addAll(exps);
            }
        }
        if (stack.isEmpty()) {
            return null;
        }
        return combinePredicateStack(stack);
    }

    private static AbstractExpression combinePredicateStack(Stack<AbstractExpression> stack) {
        // TODO: This code probably doesn't need to go through all this trouble to create AND trees
        // like "((D and C) and B) and A)" from the list "[A, B, C, D]".
        // It might be better to generate "(D and (C and (B and A)))"
        // which would short-circuit slightly faster in the executor.
        // NOTE: Any change to the structure of the trees produced by this algorithm should be
        // reflected in the algorithm used to reverse the process in uncombinePredicate(AbstractExpression expr).

        AbstractExpression ret = null;
        while (stack.size() > 0) {
            AbstractExpression child_exp = stack.pop();
            if (ret == null) {
                ret = child_exp;
                continue;
            }
            ret = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, ret, child_exp);
        }
        // Simplify combined expression if possible
        return ExpressionUtil.evaluateExpression(ret);
    }

    /**
     * Undo the effects of the combine(List<AbstractExpression> exps) method to reconstruct the list
     * of expressions in its original order, basically right-to-left in the given expression tree.
     * NOTE: This implementation is tuned to the odd shape of the trees produced by combine,
     * namely leaf-nodes-on-the-right "(((D and C) and B) and A)" from "[A,B,C,D]".
     * Any change there should have a corresponding change here.
     * @param expr
     * @return
     */
    public static List<AbstractExpression> uncombinePredicate(AbstractExpression expr) {
        List<AbstractExpression> result = new ArrayList<AbstractExpression>();
        if (expr == null) {
            return result;
        }
        while (expr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
            // Append the leaf on the right to the list.
            result.add(expr.getRight());
            // "Tail recurse" on the tree or leaf on the left.
            expr = expr.getLeft();
        }
        result.add(expr);
        return result;
    }

    /**
     * Convert one or more predicates, potentially in an arbitrarily nested conjunction tree
     * into a flattened collection. Similar to uncombine but for arbitrary tree shapes and with no
     * guarantee of the result collection type or of any ordering within the collection.
     * In fact, it currently fills an ArrayDeque via a left=to-right breadth first traversal,
     * but for no particular reason, so that's all subject to change.
     * @param expr
     * @return a Collection containing expr or if expr is a conjunction, its top-level non-conjunction
     * child expressions.
     */
    public static Collection<AbstractExpression> uncombineConjunctions(AbstractExpression expr) {
        ArrayDeque<AbstractExpression> out = new ArrayDeque<AbstractExpression>();
        if (expr != null) {
            ArrayDeque<AbstractExpression> in = new ArrayDeque<AbstractExpression>();
            // this chunk of code breaks the code into a list of expression that
            // all have to be true for the where clause to be true
            in.add(expr);
            AbstractExpression inExpr = null;
            while ((inExpr = in.poll()) != null) {
                if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                    in.add(inExpr.getLeft());
                    in.add(inExpr.getRight());
                }
                else {
                    out.add(inExpr);
                }
            }
        }
        return out;
    }

    /**
     * Find any listed expressions that qualify as potential partitioning where filters,
     * which is to say are equality comparisons with a TupleValueExpression on at least one side,
     * and a TupleValueExpression, ConstantValueExpression, or ParameterValueExpression on the other.
     * Add them to a map keyed by the TupleValueExpression(s) involved.
     * @param filterList a list of candidate expressions
     * @param the running result
     * @return a Collection containing the qualifying filter expressions.
     */
    public static void collectPartitioningFilters(Collection<AbstractExpression> filterList,
            HashMap<AbstractExpression, Set<AbstractExpression> > equivalenceSet) {
        for (AbstractExpression expr : filterList) {
            if ( ! expr.isColumnEquivalenceFilter()) {
                continue;
            }
            AbstractExpression leftExpr = expr.getLeft();
            AbstractExpression rightExpr = expr.getRight();

            // Any two asserted-equal expressions need to map to the same equivalence set,
            // which must contain them and must be the only such set that contains them.
            Set<AbstractExpression> eqSet1 = null;
            if (equivalenceSet.containsKey(leftExpr)) {
                eqSet1 = equivalenceSet.get(leftExpr);
            }
            if (equivalenceSet.containsKey(rightExpr)) {
                Set<AbstractExpression> eqSet2 = equivalenceSet.get(rightExpr);
                if (eqSet1 == null) {
                    // Add new leftExpr into existing rightExpr's eqSet.
                    equivalenceSet.put(leftExpr, eqSet2);
                    eqSet2.add(leftExpr);
                }
                else {
                    // Merge eqSets, re-mapping all the rightExpr's equivalents into leftExpr's eqset.
                    for (AbstractExpression eqMember : eqSet2) {
                        eqSet1.add(eqMember);
                        equivalenceSet.put(eqMember, eqSet1);
                    }
                }
            }
            else {
                if (eqSet1 == null) {
                    // Both leftExpr and rightExpr are new -- add leftExpr to the new eqSet first.
                    eqSet1 = new HashSet<AbstractExpression>();
                    equivalenceSet.put(leftExpr, eqSet1);
                    eqSet1.add(leftExpr);
                }
                // Add new rightExpr into leftExpr's eqSet.
                equivalenceSet.put(rightExpr, eqSet1);
                eqSet1.add(rightExpr);
            }
        }
    }

    /**
     * Evaluate/reduce/simplify an input expression at the compilation time
     *
     * @param expr Original Expression
     * @return AbstractExpression
     */
    public static AbstractExpression evaluateExpression(AbstractExpression expr) {
        if (expr == null) {
            return null;
        }

        // Evaluate children first
        expr.setLeft(evaluateExpression(expr.getLeft()));
        expr.setRight(evaluateExpression(expr.getRight()));

        // Evaluate self
        if (ExpressionType.CONJUNCTION_AND == expr.getExpressionType()) {
            if (ExpressionType.VALUE_CONSTANT == expr.getLeft().getExpressionType()) {
                if (ConstantValueExpression.isBooleanTrue(expr.getLeft())) {
                    return expr.getRight();
                }
                else {
                    return expr.getLeft();
                }
            }
            if (ExpressionType.VALUE_CONSTANT == expr.getRight().getExpressionType()) {
                if (ConstantValueExpression.isBooleanTrue(expr.getRight())) {
                    return expr.getLeft();
                }
                else {
                    return expr.getRight();
                }
            }
        }
        else if (ExpressionType.CONJUNCTION_OR == expr.getExpressionType()) {
            if (ExpressionType.VALUE_CONSTANT == expr.getLeft().getExpressionType()) {
                if (ConstantValueExpression.isBooleanTrue(expr.getLeft())) {
                    return expr.getLeft();
                }
                else {
                    return expr.getRight();
                }
            }
            if (ExpressionType.VALUE_CONSTANT == expr.getRight().getExpressionType()) {
                if (ConstantValueExpression.isBooleanTrue(expr.getRight())) {
                    return expr.getRight();
                }
                else {
                    return expr.getLeft();
                }
            }
        }
        else if (ExpressionType.OPERATOR_NOT == expr.getExpressionType()) {
            AbstractExpression leftExpr = expr.getLeft();
            // function expressions can also return boolean. So the left child expression
            // can be expression which are not constant value expressions, so don't
            // evaluate every left child expr as constant value expression
            assert(VoltType.BOOLEAN == leftExpr.getValueType());
            if (leftExpr instanceof ConstantValueExpression) {
                if (ConstantValueExpression.isBooleanTrue(leftExpr)) {
                    return ConstantValueExpression.getFalse();
                }
                else {
                    return ConstantValueExpression.getTrue();
                }
            }
            if (ExpressionType.OPERATOR_NOT == leftExpr.getExpressionType()) {
                return leftExpr.getLeft();
            }
            if (ExpressionType.CONJUNCTION_OR == leftExpr.getExpressionType()) {
                // NOT (.. OR .. OR ..) => NOT(..) AND NOT(..) AND NOT(..)
                AbstractExpression l = new OperatorExpression(ExpressionType.OPERATOR_NOT, leftExpr.getLeft(), null);
                AbstractExpression r = new OperatorExpression(ExpressionType.OPERATOR_NOT, leftExpr.getRight(), null);
                leftExpr = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, l, r);
                return evaluateExpression(leftExpr);
            }
            // NOT (expr1 AND expr2) => (NOT expr1) || (NOT expr2)
            // The above case is probably not interesting to do for short circuit purpose
        }
        return expr;
    }
}
