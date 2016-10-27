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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.voltdb.VoltType;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public abstract class ExpressionUtil {

    public static void finalizeValueTypes(AbstractExpression exp)
    {
        exp.normalizeOperandTypes_recurse();
        exp.finalizeValueTypes();
    }

    @SafeVarargs
    public static AbstractExpression cloneAndCombinePredicates(Collection<AbstractExpression>... colExps) {
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        for (Collection<AbstractExpression> exps : colExps) {
            if (exps == null) {
                continue;
            }
            for (AbstractExpression expr : exps) {
                stack.add(expr.clone());
            }
        }
        if (stack.isEmpty()) {
            return null;
        }
        return combineStack(stack);
    }

    /**
     *
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
        return combineStack(stack);
    }

    private static AbstractExpression combineStack(Stack<AbstractExpression> stack) {
        // TODO: This code probably doesn't need to go through all this trouble to create AND trees
        // like "((D and C) and B) and A)" from the list "[A, B, C, D]".
        // There is an easier algorithm that does not require stacking intermediate results.
        // Even better, it would be easier here to generate "(D and (C and (B and A)))"
        // which would also short-circuit slightly faster in the executor.
        // NOTE: Any change to the structure of the trees produced by this algorithm should be
        // reflected in the algorithm used to reverse the process in uncombine(AbstractExpression expr).

        AbstractExpression ret = null;
        while (stack.size() > 1) {
            AbstractExpression child_exp = stack.pop();
            //
            // If our return node is null, then we need to make a new one
            //
            if (ret == null) {
                ret = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND);
                ret.setLeft(child_exp);
            //
            // Check whether we can add it to the right side
            //
            } else if (ret.getRight() == null) {
                ret.setRight(child_exp);
                stack.push(ret);
                ret = null;
            }
        }
        if (ret == null) {
            ret = stack.pop();
        } else {
            ret.setRight(stack.pop());
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
    public static List<AbstractExpression> uncombinePredicate(AbstractExpression expr)
    {
        if (expr == null) {
            return new ArrayList<AbstractExpression>();
        }
        if (expr instanceof ConjunctionExpression) {
            ConjunctionExpression conj = (ConjunctionExpression)expr;
            if (conj.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                // Calculate the list for the tree or leaf on the left.
                List<AbstractExpression> branch = uncombinePredicate(conj.getLeft());
                // Insert the leaf on the right at the head of that list
                branch.add(0, conj.getRight());
                return branch;
            }
            // Any other kind of conjunction must have been a leaf. Fall through.
        }
        // At the left-most leaf, start a new list.
        List<AbstractExpression> leaf = new ArrayList<AbstractExpression>();
        leaf.add(expr);
        return leaf;
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
    public static Collection<AbstractExpression> uncombineAny(AbstractExpression expr)
    {
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

    public static boolean isColumnEquivalenceFilter(AbstractExpression expr) {
        // Ignore expressions that are not of COMPARE_EQUAL or
        // COMPARE_NOTDISTINCT type
        ExpressionType type = expr.getExpressionType();
        if (type != ExpressionType.COMPARE_EQUAL &&
                type != ExpressionType.COMPARE_NOTDISTINCT) {
            return false;
        }
        AbstractExpression leftExpr = expr.getLeft();
        AbstractExpression rightExpr = expr.getRight();
        // Can't use an expression that is based on a column value but is not just a simple column value.
        if ( ( ! (leftExpr instanceof TupleValueExpression)) &&
                leftExpr.hasAnySubexpressionOfClass(TupleValueExpression.class) ) {
            return false;
        }
        if ( ( ! (rightExpr instanceof TupleValueExpression)) &&
                rightExpr.hasAnySubexpressionOfClass(TupleValueExpression.class) ) {
            return false;
        }
        return true;
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
    public static void
    collectPartitioningFilters(Collection<AbstractExpression> filterList,
                               HashMap<AbstractExpression, Set<AbstractExpression> > equivalenceSet)
    {
        for (AbstractExpression expr : filterList) {
            if ( ! isColumnEquivalenceFilter(expr)) {
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
                } else {
                    // Merge eqSets, re-mapping all the rightExpr's equivalents into leftExpr's eqset.
                    for (AbstractExpression eqMember : eqSet2) {
                        eqSet1.add(eqMember);
                        equivalenceSet.put(eqMember, eqSet1);
                    }
                }
            } else {
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
     *
     * @param left
     * @param right
     * @return Both expressions passed in combined by an And conjunction.
     */
    public static AbstractExpression combine(AbstractExpression left, AbstractExpression right) {
        AbstractExpression retval = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, left, right);
        // Simplify combined expression if possible
        return ExpressionUtil.evaluateExpression(retval);
    }

    /**
     * Recursively walk an expression and return a list of all the tuple
     * value expressions it contains.
     */
    public static List<TupleValueExpression>
    getTupleValueExpressions(AbstractExpression input)
    {
        ArrayList<TupleValueExpression> tves =
            new ArrayList<TupleValueExpression>();
        // recursive stopping steps
        if (input == null)
        {
            return tves;
        } else if (input instanceof TupleValueExpression) {
            tves.add((TupleValueExpression) input);
            return tves;
        }

        // recursive calls
        tves.addAll(getTupleValueExpressions(input.m_left));
        tves.addAll(getTupleValueExpressions(input.m_right));
        if (input.m_args != null) {
            for (AbstractExpression argument : input.m_args) {
                tves.addAll(getTupleValueExpressions(argument));
            }
        }
        return tves;
    }

    /**
     * Method to simplify an expression by eliminating identical subexpressions (same id)
     * If the expression is a logical conjunction of the form e1 AND e2 AND e3 AND e4,
     * and subexpression e1 is identical to the subexpression e2 the simplified expression is
     * e1 AND e3 AND e4.
     *
     * @param expr to simplify
     * @return simplified expression.
     */
    public static AbstractExpression eliminateDuplicates(Collection<AbstractExpression> exprList) {
        // Eliminate duplicates by building the map of expression's ids, values.
        Map<String, AbstractExpression> subExprMap = new HashMap<String, AbstractExpression>();
        for (AbstractExpression subExpr : exprList) {
            subExprMap.put(subExpr.m_id, subExpr);
        }
        // Now reconstruct the expression
        return ExpressionUtil.combinePredicates(subExprMap.values());
    }

    /**
     *  A condition is null-rejected for a given table in the following cases:
     *      If it is of the form A IS NOT NULL, where A is an attribute of any of the inner tables
     *      If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN
     *          when one of its arguments is NULL
     *      If it is a conjunction containing a null-rejected condition as a conjunct
     *      If it is a disjunction of null-rejected conditions
     *
     * @param expr
     * @param tableAlias
     * @return
     */
    public static boolean isNullRejectingExpression(AbstractExpression expr, String tableAlias) {
        ExpressionType exprType = expr.getExpressionType();
        if (exprType == ExpressionType.CONJUNCTION_AND) {
            assert(expr.m_left != null && expr.m_right != null);
            return isNullRejectingExpression(expr.m_left, tableAlias) || isNullRejectingExpression(expr.m_right, tableAlias);
        }
        if (exprType == ExpressionType.CONJUNCTION_OR) {
            assert(expr.m_left != null && expr.m_right != null);
            return isNullRejectingExpression(expr.m_left, tableAlias) && isNullRejectingExpression(expr.m_right, tableAlias);
        }
        if (exprType == ExpressionType.COMPARE_NOTDISTINCT) {
            return false;
        }
        if (exprType == ExpressionType.OPERATOR_NOT) {
            assert(expr.m_left != null);
            // "NOT ( P and Q )" is as null-rejecting as "NOT P or NOT Q"
            // "NOT ( P or Q )" is as null-rejecting as "NOT P and NOT Q"
            // Handling AND and OR expressions requires a "negated" flag to the recursion that tweaks
            // (switches?) the handling of ANDs and ORs to enforce the above equivalences.
            if (expr.m_left.getExpressionType() == ExpressionType.OPERATOR_IS_NULL) {
                return containsMatchingTVE(expr, tableAlias);
            }
            if (expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND ||
                    expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_OR) {
                assert(expr.m_left.m_left != null && expr.m_left.m_right != null);
                // Need to test for an existing child NOT and skip it.
                // e.g. NOT (P AND NOT Q) --> (NOT P) OR NOT NOT Q --> (NOT P) OR Q
                AbstractExpression tempLeft = null;
                if (expr.m_left.m_left.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                    tempLeft = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr.m_left.m_left, null);
                }
                else {
                    assert(expr.m_left.m_left.m_left != null);
                    tempLeft = expr.m_left.m_left.m_left;
                }
                AbstractExpression tempRight = null;
                if (expr.m_left.m_right.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                    tempRight = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr.m_left.m_right, null);
                }
                else {
                    assert(expr.m_left.m_right.m_left != null);
                    tempRight = expr.m_left.m_right.m_left;
                }
                ExpressionType type = (expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND) ?
                        ExpressionType.CONJUNCTION_OR : ExpressionType.CONJUNCTION_AND;
                AbstractExpression tempExpr = new OperatorExpression(type, tempLeft, tempRight);
                return isNullRejectingExpression(tempExpr, tableAlias);
            }
            if (expr.m_left.getExpressionType() == ExpressionType.OPERATOR_NOT) {
                // It's probably safe to assume that HSQL will have stripped out other double negatives,
                // (like "NOT T.c IS NOT NULL"). Yet, we could also handle them here
                assert(expr.m_left.m_left != null);
                return isNullRejectingExpression(expr.m_left.m_left, tableAlias);
            }
            return isNullRejectingExpression(expr.m_left, tableAlias);
        }
        if (exprType == ExpressionType.COMPARE_NOTDISTINCT) {
            // IS NOT DISTINCT FROM is not NULL rejecting,
            // particularly when applied to pairs of NULL values.
            //TODO: There are subcases that actually are NULL rejecting,
            // with various degrees of easy detectability here, namely...
            // ...IS NOT DISTINCT FROM <non-null-constant>
            // ...IS NOT DISTINCT FROM <non-nullable-column>
            // ...IS NOT DISTINCT FROM <most-expressions-built-of-these>
            // but for now, we are lazy in the planner and possibly slower
            // at runtime, keeping the joins outer and relying more on the
            // runtime filters.
            return false;
        }
        if (exprType == ExpressionType.OPERATOR_IS_NULL) {
            // IS NOT NULL is NULL rejecting -- IS NULL is not
            return false;
        }
        if (expr.hasAnySubexpressionOfClass(OperatorExpression.class)) {
            // COALESCE expression is a sub-expression
            // For example, COALESCE (C1, C2) > 0
            List<OperatorExpression> coalesceExprs = expr.findAllSubexpressionsOfClass(OperatorExpression.class);
            for (OperatorExpression coalesceExpr : coalesceExprs) {
                if ((coalesceExpr.getExpressionType() == ExpressionType.OPERATOR_ALTERNATIVE) &&
                    containsMatchingTVE(coalesceExpr, tableAlias)) {
                    // This table is part of the COALESCE expression - not NULL-rejecting
                    return false;
                }
            }
            // If we get there it means that the tableAlias is not part of any of COALESCE expression
            // still need to check the catch all case
        }
        // @TODO ENG_3038 Is it safe to assume for the rest of the expressions that if
        // it contains a TVE with the matching table name then it is NULL rejection expression?
        // Presently, yes, logical expressions are not expected to appear inside other
        // generalized expressions, so since the handling of other kinds of expressions
        // is pretty much "containsMatchingTVE", this fallback should be safe.
        // The only planned developments that might contradict this restriction (AFAIK --paul)
        // would be support for standard pseudo-functions that take logical condition arguments.
        // These should probably be supported as special non-functions/operations for a number
        // of reasons and may need special casing here.
        return containsMatchingTVE(expr, tableAlias);
    }

    /**
     *  Given two equal length lists of the expressions build a combined equivalence expression
     *  (le1, le2,..., leN) (re1, re2,..., reN) =>
     *  (le1=re1) AND (le2=re2) AND .... AND (leN=reN)
     *
     * @param leftExprs
     * @param rightExprs
     * @return AbstractExpression
     */
    public static AbstractExpression buildEquavalenceExpression(Collection<AbstractExpression> leftExprs, Collection<AbstractExpression> rightExprs) {
        assert(leftExprs.size() == rightExprs.size());
        Iterator<AbstractExpression> leftIt = leftExprs.iterator();
        Iterator<AbstractExpression> rightIt = rightExprs.iterator();
        AbstractExpression result = null;
        while (leftIt.hasNext() && rightIt.hasNext()) {
            AbstractExpression leftExpr = leftIt.next();
            AbstractExpression rightExpr = rightIt.next();
            AbstractExpression eqaulityExpr = new ComparisonExpression(ExpressionType.COMPARE_EQUAL, leftExpr, rightExpr);
            if (result == null) {
                result = eqaulityExpr;
            } else {
                result = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, result, eqaulityExpr);
            }
        }
        return result;
    }

    /**
     *  Return true/false whether an expression contains any aggregate expression
     *
     * @param expr
     * @return true is expression contains an aggregate subexpression
     */
    public static boolean containsAggregateExpression(AbstractExpression expr) {
        AbstractExpression.SubexprFinderPredicate pred =
                new AbstractExpression.SubexprFinderPredicate() {
            @Override
            public boolean matches(AbstractExpression expr) {
                return expr.getExpressionType().isAggregateExpression();
            }
        };
        return expr.hasAnySubexpressionWithPredicate(pred);
    }

    private static boolean containsMatchingTVE(AbstractExpression expr,
            String tableAlias) {
        assert(expr != null);
        List<TupleValueExpression> tves = getTupleValueExpressions(expr);
        for (TupleValueExpression tve : tves) {
            if (tve.matchesTableAlias(tableAlias)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Traverse this expression tree.  Where we find a SelectSubqueryExpression, wrap it
     * in a ScalarValueExpression if its parent is not one of:
     * - comparison (=, !=, <, etc)
     * - operator exists
     * @param expr   - the expression that may contain subqueries that need to be wrapped
     * @return the expression with subqueries wrapped where needed
     */
    public static AbstractExpression wrapScalarSubqueries(AbstractExpression expr) {
        return wrapScalarSubqueriesHelper(null, expr);
    }

    private static AbstractExpression wrapScalarSubqueriesHelper(AbstractExpression parentExpr, AbstractExpression expr) {

        // Bottom-up recursion.  Proceed to the children first.
        AbstractExpression leftChild = expr.getLeft();
        if (leftChild != null) {
            AbstractExpression newLeft = wrapScalarSubqueriesHelper(expr, leftChild);
            if (newLeft != leftChild) {
                expr.setLeft(newLeft);
            }
        }

        AbstractExpression rightChild = expr.getRight();
        if (rightChild != null) {
            AbstractExpression newRight = wrapScalarSubqueriesHelper(expr, rightChild);
            if (newRight != rightChild) {
                expr.setRight(newRight);
            }
        }

        // Let's not forget the args, which may also contain subqueries.
        List<AbstractExpression> args = expr.getArgs();
        if (args != null) {
            for (int i = 0; i < args.size(); ++i) {
                AbstractExpression arg = args.get(i);
                AbstractExpression newArg = wrapScalarSubqueriesHelper(expr, arg);
                if (newArg != arg) {
                    expr.setArgAtIndex(i, newArg);
                }
            }
        }

        if (expr instanceof SelectSubqueryExpression
                && subqueryRequiresScalarValueExpressionFromContext(parentExpr)) {
            expr = addScalarValueExpression((SelectSubqueryExpression)expr);
        }
        return expr;
    }

    /**
     * Return true if we must insert a ScalarValueExpression between a subquery
     * and its parent expression.
     * @param parentExpr  the parent expression of a subquery
     * @return true if the parent expression is not a comparison, EXISTS operator, or
     *   a scalar value expression
     */
    private static boolean subqueryRequiresScalarValueExpressionFromContext(AbstractExpression parentExpr) {
        if (parentExpr == null) {
            // No context: we are a top-level expression.  E.g, an item on the
            // select list.  In this case, assume the expression must be scalar.
            return true;
        }

        // Exists and comparison operators can handle non-scalar subqueries.
        if (parentExpr.getExpressionType() == ExpressionType.OPERATOR_EXISTS
                || parentExpr instanceof ComparisonExpression) {
            return false;
        }

        // There is already a ScalarValueExpression above the subquery.
        if (parentExpr instanceof ScalarValueExpression) {
            return false;
        }

        // By default, assume that the subquery must produce a single value.
        return true;
    }

    /**
     * Add a ScalarValueExpression on top of the SubqueryExpression
     * @param expr - subquery expression
     * @return ScalarValueExpression
     */
    private static AbstractExpression addScalarValueExpression(SelectSubqueryExpression expr) {
        if (expr.getSubqueryScan().getOutputSchema().size() != 1) {
            throw new PlanningErrorException("Scalar subquery can have only one output column");
        }

        expr.changeToScalarExprType();

        AbstractExpression scalarExpr = new ScalarValueExpression();
        scalarExpr.setLeft(expr);
        scalarExpr.setValueType(expr.getValueType());
        scalarExpr.setValueSize(expr.getValueSize());
        return scalarExpr;
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
                } else {
                    return expr.getLeft();
                }
            }
            if (ExpressionType.VALUE_CONSTANT == expr.getRight().getExpressionType()) {
                if (ConstantValueExpression.isBooleanTrue(expr.getRight())) {
                    return expr.getLeft();
                } else {
                    return expr.getRight();
                }
            }
        } else if (ExpressionType.CONJUNCTION_OR == expr.getExpressionType()) {
            if (ExpressionType.VALUE_CONSTANT == expr.getLeft().getExpressionType()) {
                if (ConstantValueExpression.isBooleanTrue(expr.getLeft())) {
                    return expr.getLeft();
                } else {
                    return expr.getRight();
                }
            }
            if (ExpressionType.VALUE_CONSTANT == expr.getRight().getExpressionType()) {
                if (ConstantValueExpression.isBooleanTrue(expr.getRight())) {
                    return expr.getRight();
                } else {
                    return expr.getLeft();
                }
            }
        } else if (ExpressionType.OPERATOR_NOT == expr.getExpressionType()) {
            AbstractExpression leftExpr = expr.getLeft();
            // function expressions can also return boolean. So the left child expression
            // can be expression which are not constant value expressions, so don't
            // evaluate every left child expr as constant value expression
            if ((VoltType.BOOLEAN == leftExpr.getValueType()) &&
                    (leftExpr instanceof ConstantValueExpression)) {
                if (ConstantValueExpression.isBooleanTrue(leftExpr)) {
                    return ConstantValueExpression.getFalse();
                } else {
                    return ConstantValueExpression.getTrue();
                }
            } else if (ExpressionType.OPERATOR_NOT == leftExpr.getExpressionType()) {
                return leftExpr.getLeft();
            } else if (ExpressionType.CONJUNCTION_OR == leftExpr.getExpressionType()) {
                // NOT (.. OR .. OR ..) => NOT(..) AND NOT(..) AND NOT(..)
                AbstractExpression l = new OperatorExpression(ExpressionType.OPERATOR_NOT, leftExpr.getLeft(), null);
                AbstractExpression r = new OperatorExpression(ExpressionType.OPERATOR_NOT, leftExpr.getRight(), null);
                leftExpr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, l, r);
                return evaluateExpression(leftExpr);
            }
            // NOT (expr1 AND expr2) => (NOT expr1) || (NOT expr2)
            // The above case is probably not interesting to do for short circuit purpose
        }
        return expr;
    }
}
