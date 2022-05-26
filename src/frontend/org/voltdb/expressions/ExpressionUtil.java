/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.VoltXMLElementHelper;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QuantifierType;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Lists;

/**
 *
 */
public final class ExpressionUtil {

    private static final Map<String, ExpressionType> mapOfVoltXMLOpType = ImmutableMap.<String, ExpressionType>builder()
           // Conjunctions
           .put("or", ExpressionType.CONJUNCTION_OR)
           .put("and", ExpressionType.CONJUNCTION_AND)

           // Compares
           .put("equal", ExpressionType.COMPARE_EQUAL)
           .put("notequal", ExpressionType.COMPARE_NOTEQUAL)
           .put("lessthan", ExpressionType.COMPARE_LESSTHAN)
           .put("greaterthan", ExpressionType.COMPARE_GREATERTHAN)
           .put("lessthanorequalto", ExpressionType.COMPARE_LESSTHANOREQUALTO)
           .put("greaterthanorequalto", ExpressionType.COMPARE_GREATERTHANOREQUALTO)
           .put("like", ExpressionType.COMPARE_LIKE)
           .put("startswith", ExpressionType.COMPARE_STARTSWITH)
           .put("in", ExpressionType.COMPARE_IN)
           .put("notdistinct", ExpressionType.COMPARE_NOTDISTINCT)

           // Operators
           .put("add", ExpressionType.OPERATOR_PLUS)
           .put("subtract", ExpressionType.OPERATOR_MINUS)
           .put("multiply", ExpressionType.OPERATOR_MULTIPLY)
           .put("divide", ExpressionType.OPERATOR_DIVIDE)
           .put("concat", ExpressionType.OPERATOR_CONCAT)
           .put("mod", ExpressionType.OPERATOR_MOD)
           .put("cast", ExpressionType.OPERATOR_CAST)
           .put("not", ExpressionType.OPERATOR_NOT)
           .put("is_null", ExpressionType.OPERATOR_IS_NULL)
           .put("exists", ExpressionType.OPERATOR_EXISTS)
           .put("negate", ExpressionType.OPERATOR_UNARY_MINUS).build();

    private ExpressionUtil() {}

    /**
     * Helper to check if a VoltXMLElement contains parameter.
     * @param elm element under inspection
     * @return if elm contains parameter
     */
    private static boolean isParameterized(VoltXMLElement elm) {
        final String name = elm.name;
        if (name.equals("value")) {
            return elm.getBoolAttribute("isparam", false);
        } else if (name.equals("vector") || name.equals("row")) {
            return elm.children.stream().anyMatch(ExpressionUtil::isParameterized);
        } else if (name.equals("columnref") || name.equals("function") ||
                name.equals("tablesubquery")) {
            return false;
        } else {
            assert name.equals("operation") : "unknown VoltXMLElement type: " + name;
            final ExpressionType op = mapOfVoltXMLOpType.get(elm.attributes.get("optype"));
            assert op != null : "No operation of type: " + elm.attributes.get("optype");
            switch (op) {
                case CONJUNCTION_OR:                    // two operators
                case CONJUNCTION_AND:
                case COMPARE_GREATERTHAN:
                case COMPARE_LESSTHAN:
                case COMPARE_EQUAL:
                case COMPARE_NOTEQUAL:
                case COMPARE_NOTDISTINCT:
                case COMPARE_GREATERTHANOREQUALTO:
                case COMPARE_LESSTHANOREQUALTO:
                case COMPARE_STARTSWITH:
                case OPERATOR_PLUS:
                case OPERATOR_MINUS:
                case OPERATOR_MULTIPLY:
                case OPERATOR_DIVIDE:
                case OPERATOR_CONCAT:
                case OPERATOR_MOD:
                case COMPARE_IN:
                case COMPARE_LIKE:
                    return isParameterized(elm.children.get(0)) || isParameterized(elm.children.get(1));
                case OPERATOR_IS_NULL:      // one operator
                case OPERATOR_EXISTS:
                case OPERATOR_NOT:
                case OPERATOR_UNARY_MINUS:
                case OPERATOR_CAST:
                    return isParameterized(elm.children.get(0));
                default:
                    assert false : op;
                    return false;
            }
        }
    }

    /**
     * Get the underlying type of the VoltXMLElement node. Need reference to the catalog for PVE
     * @param db catalog
     * @param elm element under inspection
     * @return string representation of the element node
     */
    private static String getType(Database db, VoltXMLElement elm) {
        final String type = elm.getStringAttribute("valuetype", "");
        if (! type.isEmpty()) {
            return type;
        } else if (elm.name.equals("columnref")) {
            final String tblName = elm.getStringAttribute("table", "");
            final int colIndex = elm.getIntAttribute("index", 0);
            return StreamSupport.stream(db.getTables().spliterator(), false)
                    .filter(tbl -> tbl.getTypeName().equals(tblName))
                    .findAny()
                    .flatMap(tbl ->
                            StreamSupport.stream(tbl.getColumns().spliterator(), false)
                                    .filter(col -> col.getIndex() == colIndex)
                                    .findAny())
                    .map(Column::getType)
                    .map(typ -> VoltType.get((byte) ((int)typ)).getName())
                    .orElse("");
        } else {
            return "";
        }
    }

    /**
     * Guess from a parent node what are the parameter type of its child node, should one of its child node
     * contain parameter.
     * @param db catalog
     * @param elm node under inspection
     * @return string representation of the type of its child node.
     */
    private static String guessParameterType(Database db, VoltXMLElement elm) {
        if (! isParameterized(elm) || ! elm.name.equals("operation")) {
            return "";
        } else {
            final ExpressionType op = mapOfVoltXMLOpType.get(elm.attributes.get("optype"));
            assert op != null;
            switch (op) {
                case CONJUNCTION_OR:
                case CONJUNCTION_AND:
                case OPERATOR_NOT:
                    return "boolean";
                case COMPARE_GREATERTHAN: // For these 2 operator-ops, the type is what the non-parameterized part gets set to.
                case COMPARE_LESSTHAN:
                case COMPARE_EQUAL:
                case COMPARE_NOTEQUAL:
                case COMPARE_GREATERTHANOREQUALTO:
                case COMPARE_LESSTHANOREQUALTO:
                case OPERATOR_PLUS:
                case OPERATOR_MINUS:
                case OPERATOR_MULTIPLY:
                case OPERATOR_DIVIDE:
                case OPERATOR_CONCAT:
                case OPERATOR_MOD:
                case COMPARE_IN:
                case COMPARE_STARTSWITH:
                    final VoltXMLElement left = elm.children.get(0), right = elm.children.get(1);
                    return isParameterized(left) ? getType(db, right) : getType(db, left);
                case OPERATOR_UNARY_MINUS:
                    return "integer";
                case OPERATOR_IS_NULL:
                case OPERATOR_EXISTS:
                    return "";
                default:
                    assert false : op;
                    return "";
            }
        }
    }

    /**
     * Conversion from a VoltXMLElement node to abstract expression, done outside HSQL.
     * We need this overhaul, because
     * 1. We need to inspect the predicate of "MIGRATE FROM tbl WHERE ..." query for certain properties
     * 2. We are stuck with HSQL.
     *
     * @param db catalog
     * @param elm element node under inspection
     * @return converted expression
     */
    public static AbstractExpression from(Database db, VoltXMLElement elm) {
        return from(db, elm, "");
    }

    private static AbstractExpression from(Database db, VoltXMLElement elm, String typeHint) {
        if (elm == null) {
            return null;
        } else {
            switch (elm.name) {
                case "columnref":
                    final String tblName = elm.getStringAttribute("table", ""),
                            colName = elm.getStringAttribute("column", "");
                    final int colIndex = elm.getIntAttribute("index", 0);
                    assert !tblName.isEmpty();
                    assert !colName.isEmpty();
                    return new TupleValueExpression(tblName, colName, colIndex);
                case "value":
                    // add support for dyanmic parameter
                    if (elm.getStringAttribute("isparam", "").equals("true")) {
                        return new ParameterValueExpression();
                    } else {
                        final ConstantValueExpression expr = new ConstantValueExpression();
                        expr.setValue(elm.getStringAttribute("value", ""));
                        expr.setValueType(VoltType.typeFromString(elm.getStringAttribute("valuetype", typeHint)));
                        return expr;
                    }
                case "vector": {
                    final VectorValueExpression expr = new VectorValueExpression();
                    expr.setArgs(elm.children.stream().map(elem -> from(db, elem, typeHint)).collect(Collectors.toList()));
                    return expr;
                }
                case "row":
                    return from(db, VoltXMLElementHelper.getFirstChild(elm, "columnref"), typeHint);
                case "function": {
                    final FunctionExpression expr = new FunctionExpression();
                    expr.setAttributes(elm.getStringAttribute("name", ""),
                            elm.getStringAttribute("argument", null), elm.getStringAttribute("optionalArgument", null),
                            elm.getIntAttribute("id", 0));
                    expr.setArgs(elm.children.stream().map(elem -> from(db, elem, typeHint)).collect(Collectors.toList()));
                    expr.setValueType(VoltType.typeFromString(elm.getStringAttribute("valuetype", "")));
                    return expr;
                }
                case "tablesubquery": // e.g. where X in (SELECT ...)
                    // TODO: do not support parsing more complex queries
                    throw new PlanningErrorException("Expression is too complicated");
                case "operation":
                    final ExpressionType op = mapOfVoltXMLOpType.get(elm.attributes.get("optype"));
                    assert op != null;
                    final String hint = guessParameterType(db, elm);
                    switch (op) {
                        case CONJUNCTION_OR:
                        case CONJUNCTION_AND:
                            return new ConjunctionExpression(op,
                                    from(db, elm.children.get(0), hint),
                                    from(db, elm.children.get(1), hint));
                        case COMPARE_GREATERTHAN:
                        case COMPARE_LESSTHAN:
                        case COMPARE_EQUAL:
                        case COMPARE_NOTEQUAL:
                        case COMPARE_NOTDISTINCT:
                        case COMPARE_GREATERTHANOREQUALTO:
                        case COMPARE_LESSTHANOREQUALTO:
                        case COMPARE_LIKE:
                        case COMPARE_STARTSWITH: {
                            final ComparisonExpression expr = new ComparisonExpression(op,
                                    from(db, elm.children.get(0), hint),
                                    from(db, elm.children.get(1), hint));
                            expr.setQuantifier(QuantifierType.get(elm.getStringAttribute("opsubtype", "none")));
                            return expr;
                        }
                        case OPERATOR_PLUS:
                        case OPERATOR_MINUS:
                        case OPERATOR_MULTIPLY:
                        case OPERATOR_DIVIDE:
                        case OPERATOR_CONCAT:
                        case OPERATOR_MOD:
                            return new OperatorExpression(op,
                                    from(db, elm.children.get(0), hint), from(db, elm.children.get(1), hint));
                        case OPERATOR_IS_NULL:
                        case OPERATOR_EXISTS:
                        case OPERATOR_NOT:
                        case OPERATOR_UNARY_MINUS:
                            return new OperatorExpression(op, from(db, elm.children.get(0), hint), null);
                        case COMPARE_IN: {
                            final InComparisonExpression expr = new InComparisonExpression();
                            expr.setLeft(from(db, elm.children.get(0), hint));
                            expr.setRight(from(db, elm.children.get(1), hint));
                            return expr;
                        }
                        default:
                            assert false : op;
                    }
                default:
                    assert false;
            }
            return null;
        }
    }

    private static boolean containsTerminalParentPairs(
            AbstractExpression expr, AbstractExpression parent,
            Predicate<Pair<AbstractExpression, AbstractExpression>> predicate) {
        if (expr != null) {
            // if contains in left/right node
            if (containsTerminalParentPairs(expr.getLeft(), expr, predicate) ||
                    containsTerminalParentPairs(expr.getRight(), expr, predicate)) {
                return true;
            } else if (expr.getArgs() != null && expr.getArgs().size() > 0) {
                // if contains in arguments
                return expr.getArgs().stream().anyMatch(e -> containsTerminalParentPairs(e, expr, predicate));
            } else if (expr.getLeft() == null && expr.getRight() == null) { // check leaf node matches
                return predicate.test(Pair.of(expr, parent));
            }
        }
        return false;
    }

    /**
     * Recursively check if any (terminal, terminal's parent) expression pair in the given expression tree
     * satisfies given predicate.
     * @param expr      source expression tree
     * @param predicate predicate to check against (terminal, terminal's parent) expression pairs.
     * @return true if there exists a (terminal, terminal's parent)  expression pair that satisfies the predicate
     */
    public static boolean containsTerminalParentPairs(
            AbstractExpression expr, Predicate<Pair<AbstractExpression, AbstractExpression>> predicate) {
        return containsTerminalParentPairs(expr, null, predicate);
    }

    /**
     * Check if any node of given expression tree satisfies given predicate
     * @param expr source expression tree
     * @param pred predicate to check against any expression node to find if any node satisfies.
     *             Note that it should be able to handle null.
     * @return true if there exists a node that satisfies the predicate
     */
    public static boolean reduce(AbstractExpression expr, Predicate<AbstractExpression> pred) {
        final boolean current = pred.test(expr);
        if (current) {
            return true;
        } else if (expr == null) {
            return pred.test(null);
        } else {
            return pred.test(expr.getLeft()) || pred.test(expr.getRight()) ||
                    expr.getArgs() != null && expr.getArgs().stream().anyMatch(pred);
        }

    }

    public static void finalizeValueTypes(AbstractExpression exp) {
        exp.normalizeOperandTypes_recurse();
        exp.finalizeValueTypes();
    }

    @SafeVarargs
    public static AbstractExpression cloneAndCombinePredicates(Collection<AbstractExpression>... colExps) {
        Stack<AbstractExpression> stack = new Stack<>();
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
        } else {
            return combineStack(ExpressionType.CONJUNCTION_AND, stack);
        }
    }

    /**
     *
     * @param colExps
     */
    @SafeVarargs
    public static AbstractExpression combinePredicates(ExpressionType type, Collection<AbstractExpression>... colExps) {
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        for (Collection<AbstractExpression> exps : colExps) {
            if (exps != null) {
                stack.addAll(exps);
            }
        }
        if (stack.isEmpty()) {
            return null;
        } else {
            return combineStack(type, stack);
        }
    }

    private static AbstractExpression combineStack(ExpressionType type, Stack<AbstractExpression> stack) {
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
                ret = new ConjunctionExpression(type);
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
    public static List<AbstractExpression> uncombinePredicate(AbstractExpression expr) {
        if (expr == null) {
            return Lists.newArrayList();
        } else if (expr instanceof ConjunctionExpression) {
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
        return Lists.newArrayList(expr);
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
    public static Collection<AbstractExpression> uncombineAny(AbstractExpression expr) {
        Deque<AbstractExpression> out = new ArrayDeque<>();
        if (expr != null) {
            Deque<AbstractExpression> in = new ArrayDeque<>();
            // this chunk of code breaks the code into a list of expression that
            // all have to be true for the where clause to be true
            in.add(expr);
            AbstractExpression inExpr;
            while ((inExpr = in.poll()) != null) {
                if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                    in.add(inExpr.getLeft());
                    in.add(inExpr.getRight());
                } else {
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
    public static void collectPartitioningFilters(
            Collection<AbstractExpression> filterList,
            Map<AbstractExpression, Set<AbstractExpression>> equivalenceSet) {
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
                    eqSet1 = new HashSet<>();
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
    public static List<TupleValueExpression> getTupleValueExpressions(AbstractExpression input) {
        ArrayList<TupleValueExpression> tves = new ArrayList<>();
        // recursive stopping steps
        if (input == null) {
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
        return ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, subExprMap.values());
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
        switch (exprType) {
            case CONJUNCTION_AND:
                assert expr.m_left != null && expr.m_right != null;
                return isNullRejectingExpression(expr.m_left, tableAlias) ||
                        isNullRejectingExpression(expr.m_right, tableAlias);
            case CONJUNCTION_OR:
                assert expr.m_left != null && expr.m_right != null;
                return isNullRejectingExpression(expr.m_left, tableAlias) &&
                        isNullRejectingExpression(expr.m_right, tableAlias);
            case COMPARE_NOTDISTINCT:
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
            case OPERATOR_IS_NULL:
                // IS NOT NULL is NULL rejecting -- IS NULL is not
                return false;
            case OPERATOR_NOT:
                assert(expr.m_left != null);
                // "NOT ( P and Q )" is as null-rejecting as "NOT P or NOT Q"
                // "NOT ( P or Q )" is as null-rejecting as "NOT P and NOT Q"
                // Handling AND and OR expressions requires a "negated" flag to the recursion that tweaks
                // (switches?) the handling of ANDs and ORs to enforce the above equivalences.
                if (expr.m_left.getExpressionType() == ExpressionType.OPERATOR_IS_NULL) {
                    return containsMatchingTVE(expr, tableAlias);
                } else if (expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND ||
                        expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_OR) {
                    assert(expr.m_left.m_left != null && expr.m_left.m_right != null);
                    // Need to test for an existing child NOT and skip it.
                    // e.g. NOT (P AND NOT Q) --> (NOT P) OR NOT NOT Q --> (NOT P) OR Q
                    AbstractExpression tempLeft = null;
                    if (expr.m_left.m_left.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                        tempLeft = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr.m_left.m_left, null);
                    } else {
                        assert expr.m_left.m_left.m_left != null;
                        tempLeft = expr.m_left.m_left.m_left;
                    }
                    final AbstractExpression tempRight;
                    if (expr.m_left.m_right.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                        tempRight = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr.m_left.m_right, null);
                    } else {
                        assert(expr.m_left.m_right.m_left != null);
                        tempRight = expr.m_left.m_right.m_left;
                    }
                    ExpressionType type = expr.m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND ?
                            ExpressionType.CONJUNCTION_OR : ExpressionType.CONJUNCTION_AND;
                    AbstractExpression tempExpr = new OperatorExpression(type, tempLeft, tempRight);
                    return isNullRejectingExpression(tempExpr, tableAlias);
                } else if (expr.m_left.getExpressionType() == ExpressionType.OPERATOR_NOT) {
                    // It's probably safe to assume that HSQL will have stripped out other double negatives,
                    // (like "NOT T.c IS NOT NULL"). Yet, we could also handle them here
                    assert expr.m_left.m_left != null;
                    return isNullRejectingExpression(expr.m_left.m_left, tableAlias);
                } else {
                    return isNullRejectingExpression(expr.m_left, tableAlias);
                }
            default:
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
    }

    /**
     *  Return true/false whether an expression contains any aggregate expression
     *
     * @param expr
     * @return true is expression contains an aggregate subexpression
     */
    public static boolean containsAggregateExpression(AbstractExpression expr) {
        AbstractExpression.SubexprFinderPredicate pred = expression ->
                expression.getExpressionType().isAggregateExpression();
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
        } else if (parentExpr.getExpressionType() == ExpressionType.OPERATOR_EXISTS
                || parentExpr instanceof ComparisonExpression) {
            // Exists and comparison operators can handle non-scalar subqueries.
            return false;
        } else {
            // There is already a ScalarValueExpression above the subquery.
            return !(parentExpr instanceof ScalarValueExpression);
            // By default, assume that the subquery must produce a single value.
        }
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

        final AbstractExpression scalarExpr = new ScalarValueExpression();
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
            } else if (ExpressionType.VALUE_CONSTANT == expr.getRight().getExpressionType()) {
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
            } else if (ExpressionType.VALUE_CONSTANT == expr.getRight().getExpressionType()) {
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
