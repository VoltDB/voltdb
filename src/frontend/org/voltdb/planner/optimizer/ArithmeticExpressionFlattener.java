package org.voltdb.planner.optimizer;

import org.voltcore.utils.Pair;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.types.ExpressionType;
import static org.voltdb.planner.optimizer.NormalizerUtil.ArithOpType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Flattens arithmetic expression by grouping +/- together, * / together, into layered multi-way tree structure.
 * e.g. (a + b - c) * (a + c - d) / c - (a + c) / (a + d) * b ==>
 * bottom layer: [ a + b - c, a + c - d, c] and [a + c, a + d, b]
 */
final class ArithmeticExpressionFlattener {
    private final ArithOpType m_op;
    // a + b - c ==> "+", a; "+", b; "-", c
    private final List<Pair<ExpressionType, ArithmeticExpressionFlattener>> m_children;
    /**
     * m_leaf is set only if current level is Atom.
     * m_expr is set to be the source expression.
     */
    private final AbstractExpression m_leaf, m_expr;

    /**
     * API constructor.
     * Build up the tree from given expression and arithmetic operation type of interest.
     * @param e source expression
     * @param on PlusMinus / MultDiv. Client could set it to anything but Atom.
     */
    ArithmeticExpressionFlattener(AbstractExpression e, ArithOpType on) {
        m_op = on;
        m_expr = e;
        if (ArithOpType.Atom == m_op) {
            m_children = new ArrayList<>();
            m_leaf = e;
        } else {
             m_children = collectWhile(e, m_op, new ArrayList<>());
             m_leaf = null;
        }
    }

    /**
     * Special-purpose constructor only meant for internal use.
     * all elm in the list treated as bottom-est, i.e. had been processed.
     * @param bottom processed bottom layer of the tree.
     */
    private ArithmeticExpressionFlattener(List<Pair<ExpressionType, ArithmeticExpressionFlattener>> bottom) {
        assert(!bottom.isEmpty());
        m_children = bottom;
        m_expr = bottom.stream().map(p -> Pair.of(p.getFirst(), p.getSecond().m_expr))
                .reduce((a, b) -> Pair.of(a.getFirst(),
                        new OperatorExpression(b.getFirst(), a.getSecond(), b.getSecond())))
                .get().getSecond();
        m_leaf = null;
        m_op = getAlternativeOp(ArithOpType.get(bottom.get(0).getFirst()));
    }

    /**
     * Special-purpose constructor only meant for internal use.
     * Treat it as an atomic leaf node.
     * @param e expression as atomic leaf node.
     */
    private ArithmeticExpressionFlattener(AbstractExpression e) {   // no further layering needed
        m_op = ArithOpType.Atom;
        m_expr = m_leaf = e;
        m_children = new ArrayList<>();
    }
    private List<Pair<ExpressionType, ArithmeticExpressionFlattener>> getChildren() {
        return m_children;
    }

    private AbstractExpression getLeaf() {
        return m_leaf;
    }
    private ArithOpType getOp() {
        return m_op;
    }

    /**
     * Get original, un-structured expression representation.
     * @return original expression
     */
    AbstractExpression get() {
        return m_expr;
    }

    /**
     * Apply an operation on each layer of operation bottom-up to build new expression
     * @param obj source expression to transform
     * @param postCombFn transformation on the bottom layer <em>post</em>-combination
     * @return transformed expression
     */
    private static ArithmeticExpressionFlattener applyInner(
            ArithmeticExpressionFlattener obj,
            BiFunction<ArithOpType, AbstractExpression, AbstractExpression> postCombFn) {
        if (obj.getLeaf() != null) {    // leaf node
            return new ArithmeticExpressionFlattener(postCombFn.apply(obj.getOp(), obj.getLeaf()));
        } else if (obj.getChildren().stream().allMatch(p -> p.getSecond().getOp() == ArithOpType.Atom)) {   // all children are leaf nodes:
            assert(!obj.getChildren().isEmpty());       // on the bottom layer: combine && apply transformation
            return new ArithmeticExpressionFlattener(postCombFn.apply(
                    obj.getOp(),
                    obj.getChildren().stream()          // For each (leaf) child, apply function on it, then combine all children into a new arithmetic expression,
                            .map(p -> Pair.of(p.getFirst(), postCombFn.apply(ArithOpType.Atom, p.getSecond().getLeaf())))   // apply on the combined one, and
                            .reduce((a, b) -> Pair.of(a.getFirst(),                                    // create a new flattener out of it.
                                    new OperatorExpression(b.getFirst(), a.getSecond(), b.getSecond())))
                            .get().getSecond()));
        } else {            // has both leaf and non-leaf children: recurse for each child, combine into a new flattener, and then
            assert(!obj.getChildren().isEmpty());       // recurse on the flattener.
            return applyInner(new ArithmeticExpressionFlattener(obj.getChildren().stream()
                    .map(o -> Pair.of(o.getFirst(), applyInner(o.getSecond(), postCombFn)))
                    .collect(Collectors.toList())), postCombFn);
        }
    }

    /**
     * API for transforming in bottom-up fashion of all nodes.
     * @param obj source flattener to transform
     * @param fn transformation function for an arithmetic (or atomic) expression
     * @return transformed expression
     */
    static AbstractExpression apply(ArithmeticExpressionFlattener obj,
                                    BiFunction<ArithOpType, AbstractExpression, AbstractExpression> fn) {
        // In addition to apply on each node bottom up, process it both ways (PlusMinus, MultDiv) on the result.
        return fn.apply(ArithOpType.MultDiv, fn.apply(ArithOpType.PlusMinus, applyInner(obj, fn).get()));
    }

    private static List<Pair<ExpressionType, ArithmeticExpressionFlattener>> collectWhile(
            AbstractExpression e, ArithOpType op, List<Pair<ExpressionType, ArithmeticExpressionFlattener>> acc) {
        assert(e != null);
        if (ArithOpType.get(e.getExpressionType()) != op) {
            acc.add(Pair.of(getPositiveOp(op), new ArithmeticExpressionFlattener(e,
                    getAlternativeOp(op) == ArithOpType.get(e.getExpressionType()) ?
                            getAlternativeOp(op) : ArithOpType.Atom)));
            return acc;
        } else {
            acc.addAll(collectWhile(e.getLeft(), op, new ArrayList<>()));
            final List<Pair<ExpressionType, ArithmeticExpressionFlattener>> right =
                    collectWhile(e.getRight(), op, new ArrayList<>());
            acc.addAll(e.getExpressionType() == getPositiveOp(op) ? right :
                    right.stream().map(p -> Pair.of(negate(p.getFirst()), p.getSecond()))
                            .collect(Collectors.toList()));        // negate all sub-expressions of right expression if needed
            return acc;
        }
    }
    private static ExpressionType getPositiveOp(ArithOpType op) {    // + for PlusMinus; * for MultDiv
        switch (op) {
            case Atom:
                return ExpressionType.INVALID;
            case PlusMinus:
                return ExpressionType.OPERATOR_PLUS;
            case MultDiv:
            default:
                return ExpressionType.OPERATOR_MULTIPLY;
        }
    }
    private static ArithOpType getAlternativeOp(ArithOpType op) {    // + for PlusMinus; * for MultDiv
        switch (op) {
            case Atom:
                assert(false);
                return ArithOpType.Atom;
            case PlusMinus:
                return ArithOpType.MultDiv;
            case MultDiv:
            default:
                return ArithOpType.PlusMinus;
        }
    }
    private static ExpressionType negate(ExpressionType op) {
        switch (op) {
            case OPERATOR_PLUS:
                return ExpressionType.OPERATOR_MINUS;
            case OPERATOR_MINUS:
                return ExpressionType.OPERATOR_PLUS;
            case OPERATOR_MULTIPLY:
                return ExpressionType.OPERATOR_DIVIDE;
            case OPERATOR_DIVIDE:
                return ExpressionType.OPERATOR_MULTIPLY;
            default:
                assert(false);
                return ExpressionType.INVALID;
        }
    }
}
