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
import static org.voltdb.planner.optimizer.NormalizerUtil.ConjunctionRelation;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Compacts a set tree to make it flatter and shorter on logic structures, i.e. AND's (or OR's) chain of binary tree is combined into a
 * multi-tree. After compaction, each level of non-leaf node must be AND-OR-AND alternatively.
 */
final class LogicExpressionFlattener implements Comparator<LogicExpressionFlattener> {
    private final AbstractExpression m_leaf;
    private final ConjunctionRelation m_rel;
    /**
     * Collection of leaf or non-leaf nodes. Leaf-ness is indicated by whether m_rel field is ATOM.
     */
    private final Set<LogicExpressionFlattener> m_children;

    /**
     * Maintains ordering between LogicExpressionFlattener's. The order is maintained in the following way:
     * 1. Check and order conjunction relation first: ATOM < AND < OR. When equal,
     * 2. When both are ATOMs, compare between expressions themselves. When equal (or neither is ATOM),
     * 3. When neither are ATOMs, compare the multi-way children of the two as two lists, in the following way:
     *    3.1 Compare number of children first, fewer means less;
     *    3.2 Recursively compare each child pair in order, until found first unequal pair.
     * @param lhs First LogicExpressionFlattener
     * @param rhs Second LogicExpressionFlattener
     * @return the relation in between
     */
    @Override
    public int compare(LogicExpressionFlattener lhs, LogicExpressionFlattener rhs) {
        final Comparator<List<LogicExpressionFlattener>> childrenComparator = (l, r) -> {    // way to compare 2 ordered sets: size first, then one by one
            if (l.size() != r.size()) {
                return Integer.compare(l.size(), r.size());
            } else {
                return IntStream.range(0, l.size())
                        .map(index -> l.get(index).compare(l.get(index), r.get(index)))
                        .reduce(0, (acc, cur) -> acc == 0 ? cur : acc);
            }
        };
        return Comparator
                .comparing(LogicExpressionFlattener::getRelation,
                        Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparing(LogicExpressionFlattener::getLeaf,
                        Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparing(c -> new ArrayList<>(c.getChildren()),
                        Comparator.nullsFirst(childrenComparator))
                .compare(lhs, rhs);
    }

    /**
     * Helper constructor used in simplification process. Does not further evaluates/builds the tree in construction.
     * @param rel logic relation (AND/OR) for the layer. Cannot be ATOM.
     * @param children children nodes. Set as is and unevaluated.
     */
    private LogicExpressionFlattener(ConjunctionRelation rel, Set<LogicExpressionFlattener> children) {
        assert(ConjunctionRelation.ATOM != rel);
        m_rel = rel;
        m_leaf = null;
        assert(!children.isEmpty());
        m_children = children;
    }

    /**
     * Helper constructor used by API constructor. Evaluates the nested conjunctions to the last ATOM node.
     * In the construction process, ordering between children is maintained for normalization purposes.
     * @param s SetSplitter ADT used to split into multiple AND-OR-AND layers
     */
    private LogicExpressionFlattener(SetSplitter s) {
        m_leaf = s.getNode();
        m_rel = s.getRelation();
        m_children = collectWhile(s, m_rel, new TreeSet<>(this));
        logicShortCut();
    }

    /**
     * Collects all leaf nodes from a SetSplitter, and dedup.
     * @param s SetSplitter ADT to collect
     * @param r Conjunction relation interested
     * @param acc collected nodes thus far
     * @return collected nodes.
     */
    private static Set<LogicExpressionFlattener> collectWhile(SetSplitter s, ConjunctionRelation r, Set<LogicExpressionFlattener> acc) {
        if (ConjunctionRelation.ATOM == r) {
            return acc;
        } else if(s.isLeaf() || s.getRelation() != r) {
            acc.add(new LogicExpressionFlattener(s));
            return acc;
        } else {
            return collectWhile(s.getLeft(), r, collectWhile(s.getRight(), r, acc));
        }
    }

    /**
     * Eliminates all true/false constants, by checking constant booleans in all leaves (non-conjunction expressions).
     * When shortcut occurs, resets children as a single evaluated constant boolean; when it did not occur,
     * eliminate all constant boolean expressions in the children.
     */
    private void logicShortCut() {
        switch (getRelation()) {        // Search in leaves for any falsehood in AND conjunction or truth in OR conjunction. Early return.
            case ATOM:
                return;
            case AND:
                if (collectLeaves().contains(ConstantValueExpression.getFalse())) {
                    getChildren().clear();
                    getChildren().add(new LogicExpressionFlattener(ConstantValueExpression.getFalse()));
                    return;
                }
                break;
            case OR:
            default:
                if (collectLeaves().contains(ConstantValueExpression.getTrue())) {
                    getChildren().clear();
                    getChildren().add(new LogicExpressionFlattener(ConstantValueExpression.getTrue()));
                    return;
                }
        }
        // filter out any boolean constant in leaves, and reset children.
        final List<LogicExpressionFlattener> nonLeaves = collectNonLeaves(),
                leaves = collectLeaves().stream().filter(e -> ! (e.equals(ConstantValueExpression.getFalse())) &&
                        ! (e.equals(ConstantValueExpression.getTrue())))
                        .map(LogicExpressionFlattener::new).collect(Collectors.toList());
        getChildren().clear();
        getChildren().addAll(nonLeaves);
        getChildren().addAll(leaves);
    }

    /**
     * API constructor to build a LogicExpressionFlattener.
     * @param e expression to build upon
     */
    LogicExpressionFlattener(AbstractExpression e) {
        this(new SetSplitter(e));
    }

    /**
     * Convert current flattener into an expression. For terminal node, return as is; otherwise
     * combine all nodes with the flattener's conjunction relation into a leftest tree.
     * @return converted expression.
     */
    AbstractExpression toExpression() {
        if (getLeaf() != null) {
            return getLeaf();
        } else {
            assert(!getChildren().isEmpty());
            return getChildren().stream().map(LogicExpressionFlattener::toExpression)
                    .reduce((a, b) -> new ConjunctionExpression(ConjunctionRelation.conjOf(getRelation()), a, b)).get();
        }
    }
    private Set<LogicExpressionFlattener> getChildren() {
        return m_children;
    }
    ConjunctionRelation getRelation() {
        return m_rel;
    }
    // not null when no further AND/OR relation is involved.
    AbstractExpression getLeaf() {
        return m_leaf;
    }

    /**
     * Collect all terminal leaves of current node, or itself if it is a leaf node.
     * @return all leaf nodes
     */
    List<AbstractExpression> collectLeaves() {
        switch (getRelation()) {
            case ATOM:
                return new ArrayList<AbstractExpression>(){{add(getLeaf());}};
            default:
                return getChildren().stream()
                        .filter(n -> n.getRelation().equals(ConjunctionRelation.ATOM))
                        .map(LogicExpressionFlattener::getLeaf)
                        .collect(Collectors.toList());
        }
    }

    /**
     * Collect all non-leaf children, i.e. conjunctions of alternative relation (for AND-ed node, the next layer is OR-ed nodes, etc.).
     * @return non-leaf children
     */
    List<LogicExpressionFlattener> collectNonLeaves() {
        switch (getRelation()) {
            case ATOM:
                return new ArrayList<>();
            default:
                return getChildren().stream().filter(n -> !n.getRelation().equals(ConjunctionRelation.ATOM))
                        .collect(Collectors.toList());
        }
    }

    /**
     * Apply a transformation on all leaf nodes (i.e. non-conjunction-expressions) recursively. Nondestructive.
     * @param func transformation to apply on a leaf or non-leaf node as AbstractExpression
     * @return transformed object
     */
    static LogicExpressionFlattener apply(LogicExpressionFlattener obj,
                                          BiFunction<AbstractExpression, ConjunctionRelation, AbstractExpression> func) {
        assert(obj != null);
        switch (obj.getRelation()) {
            case ATOM:
                return new LogicExpressionFlattener(func.apply(obj.getLeaf(), obj.getRelation()));
            default:
                return new LogicExpressionFlattener(obj.getRelation(),
                        new TreeSet<LogicExpressionFlattener>(obj){{
                            final List<AbstractExpression> evaluatedLeaves =                            // For leaf nodes/atoms,
                                    obj.collectLeaves().stream().map(e -> func.apply(e, ConjunctionRelation.ATOM)) // first transform them;
                                            .collect(Collectors.toList());
                            if ((obj.getRelation() == ConjunctionRelation.AND &&   // catches any shortcut based on transformed leaf nodes, and ignore non-leaf nodes
                                    evaluatedLeaves.contains(ConstantValueExpression.getFalse())) ||
                                    (obj.getRelation() == ConjunctionRelation.OR &&
                                            evaluatedLeaves.contains(ConstantValueExpression.getTrue()))) {
                                add(new LogicExpressionFlattener(obj.getRelation().equals(ConjunctionRelation.AND) ?
                                        ConstantValueExpression.getFalse() : ConstantValueExpression.getTrue()));
                            } else {
                                final List<AbstractExpression> l = evaluatedLeaves.stream()        // then filter out all constant boolean values,
                                        .filter(e -> !NormalizerUtil.isBooleanCVE(e))              // and combine transformed non-constant-boolean leaves.
                                        .collect(Collectors.toList());
                                if (l.size() == 1) {                                // Combine transformed leaves into conjunctions (when necessary),
                                    add(new LogicExpressionFlattener(l.get(0)));
                                } else {                                            // create a  flattener for conjunction-combined expression,
                                    l.stream().reduce((a, b) -> new ConjunctionExpression(ConjunctionRelation.conjOf(obj.getRelation()), a, b))
                                            .map(e -> new LogicExpressionFlattener(func.apply(e, obj.getRelation())))
                                            .ifPresent(f -> {                       // when the new flattener has any children, add those children;
                                                if (!f.getChildren().isEmpty()) {
                                                    addAll(f.getChildren());
                                                } else {                            // otherwise it's a terminal node without further AND/OR's. Just add it.
                                                    add(f);
                                                }
                                            });
                                }
                                addAll(obj.collectNonLeaves().stream()              // apply transformation on non-leaf nodes, and recursively call the method
                                        .map(e -> LogicExpressionFlattener.apply(e, func))  // on each non-leaf node, and collect transformed non-leaf nodes.
                                        .collect(Collectors.toList()));
                            }
                        }});
        }
    }

    /**
     * Rehash an expression tree to the granularity that each non-leaf expression is AND/OR, and
     * each leaf expression is neither AND nor OR expression. Each SetSplitter object must either
     * have both left and right expressions, or neither.
     */
    private static final class SetSplitter {
        private final ConjunctionRelation m_rel;
        /**
         * m_node is not NULL only in leaf node.
         */
        private final AbstractExpression m_node;
        private final SetSplitter m_left, m_right;
        SetSplitter(AbstractExpression e) {
            m_rel = ConjunctionRelation.get(e);
            switch (m_rel) {
                case ATOM:
                    assert(e != null);
                    m_left = m_right = null;
                    m_node = e;
                    break;
                default:
                    assert (e.getLeft() != null && e.getRight() != null);
                    m_left = new SetSplitter(e.getLeft());
                    m_right = new SetSplitter(e.getRight());
                    m_node = null;
            }
        }
        AbstractExpression getNode() {
            return m_node;
        }
        boolean isLeaf() {
            return ConjunctionRelation.ATOM.equals(getRelation());
        }
        ConjunctionRelation getRelation() {
            return m_rel;
        }
        SetSplitter getLeft() {
            return m_left;
        }
        SetSplitter getRight() {
            return m_right;
        }
    }
}
