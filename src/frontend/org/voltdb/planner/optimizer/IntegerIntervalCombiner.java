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

import org.voltcore.utils.Pair;
import org.voltdb.expressions.*;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ExpressionType;

import static org.voltdb.planner.optimizer.NormalizerUtil.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Combines a set of relations by merging identical terms on LHS.
 * e.g. [x + y > 3 && x - y < 0 && x + y > 5 && x - y < 3] ==> [x + y > 5 && x - y < 0];
 * [x + y > 3 || x - y < 0 || x + y > 5 || x - y < 3] ==> [x + y > 5 && x - y < 0];
 * [ NOT (x in (1, 2, 3)) && x > 0] ==> [x > 3]
 * [x > 3; y < 6; x + y > 0; x - y < 8] is not further simplified.
 * \pre OpExpressionBalancer should have been called to normalize each comparison expression, so that
 * expressions such as "3 = x + y" or "x * 3 > y / z + 5" should be like "x + y = 3", "x * 3 - y / z > 5".
 * (i.e. RHS should be either a number, or simple expression when LHS is a single expression (or -expression).
 *
 * Here we are interested in 3 forms of comparisons:
 * 1. Ordering comparison, e.g. =, !=, <, >=, etc.
 * 2. In comparison, e.g. "expr in (expr1, expr2, ...)"
 * 3. Negation of in-comparison, e.g. "NOT (expr in (expr1, ...))".
 * We skip negation of ordering comparisons, e.g. NOT (x = 5), as caller should have pushed logical negation to
 * the bottom-est level.
 *
 * NOTE: currently only integer value is supported. We will add support for floating intervals (and
 * possibly strings) by using OrderedInterval interface in place of IntegerInterval class.
 */
class IntegerIntervalCombiner {
    private final AbstractExpression m_result;
    // Generate new expression -expr based on given expr.
    private static final UnaryOperator<AbstractExpression> s_negator = e ->
            UnaryMinusPushDown.eliminate(new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS, e, null));

    /**
     * Takes a list of comparison (or NOT comparison) operations and a conjunction relation, and simplify
     * by combining similar comparisons together. e.g. a > 0 and a >= 5 is combined into a > 4.
     * @param src A list of comparisons
     * @param rel Conjunction relation between those comparisons
     */
    private IntegerIntervalCombiner(List<AbstractExpression> src, ConjunctionRelation rel) {
        final Predicate<AbstractExpression> isComparison =      // test that e is either an (in)-comparison, or a "NOT (x in (expr...))" comparison.
                e -> e instanceof ComparisonExpression ||
                        e.getExpressionType() == ExpressionType.OPERATOR_NOT && e.getLeft() instanceof InComparisonExpression;
        if (src.size() == 1 && (src.get(0) instanceof InComparisonExpression || ! isComparison.test(src.get(0)))) {
            m_result = src.get(0);      // single comparison in the form of "x in (...)" or any boolean constant: return as is.
        } else {
            assert (rel != ConjunctionRelation.ATOM);                   // Sanity check: 1. conjunction relationship is sound;
            assert(src.stream().allMatch(isComparison::test));          // 2. all expressions are indeed comparisons.
            final boolean isAnd = rel == ConjunctionRelation.AND;
            src = compactNumberComparisons(src);                        // Take care of comparisons like "a - b >= 3" and "b - a <= 5" together.
            AtomicBoolean hasEmpty = new AtomicBoolean(false);      // flag for logic shortcut when merging intervals, e.g. "x > 3" && "x < 0"
            // comparisons whose LHS is not an integer constant, or TVE of integer type, and hence should not be combined by IntegerIntervals,
            final List<ComparisonExpression> nonIntegerExpr = new ArrayList<>();    // but can be compacted by compactComplexComparisons().
            final List<Pair<AbstractExpression, range_t>> tt = src.stream().flatMap(e -> {
                if (e.getExpressionType() == ExpressionType.OPERATOR_NOT) {
                    if (isIntegerExpression(e.getLeft().getLeft())) {   // "expr NOT in (...)" calls notRangeOf;
                        return Stream.of(Pair.of(e.getLeft().getLeft(), notRangeOf((ComparisonExpression) e.getLeft())));
                    }
                } else if (isLiteralConstant(e.getLeft()) && isLiteralConstant(e.getRight())) {     // both sides of comparison are numbers
                    // neither side can be VVE in this case.
                    assert(! (e.getLeft() instanceof VectorValueExpression || e.getRight() instanceof VectorValueExpression));
                    final boolean val = evalComparison(e.getExpressionType(),       // evaluate to boolean value,
                            getNumberConstant(e.getLeft()).get(), getNumberConstant(e.getRight()).get());
                    if (isAnd ^ val) {          // mark logic shortcut if it is,
                        hasEmpty.set(true);
                    }
                    return Stream.empty();      // erase the expression.
                } else if (isIntegerExpression(e.getLeft()) && isLiteralConstant(e.getRight()) &&  // when RHS is an integer, call rangeOf().
                        (e.getRight() instanceof VectorValueExpression || isInt(e.getRight()))) {  // NOTE: check that number literal is integer. We will relax this check when we support FloatInterval operations.
                    return Stream.of(Pair.of(e.getLeft(), rangeOf((ComparisonExpression) e)));
                }
                assert(e instanceof ComparisonExpression && ! (e instanceof InComparisonExpression));   // otherwise, it is an order-comparison
                nonIntegerExpr.add((ComparisonExpression) e);                                           // with non-literal expression on both sides.
                return Stream.empty();                                                              // Transfer it to non-integer expression collection.
            }).collect(Collectors.toList());
            final List<ComparisonExpression> compactedNonIntegerExpr =        // compact/merge on complex comparisons
                    compactComplexComparisons(nonIntegerExpr, isAnd, hasEmpty);
            if (hasEmpty.get() || (tt.isEmpty() && compactedNonIntegerExpr.isEmpty())) {     // When no expression-expression or integer-expression comparisons remains, or has been shortcut.
                m_result = hasEmpty.get() ?                     // If has been short-cut,
                        getDefaultConst(rel) :                  // then shortcut result;
                        getDefaultConst(rel == ConjunctionRelation.AND ? ConjunctionRelation.OR : ConjunctionRelation.AND);  // else negate of shortcut result
            } else {                                            // Not been shortcut, and has either (or both) of integer-expression, expression-expression comparisons:
                // hasEmpty == false at this point.
                final List<AbstractExpression> intIntervals =   // postpone the actual interval combination till now, because
                        tt.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond,   // we early return in case of shortcut.
                                (a, b) -> range_t.combine(a, b, rel)))
                                .entrySet().stream().flatMap(kv -> {
                            // Convert merged interval/in-list of same LHS into a congregated expression,
                            final AbstractExpression e = kv.getValue().toExpression(kv.getKey(), rel);
                            if (e == null) {        // minding that the merged interval might contradict depending on conjunction relation.
                                hasEmpty.set(true);
                                return Stream.empty();
                            } else {
                                return Stream.of(e);
                            }
                        }).collect(Collectors.toList());    // This transformation step ensures that each LHS expression appear at most once.
                // Now combine integer intervals for integer expressions, with non-integer comparison expressions.
                final AbstractExpression expr = Stream.concat(compactedNonIntegerExpr.stream(), intIntervals.stream())
                        .sorted().reduce((a, b) -> new ConjunctionExpression(ConjunctionRelation.conjOf(rel), a, b))
                        .orElse(getDefaultConst(rel));
                m_result = isAnd && hasEmpty.get() ?        // OR with one integer expression ranging from -inf to inf does not eliminate other LHS expressions
                        ConstantValueExpression.getFalse() : expr;
            }
        }
    }

    /**
     * Compact non-numerical comparisons (i.e. neither left/right side is pure literal number).
     * The compaction is by grouping by (left, right) and merge based on comparison type, e.g.
     * expr1 > expr2 && expr1 < expr2 ==> shortcut;
     * expr1 > expr2 || expr1 < expr2 ==> expr1 != expr2
     *
     * Assumes that OpExpressionBalancer had normalized each comparison, so that two comparisons:
     * "expr1 cmp expr2" and "expr2 cmp expr1" will never appear together.
     *
     * @param src source expressions to compact
     * @param isAnd conjunction between comparisons is AND relation?
     * @param shortcut flag that gets set should any shortcut be detected.
     * @return compacted expression
     */
    private static List<ComparisonExpression> compactComplexComparisons(
            List<ComparisonExpression> src, boolean isAnd, AtomicBoolean shortcut) {
        // convert to EquivalentExpression which use equivalence as equality when comparing, to help with grouping by expression.
        return src.stream()
                .map(e -> Pair.of(Pair.of(new EquivalentExpression(e.getLeft()), new EquivalentExpression(e.getRight())),
                        e.getExpressionType()))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (rel1, rel2) -> {        // found 2 comparisons with same LHS:
                    if (rel1.equals(rel2)) {                                                        // remove duplications
                        return rel1;
                    } else {                                                                        // resolve comparison type based on
                        final ExpressionType left, right;                                           // conjunction relation and comparison types,
                        if (rel1.compareTo(rel2) <= 0) {
                            left = rel1; right = rel2;
                        } else {
                            left = rel2; right = rel1;
                        }
                        if (isAnd && ComparisonTypeMerger.areContradicts(left, right) ||
                                !isAnd && ComparisonTypeMerger.coverAll(left, right)) {             // and logic shortcut when appropriate.
                            shortcut.set(true);
                            return rel1;        // In case of a shortcut, the resulting comparison type matters no more.
                        } else {
                            return ComparisonTypeMerger.resolve(left, right, isAnd);
                        }
                    }
                })).entrySet().stream()
                .map(kv -> new ComparisonExpression(kv.getValue(),          // The transformed comparison list may likely reorder,
                        kv.getKey().getFirst().get(),                       // but who cares ordering in collection of comparisons!
                        kv.getKey().getSecond().get()))
                .collect(Collectors.toList());
    }

    /**
     * Detects comparison combinations such as "2a - b > 1", "b - 2a < 1", transforming latter into "2a - b > -1".
     * OpExpressionBalancer should have normalized each comparison.
     *
     * @param src a collection of comparisons to compact
     * @return transformed/compacted list
     */
    private static List<AbstractExpression> compactNumberComparisons(List<AbstractExpression> src) {
        // collect LHS of comparison expressions
        final List<AbstractExpression> lefts = new ArrayList<>(
                src.stream()
                        .filter(e -> e instanceof ComparisonExpression)
                        .map(AbstractExpression::getLeft)
                        .collect(Collectors.toSet()));
        // filter on those LHS expressions whose -{expression} is equivalent to some entry in the collection:
        // <pseudo-code>
        // for (index1 = 0; index1 < size; ++index1) {
        //      for (index2 = index1 + 1; index2 < size; ++index2) {
        //          check if -lefts[index1] is equivalent to lefts[index2], mark index2 if so, map satisfactory index2's
        //                  into their entries.
        //      }
        // }
        // </pseudo-code>
        final Set<AbstractExpression> duplicatedIndices =
                IntStream.range(0, lefts.size()).mapToObj(i -> {
                    final AbstractExpression e1 = s_negator.apply(lefts.get(i));
                    return IntStream.range(i + 1, lefts.size()).filter(j -> lefts.get(j).equivalent(e1));
                }).reduce(IntStream::concat).orElse(IntStream.empty())
                        .mapToObj(i -> lefts.get(i)).collect(Collectors.toSet());
        return duplicatedIndices.isEmpty() ? src :      // When found any duplications, minus-negate both sides for those
                src.stream().map(e ->                   // duplications, and reverse comparisons, e.g. "a - b > 5" ==> "b - a < -5".
                        e instanceof ComparisonExpression && duplicatedIndices.contains(e.getLeft()) ?
                                new ComparisonExpression(
                                        ComparisonExpression.reverses.get(e.getExpressionType()),
                                        s_negator.apply(e.getLeft()), s_negator.apply(e.getRight())) : e)
                        .collect(Collectors.toList());
    }

    private static List<AbstractExpression> conjunctionAdder(
            AbstractExpression e, ConjunctionRelation conj, List<AbstractExpression> acc) {
        if (e == null || conj != ConjunctionRelation.get(e)) {
            if (e != null) {
                acc.add(e);
            }
            return acc;
        } else {
            return conjunctionAdder(e.getRight(), conj, conjunctionAdder(e.getLeft(), conj, acc));
        }
    }

    /**
     * Flatten a conjunction tree in the current level with given conjunction relation
     * @param e conjunction-aggregated expression
     * @param conj conjunction relation interested
     * @return one-level conjunction-flattened expression
     */
    private static List<AbstractExpression> conjunctionAdder(AbstractExpression e, ConjunctionRelation conj) {
        return conjunctionAdder(e, conj, new ArrayList<>());
    }

    /**
     * Check whether the LHS of a ComparisonExpression it's a TVE of integer type.
     * This is needed until we support combining floating intervals.
     * @param e source expression
     * @return whether the expression is of an integeral type.
     */
    private static boolean isIntegerExpression(AbstractExpression e) {
        assert(!isLiteralConstant(e));
        if (e.getValueType() != null) { // check column value type
            switch(e.getValueType()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    return true;
                default:
                    return false;
            }
        } else {    // TVE does not implicitly call setValueType()
            return false;
        }
    }

    /**
     * Default value when comparison expressions contradict with each other, e.g. logic shortcut.
     * @param rel Conjunction relationship
     * @return default boolean constant value when there are conflicts when evaluating comparisons
     */
    static private ConstantValueExpression getDefaultConst(ConjunctionRelation rel) {
        return rel == ConjunctionRelation.AND ?
                ConstantValueExpression.getFalse() : ConstantValueExpression.getTrue();
    }

    /**
     * API to use the integer interval combiner.
     * @param src list of comparisons to combine
     * @param rel conjunction relation
     * @return interval merge/combined expression
     */
    static AbstractExpression combine(List<AbstractExpression> src, ConjunctionRelation rel) {
        return new IntegerIntervalCombiner(src, rel).get();
    }
    /**
     * API to use the integer interval combiner.
     * @param e source expression
     * @param rel conjunction relation, or ATOM when no further combination is needed.
     * @return interval merge/combined expression
     */
    static AbstractExpression combine(AbstractExpression e, ConjunctionRelation rel) {
        return rel == ConjunctionRelation.ATOM ?
                new IntegerIntervalCombiner(new ArrayList<AbstractExpression>(){{ add(e); }}, ConjunctionRelation.AND).get() :
                combine(conjunctionAdder(e, rel), rel);
    }

    /**
     * Representation of integer range of a (LHS) expression, also used for interval merging.
     */
    private static final class range_t {
        /**
         * Number intervals for the expression
         */
        final private List<IntegerInterval> m_intervals;
        /**
         * "expr in (...)" or "expr not in (...)"
         */
        final private VectorValueExpression m_included, m_excluded;
        range_t(List<IntegerInterval> intervals, VectorValueExpression included, VectorValueExpression excluded) {
            m_intervals = intervals;
            m_included = included;
            m_excluded = excluded;
        }
        List<IntegerInterval> getIntervals() {
            return m_intervals;
        }
        VectorValueExpression getIncludedExpr() {
            return m_included;
        }
        VectorValueExpression getExcludedExpr() {
            return m_excluded;
        }

        /**
         * Finalize all intervals from a range_t instance, by extracting intervals that contain a single number:
         * merge two intervals in the form of [0, 3), [4, 5) into [0, 5), and add 3 to excluded list. Doing so will
         * likely decrease number of comparisons needed by EE.
         */
        private static final class finalized {
            /**
             * Number intervals
             */
            private final List<IntegerInterval> m_intervals = new ArrayList<>();
            /**
             * discrete numbers to be included/excluded.
             */
            private final List<ConstantValueExpression> m_included = new ArrayList<>(),
                    m_excluded = new ArrayList<>();
            finalized(range_t src) {
                final List<IntegerInterval> t = src.getIntervals().stream().filter(i -> {   // transfer all discrete intervals to m_included,
                    if (i.getCardinality() == 1) {                                          // collect remaining intervals with >1 elements.
                        m_included.add(new ConstantValueExpression(i.lowerBound().value_of()));
                        return false;
                    } else {
                        return true;
                    }
                }).collect(Collectors.toList());
                IntStream.range(1, t.size()).forEach(index -> {     // Since intervals are sorted by lower bound, do merging here
                    final Number<Integer> upper = t.get(index - 1).upperBound();
                    if (!upper.isPosInf() &&                        // until upper bound is +inf, merge cases like [a, x), [x+1, b) into [a, b) and
                            new Number<>(upper.value_of() + 1).equals(t.get(index).lowerBound())) {     // add x to excluded list
                        t.set(index, IntegerInterval.of(t.get(index - 1).lowerBound(), t.get(index).upperBound()));
                        m_excluded.add(new ConstantValueExpression(upper.value_of()));
                    } else {
                        m_intervals.add(t.get(index));
                    }
                });
                if (!t.isEmpty()) {     // either first or last of merged intervals needs to be added to m_intervals
                    if (m_intervals.isEmpty()) {
                        m_intervals.add(t.get(t.size() - 1));
                    } else {
                        assert(!t.get(0).lowerBound().equals(m_intervals.get(0).lowerBound()));
                        m_intervals.add(0, t.get(0));
                    }
                }
                if (!m_intervals.isEmpty() && m_intervals.get(0).lowerBound().isNegInf() &&
                        m_intervals.get(0).upperBound().isPosInf()) {   // when merged interval is (-inf, inf), we don't need it.
                    m_intervals.clear();
                }
            }
            List<IntegerInterval> getIntervals() {
                return m_intervals;
            }
            List<ConstantValueExpression> getIncluded() {
                return m_included;
            }
            List<ConstantValueExpression> getExcluded() {
                return m_excluded;
            }
        }

        /**
         * Combine two range_t instance based on conjunction relation
         * @param lhs first range_t instance
         * @param rhs second range_t instance
         * @param conj conjunction relation between ranges
         * @return combined/merged range_t
         */
        private static range_t combine(range_t lhs, range_t rhs, ConjunctionRelation conj) {
            switch (conj) {
                case AND:
                    return intersection(lhs, rhs);
                case OR:
                    return union(lhs, rhs);
                case ATOM:
                default:
                    throw new PlanningErrorException("Internal error: cannot combine two ranges for ATOM operation.");
            }
        }

        /**
         * Converts range_t object to a single AbstractExpression as conjunction of a group of comparisons (including
         * in-comparisons) in the simplest form.
         * @param lhs LHS of those composite comparisons
         * @param conj conjunction relationship between intervals
         * @return combined expression
         */
        AbstractExpression toExpression(AbstractExpression lhs, ConjunctionRelation conj) {
            assert(conj != ConjunctionRelation.ATOM);
            if (getIntervals().isEmpty() && getIncludedExpr() == null ||        // When neither integer interval nor "expr in (...)" expression exists;
                    !getIntervals().isEmpty() &&                                // or the interval spans from -inf to inf, then a shortcut is needed.
                            getIntervals().get(0).lowerBound().isNegInf() &&
                            getIntervals().get(0).upperBound().isPosInf()) {
                return null;
            } else {
                // extracts intervals with 1 number or interval pairs excluding 1 number, and merge latter.
                final finalized finalizedRange = new finalized(this);   // simplify intervals by merging/extracting to excluded integers
                final List<ConstantValueExpression> includedNumbers = finalizedRange.getIncluded(),
                        excludedNumbers = finalizedRange.getExcluded();
                final Optional<AbstractExpression> maybeExpr =
                        finalizedRange.getIntervals().stream().map(i -> {       // convert number intervals to expressions
                            assert (!i.lowerBound().isNegInf() ||       // should not contain interval (-inf, inf) as by finalized class invariant.
                                    !i.upperBound().isPosInf());
                            if (i.lowerBound().isNegInf()) {
                                return new ComparisonExpression(ExpressionType.COMPARE_LESSTHAN, lhs,
                                        new ConstantValueExpression(i.upperBound().value_of()));
                            } else if (i.upperBound().isPosInf()) {
                                return new ComparisonExpression(ExpressionType.COMPARE_GREATERTHANOREQUALTO, lhs,
                                        new ConstantValueExpression(i.lowerBound().value_of()));
                            } else {
                                return new ConjunctionExpression(ExpressionType.CONJUNCTION_AND,
                                        new ComparisonExpression(ExpressionType.COMPARE_GREATERTHANOREQUALTO, lhs,
                                                new ConstantValueExpression(i.lowerBound().value_of())),
                                        new ComparisonExpression(ExpressionType.COMPARE_LESSTHAN, lhs,
                                                new ConstantValueExpression(i.upperBound().value_of())));
                            }
                        }).reduce((a, b) -> new ConjunctionExpression(ExpressionType.CONJUNCTION_OR, a, b));
                // Decide what goes into included/excluded in-lists:
                // when there is already an in-list of non-constant-numbers; or when there are too many discrete numbers (> 2),
                // in which case the list of includedNumbers (or excludedNumbers) gets cleared.
                final VectorValueExpression included = updateInList(getIncludedExpr(), includedNumbers),
                        excluded = updateInList(getExcludedExpr(), excludedNumbers);
                // A representation of discrete inclusion/exclusion numbers, or null if not available.
                final AbstractExpression discreteNumbers = getDiscreteNumberExpression(lhs, includedNumbers, excludedNumbers);
                final AbstractExpression expr;
                if (maybeExpr.isPresent() && discreteNumbers == null) {
                    expr = maybeExpr.get();
                } else if (maybeExpr.isPresent()) {     // combine merged intervals with discrete inclusion/exclusion numbers
                    expr = new ConjunctionExpression(ConjunctionRelation.conjOf(conj), discreteNumbers, maybeExpr.get());
                } else if (discreteNumbers != null) {   // when !maybeExpr.isPresent()
                    expr = discreteNumbers;
                } else {                                // when !maybeExpr.isPresent() && discreteNumbers.isEmpty(): shortcut.
                    expr = getDefaultConst(conj);
                }
                final AbstractExpression inList = finalizeInList(conj, lhs, included, excluded);    // (nullable) expression representation for inclusion and exclusion lists.
                if (inList == null) {
                    return expr;
                } else if (NormalizerUtil.isBooleanCVE(expr)) {
                    return inList;
                } else {  // place in-list before integer intervals in final expression, as part of expression normalization.
                    return new ConjunctionExpression(ConjunctionRelation.conjOf(conj), inList, expr);
                }
            }
        }

        /**
         * Combine discrete in-list for inclusion and exclusion into an expression. When either inclusion or exclusion list
         * has too few elements (<3), use conjunction of (in-)equality comparisons; otherwise use InComparison.
         * @param conj conjunction relation
         * @param lhs lhs of (in-)comparisons
         * @param included VVE to include, nullable
         * @param excluded VVE to exclude, nullable
         * @return combined expression
         */
        private static AbstractExpression finalizeInList(
                ConjunctionRelation conj, AbstractExpression lhs, VectorValueExpression included, VectorValueExpression excluded) {
            final AbstractExpression includedExpr, excludedExpr;
            if (included == null) {
                includedExpr = null;
            } else if (included.getArgs().size() < 3) {     // inclusion list too short: combine into OR-ed equalities.
                includedExpr = included.getArgs().stream()
                        .map(e -> (AbstractExpression) new ComparisonExpression(ExpressionType.COMPARE_EQUAL, lhs, e))
                        .reduce((a, b) -> new ConjunctionExpression(ExpressionType.CONJUNCTION_OR, a, b)).get();
            } else {        // otherwise, use InComparison.
                includedExpr = new InComparisonExpression(lhs, included);
            }
            if(excluded == null) {          // Treatment for exclusion list:
                excludedExpr = null;
            } else if (excluded.getArgs().size() < 3) {     // exclusion list too short: combine into AND-ed inequalities
                excludedExpr = excluded.getArgs().stream()
                        .map(e -> (AbstractExpression) new ComparisonExpression(ExpressionType.COMPARE_NOTEQUAL, lhs, e))
                        .reduce((a, b) -> new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, a, b)).get();
            } else {       // otherwise, use negated InComparison.
                excludedExpr = new OperatorExpression(ExpressionType.OPERATOR_NOT, new InComparisonExpression(lhs, excluded), null);
            }
            if (included != null && excluded != null) {     // combine converted inclusion/exclusion representations
                return new ConjunctionExpression(ConjunctionRelation.conjOf(conj), includedExpr, excludedExpr);
            } else if (included != null) {
                return includedExpr;
            } else {
                return excludedExpr;
            }
        }

        /**
         * Check length of number literals: if too many (> 2), transfer them to VVE. Destructive for both parameters.
         * @param init  VVE expression to start with. Nullable. When not null, does not contain any number literals.
         * @param numerals list of discrete number literals, might get transferred to VVE
         * @return created/updated VVE. The returned expression can be null, when the initial VVE was null, AND (there are
         * no discrete number literals to combine, OR too many discrete number literals).
         */
        private static VectorValueExpression updateInList(VectorValueExpression init, List<ConstantValueExpression> numerals) {
            if (numerals.size() > 2 || init != null) {
                if (init != null) {
                    init.getArgs().addAll(0, numerals);   // add concrete number before other expressions
                } else {
                    init = new VectorValueExpression(new ArrayList<>(numerals));
                }
                numerals.clear();
            }
            return init;
        }

        /**
         * Combine any numeral literals in included and excluded in-list into a conjunction of equal/unequal comparisons.
         * @param lhs  LHS of resulting (conjunctions of) comparisons
         * @param included included number literals
         * @param excluded excluded number literals
         * @return transformed expression
         */
        private static AbstractExpression getDiscreteNumberExpression(
                AbstractExpression lhs, List<ConstantValueExpression> included,
                List<ConstantValueExpression> excluded) {
            if (included.isEmpty() && excluded.isEmpty()) {
                return null;
            } else if (included.isEmpty()) {
                return excluded.stream().map(e ->
                        (AbstractExpression) new ComparisonExpression(ExpressionType.COMPARE_NOTEQUAL, lhs, e))
                        .reduce((a, b) -> new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, a, b)).get();
            } else if (excluded.isEmpty()) {
                return included.stream().map(e ->
                        (AbstractExpression) new ComparisonExpression(ExpressionType.COMPARE_EQUAL, lhs, e))
                        .reduce((a, b) -> new ConjunctionExpression(ExpressionType.CONJUNCTION_OR, a, b)).get();
            } else {        // TODO: this branch seems to be dead from JUnit: range_t.combine seems to have taken care of it.
                return new ConjunctionExpression(ExpressionType.CONJUNCTION_AND,
                        included.stream().map(e ->
                                (AbstractExpression) new ComparisonExpression(ExpressionType.COMPARE_EQUAL, lhs, e))
                                .reduce((a, b) -> new ConjunctionExpression(ExpressionType.CONJUNCTION_OR, a, b)).get(),
                        excluded.stream().map(e ->
                                (AbstractExpression) new ComparisonExpression(ExpressionType.COMPARE_NOTEQUAL, lhs, e))
                                .reduce((a, b) -> new ConjunctionExpression(ExpressionType.CONJUNCTION_AND, a, b)).get());
            }
        }

        /**
         * Merged inclusion/exclusion lists for discrete number literals of two range_t instances. No dedup done.
         * @param lhs 1st range_t instance
         * @param rhs 2nd range_t instance
         * @return merged inclusion list and exclusion list of number literals.
         */
        private static Pair<List<AbstractExpression>, List<AbstractExpression>> combineDiscreteInExpressions(
                range_t lhs, range_t rhs) {
            List<AbstractExpression> included = new ArrayList<>(), excluded = new ArrayList<>();
            if (lhs.getIncludedExpr() != null)
                included.addAll(lhs.getIncludedExpr().getArgs());
            if (lhs.getExcludedExpr() != null)
                excluded.addAll(lhs.getExcludedExpr().getArgs());
            if (rhs.getIncludedExpr() != null) {
                included.addAll(rhs.getIncludedExpr().getArgs());
            }
            if (rhs.getExcludedExpr() != null) {
                excluded.addAll(rhs.getExcludedExpr().getArgs());
            }
            return Pair.of(included, excluded);
        }
        private static range_t union(range_t lhs, range_t rhs) {        // OR-ring of two ranges
            final List<IntegerInterval> intervals = IntegerInterval.Util.unions(lhs.getIntervals(), rhs.getIntervals());    // First union number-intervals,
            final Pair<List<AbstractExpression>, List<AbstractExpression>> lists =
                    combineDiscreteInExpressions(lhs, rhs);
            List<AbstractExpression> included = lists.getFirst(), excluded = lists.getSecond();
            final List<AbstractExpression> commons = NormalizerUtil.intersection(included, excluded);
            if (!intervals.isEmpty() &&                 // union lead to whole set && no exclusions ==> logic shortcut:
                    intervals.get(0).equals(IntegerInterval.of(negInf(), posInf())) &&
                    excluded.isEmpty()) {
                return new range_t(new ArrayList<>(), null, null);
            } else {
                included = minus(included, commons);        // All inclusion expressions but those present in exclusion list
                excluded = minus(excluded, commons);        // All exclusion expressions but those present in inclusion list
                return new range_t(intervals,               // convert remaining lists to VVE
                        included.isEmpty() ? null : new VectorValueExpression(included),
                        excluded.isEmpty() ? null : new VectorValueExpression(excluded));
            }
        }
        private static range_t intersection(range_t lhs, range_t rhs) {
            final Pair<List<AbstractExpression>, List<AbstractExpression>> expressions =
                    combineDiscreteInExpressions(lhs, rhs);
            final List<AbstractExpression> included = expressions.getFirst(), excluded = expressions.getSecond();
            if (!NormalizerUtil.intersection(included, excluded).isEmpty()) {
                // when some expression appeared both in `in (...)` list and `NOT int (...)` list ==> shortcut
                return new range_t(new ArrayList<>(), null, null);
            } else {
                return new range_t(IntegerInterval.Util.intersections(lhs.getIntervals(), rhs.getIntervals()),
                        included.isEmpty() ? null : new VectorValueExpression(included),
                        excluded.isEmpty() ? null : new VectorValueExpression(excluded));
            }
        }
    }

    private static range_t notRangeOf(ComparisonExpression e) {     // simply complement the interval ranges and exchange inclusion list with exclusion list
        final range_t r = rangeOf(e);
        return new range_t(IntegerInterval.Util.complement(r.getIntervals(), IntegerInterval::of),
                r.getExcludedExpr(),        // inclusion list
                r.getIncludedExpr());       // exclusion list
    }
    /**
     * Returns range of an expression based on its expression type
     * @param e a comparison or in-list InComparison expression.
     * @return a list of numerical intervals, an indicator of whether the following VVE is to be included (true) or excluded (false),
     * and a VVE of non-numerical expressions.
     */
    private static range_t rangeOf(ComparisonExpression e) {
        final AbstractExpression right = e.getRight();
        assert(right instanceof ParameterValueExpression || right instanceof ConstantValueExpression ||
                right instanceof VectorValueExpression);
        final Function<IntegerInterval, List<IntegerInterval>> singletonList =
                v -> new ArrayList<IntegerInterval>() {{ add(v); }};
        if (e instanceof InComparisonExpression) {
            assert (right instanceof VectorValueExpression);
            final List<Integer> numbers = new ArrayList<>();                // number literals in RHS VVE. NOTE: now only filtering on integer constants (or floats that's actually integer)
            final List<AbstractExpression> parameters = new ArrayList<>();  // non-number expressions in RHS VVE
            right.getArgs().forEach(expr -> {
                if (isLiteralConstant(expr) && isInt(getNumberConstant(expr).get())) {
                    numbers.add(getNumberConstant(expr).get().intValue());
                } else {
                    parameters.add(expr);
                }
            });
            return new range_t(IntegerInterval.Util.unions(                 // try combine discrete numbers into ranges
                    numbers.stream().map(IntegerInterval::of).collect(Collectors.toList())),
                    parameters.isEmpty() ? null : new VectorValueExpression(parameters),
                    null);
        } else {
            assert(isInt(right));                                           // RHS of ordered comparison must be literal integer:
            final int val = getNumberConstant(right).get().intValue();      // guaranteed by caller logic (IntegerIntervalCombiner constructor).
            switch(e.getExpressionType()) {
                case COMPARE_EQUAL:
                    return new range_t(singletonList.apply(IntegerInterval.of(val)), null, null);
                case COMPARE_NOTEQUAL:                                      // x != Y generates 2 intervals: [-Inf, Y), [Y+1, inf)
                    return new range_t(new ArrayList<IntegerInterval>() {{
                        final Optional<Pair<IntegerInterval, Optional<IntegerInterval>>> t =
                                IntegerInterval.of(val).complement();
                        assert(t.isPresent() && t.get().getSecond().isPresent());
                        add(t.get().getFirst());
                        add(t.get().getSecond().get());
                    }}, null, null);
                case COMPARE_LESSTHAN:
                    return new range_t(singletonList.apply(IntegerInterval.of(negInf(), val)), null, null);
                case COMPARE_LESSTHANOREQUALTO:
                    return new range_t(singletonList.apply(IntegerInterval.of(negInf(), val + 1)), null, null);
                case COMPARE_GREATERTHAN:
                    return new range_t(singletonList.apply(IntegerInterval.of(val + 1, posInf())), null, null);
                case COMPARE_GREATERTHANOREQUALTO:
                    return new range_t(singletonList.apply(IntegerInterval.of(val, posInf())), null, null);
                default:
                    throw new PlanningErrorException("Unsupported comparison type: \"" + e.getExpressionType().toString() +
                            "\".");
            }
        }
    }
    AbstractExpression get() {
        return m_result;
    }
}
