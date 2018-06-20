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
import org.voltdb.planner.PlanningErrorException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * An interval of some ordered type. Can be integer interval, double interval, string interval, etc.
 * Supports interval operations: intersection, union, etc. The interval itself must be comparable. (NOTE:
 * the comparison of interval is only used for storing purpose).
 *
 * @param <R> Java underlying data type backing the generic Number class
 * @param <U> the concrete interval type, e.g. IntegerInterval
 */
interface OrderedInterval<R extends Comparable<R>, U extends OrderedInterval<R, U>>
        extends Comparable<U> {
    /**
     * Lower bound of the interval
     * @return lower bound of the interval
     */
    Number<R> lowerBound();

    /**
     * Upper bound of the interval
     * @return upper bound of the interval
     */
    Number<R> upperBound();

    /**
     * @return Does current interval include its lower bound?
     */
    boolean lowerBoundInclusive();

    /**
     * @return Does current interval include its upper bound?
     */
    boolean upperBoundInclusive();

    /**
     * Cardinality of the interval. Only makes sense for countable type T. If infinite or uncountable, returns -1.
     * @return cardinality of the interval.
     */
    int getCardinality();

    /**
     * The intersection of this interval with the other one of the same interval type.
     * @param other other interval to intersect with
     * @return intersected interval, or empty if the result is empty.
     */
    Optional<U> intersection(U other);

    /**
     * Intersection with a union of intervals, i.e. a collection of interval gaps intersecting with this.
     * \pre the other interval gaps are mutually disjoint
     * @param intervals a union of disjoint intervals representing the other interval
     * @return intersection result.
     */
    List<U> intersection(List<U> intervals);

    /**
     * The union of this interval with the other one of the same interval type.
     * @param other other interval to union with
     * @return unioned interval, could be one or two depending on whether the two intervals are disjoint.
     */
    Pair<U, Optional<U>> union(U other);

    /**
     * The union of this interval with a union of intervals. Equivalent to merging current interval with a collection
     * of disjoint intervals.
     * \pre the other interval gaps are mutually disjoint
     * @param intervals a union of disjoint intervals to be union-ed with
     * @return union-ed interval result
     */
    List<U> union(List<U> intervals);

    /**
     * The complement of current interval. The result could be either empty, one-interval or two intervals.
     * @return current interval's complement
     */
    Optional<Pair<U, Optional<U>>> complement();

    /**
     * The subtraction operation between intervals. The result is the set in current interval but not in the other.
     * @param other other interval to substract
     * @return interval as a subset of current one but disjoint from the other. Could be either empty, one interval or
     * two intervals.
     */
    Optional<Pair<U, Optional<U>>> difference(U other);

    /**
     * Subtract a collection of intervals.
     * @param others intervals to subtract
     * @return the result subset interval of current that is disjoint from all other given intervals.
     */
    List<U> difference(List<U> others);

    /**
     * Utilities that helps set operation, which are identical irrespective of the concrete interval type.
     *
     * We would like to put these methods at OrderedInterval interface level, but Java8 requires that static methods
     * of interface are only visible inside the interface.
     */
    final class Util {
        private Util(){}
        /**
         * Merge all intervals when possible to make result interval set mutually exclusive (non-overlapping).
         * @param intervals src intervals
         * @param Merger function to merge two intervals
         * @param <R> Java underlying data type backing the generic Number class
         * @param <U> the concrete interval type, e.g. IntegerInterval
         * @return merged intervals
         */
        static <R extends Comparable<R>, U extends OrderedInterval<R, U>>
        List<U> merge(List<U> intervals, BiFunction<U, U, Pair<U, Optional<U>>> Merger) {
            if (intervals == null || intervals.isEmpty()) {
                return intervals;
            } else {
                intervals.sort(Comparator.naturalOrder());
                List<U> result = new ArrayList<U>() {{ add(intervals.get(0)); }};
                intervals.stream().skip(1).forEach(i -> {
                    final U last = result.get(result.size() - 1);
                    final Pair<U, Optional<U>> merged = Merger.apply(last, i);
                    if (merged.getSecond().isPresent()) {
                        result.add(i);
                    } else if (!merged.getFirst().equals(last)) {
                        result.set(result.size() - 1, merged.getFirst());
                    }
                });
                return result;
            }
        }

        /**
         * Convert a list of intervals to a type that contains at most 3 intervals
         * @param src source list of intervals
         * @param <R> Java underlying data type backing the generic Number class
         * @param <U> the concrete interval type, e.g. IntegerInterval
         * @return converted type
         */
        static <R extends Comparable<R>, U extends OrderedInterval<R, U>>
        Optional<Pair<U, Optional<U>>> toOptional2(List<U> src) {
            switch(src.size()) {
                case 0:
                    return Optional.empty();
                case 1:
                    return Optional.of(Pair.of(src.get(0), Optional.empty()));
                case 2:
                    return Optional.of(Pair.of(src.get(0), Optional.of(src.get(1))));
                default:
                    throw new PlanningErrorException("Internal error: integer interval difference `a - b' " +
                            "should never have more than 2 intervals");
            }
        }

        /**
         * Convert an ordered collection of 0-3 interval elements into an interval stream
         * @param src collection of ordered interval
         * @param <R> Java underlying data type backing the generic Number class
         * @param <U> the concrete interval type, e.g. IntegerInterval
         * @return stream of at most 3 intervals
         */
        static <R extends Comparable<R>, U extends OrderedInterval<R, U>>
        Stream<U> toStream(Pair<U, Optional<U>> src) {
            List<U> result = new ArrayList<>();
            if(src != null) {
                result.add(src.getFirst());
                if (src.getSecond().isPresent())
                    result.add(src.getSecond().get());
            }
            return result.stream();
        }

        /**
         * Merge all intervals together
         * @param intervals source intervals, could be overlapping with each other
         * @param <R> Java underlying data type backing the generic Number class
         * @param <U> the concrete interval type, e.g. IntegerInterval
         * @return merged intervals
         */
        static<R extends Comparable<R>, U extends OrderedInterval<R, U>>
        List<U> unions(List<U> intervals) {
            return merge(intervals, U::union);
        }

        /**
         * Merge two sets of intervals together. Each set of intervals could have multiple intervals overlapping with
         * each other.
         * @param lhs First set of intervals
         * @param rhs Second set of intervals
         * @param <R> Java underlying data type backing the generic Number class
         * @param <U> the concrete interval type, e.g. IntegerInterval
         * @return merged intervals from two sets
         */
        static <R extends Comparable<R>, U extends OrderedInterval<R, U>>
        List<U> unions(List<U> lhs, List<U> rhs) {
            List<U> l = new ArrayList<>(lhs);
            l.addAll(rhs);
            return unions(l);
        }

        /**
         * A set of intervals intersecting with each other.
         * @param intervals src intervals, each will be intersected with all others
         * @param <R> Java underlying data type backing the generic Number class
         * @param <U> the concrete interval type, e.g. IntegerInterval
         * @return intersection result, possibly an empty set, that is the common interval of all argument intervals.
         */
        static <R extends Comparable<R>, U extends OrderedInterval<R, U>>
        Optional<U> intersections(List<U> intervals) {
            return intervals.stream().map(Optional::of)
                    .reduce((acc, r) -> acc.isPresent() ? acc.get().intersection(r.get()) : acc)
                    .get();
        }

        /**
         * Two sets of intervals intersecting with each other. Within each set, the relation is union, generated
         * from either (NOT) InList expression, or OR-ring intervals.
         * @param lhs one set of intervals OR-ing together
         * @param rhs the other set
         * @param <R> Java underlying data type backing the generic Number class
         * @param <U> the concrete interval type, e.g. IntegerInterval
         * @return intersected union-ed interval set.
         */
        static <R extends Comparable<R>, U extends OrderedInterval<R, U>>
        List<U> intersections(List<U> lhs, List<U> rhs) {
            if (lhs.isEmpty() || rhs.isEmpty()) {
                return new ArrayList<>();
            } else {
                return Util.unions(                              // For each interval on left,
                        lhs.stream().map(i -> i.intersection(rhs))          // intersect with the unions on right,
                                .reduce((a, b) -> { a.addAll(b); return a; }).get());     // then union the residual intervals together.
            }
        }

        /**
         * Complement of a union of intervals. e.g. [[0, 2), [10, 20)] ==> [(-inf, 0), [2, 10), [20, inf)]
         * @param src a list of unioned intervals whose complements we are interested.
         * @return the implements.
         */
        static <R extends Comparable<R>, U extends OrderedInterval<R, U>>
        List<U> complement(List<U> src, BiFunction<Number<R>, Number<R>, U> Creator) {
            final List<U> merged = unions(src), result = new ArrayList<>();
            final Number<R> posInf = new Number<>(Number.Classify.POS_INF), negInf = new Number<>(Number.Classify.NEG_INF);
            if (merged.isEmpty()) {
                result.add(Creator.apply(negInf, posInf));
                return result;
            } else if (!merged.get(0).lowerBound().isNegInf()) {
                result.add(Creator.apply(negInf, merged.get(0).lowerBound()));
            }
            for (int index = 0; index < merged.size() - 1; ++index) {
                result.add(Creator.apply(merged.get(index).upperBound(), merged.get(index + 1).lowerBound()));
            }
            if (!merged.get(merged.size() - 1).upperBound().isPosInf()) {
                result.add(Creator.apply(merged.get(merged.size() - 1).upperBound(), posInf));
            }
            return result;
        }
    }
}
