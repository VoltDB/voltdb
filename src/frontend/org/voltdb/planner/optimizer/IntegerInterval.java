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

import static org.voltdb.planner.optimizer.NormalizerUtil.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integer interval, using Number<Integer> to represent a number or +- infinity.
 */
final class IntegerInterval implements OrderedInterval<Integer, IntegerInterval> {
    /**
     * Lower and upper bounds of the integer interval. The interval is lower-bound inclusive and upper-bound exclusive,
     * except when lower bound is -Inf (or when upper bound is +Inf, but inclusiveness for +Inf does not really matter).
     */
    private final Number<Integer> m_lower, m_upper;   // an interval from m_lower to m_upper, inclusively
    private final boolean m_hasInf;
    /**
     * Cardinality of the interval, or -1 when the interval is infinitely large.
     */
    private final int m_cardinality;

    private IntegerInterval(Number<Integer> lower, Number<Integer> upper) {
        assert(lower.less(upper));
        m_lower = lower;
        m_upper = upper;
        m_hasInf = m_lower.class_of() != Number.Classify.NORMAL || m_upper.class_of() != Number.Classify.NORMAL;
        m_cardinality = m_hasInf ? -1 : m_upper.value_of() - m_lower.value_of();
    }

    /**
     * Static factory of IntegerInterval.
     * @param left
     * @param right
     * @return
     */
    static IntegerInterval of(Number<Integer> left, Number<Integer> right) {
        return new IntegerInterval(left, right);
    }

    /* Convenience constructors for interval. */
    static IntegerInterval of(int value) {
        return new IntegerInterval(new Number<>(value), new Number<>(value + 1));
    }
    static IntegerInterval of(int left, Number<Integer> right) {
        return new IntegerInterval(new Number<>(left), right);
    }
    static IntegerInterval of(Number<Integer> left, int right) {
        return new IntegerInterval(left, new Number<>(right));
    }
    static IntegerInterval of(int left, int right) {
        return new IntegerInterval(new Number<>(left), new Number<>(right));
    }
    @Override
    public Number<Integer> lowerBound() {       // Left bound, inclusive
        return m_lower;
    }
    @Override
    public Number<Integer> upperBound() {      // right bound, exclusive
        return m_upper;
    }
    // integer interval is always left-inclusive and right-exclusive
    @Override
    public boolean lowerBoundInclusive() {
        return true;
    }
    @Override
    public boolean upperBoundInclusive() {
        return false;
    }
    // -1 indicates at least one side is infinity
    @Override
    public int getCardinality() {
        return m_cardinality;
    }

    @Override
    public int compareTo(IntegerInterval other) {
        final int lowerOrder = lowerBound().compareTo(other.lowerBound());
        if (lowerOrder != 0) {
            return lowerOrder;
        } else {
            return other.upperBound().compareTo(upperBound());
        }
    }
    @Override
    public boolean equals(Object other) {
        return other instanceof IntegerInterval &&
                lowerBound().equals(((IntegerInterval) other).lowerBound()) &&
                upperBound().equals(((IntegerInterval) other).upperBound());
    }
    @Override
    public int hashCode() {
        return Objects.hash(m_lower, m_upper, m_hasInf, m_cardinality);
    }

    @Override
    public Optional<IntegerInterval> intersection(IntegerInterval other) {
        if (upperBound().greater(other.lowerBound()) && lowerBound().lessEqual(other.lowerBound()) ||
                other.upperBound().greater(lowerBound()) && other.lowerBound().lessEqual(lowerBound())) {
            return Optional.of(new IntegerInterval(
                    lowerBound().max(other.lowerBound()),
                    upperBound().min(other.upperBound())));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<IntegerInterval> intersection(List<IntegerInterval> intervals) {
        return Util.unions(intervals.stream().map(i -> intersection(i))
                .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList()));
    }

    @Override
    public Pair<IntegerInterval, Optional<IntegerInterval>> union(IntegerInterval other) {
        if (upperBound().greaterEqual(other.lowerBound()) && lowerBound().lessEqual(other.lowerBound()) ||
                other.upperBound().greaterEqual(lowerBound()) && other.lowerBound().lessEqual(lowerBound())) {
            return Pair.of(new IntegerInterval(
                    lowerBound().min(other.lowerBound()),
                    upperBound().max(other.upperBound())),
                    Optional.empty());
        } else if (upperBound().less(other.lowerBound())) {
            return Pair.of(this, Optional.of(other));
        } else {
            return Pair.of(other, Optional.of(this));
        }
    }
    @Override
    public List<IntegerInterval> union(List<IntegerInterval> intervals) {
        intervals.add(this);
        return Util.merge(intervals, IntegerInterval::union);
    }

    @Override
    public Optional<Pair<IntegerInterval, Optional<IntegerInterval>>> complement() {
        if (lowerBound().isNegInf()) {
            if (upperBound().isPosInf()) {
                return Optional.empty();
            } else {
                return Optional.of(Pair.of(IntegerInterval.of(upperBound(), posInf()),
                        Optional.empty()));
            }
        } else if (upperBound().isPosInf()) {
            return Optional.of(Pair.of(IntegerInterval.of(negInf(), lowerBound().value_of()),
                    Optional.empty()));
        } else {
            return Optional.of(Pair.of(IntegerInterval.of(negInf(), lowerBound()),
                    Optional.of(IntegerInterval.of(upperBound(), posInf()))));
        }
    }

    /**
     * Calculate this interval - other interval
     * @param other interval to substract from
     * @return a list of at most 2 intervals of result. Empty list if the difference is empty.
     */
    @Override
    public Optional<Pair<IntegerInterval, Optional<IntegerInterval>>> difference(IntegerInterval other) {
        final Optional<Pair<IntegerInterval, Optional<IntegerInterval>>> otherComplement = other.complement();
        if (!otherComplement.isPresent()) {
            return otherComplement;
        } else {
            return Util.toOptional2(new ArrayList<IntegerInterval>() {{
                add(otherComplement.get().getFirst());
                if(otherComplement.get().getSecond().isPresent()) {
                    add(otherComplement.get().getSecond().get());
                }
            }}.stream().flatMap(s -> {
                final Optional<IntegerInterval> intersected = intersection(s);
                if (intersected.isPresent()) {
                    return Stream.of(intersected.get());
                } else {
                    return Stream.empty();
                }
            }).collect(Collectors.toList()));
        }
    }

    @Override
    public List<IntegerInterval> difference(List<IntegerInterval> other) {
        List<IntegerInterval> result = new ArrayList<>();
        result.add(this);
        if (other != null && !other.isEmpty()) {
            for (IntegerInterval i : other) {
                result = result.stream()
                        .flatMap(cur -> Util.toStream(cur.difference(i).orElse(null)))
                        .collect(Collectors.toList());
            }
        }
        return result;
    }

    /**
     * Assemble a collection of intervals from a collection of discrete values. Only useful for integer interval (or
     * countable set, for that matter).
     * @param src discrete values to assemble into intervals, e.g. {1, 2, 3, 5, 8, 13}
     * @return assembled interval set, e.g. [1, 6), [8, 9), [13,14)
     */
    static List<IntegerInterval> of(List<Integer> src) {
        List<IntegerInterval> result = new ArrayList<>();
        if (src == null || src.isEmpty()) {
            return result;
        } else {
            return src.stream()
                    .map(i -> Stream.of(IntegerInterval.of(i)).collect(Collectors.toList()))
                    .reduce((acc, cur) -> cur.get(0).union(acc))
                    .get();
        }
    }

    @Override
    public String toString() {
        return new StringBuilder("[").append(lowerBound().toString())
                .append(", ").append(upperBound().toString()).append(")").toString();
    }
}
