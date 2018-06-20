package org.voltdb.planner.optimizer;

import java.util.Comparator;
import java.util.Objects;

/**
 * An abstract ordered class with concept of +Inf and -Inf.
 * @param <T> actual java type backing the ordered class
 */
class Number<T extends Comparable<T>> implements Comparable<Number<T>> {
    /**
     * Classification of ordered number type, augmenting normal valued number with +- infinity.
     */
    enum Classify {
        NEG_INF(0), NORMAL(1), POS_INF(2);
        private final int m_value;
        Classify(int v) {
            m_value = v;
        }
        private int getValue() {
            return m_value;
        }

        /**
         * Comparison between different classes.
         * @param lhs
         * @param rhs
         * @return
         */
        static boolean less(Classify lhs, Classify rhs) {
            return Comparator.comparingInt(Classify::getValue).compare(lhs, rhs) < 0;
        }
    }
    private final T m_value;
    private final Classify m_class;

    /**
     * Construction of normal valued (i.e. not infinity) Number instance
     * @param value normal value of a Number object instance
     */
    Number(T value) {
        m_value = value;
        m_class = Classify.NORMAL;
    }

    /**
     * Construction of one of infinity Number instance.
     * @param class_of one of infinity class of the number object instance
     */
    Number(Classify class_of) {
        assert(!class_of.equals(Classify.NORMAL));
        m_value = null;
        m_class = class_of;
    }
    boolean isPosInf() {
        return class_of() == Classify.POS_INF;
    }
    boolean isNegInf() {
        return class_of() == Classify.NEG_INF;
    }

    /**
     * Getter for a normal number. Throws when used on infinity.
     * @return underlying number value
     */
    T value_of() {
        switch (m_class) {
            case NORMAL:
                return m_value;
            default:
                throw new RuntimeException("Infinity: use method class_of() before calling value_of()");
        }
    }
    Classify class_of() {
        return m_class;
    }
    boolean lessEqual(Number<T> other) {
        return less(other) || equals(other);
    }
    boolean equals(Number<T> other) {
        return class_of().equals(other.class_of()) &&
                (class_of() != Classify.NORMAL || value_of().equals(other.value_of()));
    }
    boolean less(Number<T> other) {
        return Classify.less(class_of(), other.class_of()) ||
                (class_of().equals(other.class_of()) && class_of().equals(Classify.NORMAL) &&
                        value_of().compareTo(other.value_of()) < 0);
    }
    boolean greater(Number<T> other) {
        return !lessEqual(other);
    }
    boolean greaterEqual(Number<T> other) {
        return !less(other);
    }
    Number<T> min(Number<T> other) {
        return less(other) ? this : other;
    }
    Number<T> max(Number<T> other) {
        return greater(other) ? this : other;
    }
    @Override
    public int compareTo(Number<T> other) {
        return equals(other) ? 0 : (less(other) ? -1 : 1);
    }
    @Override
    public String toString() {
        switch (class_of()) {
            case NORMAL:
                return value_of().toString();
            case NEG_INF:
                return "-Inf";
            case POS_INF:
            default:
                return "Inf";
        }
    }
    @Override
    public int hashCode() {
        return Objects.hash(m_value, m_class);
    }
}
