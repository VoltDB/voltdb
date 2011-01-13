/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark;

import java.util.*;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

/**
 * Utility class for result set verification. Use the methods to create
 * constraints and add the constraints to ClientMain by calling addConstraint().
 */
public class Verification {
    /**
     * The generic interface of constraint expression.
     */
    public interface Expression {
        public <T> Object evaluate(T tuple);
        public <T> String toString(T tuple);
    }


    /**
     * Some shared code useful for validating foreign key contraints.
     * Briefly, this works by scanning the FK table first and storing
     * the relevant data, then scanning the constrained table to verify
     * rows match the  corresponding FK.
     */
    public static abstract class ForeignKeyConstraintBase implements Expression {
        protected final String m_table;

        public ForeignKeyConstraintBase(String table) {
            m_table = table;
        }

        @Override
        abstract public <T> Object evaluate(T tuple);

        @SuppressWarnings("unchecked")
        protected static <T> void getKey(VoltTable tuple, String columnName,
                                       Set<T> keySet) {
            final int index = tuple.getColumnIndex(columnName);
            final VoltType type = tuple.getColumnType(index);
            keySet.add((T) tuple.get(index, type));
        }

        protected static <T> void getKeys(VoltTable tuple, String[] columnNames,
                                        Set<List<Number>> keySet) {
            final List<Number> key = new ArrayList<Number>(columnNames.length);
            for (String name : columnNames) {
                final int index = tuple.getColumnIndex(name);
                final VoltType type = tuple.getColumnType(index);
                key.add((Number) tuple.get(index, type));
            }
            keySet.add(key);
        }

        @Override
        public <T> String toString(T tuple) {
            return ("foreign key check on " + m_table);
        }
    }

    /**
     * Class representing a constant.
     *
     * @param <T>
     *            Type of the constant
     */
    public static class Constant<T> implements Expression {
        private final T m_value;

        public Constant(T value) {
            m_value = value;
        }

        @Override
        public <T1> T evaluate(T1 tuple) {
            return m_value;
        }

        @Override
        public String toString() {
            return m_value.toString();
        }

        @Override
        public <T1> String toString(T1 tuple) {
            return toString();
        }
    }

    /**
     * Class representing a single value in a tuple.
     */
    public static class Value implements Expression {
        private final String m_name;

        public Value(String name) {
            m_name = name;
        }

        @Override
        public <T> Object evaluate(T tuple) {
            VoltTable row = (VoltTable) tuple;
            VoltType type = row.getColumnType(row.getColumnIndex(m_name));
            return row.get(m_name, type);
        }

        public <T> String toString(T tuple) {
            return (m_name + "(" + evaluate(tuple) + ")");
        }
    }

    protected abstract static class AbstractOperator implements Expression {
        protected final Expression m_left;
        protected final Expression m_right;

        public AbstractOperator(Expression left, Expression right) {
            m_left = left;
            m_right = right;
        }
    }

    /**
     * Class representing the OR conjunction.
     */
    public static class ConjunctionOr extends AbstractOperator {
        public ConjunctionOr(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            if ((Boolean) m_left.evaluate(tuple))
                return true;
            else
                return m_right.evaluate(tuple);
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") || ("
                    + m_right.toString(tuple) + ")");
        }
    }

    /**
     * Class representing the AND conjunction.
     */
    public static class ConjunctionAnd extends AbstractOperator {
        public ConjunctionAnd(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            return (Boolean) m_left.evaluate(tuple) && (Boolean) m_right.evaluate(tuple);
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") && ("
                    + m_right.toString(tuple) + ")");
        }
    }

    protected abstract static class AbstractCompare extends AbstractOperator {
        public AbstractCompare(Expression left, Expression right) {
            super(left, right);
        }

        @SuppressWarnings("unchecked")
        protected <T> int compare(T tuple) {
            return ((Comparable<T>) m_left.evaluate(tuple)).compareTo((T) (m_right.evaluate(tuple)));
        }
    }

    /**
     * Class representing the equality comparison.
     */
    public static class CompareEqual extends AbstractCompare {
        public CompareEqual(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            return compare(tuple) == 0;
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") == ("
                    + m_right.toString(tuple) + ")");
        }
    }

    /**
     * Class representing the inequality comparison.
     */
    public static class CompareNotEqual extends AbstractCompare {
        public CompareNotEqual(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            return compare(tuple) != 0;
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") != ("
                    + m_right.toString(tuple) + ")");
        }
    }

    /**
     * Class representing the greater than comparison.
     */
    public static class CompareGreaterThan extends AbstractCompare {
        public CompareGreaterThan(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            return compare(tuple) > 0;
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") > ("
                    + m_right.toString(tuple) + ")");
        }
    }

    /**
     * Class representing the greater than or equal to comparison.
     */
    public static class CompareGreaterThanOrEqualTo extends AbstractCompare {
        public CompareGreaterThanOrEqualTo(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            return compare(tuple) >= 0;
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") >= ("
                    + m_right.toString(tuple) + ")");
        }
    }

    /**
     * Class representing the less than comparison.
     */
    public static class CompareLessThan extends AbstractCompare {
        public CompareLessThan(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            return compare(tuple) < 0;
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") < ("
                    + m_right.toString(tuple) + ")");
        }
    }

    /**
     * Class representing the less than or equal to comparison.
     */
    public static class CompareLessThanOrEqualTo extends AbstractCompare {
        public CompareLessThanOrEqualTo(Expression left, Expression right) {
            super(left, right);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            return compare(tuple) <= 0;
        }

        @Override
        public <T> String toString(T tuple) {
            return ("(" + m_left.toString(tuple) + ") <= ("
                    + m_right.toString(tuple) + ")");
        }
    }

    /**
     * Checks a single row against the constraint exp.
     *
     * @param exp
     *            The constraint to check against.
     * @param row
     *            The row to check.
     * @return true if the constraint is satisfied, false otherwise.
     */
    public static boolean checkRow(Expression exp, VoltTable row) {
        Object result = exp.evaluate(row);
        if (!(result instanceof Boolean))
            return false;
        if (!((Boolean) result))
            System.err.println("Failed check on: " + exp.toString(row));
        return (Boolean) result;
    }

    /**
     * Handy method for creating tuple value expression. The name of the column
     * must exist in the target table so that the value can be read.
     *
     * @param name
     *            The name of the column.
     * @return The expression representing the tuple value.
     */
    public static Expression value(String name) {
        return new Value(name);
    }

    /**
     * Handy method for creating constant value expression.
     *
     * @param <T>
     *            The type of the constant.
     * @param c
     *            The constant.
     * @return The expression representing the constant value.
     */
    public static <T> Expression constant(T c) {
        return new Constant<T>(c);
    }

    /**
     * Handy method for creating a comparison expression between two tuple
     * values.
     *
     * @param type
     *            The type of comparison.
     * @param left
     *            The name of the column of the left operand.
     * @param right
     *            The name of the column of the right operand.
     * @return The expression representing the comparison operation.
     */
    public static Expression compareTupleValues(ExpressionType type,
                                                String left,
                                                String right) {
        switch (type) {
        case COMPARE_EQUAL:
            return new CompareEqual(value(left), value(right));
        case COMPARE_NOTEQUAL:
            return new CompareNotEqual(value(left), value(right));
        case COMPARE_GREATERTHAN:
            return new CompareGreaterThan(value(left), value(right));
        case COMPARE_GREATERTHANOREQUALTO:
            return new CompareGreaterThanOrEqualTo(value(left), value(right));
        case COMPARE_LESSTHAN:
            return new CompareLessThan(value(left), value(right));
        case COMPARE_LESSTHANOREQUALTO:
            return new CompareLessThanOrEqualTo(value(left), value(right));
        default:
            throw new IllegalArgumentException("Type must be a comparison type");
        }
    }

    /**
     * Handy method for creating a comparison expression between a tuple value
     * and a constant.
     *
     * @param <T>
     *            The type of the constant.
     * @param type
     *            The type of comparison.
     * @param left
     *            The name of the column of the left operand.
     * @param right
     *            The constant.
     * @return The expression representing the comparison operation.
     */
    public static <T> Expression compareWithConstant(ExpressionType type,
                                                     String left,
                                                     T right) {
        switch (type) {
        case COMPARE_EQUAL:
            return new CompareEqual(value(left), constant(right));
        case COMPARE_NOTEQUAL:
            return new CompareNotEqual(value(left), constant(right));
        case COMPARE_GREATERTHAN:
            return new CompareGreaterThan(value(left), constant(right));
        case COMPARE_GREATERTHANOREQUALTO:
            return new CompareGreaterThanOrEqualTo(value(left), constant(right));
        case COMPARE_LESSTHAN:
            return new CompareLessThan(value(left), constant(right));
        case COMPARE_LESSTHANOREQUALTO:
            return new CompareLessThanOrEqualTo(value(left), constant(right));
        default:
            throw new IllegalArgumentException("Type must be a comparison type");
        }
    }

    /**
     * Handy method for creating a conjunction expression. Multiple expressions
     * can be given and they will all be joined with the same type.
     *
     * @param type
     *            The type of conjunction.
     * @param exps
     *            The expressions to conjunct.
     * @return The expression representing the conjunction.
     */
    public static Expression conjunction(ExpressionType type, Expression... exps) {
        Expression prev = null;

        for (Expression exp : exps) {
            if (prev != null) {
                if (type == ExpressionType.CONJUNCTION_AND)
                    prev = new ConjunctionAnd(prev, exp);
                else if (type == ExpressionType.CONJUNCTION_OR)
                    prev = new ConjunctionOr(prev, exp);
                else
                    throw new IllegalArgumentException("Type must be a conjunction type");
            } else {
                prev = exp;
            }
        }

        return prev;
    }

    /**
     * Handy method for creating a expression testing if a tuple value is in
     * the range [low, high].
     *
     * @param <T> The type of the constants.
     * @param columnName The name of the column.
     * @param low The lower bound constant.
     * @param high The upper bound constant.
     * @return The expression representing the range check.
     */
    public static <T> Expression inRange(String columnName, T low, T high) {
        return conjunction(ExpressionType.CONJUNCTION_AND,
                           compareWithConstant(ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                                               columnName, low),
                           compareWithConstant(ExpressionType.COMPARE_LESSTHANOREQUALTO,
                                               columnName, high));
    }
}
