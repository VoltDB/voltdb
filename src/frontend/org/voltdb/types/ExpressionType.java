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

package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.HashRangeExpression;
import org.voltdb.expressions.InComparisonExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.RowSubqueryExpression;
import org.voltdb.expressions.ScalarValueExpression;
import org.voltdb.expressions.SelectSubqueryExpression;
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.expressions.WindowFunctionExpression;

/**
 *
 */
public enum ExpressionType {
    INVALID                                            (null,  0, "<invalid>"),

    // ----------------------------
    // Arthimetic Operators
    // ----------------------------
    OPERATOR_PLUS                  (OperatorExpression.class,  1, "+"),
        // left + right (both must be number. implicitly casted)
    OPERATOR_MINUS                 (OperatorExpression.class,  2, "-"),
        // left - right (both must be number. implicitly casted)
    OPERATOR_MULTIPLY              (OperatorExpression.class,  3, "*"),
        // left * right (both must be number. implicitly casted)
    OPERATOR_DIVIDE                (OperatorExpression.class,  4, "/"),
        // left / right (both must be number. implicitly casted)
    OPERATOR_CONCAT                (OperatorExpression.class,  5, "||"),
        // left || right (both must be char/varchar)
    OPERATOR_MOD                   (OperatorExpression.class,  6, "%"),
        // left % right (both must be integer)
    OPERATOR_CAST                  (OperatorExpression.class,  7, "<cast>"),
        // explicitly cast left as right (right is integer in ValueType enum)
    OPERATOR_NOT                   (OperatorExpression.class,  8, "NOT", true),
        // logical not
    OPERATOR_IS_NULL               (OperatorExpression.class,  9, "IS NULL", true),
    // unary null evaluation
    OPERATOR_EXISTS                (OperatorExpression.class, 18, "EXISTS", true),
    // unary exists evaluation
    // 19 is assigned to COMPARE_NOTDISTINCT, 20, 21 to CONJUNCTION_AND and CONJUNCTION_OR
    OPERATOR_UNARY_MINUS           (OperatorExpression.class, 22, "UNARY MINUS", true),
    // unary exists evaluation

    // ----------------------------
    // Binary Comparison
    // ----------------------------
    COMPARE_EQUAL                (ComparisonExpression.class, 10, "=", true),
        // equal operator between left and right
    COMPARE_NOTEQUAL             (ComparisonExpression.class, 11, "<>", true),
        // inequal operator between left and right
    COMPARE_LESSTHAN             (ComparisonExpression.class, 12, "<", true),
        // less than operator between left and right
    COMPARE_GREATERTHAN          (ComparisonExpression.class, 13, ">", true),
        // greater than operator between left and right
    COMPARE_LESSTHANOREQUALTO    (ComparisonExpression.class, 14, "<=", true),
        // less than equal operator between left and right
    COMPARE_GREATERTHANOREQUALTO (ComparisonExpression.class, 15, ">=", true),
        // greater than equal operator between left and right
    COMPARE_LIKE                 (ComparisonExpression.class, 16, "LIKE", true),
        // LIKE operator (left LIKE right). both children must be string.
    COMPARE_STARTSWITH           (ComparisonExpression.class, 150, "STARTS WITH", true),
        // STARTS WITH operator. string STARTS WITH pattern. both sides must be string.
    COMPARE_IN                   (InComparisonExpression.class, 17, "IN", true),
        // IN operator. left IN right. right must be VectorValue
    // value 18 is assigned to OPERATOR_EXISTS
    COMPARE_NOTDISTINCT          (ComparisonExpression.class, 19, "IS NOT DISTINCT FROM", true),
        // Not distinct operator between left and right

    // ----------------------------
    // Conjunction Operator
    // ----------------------------
    CONJUNCTION_AND             (ConjunctionExpression.class, 20, "AND", true),
    CONJUNCTION_OR              (ConjunctionExpression.class, 21, "OR", true),
    // value 22 is assigned to OPERATOR_UNARY_MINUS

    // ----------------------------
    // Values
    // ----------------------------
    VALUE_CONSTANT            (ConstantValueExpression.class, 30, "<constant>", true),
    VALUE_PARAMETER          (ParameterValueExpression.class, 31, "<parameter>", true),
    VALUE_TUPLE                  (TupleValueExpression.class, 32, "<column>", true),
    VALUE_TUPLE_ADDRESS        (TupleAddressExpression.class, 33, "<address>", true),
    VALUE_VECTOR                (VectorValueExpression.class, 35, "<vector>", true),
    VALUE_SCALAR                (ScalarValueExpression.class, 36, "<scalar>", true),

    // ----------------------------
    // Aggregate
    // ----------------------------
    AGGREGATE_COUNT               (AggregateExpression.class, 40, "COUNT", true),
    AGGREGATE_COUNT_STAR          (AggregateExpression.class, 41, "COUNT(*)", true),
    AGGREGATE_SUM                 (AggregateExpression.class, 42, "SUM"),
    AGGREGATE_MIN                 (AggregateExpression.class, 43, "MIN", true),
    AGGREGATE_MAX                 (AggregateExpression.class, 44, "MAX", true),
    AGGREGATE_AVG                 (AggregateExpression.class, 45, "AVG"),
    AGGREGATE_APPROX_COUNT_DISTINCT(AggregateExpression.class, 46, "APPROX_COUNT_DISTINCT", true),
    AGGREGATE_VALS_TO_HYPERLOGLOG (AggregateExpression.class, 47, "VALS_TO_HYPERLOGLOG"),
    AGGREGATE_HYPERLOGLOGS_TO_CARD(AggregateExpression.class, 48, "HYPERLOGLOGS_TO_CARD"),
    // ----------------------------
    // Windowed Aggregates.  We need to treat these
    // somewhat differently than the non-windowed
    // aggregates.  For example, AGGREGATE_MAX is a
    // different kind of thing from AGGREGATE_WINDOWED_MAX.
    // For one thing, windowed aggregates have class WindowFunctionExpression.class,
    // and non-windowed aggregates have class AggregateExpression.class.
    //
    // We only support RANK and DENSE_RANK now.  But when we support different
    // aggregate functions we will want to keep them as
    // separate ExpressionType enumerals.
    // ----------------------------
    AGGREGATE_WINDOWED_RANK       (WindowFunctionExpression.class,  70, "RANK"),
    AGGREGATE_WINDOWED_DENSE_RANK (WindowFunctionExpression.class,  71, "DENSE_RANK"),
    AGGREGATE_WINDOWED_COUNT      (WindowFunctionExpression.class,  72, "COUNT"),
    AGGREGATE_WINDOWED_MAX        (WindowFunctionExpression.class,  73, "MAX"),
    AGGREGATE_WINDOWED_MIN        (WindowFunctionExpression.class,  74, "MIN"),
    AGGREGATE_WINDOWED_SUM        (WindowFunctionExpression.class,  75, "SUM"),
    AGGREGATE_WINDOWED_ROW_NUMBER (WindowFunctionExpression.class,  76, "ROW_NUMBER"),
    // No support for PERCENT_RANK yet.
    // AGGREGATE_WINDOWED_PERCENT_RANK(WindowFunctionExpression.class, 73, "PERCENT_RANK"),
    // No support for CUME_DIST yet.
    // AGGREGATE_WINDOWED_CUME_DIST  (WindowFunctionExpression.class,  74, "CUME_DIST"),

    // ----------------------------
    // Function
    // ----------------------------
    //TODO: Should there be multiple classes for function expressions
    // maybe based on their support for optimization methods?
    //TODO: Should there be multiple FunctionExpression ExpressionTypes?
    FUNCTION                      (FunctionExpression.class,  100, "<function>"),

    // -----------------------------
    // Internals added for Elastic
    // -----------------------------
    HASH_RANGE    (HashRangeExpression.class, 200, "#"),

    // -----------------------------
    // Internals added for CASE WHEN expression.
    // -----------------------------
    OPERATOR_CASE_WHEN       (OperatorExpression.class,  300, "CASEWHEN"),
    OPERATOR_ALTERNATIVE     (OperatorExpression.class,  301, "ALTERNATIVE"),

    // -----------------------------
    // Subquery
    // -----------------------------
    ROW_SUBQUERY                 (RowSubqueryExpression.class, 400, "<row subquery>"),
    SELECT_SUBQUERY              (SelectSubqueryExpression.class, 401, "<select subquery>"),

    // -----------------------------
    // User define function
    // -----------------------------
    USER_DEFINED_AGGREGATE   (AggregateExpression.class, 106, "USER_DEFINED_AGGREGATE"),
    ;

    private final int m_value;
    private final String m_symbol;
    private final Class<? extends AbstractExpression> m_expressionClass;
    // Does this expression type have the risk to fail a DDL.
    private boolean m_isSafeForDDL;

    ExpressionType(Class<? extends AbstractExpression> expressionClass,
                   int val, String symbol) {
        this(expressionClass, val, symbol, false);
    }

    ExpressionType(Class<? extends AbstractExpression> expressionClass,
                   int val, String symbol, boolean isSafeForDDL) {
        m_value = val;
        m_symbol = symbol;
        m_expressionClass = expressionClass;
        m_isSafeForDDL = isSafeForDDL;
    }

    public Class<? extends AbstractExpression> getExpressionClass() {
        return m_expressionClass;
    }

    private static final Map<Integer, ExpressionType> idx_lookup =
        new HashMap<>();
    private static final Map<String, ExpressionType> name_lookup =
        new HashMap<>();

    static {
        for (ExpressionType vt : EnumSet.allOf(ExpressionType.class)) {
            ExpressionType.idx_lookup.put(vt.m_value, vt);
            String name = vt.name().toLowerCase();
            ExpressionType.name_lookup.put(name, vt);
            //
            // Also store the name of the operation without the prefix
            // This makes it easier to parse plans
            //
            String shortName = name.substring(name.indexOf("_") + 1);
            ExpressionType.name_lookup.put(shortName, vt);
        }
        //
        // Alternative Operation Names
        //
        ExpressionType.name_lookup.put("add", ExpressionType.OPERATOR_PLUS);
        ExpressionType.name_lookup.put("sub", ExpressionType.OPERATOR_MINUS);
        ExpressionType.name_lookup.put("subtract", ExpressionType.OPERATOR_MINUS);
        ExpressionType.name_lookup.put("negate", ExpressionType.OPERATOR_UNARY_MINUS);
    }

    public int getValue() {
        return m_value;
    }

    public static ExpressionType get(Integer idx) {
        ExpressionType ret = ExpressionType.idx_lookup.get(idx);
        return (ret == null ? ExpressionType.INVALID : ret);
    }

    public static ExpressionType get(String name) {
        // interned strings can't be garbage collected, so it's a potential for a memory leak.
        // See link: http://stackoverflow.com/questions/2431540/garbage-collection-behaviour-for-string-intern
        // or link: http://stackoverflow.com/questions/1091045/is-it-good-practice-to-use-java-lang-string-intern

        ExpressionType ret = ExpressionType.name_lookup.get(name.toLowerCase());
        return (ret == null ? ExpressionType.INVALID : ret);
    }

    public String symbol() {
        return m_symbol;
    }

    public boolean isAggregateExpression() {
        return getExpressionClass() == AggregateExpression.class;
    }

    /**
     * Return true iff this expression type is safe for
     * creating materialized views on non-empty tables.
     */
    public boolean isSafeForDDL() {
        return m_isSafeForDDL;
    }
    /**
     * When generating an output schema for a projection node we need to
     * know if the preceeding aggregate expression is going to create a
     * column's value.  This is true when the expression is an aggregate
     * or a windowed aggregate.  We can't just make the windowed aggregate
     * operations have class AggregateExpression.class because we will need
     * the class to create WindowFunctionExpression objects, and these have a
     * different representation than other aggregate expression objects.
     * For example, they have a PartitionBy list and an OrderBy list.
     *
     * @return true if an expression's value is generated by an early plan node.
     */
    public boolean isGeneratedAggregateExpression() {
        return (getExpressionClass() == AggregateExpression.class)
                || (getExpressionClass() == WindowFunctionExpression.class);
    }

    public boolean isNullary() {
        return this == ExpressionType.AGGREGATE_COUNT_STAR;
    }

    private static Map<ExpressionType, String> m_windowedAggName;

    static {
        m_windowedAggName = new HashMap<>();
        m_windowedAggName.put(ExpressionType.AGGREGATE_WINDOWED_RANK, "RANK");
        m_windowedAggName.put(ExpressionType.AGGREGATE_WINDOWED_DENSE_RANK, "DENSE_RANK");
        m_windowedAggName.put(ExpressionType.AGGREGATE_WINDOWED_ROW_NUMBER, "ROW_NUMBER");
        m_windowedAggName.put(ExpressionType.AGGREGATE_WINDOWED_COUNT, "COUNT");
        m_windowedAggName.put(ExpressionType.AGGREGATE_MAX, "MAX");
        m_windowedAggName.put(ExpressionType.AGGREGATE_MIN, "MIN");
        m_windowedAggName.put(ExpressionType.AGGREGATE_SUM, "SUM");
    }
}
