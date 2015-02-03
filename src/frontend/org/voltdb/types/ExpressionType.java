/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;

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
    OPERATOR_NOT                   (OperatorExpression.class,  8, "NOT"),
        // logical not
    OPERATOR_IS_NULL               (OperatorExpression.class, 9, "IS NULL"),
        // unary null evaluation

    // ----------------------------
    // Binary Comparison
    // ----------------------------
    COMPARE_EQUAL                (ComparisonExpression.class, 10, "="),
        // equal operator between left and right
    COMPARE_NOTEQUAL             (ComparisonExpression.class, 11, "<>"),
        // inequal operator between left and right
    COMPARE_LESSTHAN             (ComparisonExpression.class, 12, "<"),
        // less than operator between left and right
    COMPARE_GREATERTHAN          (ComparisonExpression.class, 13, ">"),
        // greater than operator between left and right
    COMPARE_LESSTHANOREQUALTO    (ComparisonExpression.class, 14, "<="),
        // less than equal operator between left and right
    COMPARE_GREATERTHANOREQUALTO (ComparisonExpression.class, 15, ">="),
        // greater than equal operator between left and right
    COMPARE_LIKE                 (ComparisonExpression.class, 16, "LIKE"),
        // LIKE operator (left LIKE right). both children must be string.
    COMPARE_IN                 (InComparisonExpression.class, 17, "IN"),
        // IN operator. left IN right. right must be VectorValue

    // ----------------------------
    // Conjunction Operator
    // ----------------------------
    CONJUNCTION_AND             (ConjunctionExpression.class, 20, "AND"),
    CONJUNCTION_OR              (ConjunctionExpression.class, 21, "OR"),

    // ----------------------------
    // Values
    // ----------------------------
    VALUE_CONSTANT            (ConstantValueExpression.class, 30, "<constant>"),
    VALUE_PARAMETER          (ParameterValueExpression.class, 31, "<parameter>"),
    VALUE_TUPLE                  (TupleValueExpression.class, 32, "<column>"),
    VALUE_TUPLE_ADDRESS        (TupleAddressExpression.class, 33, "<address>"),
    VALUE_VECTOR                (VectorValueExpression.class, 35, "<vector>"),

    // ----------------------------
    // Aggregate
    // ----------------------------
    AGGREGATE_COUNT               (AggregateExpression.class, 40, "COUNT"),
    AGGREGATE_COUNT_STAR          (AggregateExpression.class, 41, "COUNT(*)"),
    AGGREGATE_SUM                 (AggregateExpression.class, 42, "SUM"),
    AGGREGATE_MIN                 (AggregateExpression.class, 43, "MIN"),
    AGGREGATE_MAX                 (AggregateExpression.class, 44, "MAX"),
    AGGREGATE_AVG                 (AggregateExpression.class, 45, "AVG"),

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
    OPERATOR_ALTERNATIVE     (OperatorExpression.class,  301, "ALTERNATIVE")
    ;

    private final int m_value;
    private final String m_symbol;
    private final Class<? extends AbstractExpression> m_expressionClass;

    ExpressionType(Class<? extends AbstractExpression> expressionClass,
                   int val, String symbol) {
        m_value = val;
        m_symbol = symbol;
        m_expressionClass = expressionClass;
    }

    public Class<? extends AbstractExpression> getExpressionClass() {
        return m_expressionClass;
    }

    private static final Map<Integer, ExpressionType> idx_lookup =
        new HashMap<Integer, ExpressionType>();
    private static final Map<String, ExpressionType> name_lookup =
        new HashMap<String, ExpressionType>();

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

    /**
     * A map to keep association between a query expression (QE) type and a partial index predicate expression (IE) types
     * that this expression can cover. During the index selection step, the partial index can be selected only if
     * its predicate is covered by the QE. For example, a partial index predicate "WHERE COL > 0" is covered
     * by the query expressions "COL > 0" or  "COL = 3"
     * Keys are the query expression types
     * Values are the partial index expression types that can be covered by the query expression type
    */
    private final static Map<ExpressionType, ExpressionType[]> PARTIAL_INDEX_COMPARATOR_MAP =
            new HashMap<ExpressionType, ExpressionType[]>();

    static {
        // PARTIAL_INDEX_COMPARATOR_MAP initialization block.
        // Uncomment the other types when non-exact match will be allowed
        // IN comparison ?
        PARTIAL_INDEX_COMPARATOR_MAP.put(COMPARE_EQUAL,
                new ExpressionType[]{COMPARE_EQUAL /*, COMPARE_NOTEQUAL, COMPARE_LESSTHAN, COMPARE_GREATERTHAN,
                COMPARE_LESSTHANOREQUALTO, COMPARE_GREATERTHANOREQUALTO, COMPARE_LIKE */});
        PARTIAL_INDEX_COMPARATOR_MAP.put(COMPARE_NOTEQUAL, new ExpressionType[]{COMPARE_NOTEQUAL});
        PARTIAL_INDEX_COMPARATOR_MAP.put(COMPARE_LESSTHAN,
                new ExpressionType[]{COMPARE_LESSTHAN /*, COMPARE_LESSTHANOREQUALTO*/});
        PARTIAL_INDEX_COMPARATOR_MAP.put(COMPARE_GREATERTHAN,
                new ExpressionType[]{COMPARE_GREATERTHAN /*, COMPARE_GREATERTHANOREQUALTO*/});
        PARTIAL_INDEX_COMPARATOR_MAP.put(COMPARE_LESSTHANOREQUALTO,
                new ExpressionType[]{/*COMPARE_LESSTHAN, */COMPARE_LESSTHANOREQUALTO});
        PARTIAL_INDEX_COMPARATOR_MAP.put(COMPARE_GREATERTHANOREQUALTO,
                new ExpressionType[]{/*COMPARE_GREATERTHAN, */COMPARE_GREATERTHANOREQUALTO});
        PARTIAL_INDEX_COMPARATOR_MAP.put(COMPARE_LIKE, new ExpressionType[]{COMPARE_LIKE});
    }

    public static ExpressionType[] getCoveringPartialIndexComparators(ExpressionType type) {
        return PARTIAL_INDEX_COMPARATOR_MAP.get(type);
    }
}
