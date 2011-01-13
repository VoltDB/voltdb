/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.types;

import java.util.*;

import org.voltdb.expressions.*;

/**
 *
 */
public enum ExpressionType {
    INVALID                                            (null,  0),

    // ----------------------------
    // Arthimetic Operators
    // ----------------------------
    OPERATOR_PLUS                  (OperatorExpression.class,  1),
        // left + right (both must be number. implicitly casted)
    OPERATOR_MINUS                 (OperatorExpression.class,  2),
        // left - right (both must be number. implicitly casted)
    OPERATOR_MULTIPLY              (OperatorExpression.class,  3),
        // left * right (both must be number. implicitly casted)
    OPERATOR_DIVIDE                (OperatorExpression.class,  4),
        // left / right (both must be number. implicitly casted)
    OPERATOR_CONCAT                (OperatorExpression.class,  5),
        // left || right (both must be char/varchar)
    OPERATOR_MOD                   (OperatorExpression.class,  6),
        // left % right (both must be integer)
    OPERATOR_CAST                  (OperatorExpression.class,  7),
        // explicitly cast left as right (right is integer in ValueType enum)
    OPERATOR_NOT                   (OperatorExpression.class,  8),
        // logical not

    // ----------------------------
    // Binary Comparison
    // ----------------------------
    COMPARE_EQUAL                (ComparisonExpression.class, 10),
        // equal operator between left and right
    COMPARE_NOTEQUAL             (ComparisonExpression.class, 11),
        // inequal operator between left and right
    COMPARE_LESSTHAN             (ComparisonExpression.class, 12),
        // less than operator between left and right
    COMPARE_GREATERTHAN          (ComparisonExpression.class, 13),
        // greater than operator between left and right
    COMPARE_LESSTHANOREQUALTO    (ComparisonExpression.class, 14),
        // less than equal operator between left and right
    COMPARE_GREATERTHANOREQUALTO (ComparisonExpression.class, 15),
        // greater than equal operator between left and right
    COMPARE_LIKE                 (ComparisonExpression.class, 16),
        // LIKE operator (left LIKE right). both children must be string.
    COMPARE_IN                 (InComparisonExpression.class, 17),
        // IN operator. left IN right. right must be VectorValue

    // ----------------------------
    // Conjunction Operator
    // ----------------------------
    CONJUNCTION_AND             (ConjunctionExpression.class, 20),
    CONJUNCTION_OR              (ConjunctionExpression.class, 21),

    // ----------------------------
    // Values
    // ----------------------------
    VALUE_CONSTANT            (ConstantValueExpression.class, 30),
    VALUE_PARAMETER          (ParameterValueExpression.class, 31),
    VALUE_TUPLE                  (TupleValueExpression.class, 32),
    VALUE_TUPLE_ADDRESS        (TupleAddressExpression.class, 33),
    VALUE_NULL                    (NullValueExpression.class, 34),

    // ----------------------------
    // Aggregate
    // ----------------------------
    AGGREGATE_COUNT               (AggregateExpression.class, 40),
    AGGREGATE_COUNT_STAR          (AggregateExpression.class, 41),
    AGGREGATE_SUM                 (AggregateExpression.class, 42),
    AGGREGATE_MIN                 (AggregateExpression.class, 43),
    AGGREGATE_MAX                 (AggregateExpression.class, 44),
    AGGREGATE_AVG                 (AggregateExpression.class, 45)
    ;

    private final int val;
    private final Class<? extends AbstractExpression> expressionClass;

    ExpressionType(Class<? extends AbstractExpression> expressionClass,
                   int val) {
        this.val = val;
        this.expressionClass = expressionClass;
    }

    public Class<? extends AbstractExpression> getExpressionClass() {
        return this.expressionClass;
    }

    protected static final Map<Integer, ExpressionType> idx_lookup =
        new HashMap<Integer, ExpressionType>();
    protected static final Map<String, ExpressionType> name_lookup =
        new HashMap<String, ExpressionType>();

    static {
        for (ExpressionType vt : EnumSet.allOf(ExpressionType.class)) {
            ExpressionType.idx_lookup.put(vt.val, vt);
            String name = vt.name().toLowerCase();
            ExpressionType.name_lookup.put(name.intern(), vt);
            //
            // Also store the name of the operation without the prefix
            // This makes it easier to parse plans
            //
            String shortName = name.substring(name.indexOf("_") + 1);
            ExpressionType.name_lookup.put(shortName.intern(), vt);
        }
        //
        // Alternative Operation Names
        //
        ExpressionType.name_lookup.put("add".intern(),
                                       ExpressionType.OPERATOR_PLUS);
        ExpressionType.name_lookup.put("sub".intern(),
                                       ExpressionType.OPERATOR_MINUS);
        ExpressionType.name_lookup.put("subtract".intern(),
                                       ExpressionType.OPERATOR_MINUS);
        // Let's try this and see what happens
        ExpressionType.name_lookup.put("simplecolumn".intern(),
                                       ExpressionType.VALUE_TUPLE);
    }

    public static Map<Integer, ExpressionType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, ExpressionType> getNameMap() {
        return name_lookup;
    }

    public int getValue() {
        return val;
    }

    public static ExpressionType get(Integer idx) {
        ExpressionType ret = ExpressionType.idx_lookup.get(idx);
        return (ret == null ? ExpressionType.INVALID : ret);
    }

    public static ExpressionType get(String name) {
        ExpressionType ret =
            ExpressionType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? ExpressionType.INVALID : ret);
    }

}
