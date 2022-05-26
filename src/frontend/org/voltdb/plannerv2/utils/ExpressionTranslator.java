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

package org.voltdb.plannerv2.utils;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.hsqldb_voltpatches.FunctionCustom;
import org.hsqldb_voltpatches.FunctionSQL;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.InComparisonExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.plannerv2.ColumnTypes;
import org.voltdb.types.ExpressionType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExpressionTranslator {
    private static final Map<SqlKind, ExpressionType> MONATONIC_BINARY_OPS = Stream.of(
            Pair.of(SqlKind.PLUS, ExpressionType.OPERATOR_PLUS),
            Pair.of(SqlKind.MINUS, ExpressionType.OPERATOR_MINUS),
            Pair.of(SqlKind.TIMES, ExpressionType.OPERATOR_MULTIPLY),
            Pair.of(SqlKind.DIVIDE, ExpressionType.OPERATOR_DIVIDE),
            Pair.of(SqlKind.MOD, ExpressionType.OPERATOR_MOD))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    private static final Map<SqlKind, ExpressionType> COMP_BINARY_OPS = Stream.of(
            Pair.of(SqlKind.EQUALS, ExpressionType.COMPARE_EQUAL),
            Pair.of(SqlKind.NOT_EQUALS, ExpressionType.COMPARE_NOTEQUAL),
            Pair.of(SqlKind.LESS_THAN, ExpressionType.COMPARE_LESSTHAN),
            Pair.of(SqlKind.LESS_THAN_OR_EQUAL, ExpressionType.COMPARE_LESSTHANOREQUALTO),
            Pair.of(SqlKind.GREATER_THAN, ExpressionType.COMPARE_GREATERTHAN),
            Pair.of(SqlKind.GREATER_THAN_OR_EQUAL, ExpressionType.COMPARE_GREATERTHANOREQUALTO))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    private static final Map<SqlKind, ExpressionType> LOGICAL_BINARY_OPS = Stream.of(
            Pair.of(SqlKind.AND, ExpressionType.CONJUNCTION_AND),
            Pair.of(SqlKind.OR, ExpressionType.CONJUNCTION_OR))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    private ExpressionTranslator() {}

    /**
     * Get the SQL function id recognized by EE.
     * TODO: Now we get function ID from HSql, which we should get rid of eventually.
     * @param funName name of SQL function
     * @return function ID, or -1 if not found.
     */
    static int getSqlFunId(String funName) {
        int funId = FunctionSQL.regularFuncMap.get(funName, -1);
        if (funId < 0) {
            funId = FunctionCustom.getFunctionId(funName);
        }
        return funId;
    }

    private static ConstantValueExpression of(SqlLiteral literal) {
        final ConstantValueExpression ret = new ConstantValueExpression();
        ret.setValueType(ColumnTypes.getVoltType(literal.getTypeName()));
        ret.setValue(literal.toValue());
        return ret;
    }

    /**
     * Convert a Calcite SqlIdentifier referring to a column, and the table, to a TupleValueExpression.
     * @param id identifier for the column
     * @param t table Catalog object
     * @return create TVE object
     */
    private static TupleValueExpression toTVE(SqlIdentifier id, Table t) {
        final String tableName = t.getTypeName(), colName = id.toString();
        CalciteUtils.exceptWhen(t.getColumns().get(colName) == null,
                String.format("Tuple value %s.%s refers to a non-existing column", tableName, colName));
        return new TupleValueExpression(tableName, colName, t.getColumns().get(colName).getIndex());
    }

    static public AbstractExpression translate(SqlBasicCall call, Table t) {
        final SqlOperator operator = call.getOperator();
        final AbstractExpression result;
        if (operator instanceof SqlFunction) {
            final SqlFunction calciteFunc = (SqlFunction) operator;
            final FunctionExpression expr = new FunctionExpression();
            final String funName = operator.getName();
            final int funId = getSqlFunId(funName);
            assert funId > 0 : String.format("Unrecognized function %s", funName);
            expr.setAttributes(funName, null, null, funId);
            expr.setArgs(call.getOperandList().stream().flatMap(node -> {
                if (node instanceof SqlBasicCall) {
                    return Stream.of(translate((SqlBasicCall) node, t));
                } else if (node instanceof SqlIdentifier){
                    return Stream.of(toTVE((SqlIdentifier) node, t));
                } else {
                    CalciteUtils.except(String.format("Error parsing the function for index: %s", node.toString()));
                    return Stream.empty();
                }
            }).collect(Collectors.toList()));
            final VoltType returnType;
            switch (calciteFunc.getFunctionType()) {
                case STRING:
                    returnType = VoltType.STRING;
                    break;
                case NUMERIC:       // VoltDB does not have NUMERIC(len, prec), so this type promotion is safe.
                    returnType = VoltType.FLOAT;
                    break;
                case TIMEDATE:
                    returnType = VoltType.TIMESTAMP;
                    break;
                default:
                    returnType = null;
                    CalciteUtils.except(String.format("Unsupported function return type %s for function %s",
                            calciteFunc.getFunctionType().toString(), funName));
            }
            expr.setValueType(returnType);
            result = expr;
        } else if (operator instanceof SqlInOperator) {
            final String colName = call.operand(0).toString();
            final Column col = t.getColumns().get(colName);
            CalciteUtils.exceptWhen(col == null,
                    "Column %s does not exist in table %s.", colName, t.getTypeName());
            final VoltType type = VoltType.get((byte) col.getType());
            final VectorValueExpression right = new VectorValueExpression(
                    ((SqlNodeList) call.operand(1)).getList().stream()
                            .map(v -> {
                                CalciteUtils.exceptWhen(v.getKind() != SqlKind.LITERAL,
                                        String.format("Encountered non-constant value %s in the expression %s",
                                                v.toString(), call.toString()));
                                return of((SqlLiteral) v);
                            }).collect(Collectors.toList()));
            right.setValueType(type);
            return new InComparisonExpression(new TupleValueExpression(col), right);
        } else {
            List<AbstractExpression> exprs = call.getOperandList().stream()
                    .map(node -> {
                        if (node instanceof SqlIdentifier) {
                            return toTVE((SqlIdentifier) node, t);
                        } else if (node instanceof SqlNumericLiteral) {
                            final SqlNumericLiteral literal = (SqlNumericLiteral) node;
                            final ConstantValueExpression e = new ConstantValueExpression();
                            e.setValue(literal.toValue());
                            e.setValueType(literal.isInteger() ? VoltType.BIGINT : VoltType.FLOAT);
                            return e;
                        } else {
                            assert (node instanceof SqlBasicCall) : "Unsupported SqlNode type: " + node.toString();
                            return translate((SqlBasicCall) node, t);
                        }
                    }).collect(Collectors.toList());
            if (operator instanceof SqlMonotonicBinaryOperator) {
                CalciteUtils.exceptWhen(! MONATONIC_BINARY_OPS.containsKey(call.getKind()),
                        String.format("Found unexpected binary expression operator \"%s\" in %s",
                                call.getOperator().getName(), call.toString()));
                result = new OperatorExpression(MONATONIC_BINARY_OPS.get(call.getKind()), exprs.get(0), exprs.get(1));
            } else if (operator instanceof SqlBinaryOperator) {
                final boolean isComp = COMP_BINARY_OPS.containsKey(call.getKind()),
                        isLogical = LOGICAL_BINARY_OPS.containsKey(call.getKind());
                CalciteUtils.exceptWhen (! isComp && ! isLogical,
                        String.format("Found unknown binary operator \"%s\" in %s",
                                call.getOperator().getName(), call.toString()));
                if (isComp) {
                    result = new ComparisonExpression(
                            COMP_BINARY_OPS.get(call.getKind()), exprs.get(0), exprs.get(1));
                } else {
                    result = new ConjunctionExpression(
                            LOGICAL_BINARY_OPS.get(call.getKind()), exprs.get(0), exprs.get(1));
                }
            } else if (operator instanceof SqlPrefixOperator) {
                switch (call.getKind()) {
                    case MINUS_PREFIX:
                        result = new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS, exprs.get(0), null);
                        break;
                    case PLUS_PREFIX:       // swallow unary plus
                        result = exprs.get(0);
                        break;
                    case NOT:
                        result = new OperatorExpression(ExpressionType.OPERATOR_NOT, exprs.get(0), null);
                        break;
                    default:
                        result = null;
                        CalciteUtils.except(String.format("Found unexpected unary expression operator \"%s\" in %s",
                                call.getOperator().getName(), call.toString()));
                }
            } else {
                result = null;
                CalciteUtils.except(String.format("Found Unknown expression operator \"%s\" in %s",
                        call.getOperator().getName(), call.toString()));
            }
        }
        result.resolveForTable(t);
        result.finalizeValueTypes();
        return result;
    }
}
