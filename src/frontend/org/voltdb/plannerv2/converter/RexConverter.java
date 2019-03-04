/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.converter;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlDatetimePlusOperator;
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannerv2.ColumnTypes;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;

/**
 * The utility class that covert Calcite row expression node to Volt AbstractExpression.
 */
public class RexConverter {
    private RexConverter() {}

    // used to keep track of a RexDynamicParam's index.
    private static ThreadLocal<Integer> NEXT_PARAMETER_ID = ThreadLocal.withInitial(() -> 0);

    public static void resetParameterIndex() {
        NEXT_PARAMETER_ID.set(0);
    }

    public static int getParameterIndex() {
        return NEXT_PARAMETER_ID.get();
    }

    public static void setParameterIndex(int index) {
        NEXT_PARAMETER_ID.set(index);
    }

    public static int getAndIncrementParameterIndex() {
        int val = NEXT_PARAMETER_ID.get();
        setParameterIndex(val + 1);
        return val;
    }

    public static void setType(AbstractExpression ae, RelDataType rdt) {
        VoltType vt = ColumnTypes.getVoltType(rdt.getSqlTypeName());
        Preconditions.checkNotNull(vt);
        setType(ae, vt, rdt.getPrecision());
    }

    public static void setType(AbstractExpression ae, VoltType vt, int precision) {

        ae.setValueType(vt);
        if (precision == PRECISION_NOT_SPECIFIED) {
            precision = vt.getLengthInBytesForFixedTypes();
        }

        if (vt.isVariableLength()) {
            int size;
            if ((ae instanceof ConstantValueExpression ||
                    ae instanceof FunctionExpression)
                    &&
                    (vt != VoltType.NULL) && (vt != VoltType.NUMERIC)) {
                size = vt.getMaxLengthInBytes();
            } else {
                size = precision;
            }
            if (!(ae instanceof ParameterValueExpression)) {
                ae.setValueSize(size);
            }
        }
    }

    /**
     * Build binary conjunction expression tree out of flat list of operands
     *
     * @param exprType
     * @param aeOperands
     * @return
     */
    private static AbstractExpression buildExprTree(ExpressionType exprType, List<AbstractExpression> aeOperands) {
        Preconditions.checkArgument(aeOperands.size() > 1);
        return aeOperands.stream().skip(2)
                .reduce(new ConjunctionExpression(exprType, aeOperands.get(0), aeOperands.get(1)),
                        (left, right) -> new ConjunctionExpression(exprType, left, right));
    }

    /**
     * Convert a Calcite RexCall to Volt AbstractExpression.
     *
     * @param call
     * @param aeOperands
     * @return The converted AbstractExpression.
     */
    public static AbstractExpression rexCallToAbstractExpression(RexCall call, List<AbstractExpression> aeOperands) {
        AbstractExpression ae;
        switch (call.op.kind) {
            // Conjunction
            case AND:
                ae = buildExprTree(ExpressionType.CONJUNCTION_AND, aeOperands);
                break;
            case OR:
                if (aeOperands.size() == 2) {
                    // Binary OR
                    ae = new ConjunctionExpression(
                            ExpressionType.CONJUNCTION_OR,
                            aeOperands.get(0),
                            aeOperands.get(1));
                } else {
                    // COMPARE_IN
                    ae = RexConverterHelper.createInComparisonExpression(call.getType(), aeOperands);
                }
                break;

            // Binary Comparison
            case EQUALS:
                ae = new ComparisonExpression(
                        ExpressionType.COMPARE_EQUAL,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case NOT_EQUALS:
                ae = new ComparisonExpression(
                        ExpressionType.COMPARE_NOTEQUAL,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case LESS_THAN:
                ae = new ComparisonExpression(
                        ExpressionType.COMPARE_LESSTHAN,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case GREATER_THAN:
                ae = new ComparisonExpression(
                        ExpressionType.COMPARE_GREATERTHAN,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case LESS_THAN_OR_EQUAL:
                ae = new ComparisonExpression(
                        ExpressionType.COMPARE_LESSTHANOREQUALTO,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case GREATER_THAN_OR_EQUAL:
                ae = new ComparisonExpression(
                        ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case LIKE:
                ae = new ComparisonExpression(
                        ExpressionType.COMPARE_LIKE,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
//            COMPARE_NOTDISTINCT          (ComparisonExpression.class, 19, "NOT DISTINCT", true),

            // Arthimetic Operators
            case PLUS:
                // Check for DATETIME + INTERVAL expression first
                if (call.op instanceof SqlDatetimePlusOperator) {
                    // At this point left and right operands are converted to MICROSECONDS
                    ae = RexConverterHelper.createToTimestampFunctionExpression(
                            call.getType(),
                            ExpressionType.OPERATOR_PLUS,
                            aeOperands);
                } else {
                    ae = new OperatorExpression(
                            ExpressionType.OPERATOR_PLUS,
                            aeOperands.get(0),
                            aeOperands.get(1));
                }
                break;
            case MINUS:
                // Check for DATETIME - INTERVAL expression first
                // For whatever reason Calcite treats + and - DATETIME operation differently
                if (call.op instanceof SqlDatetimeSubtractionOperator) {
                    ae = RexConverterHelper.createToTimestampFunctionExpression(
                            call.getType(),
                            ExpressionType.OPERATOR_MINUS,
                            aeOperands);
                } else {
                    ae = new OperatorExpression(
                            ExpressionType.OPERATOR_MINUS,
                            aeOperands.get(0),
                            aeOperands.get(1));
                }
                break;
            case TIMES:
                ae = new OperatorExpression(
                        ExpressionType.OPERATOR_MULTIPLY,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case DIVIDE:
                ae = new OperatorExpression(
                        ExpressionType.OPERATOR_DIVIDE,
                        aeOperands.get(0),
                        aeOperands.get(1));
                break;
            case CAST:
                ae = new OperatorExpression(
                        ExpressionType.OPERATOR_CAST,
                        aeOperands.get(0),
                        null);
                RexConverter.setType(ae, call.getType());
                break;
            case NOT:
                ae = new OperatorExpression(
                        ExpressionType.OPERATOR_NOT,
                        aeOperands.get(0),
                        null);
                RexConverter.setType(ae, call.getType());
                break;
            case IS_NULL:
                ae = new OperatorExpression(
                        ExpressionType.OPERATOR_IS_NULL,
                        aeOperands.get(0),
                        null);
                RexConverter.setType(ae, call.getType());
                break;
            case IS_NOT_NULL:
                AbstractExpression isnullexpr = new OperatorExpression(
                        ExpressionType.OPERATOR_IS_NULL,
                        aeOperands.get(0),
                        null);
                ae = new OperatorExpression(
                        ExpressionType.OPERATOR_NOT,
                        isnullexpr,
                        null);
                RexConverter.setType(ae, call.getType());
                break;
            case EXISTS:
                ae = new OperatorExpression(
                        ExpressionType.OPERATOR_EXISTS,
                        aeOperands.get(0),
                        null);
                RexConverter.setType(ae, call.getType());
                break;

//            OPERATOR_CONCAT                (OperatorExpression.class,  5, "||"),
//                // left || right (both must be char/varchar)
//            OPERATOR_MOD                   (OperatorExpression.class,  6, "%"),
//                // left % right (both must be integer)

            case OTHER:
                if ("||".equals(call.op.getName())) {
                    // CONCAT
                    ae = RexConverterHelper.createFunctionExpression(call.getType(), "concat", aeOperands, null);
                    RexConverter.setType(ae, call.getType());
                } else {
                    throw new CalcitePlanningException("Unsupported Calcite expression type: " +
                            call.op.kind.toString());
                }
                break;
            case OTHER_FUNCTION:
                ae = RexConverterHelper.createFunctionExpression(call.getType(), call.op.getName().toLowerCase(), aeOperands, null);
                RexConverter.setType(ae, call.getType());
                break;
            default:
                throw new CalcitePlanningException("Unsupported Calcite expression type: " +
                        call.op.kind.toString());
        }

        Preconditions.checkNotNull(ae);
        RexConverter.setType(ae, call.getType());
        return ae;
    }

    /**
     * A visitor that covert Calcite row expression node to Volt AbstractExpression.
     */
    private static class ConvertingVisitor extends RexVisitorImpl<AbstractExpression> {

        public static final ConvertingVisitor INSTANCE = new ConvertingVisitor();

        // the number of the outer table column in the select query.
        // For example "SELECT foo.a, bar.b, bar.c FROM foo, bar" gets 1
        // because only one column of outer table gets selected.
        int m_numOuterFieldsForJoin = -1;

        ConvertingVisitor() {
            super(false);
        }

        public ConvertingVisitor(int numLhsFields) {
            super(false);
            m_numOuterFieldsForJoin = numLhsFields;
        }

        boolean isFromInnerTable(int columnIndex) {
            return m_numOuterFieldsForJoin >= 0 && columnIndex >= m_numOuterFieldsForJoin;
        }

        TupleValueExpression visitInputRef(int tableIndex, int inputColumnIdx, RelDataType inputType, String tableName, String columnName) {
            // null if the column comes from RexInputRef
            if (tableName == null) {
                tableName = "";
            }
            if (columnName == null) {
                // Generate a column name out of its index in the original table 1 -> "001"
                columnName = String.format("%03d", inputColumnIdx);
            }

            TupleValueExpression tve = new TupleValueExpression(tableName, tableName, columnName, columnName, inputColumnIdx, inputColumnIdx);
            tve.setTableIndex(tableIndex);
            RexConverter.setType(tve, inputType);
            return tve;
        }

        @Override
        public TupleValueExpression visitInputRef(RexInputRef inputRef) {
            int inputRefIdx = inputRef.getIndex();
            int tableIndex = 0;

            if (isFromInnerTable(inputRefIdx)) {
                inputRefIdx -= m_numOuterFieldsForJoin;
                tableIndex = 1;
            }
            return visitInputRef(tableIndex, inputRefIdx, inputRef.getType(), null, null);
        }

        @Override
        public ParameterValueExpression visitDynamicParam(RexDynamicParam inputParam) {
            ParameterValueExpression pve = new ParameterValueExpression();
            pve.setParameterIndex(getAndIncrementParameterIndex());
            RexConverter.setType(pve, inputParam.getType());
            return pve;
        }

        @Override
        public ConstantValueExpression visitLiteral(RexLiteral literal) {
            ConstantValueExpression cve = new ConstantValueExpression();

            final String value;
            if (literal.getValue() instanceof NlsString) {
                NlsString nlsString = (NlsString) literal.getValue();
                value = nlsString.getValue();
            } else if (literal.getValue() instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) literal.getValue();
                // Special treatment for intervals - VoltDB TIMESTAMP expects value in microseconds
                if (literal.getType() instanceof IntervalSqlType) {
                    BigDecimal thousand = BigDecimal.valueOf(1000);
                    bd = bd.multiply(thousand);
                }
                value = bd.toPlainString();
            } else if (literal.getValue() instanceof GregorianCalendar) {
                // VoltDB TIMESTAMPS expects time in microseconds
                long time = ((GregorianCalendar) literal.getValue()).getTimeInMillis() * 1000;
                value = Long.toString(time);
            } else {
                // @TODO Catch all
                value = literal.getValue().toString();
            }

            cve.setValue(value);
            RexConverter.setType(cve, literal.getType());

            return cve;
        }

        @Override
        public AbstractExpression visitCall(RexCall call) {
            List<AbstractExpression> aeOperands = new ArrayList<>();
            for (RexNode operand : call.operands) {
                AbstractExpression ae = operand.accept(this);
                Preconditions.checkNotNull(ae);
                aeOperands.add(ae);
            }
            return RexConverter.rexCallToAbstractExpression(call, aeOperands);
        }
    }

    public static AbstractExpression convert(RexNode rexNode) {
        AbstractExpression ae = rexNode.accept(ConvertingVisitor.INSTANCE);
        Preconditions.checkNotNull(ae);
        return ae;
    }

    public static NodeSchema convertToVoltDBNodeSchema(RelDataType rowType) {
        NodeSchema nodeSchema = new NodeSchema();

        RelRecordType ty = (RelRecordType) rowType;
        List<String> names = ty.getFieldNames();
        int i = 0;
        for (RelDataTypeField item : ty.getFieldList()) {
            TupleValueExpression tve = new TupleValueExpression("", "", "", names.get(i), i, i);
            RexConverter.setType(tve, item.getType());
            nodeSchema.addColumn(new SchemaColumn("", "", "", names.get(i), tve, i));
            ++i;
        }
        return nodeSchema;
    }

    public static NodeSchema convertToVoltDBNodeSchema(RexProgram program) {
        NodeSchema newNodeSchema = new NodeSchema();
        int i = 0;
        for (Pair<RexLocalRef, String> item : program.getNamedProjects()) {
            String name = item.right;
            RexNode rexNode = program.expandLocalRef(item.left);
            AbstractExpression ae = rexNode.accept(ConvertingVisitor.INSTANCE);
            Preconditions.checkNotNull(ae);
            newNodeSchema.addColumn(new SchemaColumn("", "", "", name, ae, i));
            ++i;
        }

        return newNodeSchema;
    }

    public static AbstractExpression convertDataTypeField(RelDataTypeField dataTypeField) {
        int columnIndex = dataTypeField.getIndex();
        int tableIndex = 0;
        String tableName = "";
        String columnName = String.format("%03d", columnIndex);

        TupleValueExpression tve = new TupleValueExpression(tableName, tableName, columnName, columnName, columnIndex, columnIndex);
        tve.setTableIndex(tableIndex);
        RexConverter.setType(tve, dataTypeField.getType());
        return tve;
    }
}
