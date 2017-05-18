/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.hsqldb_voltpatches.FunctionCustom;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.CatalogUtil;

public class RexConverter {

    private static int NEXT_PARAMETER_ID = 0;

    public static void resetParameterIndex() {
        NEXT_PARAMETER_ID = 0;
    }

    private static class ConvertingVisitor extends RexVisitorImpl<AbstractExpression> {

        public static final ConvertingVisitor INSTANCE = new ConvertingVisitor();

        private int m_numLhsFieldsForJoin = -1;

        protected ConvertingVisitor() {
            super(false);
        }

        public ConvertingVisitor(int numLhsFields) {
            super(false);
            m_numLhsFieldsForJoin = numLhsFields;
        }

        @Override
        public TupleValueExpression visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();
            int tableIndex = 0;

            if (m_numLhsFieldsForJoin >= 0 && index >= m_numLhsFieldsForJoin) {
                index -= m_numLhsFieldsForJoin;
                tableIndex = 1;
            }


            TupleValueExpression tve = new TupleValueExpression("", "", "", "", index, index);
            tve.setTableIndex(tableIndex);
            TypeConverter.setType(tve, inputRef.getType());
            return tve;
          }

        @Override
        public ParameterValueExpression visitDynamicParam(RexDynamicParam inputParam) {
            ParameterValueExpression pve = new ParameterValueExpression();
            pve.setParameterIndex(NEXT_PARAMETER_ID++);
            TypeConverter.setType(pve, inputParam.getType());
            return pve;
        }

        @Override
        public ConstantValueExpression visitLiteral(RexLiteral literal) {
            ConstantValueExpression cve = new ConstantValueExpression();

            String value = null;
            if (literal.getValue() instanceof NlsString) {
                NlsString nlsString = (NlsString) literal.getValue();
                value = nlsString.getValue();
            }
            else if (literal.getValue() instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) literal.getValue();
                value = bd.toPlainString();
            }
            assert value != null;

            cve.setValue(value);
            TypeConverter.setType(cve, literal.getType());

            return cve;
        }

        @Override
        public AbstractExpression visitCall(RexCall call) {

            List<AbstractExpression> aeOperands = new ArrayList<>();
            for (RexNode operand : call.operands) {
                AbstractExpression ae = operand.accept(this);
                assert ae != null;
                aeOperands.add(ae);
            }

            AbstractExpression ae = null;
            switch (call.op.kind) {
            // Conjunction
            case AND:
                    ae = new ConjunctionExpression(
                            ExpressionType.CONJUNCTION_AND,
                            aeOperands.get(0),
                            aeOperands.get(1));
                break;
            case OR:
                ae = new ConjunctionExpression(
                        ExpressionType.CONJUNCTION_OR,
                        aeOperands.get(0),
                        aeOperands.get(1));

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
//            COMPARE_IN                   (InComparisonExpression.class, 17, "IN"),
//                // IN operator. left IN right. right must be VectorValue
//            COMPARE_NOTDISTINCT          (ComparisonExpression.class, 19, "NOT DISTINCT", true),

             // Arthimetic Operators
            case PLUS:
                ae = new OperatorExpression(
                            ExpressionType.OPERATOR_PLUS,
                            aeOperands.get(0),
                            aeOperands.get(1));
                break;
            case MINUS:
                ae = new OperatorExpression(
                            ExpressionType.OPERATOR_MINUS,
                            aeOperands.get(0),
                            aeOperands.get(1));
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
                TypeConverter.setType(ae, call.getType());
                break;
            case NOT:
                ae = new OperatorExpression(
                            ExpressionType.OPERATOR_NOT,
                            aeOperands.get(0),
                            null);
                TypeConverter.setType(ae, call.getType());
                break;
            case IS_NULL:
                ae = new OperatorExpression(
                            ExpressionType.OPERATOR_IS_NULL,
                            aeOperands.get(0),
                            null);
                TypeConverter.setType(ae, call.getType());
                break;
            case EXISTS:
                ae = new OperatorExpression(
                            ExpressionType.OPERATOR_EXISTS,
                            aeOperands.get(0),
                            null);
                TypeConverter.setType(ae, call.getType());
                break;

//            OPERATOR_CONCAT                (OperatorExpression.class,  5, "||"),
//                // left || right (both must be char/varchar)
//            OPERATOR_MOD                   (OperatorExpression.class,  6, "%"),
//                // left % right (both must be integer)

            case OTHER:
                if ("||".equals(call.op.getName())) {
                    // CONCAT
                    ae = RexConverterHelper.createFunctionExpression(call.getType(), "concat", FunctionCustom.FUNC_CONCAT_ID, aeOperands);
                    TypeConverter.setType(ae, call.getType());
                } else {
                    throw new CalcitePlanningException("Unsupported Calcite expression type: " +
                            call.op.kind.toString());
                }
                break;
            default:
                throw new CalcitePlanningException("Unsupported Calcite expression type: " +
                        call.op.kind.toString());
            }

            assert ae != null;
            assert ae.getValueType() != VoltType.INVALID;
            ExpressionUtil.finalizeValueTypes(ae);
            return ae;
        }

    }

    public static NodeSchema convertToVoltDBNodeSchema(RexProgram program) {
        NodeSchema newNodeSchema = new NodeSchema();
        int i = 0;

        for (Pair<RexLocalRef, String> item : program.getNamedProjects()) {
            String name = item.right;
            RexNode rexNode = program.expandLocalRef(item.left);
            AbstractExpression ae = rexNode.accept(ConvertingVisitor.INSTANCE);
            assert (ae != null);

            newNodeSchema.addColumn(new SchemaColumn("", "", "", name, ae, i));
            ++i;
        }

        return newNodeSchema;
    }

    public static AbstractExpression convert(RexNode rexNode) {
        AbstractExpression ae = rexNode.accept(ConvertingVisitor.INSTANCE);
        assert ae != null;
        return ae;
    }

    public static AbstractExpression convertJoinPred(int numLhsFields,
            RexNode condition) {
        AbstractExpression ae = condition.accept(new ConvertingVisitor(numLhsFields));
        assert ae != null;
        return ae;
    }

    public static NodeSchema convertToVoltDBNodeSchema(RelDataType rowType) {
        NodeSchema nodeSchema = new NodeSchema();

        RelRecordType ty = (RelRecordType) rowType;
        List<String> names = ty.getFieldNames();
        int i = 0;
        for (RelDataTypeField item : ty.getFieldList()) {
            TupleValueExpression tve = new TupleValueExpression("", "", "", names.get(i), i, i);
            TypeConverter.setType(tve, item.getType());
            nodeSchema.addColumn(new SchemaColumn("", "", "", names.get(i), tve, i));
            ++i;
        }
        return nodeSchema;
    }

    public static NodeSchema convertToVoltDBNodeSchema(
            List<Pair<RexNode, String>> namedProjects) {
        NodeSchema nodeSchema = new NodeSchema();
        int i = 0;
        for (Pair<RexNode, String> item : namedProjects) {
            AbstractExpression ae = item.left.accept(ConvertingVisitor.INSTANCE);
            nodeSchema.addColumn(new SchemaColumn("", "", "", item.right, ae, i));
        }

        return nodeSchema;
    }

}
