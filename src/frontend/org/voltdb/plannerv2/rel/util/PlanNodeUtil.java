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

package org.voltdb.plannerv2.rel.util;

import java.util.stream.IntStream;

import org.apache.calcite.rel.core.SetOp;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.plannerv2.converter.RelConverter;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.UnionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

/**
 * Misc Helper utilities.
 */
public final class PlanNodeUtil {

    public static AbstractPlanNode setOpToPlanNode(VoltPhysicalRel setOpNode) {
        assert(setOpNode instanceof SetOp);
        SetOp setOp = (SetOp) setOpNode;
        final UnionPlanNode upn = new UnionPlanNode(RelConverter.convertSetOpType(setOp.kind, setOp.all));
        IntStream.range(0, setOp.getInputs().size())
        .forEach(i -> {
            final AbstractPlanNode child = setOpNode.inputRelNodeToPlanNode(setOp, i);

            // ENG-6291: Voltdb wants to make inline varchar / varbynary to be outlined
            // for VAR types in its output schema
            if (child.getOutputSchema() != null) {
                NodeSchema newSchema = verifyOutputSchema(child.getOutputSchema());
                // @TODO This is a hack to reset a schema. VoltDB prohibit it
                // if the node already has one.
                child.setHaveSignificantOutputSchema(false);
                child.setOutputSchema(newSchema);
                child.setHaveSignificantOutputSchema(true);
            } else {
                AbstractPlanNode projection = child.getInlinePlanNode(PlanNodeType.PROJECTION);
                if (projection != null) {
                    NodeSchema newSchema = verifyOutputSchema(projection.getOutputSchema());
                    AbstractPlanNode newProjection = new ProjectionPlanNode(newSchema);
                    child.addInlinePlanNode(newProjection);
                }
            }
            upn.addAndLinkChild(child);

        });
        return upn;
    }

    /**
     * Iterates over schema columns and adds CAST expression on top of inlined
     * VARCHAR or VARBINARY columns
     * @param schema
     * @return new schema
     */
    private static NodeSchema verifyOutputSchema(NodeSchema schema) {
        NodeSchema newSchema = new NodeSchema();
        for (SchemaColumn scol : schema) {
            AbstractExpression scolExpr = scol.getExpression();
            if (AbstractExpression.hasInlineVarType(scolExpr)) {
                AbstractExpression expr = new OperatorExpression();;
                expr.setExpressionType(ExpressionType.OPERATOR_CAST);

                VoltType voltType = scolExpr.getValueType();
                // We don't support parameterized casting,
                // such as specifically to "VARCHAR(3)" vs. VARCHAR,
                // so assume max length for variable-length types
                // (VARCHAR and VARBINARY).
                int size = expr.getInBytes() ?
                        voltType.getMaxLengthInBytes() :
                        VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS;
                expr.setValueType(voltType);
                expr.setValueSize(size);
                expr.setInBytes(scolExpr.getInBytes());

                expr.setLeft(scolExpr);
                scol = new SchemaColumn(scol.getTableName(),
                        scol.getTableAlias(),
                        scol.getColumnName(),
                        scol.getColumnAlias(),
                        expr);
            }
            newSchema.addColumn(scol);
        }
        return newSchema;
    }
}
