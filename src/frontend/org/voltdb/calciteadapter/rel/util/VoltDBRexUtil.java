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

package org.voltdb.calciteadapter.rel.util;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.voltdb.calciteadapter.converter.RelConverter;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.types.IndexType;
import org.voltdb.types.SortDirectionType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VoltDBRexUtil {

    /**
     * Convert a collation into a new collation which column indexes are adjusted for a possible projection.
     * Adopted from the RexProgram.deduceCollations
     *
     * @param program        - New program
     * @param inputCollation - index collation
     * @return RelCollation
     */
    public static RelCollation adjustCollationForProgram(
            RexBuilder builder,
            RexProgram program,
            RelCollation inputCollation) {
        assert (program != null);

        int sourceCount = program.getInputRowType().getFieldCount();
        List<RexLocalRef> refs = program.getProjectList();
        int[] targets = new int[sourceCount];
        Arrays.fill(targets, -1);
        for (int i = 0; i < refs.size(); i++) {
            final RexLocalRef ref = refs.get(i);
            final int source = ref.getIndex();
            if ((source < sourceCount) && (targets[source] == -1)) {
                targets[source] = i;
            }
        }

        final List<RelFieldCollation> fieldCollations = new ArrayList<>(0);
        for (RelFieldCollation fieldCollation : inputCollation.getFieldCollations()) {
            final int source = fieldCollation.getFieldIndex();
            if (source < sourceCount) {
                final int target = targets[source];
                // @TODO Handle collation fields that are expressions based on project fields.
                // At the moment, the conversion stops at the first expression
                if (target < 0) {
                    // Stop at the first mismatched field
                    return RelCollations.of(fieldCollations);
                }
                fieldCollations.add(
                        fieldCollation.copy(target));
            } else {
                // @TODO Index expression
//                RexLocalRef adjustedCollationExpr = adjustExpression(builder, program, fieldCollation.getFieldIndex());
//                final int exprSource = adjustedCollationExpr.getIndex();
//                if ((exprSource < sourceCount) && (targets[exprSource] == -1)) {
//                    final int target = targets[exprSource];
//                    if (target < 0) {
//                        // Stop at the first mismatched field
//                        return RelCollations.of(fieldCollations);
//                    }
//                    fieldCollations.add(
//                            fieldCollation.copy(target));
//                }
            }
        }

        // Success -- all of the source fields of this key are mapped
        // to the output.
        return RelCollations.of(fieldCollations);
    }

    private static RexLocalRef adjustExpression(
            RexBuilder rexBuilder,
            RexProgram program,
            int exprIndex) {
        RexNode inputExpression = program.getExprList().get(exprIndex);
        final RexProgramBuilder progBuilder = RexProgramBuilder.create(
                rexBuilder,
                program.getInputRowType(),
                program.getExprList(),
                program.getProjectList(),
                inputExpression,
                program.getOutputRowType(),
                false,
                null);
        RexProgram newProg = progBuilder.getProgram();
        return newProg.getCondition();
    }

    /**
     * Reverse the direction for each collation field in the input collation
     * ASCENDING -> DESCENDING
     * STRICTLY_ASCENDING -> STRICTLY_DESCENDING
     * DESCENDING -> ASCENDING
     * STRICTLY_DESCENDING -> STRICTLY_ASCENDING
     *
     * @param inputCollation
     * @returned RelCollation
     */
    public static RelCollation reverseCollation(RelCollation inputCollation) {
        final List<RelFieldCollation> fieldCollations = new ArrayList<>(0);
        for (RelFieldCollation fieldCollation : inputCollation.getFieldCollations()) {
            final int fieldIndex = fieldCollation.getFieldIndex();
            final Direction direction = fieldCollation.getDirection();
            RelFieldCollation newfieldCollation;
            if (direction == Direction.ASCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.DESCENDING);
            } else if (direction == Direction.STRICTLY_ASCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.STRICTLY_DESCENDING);
            } else if (direction == Direction.DESCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.ASCENDING);
            } else if (direction == Direction.STRICTLY_DESCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.STRICTLY_ASCENDING);
            } else {
                newfieldCollation = new RelFieldCollation(fieldIndex, direction);
            }
            fieldCollations.add(newfieldCollation);
        }
        return RelCollations.of(fieldCollations);
    }

    /**
     * Compare sort and index collations to determine whether they are compatible or not
     * - both collations are not empty
     * - the collation direction is the same for all fields for each collation
     * - all fields from the sort collation have matching fields from the index collation
     * If collations are compatible return the sort direction (ASC, DESC).
     * <p>
     * If collations are not compatible the returned sort direction is INVALID.
     *
     * @param sortCollation
     * @param indexCollation
     * @return SortDirectionType
     */
    public static SortDirectionType areCollationsCompartible(RelCollation sortCollation, RelCollation indexCollation) {
        if (sortCollation == RelCollations.EMPTY || indexCollation == RelCollations.EMPTY) {
            return SortDirectionType.INVALID;
        }

        List<RelFieldCollation> collationFields1 = sortCollation.getFieldCollations();
        List<RelFieldCollation> collationFields2 = indexCollation.getFieldCollations();
        if (collationFields2.size() < collationFields1.size()) {
            return SortDirectionType.INVALID;
        }
        assert (collationFields1.size() > 0);
        RelFieldCollation.Direction collationDirection1 = collationFields1.get(0).getDirection();
        assert (collationFields2.size() > 0);
        RelFieldCollation.Direction collationDirection2 = collationFields2.get(0).getDirection();

        for (int i = 0; i < collationFields1.size(); ++i) {
            RelFieldCollation fieldCollation1 = collationFields1.get(i);
            RelFieldCollation fieldCollation2 = collationFields2.get(i);
            if (fieldCollation1.direction != collationDirection1 ||
                    fieldCollation2.direction != collationDirection2 ||
                    fieldCollation1.getFieldIndex() != fieldCollation2.getFieldIndex()) {
                return SortDirectionType.INVALID;
            }
        }
        SortDirectionType sortDirection =
                (Direction.ASCENDING == collationDirection1 || Direction.STRICTLY_ASCENDING == collationDirection1) ?
                        SortDirectionType.ASC : SortDirectionType.DESC;
        return sortDirection;
    }

    /**
     * Convert a collation into an OrderByPlanNode
     *
     * @param collation
     * @param collationFieldExps
     * @return
     */
    public static OrderByPlanNode collationToOrderByNode(RelCollation collation, List<RexNode> collationFieldExps) {
        assert (collation != null);
        OrderByPlanNode opn = new OrderByPlanNode();

        // Convert ORDER BY Calcite expressions to VoltDB
        List<AbstractExpression> voltExprList = new ArrayList<>();
        for (RexNode expr : collationFieldExps) {
            AbstractExpression voltExpr = RexConverter.convert(expr);
            voltExprList.add(voltExpr);
        }
        List<Pair<Integer, SortDirectionType>> collFields = RelConverter.convertCollation(collation);
        assert (voltExprList.size() == collFields.size());
        int index = 0;
        for (Pair<Integer, SortDirectionType> collField : collFields) {
            opn.getSortExpressions().add(voltExprList.get(index++));
            opn.getSortDirections().add(collField.right);
        }
        return opn;
    }

    /**
     * Merges two programs together.
     *
     * @param topProgram    Top program. Its expressions are in terms of the
     *                      outputs of the bottom program.
     * @param bottomProgram Bottom program.
     * @param rexBuilder    Rex builder
     * @return Merged program
     */
    public static RexProgram mergeProgram(RexProgram bottomProgram, RexProgram topProgram, RexBuilder rexBuilder) {
        assert (topProgram != null);
        RexProgram mergedProgram;
        if (bottomProgram != null) {
            // Merge two programs topProgram / bottomProgram into a new merged program
            mergedProgram = RexProgramBuilder.mergePrograms(
                    topProgram,
                    bottomProgram,
                    rexBuilder);
        } else {
            mergedProgram = topProgram;
        }
        return mergedProgram;
    }

    public static List<RexNode> extractValueEquivalenceExpr(RexNode expression) {
        List<RexNode> valueExprs = new ArrayList<>();
        List<RexNode> exprs = RelOptUtil.conjunctions(expression);
        for (RexNode expr : exprs) {
            if (expr.getKind() != SqlKind.EQUALS) {
                continue;
            }
            assert (expr instanceof RexCall);
            RexCall call = (RexCall) expr;
            assert (call.operands.size() == 2);
            boolean isValueEquivalence1 =
                    // CAST(literal AS type) is TRUE
                    org.apache.calcite.rex.RexUtil.isLiteral(call.operands.get(0), true) &&
                            org.apache.calcite.rex.RexUtil.isReferenceOrAccess(call.operands.get(1), true);
            boolean isValueEquivalence2 =
                    // CAST(literal AS type) is TRUE
                    org.apache.calcite.rex.RexUtil.isLiteral(call.operands.get(1), true) &&
                            org.apache.calcite.rex.RexUtil.isReferenceOrAccess(call.operands.get(0), true);
            if (isValueEquivalence1 || isValueEquivalence2) {
                valueExprs.add(expr);
            }
        }
        return exprs;
    }
}
