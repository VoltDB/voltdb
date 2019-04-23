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

package org.voltdb.plannerv2.utils;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.types.IndexType;
import org.voltdb.types.SortDirectionType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VoltRexUtil {
    private VoltRexUtil() {}

    /**
     * Convert a collation into an OrderByPlanNode.
     *
     * @param collation
     * @param collationFieldExps
     * @return
     */
    public static OrderByPlanNode collationToOrderByNode(RelCollation collation, List<RexNode> collationFieldExps) {
        Preconditions.checkNotNull("Null collation", collation);
        // Convert ORDER BY Calcite expressions to VoltDB
        final List<AbstractExpression> voltExprList = collationFieldExps.stream()
                .map(RexConverter::convert).collect(Collectors.toList());
        final List<Pair<Integer, SortDirectionType>> collFields = convertCollation(collation);
        Preconditions.checkArgument(voltExprList.size() == collFields.size());
        final OrderByPlanNode opn = new OrderByPlanNode();
        int index = 0;
        for (Pair<Integer, SortDirectionType> collField : collFields) {
            opn.getSortExpressions().add(voltExprList.get(index++));
            opn.getSortDirections().add(collField.right);
        }
        return opn;
    }

    /**
     * Create RexCollation corresponding to a given index. If the index is not scannable
     * an empty collation is returned
     * @param index
     * @param catTable
     * @param builder
     * @param program
     * @return
     */
    public static RelCollation createIndexCollation(
            Index index, Table catTable, RexBuilder builder, RexProgram program) throws JSONException {
        if (IndexType.isScannable(index.getType())) {
            // Convert index collation to take the program into an account
            return VoltRexUtil.adjustCollationForProgram(builder, program,
                    RelCollations.of(IndexUtil.getIndexCollationFields(catTable, index, program)));
        } else {
            return RelCollations.EMPTY;
        }
    }


    private static List<Pair<Integer, SortDirectionType>> convertCollation(RelCollation collation) {
        return collation.getFieldCollations().stream().map(col ->
                Pair.of(col.getFieldIndex(),
                        "ASC".equalsIgnoreCase(col.getDirection().shortString) ?
                                SortDirectionType.ASC : SortDirectionType.DESC))
                .collect(Collectors.toList());
    }

    /**
     * Compare sort and index collations to determine whether they are compatible or not
     *  - both collations are not empty
     *  - the collation direction is the same for all fields for each collation
     *  - all fields from the sort collation have matching fields from the index collation
     * If collations are compatible return the sort direction (ASC, DESC).
     *
     * If collations are not compatible the returned sort direction is INVALID.
     *
     * @param sortCollation
     * @param indexCollation
     * @return SortDirectionType
     */
    public static SortDirectionType areCollationsCompatible(RelCollation sortCollation, RelCollation indexCollation) {
        if (sortCollation == RelCollations.EMPTY || indexCollation == RelCollations.EMPTY) {
            return SortDirectionType.INVALID;
        }

        List<RelFieldCollation> collationFields1 = sortCollation.getFieldCollations();
        List<RelFieldCollation> collationFields2 = indexCollation.getFieldCollations();
        if (collationFields2.size() < collationFields1.size()) {
            return SortDirectionType.INVALID;
        }
        assert(collationFields1.size() > 0);
        final RelFieldCollation.Direction collationDirection1 = collationFields1.get(0).getDirection();
        assert(collationFields2.size() > 0);
        final RelFieldCollation.Direction collationDirection2 = collationFields2.get(0).getDirection();
        if (IntStream.range(0, collationFields1.size()).anyMatch(i -> {
            final RelFieldCollation col1 = collationFields1.get(i);
            final RelFieldCollation col2 = collationFields2.get(i);
            return col1.direction != collationDirection1 || col2.direction != collationDirection2 ||
                    col1.getFieldIndex() != col2.getFieldIndex();
        })) {
            return SortDirectionType.INVALID;
        } else {
            switch (collationDirection1) {
                case STRICTLY_ASCENDING:
                case ASCENDING:
                    return SortDirectionType.ASC;
                default:
                    return SortDirectionType.DESC;
            }
        }
    }


    /**
     * Convert a {@link #RelDistribution} of HASH type into a new {@link #RelDistribution} where partitioning column index
     * is adjusted for a given projection. If the partitioning column is projected out, the output index array will be empty
     *
     * If the input distribution is not HAS_DISTRIBUTED it is returned unmodified.
     *
     * @param builder - RexBuilder
     * @param program - RexProgram
     * @param distribution - initial RelDistribution
     * @return RelDistribution
     */
    public static RelDistribution adjustRelDistributionForProgram(
            RexBuilder builder, RexProgram program, RelDistribution distribution) {
        Preconditions.checkArgument(program != null, "Cannot adjust RelDistribution for null Program");
        if (distribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
            return distribution;
        }

        final List<Integer> distColumnIndexes = distribution.getKeys();
        if (distColumnIndexes.isEmpty()) {
            return distribution;
        }
        // VoltDB supports only one partition column per table.
        assert(distColumnIndexes.size() == 1);
        final int partitionIndex = distColumnIndexes.get(0);
        final List<Integer> newPartitionIndexes = new ArrayList<>(1);

        // Iterate over the project list to find a corresponding input reference
        // which index matches the partitioning column index from the input distribution
        // The index of this project will be the new index of a partitioning column
        for (int idx = 0; idx < program.getProjectList().size(); ++idx) {
            RexLocalRef projRef = program.getProjectList().get(idx);
            int source = projRef.getIndex();
            assert(source < program.getExprList().size());
            RexNode rexNode = program.getExprList().get(source);
            if (rexNode instanceof RexInputRef) {
                // This is an actual field
                int exprInputIndex = ((RexInputRef)rexNode).getIndex();
                if (partitionIndex == exprInputIndex) {
                    newPartitionIndexes.add(idx);
                    break;
                }
            }
        }
        return RelDistributions.hash(newPartitionIndexes);
    }

    public static RelCollation adjustCollationForProgram(
            RexBuilder builder, RexProgram program, RelCollation inputCollation) {
        return adjustCollationForProgram(builder, program, inputCollation, false);
    }

    /**
     * Convert a collation into a new collation which column indexes are adjusted for a possible projection.
     * Adopted from the RexProgram.deduceCollations
     *
     * @param program - New program
     * @param inputCollation - index collation
     * @param allowCast
     * @return RelCollation
     */
    public static RelCollation adjustCollationForProgram(
            RexBuilder builder, RexProgram program, RelCollation inputCollation, boolean allowCast) {
        Preconditions.checkNotNull("Program is null", program);
        int sourceCount = program.getInputRowType().getFieldCount();
        List<RexLocalRef> refs = program.getProjectList();
        int[] targets = new int[sourceCount];
        Arrays.fill(targets, -1);
        for (int i = 0; i < refs.size(); i++) {
            final RexLocalRef ref = refs.get(i);
            int source = ref.getIndex();
            if (source > sourceCount && allowCast && source < program.getExprCount()) {
                // Possibly CAST expression
                final RexNode expr = program.getExprList().get(source);
                final int adjustedSource = getReferenceOrAccessIndex(program, expr, true);
                if (adjustedSource != -1) {
                    source = adjustedSource;
                }
            }
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
                fieldCollations.add(fieldCollation.copy(target));
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


    /**
     * Return field index if a {@link #RexNode} is an input field or a local reference that points to a one. -1 otherwise
     * @param program
     * @param node
     * @param allowCast
     * @return
     */
    public static int getReferenceOrAccessIndex(RexProgram program, RexNode node, boolean allowCast) {
        if (allowCast && node.isA(SqlKind.CAST)){
            return getReferenceOrAccessIndex(program,
                    ((RexCall) node).getOperands().get(0),
                    false);
        } else if (node instanceof RexLocalRef && program != null) {
            return getReferenceOrAccessIndex(program,
                    program.getExprList().get(((RexLocalRef) node).getIndex()),
                    true);
        } else {
            return getReferenceOrAccessIndex(node, false);
        }
    }

    /**
     * Return TRUE if a join's expression is a field equivalence expression (field1 = field2)
     * where field1 and field2 represent fields from inner and outer tables
     * or a conjunction of field equivalence expressions
     *
     * @param expression
     * @param numLhsFields for joins the count of a outer table fields
     * @return
     */
    public static boolean isFieldEquivalenceExpr(RexNode expression, int numLhsFields) {
        boolean isEquivExpr = true;
        for (RexNode expr : RelOptUtil.conjunctions(expression)) {
            if (!expr.isA(SqlKind.EQUALS)) {
                return false;
            }
            Preconditions.checkState(expr instanceof RexCall);
            final RexCall call = (RexCall) expr;
            if (numLhsFields != -1) {
                final int index0 = getReferenceOrAccessIndex(call.operands.get(0), true);
                final int index1 = getReferenceOrAccessIndex(call.operands.get(1), true);
                isEquivExpr = index0 >= 0 && index1 >= 0 &&
                        ((index0 < numLhsFields && index1 >= numLhsFields) ||
                        (index1 < numLhsFields && index0 >= numLhsFields));
            } else {
                isEquivExpr = RexUtil.isReferenceOrAccess(call.operands.get(0), true) &&
                        RexUtil.isReferenceOrAccess(call.operands.get(1), true);
            }
            if (! isEquivExpr) {
                break;
            }
        }
        return isEquivExpr;
    }

    private static int getReferenceOrAccessIndex(RexNode node, boolean allowCast) {
        if (node instanceof RexInputRef) {
            return ((RexInputRef)node).getIndex();
        } else if (node instanceof RexFieldAccess) {
            return ((RexFieldAccess) node).getField().getIndex();
        } else if (allowCast && node.isA(SqlKind.CAST)){
            RexCall call = (RexCall) node;
            return getReferenceOrAccessIndex(call.operands.get(0), false);
        }
        return -1;
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
        Preconditions.checkArgument(topProgram != null,
                "top program cannot be null when merging programs");
        final RexProgram mergedProgram;
        if (bottomProgram != null) {
            // Merge two programs topProgram / bottomProgram into a new merged program
            mergedProgram = RexProgramBuilder.mergePrograms(topProgram, bottomProgram, rexBuilder);
        } else {
            mergedProgram = topProgram;
        }
        return mergedProgram;
    }

    /**
     * For a given join's field equivalence expression return a collection of pairs where each pair
     * represents field's indexes for outer and inner tables.
     * Each pair from the collection represents a tuple of outer and inner fields indexes for each equivalence
     * sub-expression
     *
     * @param expression
     * @param numLhsFields for joins the count of a outer table fields
     * @return
     */
    public static List<Pair<Integer, Integer>> extractFieldIndexes(RexNode expression, int numLhsFields) {
        Preconditions.checkState(isFieldEquivalenceExpr(expression, numLhsFields));
        return RelOptUtil.conjunctions(expression).stream()
                .map(expr -> {
                    assert(expr instanceof RexCall);
                    final RexCall call = (RexCall) expr;
                    final int index0 = getReferenceOrAccessIndex(call.operands.get(0), true);
                    final int index1 = getReferenceOrAccessIndex(call.operands.get(1), true);
                    assert index0 >= 0 && index1 >= 0;
                    return index0 < numLhsFields ? Pair.of(index0, index1) : Pair.of(index1, index0);
                })
                .collect(Collectors.toList());
    }

}
