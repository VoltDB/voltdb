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

import java.util.*;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
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

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.Lists;

public class VoltRexUtil {

    private static final Map<RelFieldCollation.Direction, SortDirectionType> COLLATION_MAP =
            new HashMap<RelFieldCollation.Direction, SortDirectionType>() {{
                put(RelFieldCollation.Direction.STRICTLY_ASCENDING, SortDirectionType.ASC);
                put(RelFieldCollation.Direction.ASCENDING, SortDirectionType.ASC);
                put(RelFieldCollation.Direction.STRICTLY_DESCENDING, SortDirectionType.DESC);
                put(RelFieldCollation.Direction.DESCENDING, SortDirectionType.DESC);
                put(RelFieldCollation.Direction.CLUSTERED, SortDirectionType.INVALID);
            }};

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
        final List<AbstractExpression> voltExprList = Lists.transform(collationFieldExps, RexConverter::convert);
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
        // HASH indexes are no longer supported, only an INVALID ones
        // can't be used to provide ordering
        if (IndexType.INVALID != IndexType.get(index.getType())) {
            // Convert index collation to take the program into an account
            return VoltRexUtil.adjustCollationForProgram(builder, program,
                    RelCollations.of(IndexUtil.getIndexCollationFields(catTable, index, program)));
        } else {
            return RelCollations.EMPTY;
        }
    }


    private static List<Pair<Integer, SortDirectionType>> convertCollation(RelCollation collation) {
        return Lists.transform(collation.getFieldCollations(),
                col -> Pair.of(col.getFieldIndex(),
                        col.getDirection() == RelFieldCollation.Direction.ASCENDING ?
                                SortDirectionType.ASC : SortDirectionType.DESC));
    }

    /**
     * Compare sort and index collations to determine whether they are compatible or not
     *  - both collations are not empty
     *  - the collation direction is the same for all fields for each collation
     *  - all fields from the sort collation have matching fields from the index collation
     *    in the same order.
     *  - if there is not a matching field from the index collation for a given field from the
     *    sort collation there must be an equality expression for this index field to constraint
     *    its value to some CONST literal value.
     *    FOR EACH ENTRY IN THE SORT COLLATION
     *      THERE IS AN ENTRY FROM THE INDEX COLLATION IN WITH THE SAME INDEX
     *          POINTING TO THE SAME FIELD
     *      OR IF THEY POINT TO A DIFFERENT FIELDS THEN THERE MUST BE
     *          AN EQUALITY EXPRESSION IN PROGRAM THAT CONSTRAINTS THE INDEX FIELD VALUE
     *    For example,
     *      SELECT * FROM R WHERE I = 5 ORDER BY II;
     *      SELECT * FROM R ORDER BY I, II;
     *    If there is an index (I, II) its collation would be compatible with sort's ones.
     *
     * If collations are compatible return the sort direction (ASC, DESC) or INVALID if they are not.
     *
     * @param sortCollation - Sort Collation
     * @param indexCollation - Index Collation
     * @param indexProgram - Index Scan's Program
     * @return SortDirectionType - Sort Direction
     */
    public static SortDirectionType areCollationsCompatible(
            RelCollation sortCollation, RelCollation indexCollation, RexProgram indexProgram) {
        if (sortCollation == RelCollations.EMPTY || indexCollation == RelCollations.EMPTY) {
            return SortDirectionType.INVALID;
        }

        final List<RelFieldCollation> sortCollationFields = sortCollation.getFieldCollations();
        final List<RelFieldCollation> indexCollationFields = indexCollation.getFieldCollations();
        if (indexCollationFields.size() < sortCollationFields.size()) {
            return SortDirectionType.INVALID;
        }
        // Resolve all local references in the index's condition
        RexNode programCondition = indexProgram.getCondition();
        if (programCondition != null)  {
            programCondition = indexProgram.expandLocalRef((RexLocalRef) programCondition);
        }
        // Decompose it by the AND
        List<RexNode> indexConditions = RelOptUtil.conjunctions(programCondition);
        assert(!sortCollationFields.isEmpty());
        final RelFieldCollation.Direction sortDirection = sortCollationFields.get(0).getDirection();
        assert sortDirection != RelFieldCollation.Direction.CLUSTERED;
        assert(!indexCollationFields.isEmpty());
        final RelFieldCollation.Direction indexDirection = indexCollationFields.get(0).getDirection();
        assert indexDirection != RelFieldCollation.Direction.CLUSTERED;
        int indexIdxStart = 0;
        for (RelFieldCollation sortCollationField : sortCollationFields) {
            boolean isCovered = false;
            for (int indexIdx = indexIdxStart; indexIdx < indexCollationFields.size(); ++indexIdx) {
                final RelFieldCollation indexField = indexCollationFields.get(indexIdx);
                if (sortDirection != sortCollationField.direction || indexDirection != indexField.direction) {
                    return SortDirectionType.INVALID;
                } else {
                    final int result = compareSortAndIndexCollationFields(sortCollationField, indexField, indexConditions);
                    if (result < 0) { // sort field and index field do not match
                        return SortDirectionType.INVALID;
                    } else if (result == 0) { // sort field and index field match
                        isCovered = true;
                        indexIdxStart = indexIdx + 1;
                        break;
                    }
                    // else sort field and index field do not match; but there is a condition indexField = CONST.
                    // Move to the next index collation field
                }
            }
            if (!isCovered) {
                return SortDirectionType.INVALID;
            }
        }
        return COLLATION_MAP.get(sortDirection);
    }

    /**
     * Compare Sort Collation field with Index Collation field
     *
     * @param sortCollationField
     * @param indexCollationField
     * @param indexConditions
     * @return -1 - Fields are incompatible (different)
     *          0 - Fields are same
     *          1 - Fields are different but the Index field is a part of an equality expression
     *              Index Filed = CONST literal
     */
    private static int compareSortAndIndexCollationFields(
            RelFieldCollation sortCollationField, RelFieldCollation indexCollationField, List<RexNode> indexConditions) {
        if (sortCollationField.getFieldIndex() != indexCollationField.getFieldIndex()) {
            // Fields are different. Looking for an equality expression that compares
            // the index field to a CONST literal
            final int indexFieldIndex = indexCollationField.getFieldIndex();
            return indexConditions.stream().anyMatch(expr -> {
                if (expr instanceof RexCall && expr.getKind() == SqlKind.EQUALS) {
                    final RexCall call = (RexCall) expr;
                    if (RexUtil.isConstant(call.operands.get(0))) {
                        return indexFieldIndex == getReferenceOrAccessIndex(call.operands.get(1), true);
                    } else if (RexUtil.isConstant(call.operands.get(1))) {
                        return indexFieldIndex == getReferenceOrAccessIndex(call.operands.get(0), true);
                    }
                }
                return false;
            }) ? 1 : -1;
        } else { // Fields are identical
            return 0;
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
        assert distColumnIndexes.size() == 1 : "VoltDB supports only one partition column per table";
        final int partitionIndex = distColumnIndexes.get(0);
        final List<Integer> newPartitionIndexes = new ArrayList<>(1);

        // Iterate over the project list to find a corresponding input reference
        // which index matches the partitioning column index from the input distribution
        // The index of this project will be the new index of a partitioning column
        for (int idx = 0; idx < program.getProjectList().size(); ++idx) {
            final int source = program.getProjectList().get(idx).getIndex();
            assert source < program.getExprList().size();
            final RexNode rexNode = program.getExprList().get(source);
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
        return Lists.transform(RelOptUtil.conjunctions(expression),
                expr -> {
                    assert(expr instanceof RexCall);
                    final RexCall call = (RexCall) expr;
                    final int index0 = getReferenceOrAccessIndex(call.operands.get(0), true);
                    final int index1 = getReferenceOrAccessIndex(call.operands.get(1), true);
                    assert index0 >= 0 && index1 >= 0;
                    return index0 < numLhsFields ? Pair.of(index0, index1) : Pair.of(index1, index0);
                });
    }

    /**
     * Extract partitioning object if any from a RelDistribution object
     * Since only integer, string, or byte array column can be a partitioning column
     * the partitioning value should be either Long or String
     *
     * @param dist
     * @return Partitioning object (Long or String) or null
     */
    public static Object extractPartitioningValue(RelDistribution dist) {
        RexNode partitioningNode = dist.getPartitionEqualValue();
        if (partitioningNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) partitioningNode;
            Comparable value = literal.getValue();
            if (value instanceof Number) {
                return ((Number) value).longValue();
            } else {
                try {
                    return RexLiteral.stringValue(literal);
                } catch(Exception ex) {
                }
            }
        }
        return null;
    }
}
