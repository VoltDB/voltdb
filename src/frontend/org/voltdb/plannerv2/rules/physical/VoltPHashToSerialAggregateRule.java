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

package org.voltdb.plannerv2.rules.physical;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalHashAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSerialAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableIndexScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableSequentialScan;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

import com.google_voltpatches.common.base.Preconditions;


/**
 * VoltDB physical rule that transform {@link VoltPhysicalHashAggregate} to {@link VoltPhysicalSerialAggregate}
 *   VoltPhysicalHashAggregate                        to VoltPhysicalSerialAggregate
 *     (VoltPhysicalCacl) VoltPhysicalTableIndexScan       (VoltPhysicalCacl) VoltPhysicalTableIndexScan
 *
 *   VoltPhysicalHashAggregate                               to VoltPhysicalSerialAggregate
 *     (VoltPhysicalCacl) VoltPhysicalTableSequentialScan         (VoltPhysicalCacl) VoltPhysicalTableIndexScan
 *
 * Given a table T with indexes (A, B),  (A, B, C), (A, C, B) and (C, A, B) and a query
 *  select max(B), a, b from T group by b ,a;
 *
 * The first index (A,B) would provide a right ordering for the SerialAggregate -
 *      GROUP BY columns (B,A) match the all index columns
 * The second index (A, B, C) also works because ORDER BY columns again match the first two index columns
 * The (A, C, B) index won't because the GROUP BY columns B and A
 *      match the first and the third index columns
 * The (C, A, B) index is also in-eligible because the first index column is not part of the CROUP BY columns.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPHashToSerialAggregateRule extends RelOptRule {

    enum AggrMatchType {
        AGGR_INDEX_SCAN,
        AGGR_CALC_INDEX_SCAN,
        AGGR_SEQ_SCAN,
        AGGR_CALC_SEQ_SCAN
    }
    /**
     * Predicate to stop LIMIT_EXCHANGE to match on LIMIT / EXCHANGE / LIMIT
    */
    private static final Predicate<VoltPhysicalHashAggregate> HAS_GROUP_BY_PREDICATE = aggr -> !aggr.getGroupSet().isEmpty();

    public static final VoltPHashToSerialAggregateRule INSTANCE_AGGR_INDEX_SCAN =
            new VoltPHashToSerialAggregateRule(
                    operandJ(VoltPhysicalHashAggregate.class, null, HAS_GROUP_BY_PREDICATE,
                            operand(VoltPhysicalTableIndexScan.class, none())),
                    AggrMatchType.AGGR_INDEX_SCAN);

    public static final VoltPHashToSerialAggregateRule INSTANCE_AGGR_CALC_INDEX_SCAN =
            new VoltPHashToSerialAggregateRule(
                    operandJ(VoltPhysicalHashAggregate.class, null, HAS_GROUP_BY_PREDICATE,
                            operand(VoltPhysicalCalc.class,
                                    operand(VoltPhysicalTableIndexScan.class, none()))),
                    AggrMatchType.AGGR_CALC_INDEX_SCAN);

    public static final VoltPHashToSerialAggregateRule INSTANCE_AGGR_SEQ_SCAN =
            new VoltPHashToSerialAggregateRule(
                    operandJ(VoltPhysicalHashAggregate.class, null, HAS_GROUP_BY_PREDICATE,
                            operand(VoltPhysicalTableSequentialScan.class, none())),
                    AggrMatchType.AGGR_SEQ_SCAN);

    public static final VoltPHashToSerialAggregateRule INSTANCE_AGGR_CALC_SEQ_SCAN =
            new VoltPHashToSerialAggregateRule(
                    operandJ(VoltPhysicalHashAggregate.class, null, HAS_GROUP_BY_PREDICATE,
                            operand(VoltPhysicalCalc.class,
                                    operand(VoltPhysicalTableSequentialScan.class, none()))),
                    AggrMatchType.AGGR_CALC_SEQ_SCAN);

    private final AggrMatchType maggrMatchType;

    private VoltPHashToSerialAggregateRule(RelOptRuleOperand operand, AggrMatchType aggrMatchType) {
        super(operand, aggrMatchType.toString());
        maggrMatchType = aggrMatchType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        switch (maggrMatchType) {
        case AGGR_INDEX_SCAN:
        case AGGR_CALC_INDEX_SCAN:
            aggrIndexScan(call);
            break;
        case AGGR_SEQ_SCAN:
        case AGGR_CALC_SEQ_SCAN:
            aggrSeqScan(call);
            break;
            }
    }

    private void aggrIndexScan(RelOptRuleCall call) {
        final VoltPhysicalHashAggregate hashAggregate = call.rel(0);
        final VoltPhysicalTableIndexScan scan;
        final VoltPhysicalCalc calc;
        final RelNode aggregateInput;
        final RelCollation adjustedIndexCollation;
        if (maggrMatchType == AggrMatchType.AGGR_INDEX_SCAN) {
            scan = call.rel(1);
            calc = null;
            aggregateInput = scan;
            adjustedIndexCollation = scan.getIndexCollation();
        } else {
            scan = call.rel(2);
            calc = call.rel(1);
            aggregateInput = calc;
            adjustedIndexCollation = VoltRexUtil.adjustCollationForProgram(
                    calc.getCluster().getRexBuilder(), calc.getProgram(), scan.getIndexCollation());
        }

        final ImmutableBitSet groupBy = hashAggregate.getGroupSet();
        if (!collationMatchesGroupBy(adjustedIndexCollation, groupBy)) {
            return;
        }

        final RelNode serialAggregate = new VoltPhysicalSerialAggregate(
                hashAggregate.getCluster(),
                hashAggregate.getTraitSet(),
                aggregateInput,
                hashAggregate.indicator,
                hashAggregate.getGroupSet(),
                hashAggregate.getGroupSets(),hashAggregate.getAggCallList(),
                null,
                false);
        call.transformTo(serialAggregate);
    }

    /**
     * A Collation matches a set of GROUP BY columns if it covers all GROUP BY columns
     * starting from collations index 0 without gaps.
     *
     * @param collation
     * @param groupBy
     * @return
     */
    private boolean collationMatchesGroupBy(RelCollation collation, ImmutableBitSet groupBy) {
        List<RelFieldCollation> collationFields = collation.getFieldCollations();
        int[] targets = new int[collationFields.size()];
        Arrays.fill(targets, -1);

        for(int groupByIdx : groupBy.asList()) {
            RelFieldCollation collationField = new RelFieldCollation(groupByIdx);
            int index = collationFields.indexOf(collationField);
            if (index == -1) {
                // Group By filed is not part of the index
                return false;
            }
            targets[index] = 0;
        }
        // All GROUP BY fields are covered by index's ones
        Preconditions.checkState(groupBy.asList().size() <= targets.length);
        int firstMiss = Arrays.asList(targets).indexOf(-1);
        return firstMiss == -1 || firstMiss > groupBy.size();
    }

    private void aggrSeqScan(RelOptRuleCall call) {
        final VoltPhysicalHashAggregate hashAggregate = call.rel(0);
        final ImmutableBitSet groupBy = hashAggregate.getGroupSet();
        final VoltPhysicalTableSequentialScan scan;
        final VoltPhysicalCalc calc;
        if (maggrMatchType == AggrMatchType.AGGR_SEQ_SCAN) {
            scan = call.rel(1);
            calc = null;
        } else {
            scan = call.rel(2);
            calc = call.rel(1);
        }

        RelNode equivRel = null;
        final Map<RelNode, RelNode> equivMap = new HashMap<>();

        final Table catTable = scan.getVoltTable().getCatalogTable();
        for (Index index : catTable.getIndexes()) {
            RelCollation indexCollation = null;
            try {
                indexCollation = VoltRexUtil.createIndexCollation(index,
                        catTable,
                        scan.getCluster().getRexBuilder(),
                        scan.getProgram());
            } catch (JSONException ignored) { }
            Preconditions.checkNotNull(indexCollation);
            // Adjust index collation for a possible cacl
            if (calc != null) {
                indexCollation = VoltRexUtil.adjustCollationForProgram(
                        calc.getCluster().getRexBuilder(), calc.getProgram(), indexCollation);
            }
            if (collationMatchesGroupBy(indexCollation, groupBy)) {
                final AccessPath accessPath = new AccessPath(
                        index,
                        // With no index expression, the lookup type will be ignored and
                        // the sort direction will determine the scan direction;
                        IndexLookupType.EQ, SortDirectionType.INVALID, false);
                final RelNode indexScan = new VoltPhysicalTableIndexScan(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        scan.getTable(), scan.getVoltTable(), scan.getProgram(),
                        index, accessPath, scan.getLimitRexNode(), scan.getOffsetRexNode(),
                        scan.getAggregateRelNode(),
                        scan.getPreAggregateRowType(),
                        scan.getPreAggregateProgram(),
                        indexCollation,
                        false);
                final RelNode aggregateInput;
                if (calc == null) {
                    aggregateInput = indexScan;
                } else {
                    aggregateInput = calc.copy(calc.getTraitSet(), indexScan, calc.getProgram());
                }
                final RelNode serialAggregate = new VoltPhysicalSerialAggregate(
                        hashAggregate.getCluster(),
                        hashAggregate.getTraitSet(),
                        aggregateInput,
                        hashAggregate.indicator,
                        hashAggregate.getGroupSet(),
                        hashAggregate.getGroupSets(),hashAggregate.getAggCallList(),
                        null,
                        false);
                if (equivRel == null) {
                    equivRel = serialAggregate;
                } else {
                    equivMap.put(serialAggregate, hashAggregate);
                }
            }
        }
        if (equivRel != null) {
            call.transformTo(equivRel, equivMap);
        }
    }

}
