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

package org.voltdb.plannerv2.rules.logical;

import org.aeonbits.owner.util.Collections;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.voltcore.utils.Pair;
import org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltLogicalExchange;
import org.voltdb.plannerv2.rel.logical.VoltLogicalLimit;
import org.voltdb.plannerv2.rel.logical.VoltLogicalSort;

/**
 * Rule that fallback the processing of a multi-partition query without joins to
 * the legacy planner.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class MPQueryFallBackRule extends RelOptRule {

    public static final MPQueryFallBackRule INSTANCE = new MPQueryFallBackRule();

    private MPQueryFallBackRule() {
        super(operand(SingleRel.class, RelDistributions.ANY,
                some(operand(RelNode.class, any()))));
    }

    @Override public void onMatch(RelOptRuleCall call) {
        // NOTE:
        // This rule depends on the VoltLogicalCalc and the VoltLogicalTableScan nodes at the leaf level to
        // determine whether this query is SP or not first, then propagate this information upwards to the root.
        // Therefore, it can only be run using a Hep planner following the bottom-up order.
        // It will not work properly with Hep planners following other orders or Volcano planners.

        if (call.rel(0) instanceof Calc && call.rel(1) instanceof TableScan) {
            // If it is a VoltLogicalCalc / VoltLogicalTableScan pattern, check the filter and see if it can be run SP.
            // VoltLogicalTableScan gives the partitioning scheme information,
            // and the VoltLogicalCalc above it gives the information about the filters.
            final Calc calc = call.rel(0);
            final TableScan tableScan = call.rel(1);
            final Pair<Boolean, RexNode> r = RelDistributionUtils.isCalcScanSP(tableScan, calc);
            final RelDistribution tableDist = tableScan.getTable().getDistribution();
            call.transformTo(calc.copy(calc.getTraitSet().replace(tableDist.with(
                    tableDist.getType(),
                    RelDistributionUtils.adjustProjection(calc.getProgram(), tableDist.getKeys()),
                    r.getSecond(), r.getFirst())),
                    calc.getInputs()));
        } else {
            // Otherwise, propagate the DistributionTrait bottom up.
            final RelDistribution childDist =
                    call.rel(1).getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            if (childDist != RelDistributions.ANY) {
                final SingleRel node = call.rel(0);
                final RelDistribution dist;
                if (node instanceof Calc) {
                    dist = childDist.with(childDist.getType(),
                            RelDistributionUtils.adjustProjection(((Calc) node).getProgram(), childDist.getKeys()),
                            childDist.getPartitionEqualValue(),
                            childDist.getIsSP());
                } else {
                    dist = childDist;
                }
                // Nodes that require LogicalExchange for a multi-partitioned query-
                // Sort, Limit, Aggregate
                if (RelDistributions.SINGLETON.getType() != dist.getType() &&
                        dist.getPartitionEqualValue() == null) {
                    // Create a new multi partitioned SINGLETON distribution for the coordinator fragment
                    RelDistribution topDist = RelDistributions.SINGLETON.with(dist.getPartitionEqualValue(), false);
                    if (node instanceof VoltLogicalLimit ||
                            node instanceof VoltLogicalSort ||
                            node instanceof VoltLogicalAggregate) {
                        VoltLogicalExchange exchange = new VoltLogicalExchange(node.getCluster(),
                                node.getTraitSet().replace(dist), node.getInput(), dist);
                        // Transforming COUNT, AVG aggregates for MP queries would happen during the physical transformation phase
                        RelNode coordinatorLimit = node.copy(node.getTraitSet().replace(topDist), Collections.list(exchange));
                        call.transformTo(coordinatorLimit);
                    } else {
                        call.transformTo(node.copy(node.getTraitSet().replace(dist), node.getInputs()));
                    }
                } else {
                    call.transformTo(node.copy(node.getTraitSet().replace(dist), node.getInputs()));
                }
            }
        }
    }
}

