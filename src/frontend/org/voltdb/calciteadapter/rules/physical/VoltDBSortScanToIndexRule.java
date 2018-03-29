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

package org.voltdb.calciteadapter.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltcore.utils.Pair;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.calciteadapter.rel.physical.VoltDBCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBSort;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableSeqScan;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class VoltDBSortScanToIndexRule extends RelOptRule {

    public static final VoltDBSortScanToIndexRule INSTANCE_SORT_SCAN = new VoltDBSortScanToIndexRule();
    public static final VoltDBSortScanToIndexRule INSTANCE_SORT_CALC_SCAN = new VoltDBSortScanToIndexRule(1);

    private VoltDBSortScanToIndexRule() {
        super(operand(VoltDBSort.class,
                operand(VoltDBTableSeqScan.class, none())), "VoltDBSortScanToIndexRule");
    }

    private VoltDBSortScanToIndexRule(int dummy) {
        super(operand(VoltDBSort.class,
                operand(VoltDBCalc.class,
                        operand(VoltDBTableSeqScan.class, none()))), "VoltDBSortCalcScanToIndexRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltDBSort sort = call.rel(0);
        VoltDBTableSeqScan scan = (call.rels.length == 2) ?
                call.rel(1) : call.rel(2);
        VoltDBTable table = scan.getVoltDBTable();
        assert(table != null);
        boolean matches = !table.getCatTable().getIndexes().isEmpty() &&
                !sort.getCollation().getFieldCollations().isEmpty() &&
                sort.fetch == null &&
                sort.offset == null;
        return matches;
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBSort sort = call.rel(0);
        RelCollation sortCollation = sort.getCollation();

        VoltDBTableSeqScan scan = (call.rels.length == 2) ?
                call.rel(1) : call.rel(2);
        Table catTable = scan.getVoltDBTable().getCatTable();

        RexBuilder builder = scan.getCluster().getRexBuilder();
        RexProgram program = getProgram(call);
        assert(program != null);

        for (Index index : catTable.getIndexes()) {
            RelCollation indexCollation =
                    VoltDBRexUtil.createIndexCollation(index, catTable, builder, program);
            Pair<SortDirectionType, Boolean> collationsCompatibility =
                    VoltDBRexUtil.areCollationsCompartible(sortCollation, indexCollation);
            //@TODO Cutting corner here. Should probably use something similar to
            // the SubPlanAssembler.WindowFunctionScoreboard
            if (SortDirectionType.INVALID != collationsCompatibility.getFirst()) {
                AccessPath accessPath = new AccessPath(
                        index,
                        // With no index expression, the lookup type will be ignored and
                        // the sort direction will determine the scan direction;
                        IndexLookupType.EQ,
                        collationsCompatibility.getFirst(),
                        true);
                RelNode transformedNode = buildTransformedNode(call, sortCollation, index, accessPath);
                call.transformTo(transformedNode);
            }
        }
    }

    RexProgram getProgram(RelOptRuleCall call) {
        VoltDBTableSeqScan scan = null;
        VoltDBCalc calc = null;
        if (call.rels.length == 2) {
            scan = call.rel(1);
            return scan.getProgram();
        } else {
            calc = call.rel(1);
            scan = call.rel(2);
            RexProgram calcProgram = calc.getProgram();
            RexProgram scanProgram = scan.getProgram();
            // Merge two programs
            RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
            RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
                    calcProgram,
                    scanProgram,
                    rexBuilder);
            return mergedProgram;
        }
    }

    RelNode buildTransformedNode(
            RelOptRuleCall call,
            RelCollation sortCollation,
            Index index,
            AccessPath accessPath) {
        VoltDBTableSeqScan scan = null;
        VoltDBCalc calc = null;
        if (call.rels.length == 2) {
            scan = call.rel(1);
        } else {
            calc = call.rel(1);
            scan = call.rel(2);
        }
        VoltDBTableIndexScan indexScan = new VoltDBTableIndexScan(
                scan.getCluster(),
                scan.getTraitSet(),
                scan.getTable(),
                scan.getVoltDBTable(),
                scan.getProgram(),
                index,
                accessPath,
                scan.getLimitRexNode(),
                scan.getOffsetRexNode());

        if (calc == null) {
            return indexScan;
        } else {
            // Copy calc with the sort collation added
            Calc newCalc = calc.copy(
                    calc.getTraitSet().plus(sortCollation),
                    indexScan,
                    calc.getProgram());
            return newCalc;
        }
    }

}
