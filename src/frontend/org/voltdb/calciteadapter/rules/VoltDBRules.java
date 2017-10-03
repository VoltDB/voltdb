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

package org.voltdb.calciteadapter.rules;

import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.voltdb.calciteadapter.rules.convert.VoltDBJoinRule;
import org.voltdb.calciteadapter.rules.convert.VoltDBProjectRule;
import org.voltdb.calciteadapter.rules.convert.VoltDBSortRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBCalcScanMergeRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBFilterSendTransposeRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBNLJToNLIJRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBProjectScanMergeRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBProjectSendTransposeRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBSeqToIndexScansRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBSortIndexScanMergeRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBSortSeqScanMergeRule;
import org.voltdb.calciteadapter.rules.rel.calcite.SortProjectTransposeRule;

public class VoltDBRules {
    //public static final ConverterRule PROJECT_RULE = new VoltDBProjectRule();
    //public static final RelOptRule PROJECT_SCAN_MERGE_RULE = new VoltDBProjectScanMergeRule();

    public static Program[] getProgram() {

        Program program0 = Programs.ofRules(
                CalcMergeRule.INSTANCE
                , FilterCalcMergeRule.INSTANCE
                , FilterToCalcRule.INSTANCE
                , ProjectCalcMergeRule.INSTANCE
                , ProjectToCalcRule.INSTANCE
                , ProjectMergeRule.INSTANCE
                , FilterProjectTransposeRule.INSTANCE
                , SortProjectTransposeRule.INSTANCE
                , ProjectJoinTransposeRule.INSTANCE

                // Joins
                , FilterJoinRule.FILTER_ON_JOIN
                , FilterJoinRule.JOIN
                , JoinCommuteRule.INSTANCE
                , JoinPushThroughJoinRule.LEFT
                , JoinPushThroughJoinRule.RIGHT

                , VoltDBCalcScanMergeRule.INSTANCE
//                , VoltDBCalcJoinMergeRule.INSTANCE
                , VoltDBProjectScanMergeRule.INSTANCE

                // Convert rules
                , VoltDBProjectRule.INSTANCE
                , VoltDBJoinRule.INSTANCE
                , VoltDBSortRule.INSTANCE

                , VoltDBSortIndexScanMergeRule.INSTANCE
                , VoltDBSortSeqScanMergeRule.INSTANCE
                , VoltDBSeqToIndexScansRule.INSTANCE
                , VoltDBProjectScanMergeRule.INSTANCE

                // Send Pull Up
                , VoltDBFilterSendTransposeRule.INSTANCE
                , VoltDBProjectSendTransposeRule.INSTANCE

                // Join Order
//              LoptOptimizeJoinRule.INSTANCE,
//              MultiJoinOptimizeBushyRule.INSTANCE,
//                , VoltDBJoinCommuteRule.INSTANCE

                , VoltDBNLJToNLIJRule.INSTANCE

                );

        Program program1 = Programs.ofRules(
                );


        return new Program[] {program0, program1};
//        Program metaProgram = Programs.sequence(
//                standardRules
//                , voltDBRules);//,
//        //pullUpSendProg,
//          //      voltDBConversionRules);
//
//        // We don't actually handle this.
//        //    VoltDBProjectJoinMergeRule.INSTANCE
//
//        return metaProgram;
    }
}