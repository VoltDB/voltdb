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

package org.voltdb.calciteadapter.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPAggregateScanMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPCalcAggregateMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPCalcScanMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPLimitMergeExchangeMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPLimitScanMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPLimitSortMergeRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLAggregateRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLCalcRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLSortRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLTableScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPAggregateRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPCalcExchangeTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPCalcRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPCalcScanToIndexRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPLimitExchangeTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPLimitRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSeqScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortCalcTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortConvertRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortExchangeTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortIndexScanRemoveRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortScanToIndexRule;

public class VoltDBRules {

    public static RelOptRule[] VOLCANO_RULES_0 = {
            // Calcite's Logical Rules
            CalcMergeRule.INSTANCE
            , FilterCalcMergeRule.INSTANCE
            , FilterToCalcRule.INSTANCE
            , ProjectCalcMergeRule.INSTANCE
            , ProjectToCalcRule.INSTANCE
            , ProjectMergeRule.INSTANCE
            , FilterProjectTransposeRule.INSTANCE

            // VoltDBLogical Conversion Rules
            , VoltDBLSortRule.INSTANCE
            , VoltDBLTableScanRule.INSTANCE
            , VoltDBLCalcRule.INSTANCE
            , VoltDBLAggregateRule.INSTANCE
    };

    public static RelOptRule[] VOLCANO_RULES_1 = {
            // Calcite's Rules
            AbstractConverter.ExpandConversionRule.INSTANCE
            , CalcMergeRule.INSTANCE

            // VoltDB Logical Rules

            // VoltDB Physical Rules
            , VoltDBPSortScanToIndexRule.INSTANCE
            , VoltDBPCalcScanToIndexRule.INSTANCE
            , VoltDBPSortCalcTransposeRule.INSTANCE
            , VoltDBPSortIndexScanRemoveRule.INSTANCE

//            , VoltDBCalcMergeRule.INSTANCE

            // VoltDB Physical Conversion Rules
            , VoltDBPCalcRule.INSTANCE
            , VoltDBPSeqScanRule.INSTANCE
            , VoltDBPSortRule.INSTANCE
            , VoltDBPSortConvertRule.INSTANCE_NONE
            , VoltDBPSortConvertRule.INSTANCE_VOLTDB
            , VoltDBPLimitRule.INSTANCE
            , VoltDBPAggregateRule.INSTANCE

            // Exchage Rules
            , VoltDBPCalcExchangeTransposeRule.INSTANCE
            , VoltDBPLimitExchangeTransposeRule.INSTANCE
            , VoltDBPSortExchangeTransposeRule.INSTANCE
    };

    public static RelOptRule[] INLINING_RULES = {
            // VoltDB Inline Rules. The rules order declaration
            // has to match the order of rels from a real plan produced by the previous stage.

            VoltDBPLimitMergeExchangeMergeRule.INSTANCE_1
            , VoltDBPLimitMergeExchangeMergeRule.INSTANCE_2
            , VoltDBPCalcAggregateMergeRule.INSTANCE
            , VoltDBPCalcScanMergeRule.INSTANCE
            , VoltDBPLimitSortMergeRule.INSTANCE
            , VoltDBPAggregateScanMergeRule.INSTANCE
            , VoltDBPLimitScanMergeRule.INSTANCE_1
            , VoltDBPLimitScanMergeRule.INSTANCE_2


    };

    public static Program VOLCANO_PROGRAM_0 = Programs.ofRules(
            VOLCANO_RULES_0
            );

    public static Program VOLCANO_PROGRAM_1 = Programs.ofRules(
            VOLCANO_RULES_1
            );

    public static Program INLINING_PROGRAM = Programs.ofRules(
            INLINING_RULES
            );

    public static Program[] getVolcanoPrograms() {
        return new Program[] {VOLCANO_PROGRAM_0, VOLCANO_PROGRAM_1};
    }

    public static Program getInliningProgram() {
        return INLINING_PROGRAM;
    }

}
