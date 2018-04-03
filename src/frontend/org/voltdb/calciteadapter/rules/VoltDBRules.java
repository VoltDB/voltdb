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
import org.voltdb.calciteadapter.rules.logical.VoltDBLogicalCalcRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLogicalSortRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLogicalTableScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBCalcMergeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBCalcScanMergeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBCalcScanToIndexRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBLimitScanMergeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBLimitSortMergeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalCalcRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalLimitRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalSeqScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalSortRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBSortCalcTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBSortConvertRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBSortIndexScanRemoveRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBSortScanToIndexRule;

public class VoltDBRules {

    public static Program RULES_SET_0 = Programs.ofRules(
            // Calcite's Logical Rules
            CalcMergeRule.INSTANCE
            , FilterCalcMergeRule.INSTANCE
            , FilterToCalcRule.INSTANCE
            , ProjectCalcMergeRule.INSTANCE
            , ProjectToCalcRule.INSTANCE
            , ProjectMergeRule.INSTANCE
            , FilterProjectTransposeRule.INSTANCE

            // VoltDBLogical Conversion Rules
            , VoltDBLogicalSortRule.INSTANCE
            , VoltDBLogicalTableScanRule.INSTANCE
            , VoltDBLogicalCalcRule.INSTANCE

            );

    public static Program RULES_SET_1 = Programs.ofRules(
            // Calcite's Rules
            AbstractConverter.ExpandConversionRule.INSTANCE

            // VoltDB Logical Rules

            // VoltDB Physical Rules
            , VoltDBSortScanToIndexRule.INSTANCE
            , VoltDBCalcScanToIndexRule.INSTANCE
            , VoltDBSortCalcTransposeRule.INSTANCE
            , VoltDBSortIndexScanRemoveRule.INSTANCE
//            , VoltDBCalcMergeRule.INSTANCE

            // VoltDB Physical Conversion Rules
            , VoltDBPhysicalCalcRule.INSTANCE
            , VoltDBPhysicalSeqScanRule.INSTANCE
            , VoltDBPhysicalSortRule.INSTANCE
            , VoltDBSortConvertRule.INSTANCE_NONE
            , VoltDBSortConvertRule.INSTANCE_VOLTDB
            , VoltDBPhysicalLimitRule.INSTANCE

            );

    public static Program RULES_SET_2 = Programs.ofRules(
            // VoltDB Inline Rules

            VoltDBCalcScanMergeRule.INSTANCE
            , VoltDBLimitScanMergeRule.INSTANCE
            , VoltDBLimitSortMergeRule.INSTANCE

            );

    public static Program[] getProgram() {

        return new Program[] {RULES_SET_0, RULES_SET_1, RULES_SET_2};
    }
}
