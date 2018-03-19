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

import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.voltdb.calciteadapter.rules.logical.VoltDBLogicalFilterRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLogicalProjectRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLogicalSortRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLogicalTableScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBFilterScanMergeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBFilterScanToIndexRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalFilterRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalProjectRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalSeqScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPhysicalSortRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBProjectScanMergeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBSortConvertRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBSortScanToIndexRule;

public class VoltDBRules {

    public static Program RULES_SET_1 = Programs.ofRules(
                // Calcite's Logical Rules
                CalcMergeRule.INSTANCE
                , FilterCalcMergeRule.INSTANCE
                , FilterToCalcRule.INSTANCE
                , ProjectCalcMergeRule.INSTANCE
                , ProjectToCalcRule.INSTANCE
                , ProjectMergeRule.INSTANCE
                , FilterProjectTransposeRule.INSTANCE
                , SortProjectTransposeRule.INSTANCE

                // VoltDBLogical Conversion Rules
                , VoltDBLogicalSortRule.INSTANCE
                , VoltDBLogicalProjectRule.INSTANCE
                , VoltDBLogicalFilterRule.INSTANCE
                , VoltDBLogicalTableScanRule.INSTANCE
                , VoltDBLogicalFilterRule.INSTANCE
                , VoltDBLogicalSortRule.INSTANCE
                );

    public static Program RULES_SET_2 = Programs.ofRules(
                // Calcite's Rules
                AbstractConverter.ExpandConversionRule.INSTANCE

                // VoltDB Logical Rules

                // VoltDB Physical Rules

                // VoltDB Physical Conversion Rules
                , VoltDBPhysicalProjectRule.INSTANCE
                , VoltDBPhysicalSeqScanRule.INSTANCE
                , VoltDBPhysicalFilterRule.INSTANCE
                , VoltDBPhysicalSortRule.INSTANCE
                , VoltDBSortConvertRule.INSTANCE
                , VoltDBSortScanToIndexRule.INSTANCE
                , VoltDBFilterScanToIndexRule.INSTANCE

                );

    public static Program RULES_SET_3 = Programs.ofRules(
                // VoltDB Inline Rules

                VoltDBProjectScanMergeRule.INSTANCE
                , VoltDBFilterScanMergeRule.INSTANCE

            );

    public static Program[] getProgram() {

        return new Program[] {RULES_SET_1, RULES_SET_2, RULES_SET_3};
    }
}