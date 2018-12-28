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

package org.voltdb.plannerv2.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.voltdb.plannerv2.rules.logical.VoltLAggregateRule;
import org.voltdb.plannerv2.rules.logical.VoltLCalcRule;
import org.voltdb.plannerv2.rules.logical.VoltLJoinRule;
import org.voltdb.plannerv2.rules.logical.VoltLSortRule;
import org.voltdb.plannerv2.rules.logical.VoltLTableScanRule;
import org.voltdb.plannerv2.rules.physical.VoltPAggregateRule;
import org.voltdb.plannerv2.rules.physical.VoltPCalcRule;
import org.voltdb.plannerv2.rules.physical.VoltPJoinRule;
import org.voltdb.plannerv2.rules.physical.VoltPLimitRule;
import org.voltdb.plannerv2.rules.physical.VoltPSeqScanRule;
import org.voltdb.plannerv2.rules.physical.VoltPSortConvertRule;

import com.google_voltpatches.common.collect.ImmutableCollection;
import com.google_voltpatches.common.collect.ImmutableSet;

/**
 * Returns RuleSet for concrete planner phase.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public enum PlannerPhase {
    CALCITE_LOGICAL("Calcite logical rules") {
        @Override
        public RuleSet getRules() {
            return getCalciteLogicalRules();
        }
    },

    VOLT_LOGICAL("VoltDB logical rules") {
        @Override
        public RuleSet getRules() {
            return getVoltLogicalRules();
        }
    },

    LOGICAL("Calcite and VoltDB logical rules") {
        @Override
        public RuleSet getRules() {
            return mergedRuleSets(getCalciteLogicalRules(),
                    getVoltLogicalRules());
        }
    },

    PHYSICAL_CONVERSION("VoltDBPhysical Conversion Rules") {
        @Override
        public RuleSet getRules() {
            return getVoltPhysicalConversionRules();
        }
    };

    public final String description;

    PlannerPhase(String description) {
        this.description = description;
    }

    private static final RuleSet s_CalciteLogicalRules;
    private static final RuleSet s_VoltLogicalRules;
    private static final RuleSet s_VoltPhysicalConversionRules;

    static {
        s_CalciteLogicalRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        CalcMergeRule.INSTANCE,
                        FilterCalcMergeRule.INSTANCE,
                        FilterToCalcRule.INSTANCE,
                        ProjectCalcMergeRule.INSTANCE,
                        ProjectToCalcRule.INSTANCE,
                        ProjectMergeRule.INSTANCE,
                        FilterProjectTransposeRule.INSTANCE
                ).build());
        s_VoltLogicalRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        VoltLSortRule.INSTANCE,
                        VoltLTableScanRule.INSTANCE,
                        VoltLCalcRule.INSTANCE,
                        VoltLAggregateRule.INSTANCE,
                        VoltLJoinRule.INSTANCE
                ).build());
        s_VoltPhysicalConversionRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        VoltPCalcRule.INSTANCE,
                        VoltPSeqScanRule.INSTANCE,
                        VoltPSortConvertRule.INSTANCE_VOLTDB,
                        VoltPLimitRule.INSTANCE,
                        VoltPAggregateRule.INSTANCE,
                        VoltPJoinRule.INSTANCE
                ).build());
    }

    public abstract RuleSet getRules();

    static RuleSet mergedRuleSets(RuleSet... ruleSets) {
        final ImmutableCollection.Builder<RelOptRule> relOptRuleSetBuilder = ImmutableSet.builder();
        for (final RuleSet ruleSet : ruleSets) {
            for (final RelOptRule relOptRule : ruleSet) {
                relOptRuleSetBuilder.add(relOptRule);
            }
        }
        return RuleSets.ofList(relOptRuleSetBuilder.build());
    }

    // Calcite's Logical Rules
    static RuleSet getCalciteLogicalRules() {
        /*
        LogicalCalc
        calc = proj & filter
        A relational expression which computes project expressions and also filters.
        This relational expression combines the functionality of LogicalProject and LogicalFilter.
        It should be created in the later stages of optimization,
        by merging consecutive LogicalProject and LogicalFilter nodes together.

        The following rules relate to LogicalCalc:

        FilterToCalcRule creates this from a LogicalFilter
        ProjectToCalcRule creates this from a LogicalFilter
        FilterCalcMergeRule merges this with a LogicalFilter
        ProjectCalcMergeRule merges this with a LogicalProject
        CalcMergeRule merges two LogicalCalcs
        */
        return s_CalciteLogicalRules;
    }

    /**
     * VoltDBLogical Conversion Rules.
     * <p>
     * Use to convert the convention from {@link org.apache.calcite.plan.Convention#NONE} to
     * {@link org.voltdb.plannerv2.rel.logical.VoltLogicalRel#CONVENTION}.
     * <p>
     * <b>Why?</b>
     * <p>
     * {@link org.apache.calcite.plan.Convention#NONE} is not implementable, and has to be transformed to
     * something else in order to be implemented. Otherwise, the Volcano Planner will throw a
     * CannotPlanException.
     *
     * @return
     */
    static RuleSet getVoltLogicalRules() {
        return s_VoltLogicalRules;
    }

    static RuleSet getVoltPhysicalConversionRules() {
        return s_VoltPhysicalConversionRules;
    }
}
