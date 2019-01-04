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

package org.voltdb.newplanner.rules;

import com.google_voltpatches.common.collect.ImmutableCollection;
import com.google_voltpatches.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.voltdb.newplanner.rules.inlining.VoltDBPLimitScanMergeRule;
import org.voltdb.newplanner.rules.inlining.VoltPhysicalCalcScanMergeRule;
import org.voltdb.newplanner.rules.logical.MPJoinQueryFallBackRule;
import org.voltdb.newplanner.rules.logical.MPQueryFallBackRule;
import org.voltdb.newplanner.rules.logical.VoltDBLAggregateRule;
import org.voltdb.newplanner.rules.logical.VoltDBLCalcRule;
import org.voltdb.newplanner.rules.logical.VoltDBLJoinRule;
import org.voltdb.newplanner.rules.logical.VoltDBLSortRule;
import org.voltdb.newplanner.rules.logical.VoltDBLTableScanRule;
import org.voltdb.newplanner.rules.physical.VoltDBPAggregateRule;
import org.voltdb.newplanner.rules.physical.VoltDBPCalcRule;
import org.voltdb.newplanner.rules.physical.VoltDBPJoinRule;
import org.voltdb.newplanner.rules.physical.VoltDBPLimitRule;
import org.voltdb.newplanner.rules.physical.VoltDBPSeqScanRule;
import org.voltdb.newplanner.rules.physical.VoltDBPSortConvertRule;

/**
 * Returns RuleSet for concrete planner phase.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public enum PlannerPhase {
    CALCITE_LOGICAL("Calcite logical rules") {
        public RuleSet getRules() {
            return getCalciteLogicalRules();
        }
    },

    VOLT_LOGICAL("VoltDB logical rules") {
        public RuleSet getRules() {
            return getVoltLogicalRules();
        }
    },

    LOGICAL("Calcite and VoltDB logical rules") {
        public RuleSet getRules() {
            return mergedRuleSets(getCalciteLogicalRules(),
                    getVoltLogicalRules());
        }
    },
    // always use a HEP_BOTTOM_UP planner for MP_FALLBACK, it is match order sensitive.
    MP_FALLBACK("MP query fallback rules") {
        public RuleSet getRules() {
            return getVoltMPFallbackRules();
        }
    },

    PHYSICAL_CONVERSION("VoltDBPhysical Conversion Rules") {
        public RuleSet getRules() {
            return getVoltPhysicalConversionRules();
        }
    },
    INLINING("Inlining rules") {
        public RuleSet getRules() {
            return getInliningRules();
        }
    };

    public final String description;

    PlannerPhase(String description) {
        this.description = description;
    }

    private static final RuleSet s_CalciteLogicalRules;
    private static final RuleSet s_VoltLogicalRules;
    private static final RuleSet s_VoltMPFallbackRules;
    private static final RuleSet s_VoltPhysicalConversionRules;
    private static final RuleSet s_InliningRules;

    static {
        s_CalciteLogicalRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        CalcMergeRule.INSTANCE,
                        FilterCalcMergeRule.INSTANCE,
                        FilterToCalcRule.INSTANCE,
                        ProjectCalcMergeRule.INSTANCE,
                        ProjectToCalcRule.INSTANCE,
                        ProjectMergeRule.INSTANCE,
                        FilterProjectTransposeRule.INSTANCE,
                        FilterJoinRule.FILTER_ON_JOIN,
                        FilterJoinRule.JOIN
                ).build());
        s_VoltLogicalRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        VoltDBLSortRule.INSTANCE,
                        VoltDBLTableScanRule.INSTANCE,
                        VoltDBLCalcRule.INSTANCE,
                        VoltDBLAggregateRule.INSTANCE,
                        VoltDBLJoinRule.INSTANCE
                ).build());
        s_VoltMPFallbackRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        MPQueryFallBackRule.INSTANCE,
                        MPJoinQueryFallBackRule.INSTANCE
                ).build());
        s_VoltPhysicalConversionRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        VoltDBPCalcRule.INSTANCE,
                        VoltDBPSeqScanRule.INSTANCE,
                        VoltDBPSortConvertRule.INSTANCE_VOLTDB,
                        VoltDBPLimitRule.INSTANCE,
                        VoltDBPAggregateRule.INSTANCE,
                        VoltDBPJoinRule.INSTANCE
                ).build());
        s_InliningRules = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
                .add(
                        VoltPhysicalCalcScanMergeRule.INSTANCE,
                        VoltDBPLimitScanMergeRule.INSTANCE_2
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
     * {@link org.voltdb.calciteadapter.rel.logical.VoltDBLRel#VOLTDB_LOGICAL}.
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

    static RuleSet getVoltMPFallbackRules() {
        return s_VoltMPFallbackRules;
    }

    static RuleSet getInliningRules() {
        return s_InliningRules;
    }
}
