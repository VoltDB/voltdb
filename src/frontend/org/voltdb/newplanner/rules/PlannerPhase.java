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


import com.google_voltpatches.common.collect.ImmutableSet;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Returns RuleSet for concrete planner phase.
 */
public enum PlannerPhase {
    CALCITE_LOGICAL("Calcite logical rules") {
        public RuleSet getRules () {
            return getCalciteLogicalRules();
        }
    };
    public final String description;
    PlannerPhase(String description) {
        this.description = description;
    }

    public abstract RuleSet getRules();

    static RuleSet getCalciteLogicalRules() {
        final List<RelOptRule> ruleList = new ArrayList<>();
        ruleList.add(CalcMergeRule.INSTANCE);
        ruleList.add(FilterCalcMergeRule.INSTANCE);
        ruleList.add(FilterToCalcRule.INSTANCE);
        ruleList.add(ProjectCalcMergeRule.INSTANCE);
        ruleList.add(ProjectToCalcRule.INSTANCE);
        ruleList.add(ProjectMergeRule.INSTANCE);
        ruleList.add(FilterProjectTransposeRule.INSTANCE);

        return RuleSets.ofList(ImmutableSet.copyOf(ruleList));
    }
}
