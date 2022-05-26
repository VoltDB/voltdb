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

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PlanSelector;

public class MicroOptimizationRunner {
    private static final VoltLogger m_logger = new VoltLogger("MICROOPTIMIZER");

    public enum Phases {
        DURING_PLAN_ASSEMBLY,
        AFTER_COMPLETE_PLAN_ASSEMBLY
    };

    // list all of the micro optimizations here
    static Map<Phases, List<MicroOptimization>> optimizations = new HashMap<>();

    static void addOptimization(MicroOptimization opt) {
        Phases phase = opt.getPhase();
        List<MicroOptimization> optlist = optimizations.get(phase);
        if (optlist == null) {
            optlist = new ArrayList<>();
            optimizations.put(phase, optlist);
        }
        optlist.add(opt);
    }
    static {
        // The orders here is important
        addOptimization(new PushdownLimits());
        addOptimization(new ReplaceWithIndexCounter());
        addOptimization(new ReplaceWithIndexLimit());

        // Inline aggregation has to be applied after Index counter and Index Limit with MIN/MAX.
        addOptimization(new InlineAggregation());

        // MP ORDER BY Optimization
        addOptimization(new InlineOrderByIntoMergeReceive());

        // Remove Unnecessary Projection nodes.  This is applied
        // at a later phase then the previous optimizations.
        addOptimization(new RemoveUnnecessaryProjectNodes());
        addOptimization(new MakeInsertNodesInlineIfPossible());
        addOptimization(new OffsetQueryUsingCountingIndex());
    }

    public static void applyAll(CompiledPlan plan, AbstractParsedStmt parsedStmt, Phases phase)
    {
        List<MicroOptimization> opts = optimizations.get(phase);
        if (opts != null) {
            if (m_logger.isDebugEnabled()) {
                String sqlString = plan.sql;
                if (sqlString == null) {
                    sqlString = plan.toString();
                }
                m_logger.debug("Micro Optimizer phase " + phase.name());
                m_logger.debug("SQL: " + sqlString + "\n");
            }
        }
        for (MicroOptimization opt : opts) {
            if (m_logger.isDebugEnabled()) {
                String planString = null;
                try {
                    planString = PlanSelector.outputPlanDebugString(plan.rootPlanGraph);
                } catch (JSONException ex) {
                    planString = ex.getMessage();
                }
                m_logger.debug("Microoptimization: " + opt + "\n"
                           + "Input:\n" + plan.explainedPlan + "\n"
                           + ":-----------:\n"
                           + planString
                           + "\n");
            }
            opt.apply(plan, parsedStmt);
            if (m_logger.isDebugEnabled()) {
                String planString = null;
                try {
                    planString = PlanSelector.outputPlanDebugString(plan.rootPlanGraph);
                } catch (JSONException ex) {
                    planString = ex.getMessage();
                }
                m_logger.debug("Output:\n" + planString + "\n");
            }
        }
    }
}
