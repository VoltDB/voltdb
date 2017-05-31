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

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;

public class MicroOptimizationRunner {
    public enum Phases {
        SELECT_CONSTRUCTION_PHASE,
        AFTER_BEST_SELECTION
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
    }

    public static void applyAll(CompiledPlan plan, AbstractParsedStmt parsedStmt, Phases phase)
    {
        List<MicroOptimization> opts = optimizations.get(phase);
        if (opts != null) {
            for (MicroOptimization opt : opts) {
                opt.apply(plan, parsedStmt);
            }
        }
    }
}
