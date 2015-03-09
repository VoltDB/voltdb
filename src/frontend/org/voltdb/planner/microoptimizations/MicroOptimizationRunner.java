/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;

public class MicroOptimizationRunner {

    // list all of the micro optimizations here
    static ArrayList<MicroOptimization> optimizations = new ArrayList<MicroOptimization>();
    static {
        // The orders here is important
        optimizations.add(new PushdownLimits());
        optimizations.add(new ReplaceWithIndexCounter());
        optimizations.add(new ReplaceWithIndexLimit());

        // Inline aggregation has to be applied after Index counter and Index Limit with MIN/MAX.
        optimizations.add(new InlineAggregation());
    }

    public static void applyAll(CompiledPlan plan, AbstractParsedStmt parsedStmt)
    {
        for (int i = 0; i < optimizations.size(); i++) {
            MicroOptimization opt = optimizations.get(i);
            opt.apply(plan, parsedStmt);
        }
    }
}
