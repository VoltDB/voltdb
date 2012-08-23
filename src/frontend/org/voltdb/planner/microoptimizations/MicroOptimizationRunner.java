/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.planner.CompiledPlan;

public class MicroOptimizationRunner {

    // list all of the micro optimizations here
    static ArrayList<MicroOptimization> optimizations = new ArrayList<MicroOptimization>();
    static {
        optimizations.add(new PushdownLimitsIntoScans());
        optimizations.add(new ReplaceWithIndexCounter());
        // optimizations.add(new PushdownReceiveDominators());
    }

    public static List<CompiledPlan> applyAll(CompiledPlan plan) {
        ArrayList<CompiledPlan> input = new ArrayList<CompiledPlan>();
        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();

        retval.add(plan);

        for (MicroOptimization opt : optimizations) {
            // swap input and return lists
            ArrayList<CompiledPlan> temp = input;
            input = retval;
            retval = temp;
            // empty the retval list
            retval.clear();

            for (CompiledPlan inPlan : input) {
                List<CompiledPlan> newPlans = opt.apply(inPlan);
                assert(newPlans != null);
                assert(newPlans.size() >= 1);
                retval.addAll(newPlans);
            }
        }

        return retval;
    }

}
