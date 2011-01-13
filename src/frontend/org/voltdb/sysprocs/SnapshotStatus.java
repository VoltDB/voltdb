/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;

@ProcInfo(singlePartition = false)
public class SnapshotStatus extends VoltSystemProcedure {
    @Override
    public DependencyPair
    executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
                        final SystemProcedureExecutionContext context)
    {
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx) throws VoltAbortException
    {
        ArrayList<Integer> catalogIds = new ArrayList<Integer>();
        catalogIds.add(0);
        return new VoltTable[] {
            VoltDB.instance().getStatsAgent().getStats(SysProcSelector.SNAPSHOTSTATUS,
                                                       catalogIds,
                                                       false,
                                                       System.currentTimeMillis())
        };
    }
}
