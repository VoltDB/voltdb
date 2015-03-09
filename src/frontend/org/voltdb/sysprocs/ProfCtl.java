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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * This procedure is not available to users. It is not added to
 * the users catalog.  This procedure should not be described
 * in the documentation.
 */
@ProcInfo(singlePartition = false)
public class ProfCtl extends VoltSystemProcedure {

    @Override
    public void init() {
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        throw new RuntimeException("Promote was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command)
    {
        VoltTable table = new VoltTable(new ColumnInfo("Result", VoltType.STRING));

        if (command.equalsIgnoreCase("SAMPLER_START")) {
            VoltDB.instance().startSampler();
            table.addRow(command);
        }
        else if (command.equalsIgnoreCase("GPERF_ENABLE") || command.equalsIgnoreCase("GPERF_DISABLE")) {
            // Choose the lowest site ID on this host to do the work.
            table.addRow(command);
            if (ctx.isLowestSiteId()) {
                if (command.equalsIgnoreCase("GPERF_ENABLE")) {
                    ctx.getSiteProcedureConnection().toggleProfiler(1);
                }
                else {
                    ctx.getSiteProcedureConnection().toggleProfiler(0);
                }
            }
        }
        else {
            table.addRow("Invalid command: " + command);
        }
        return (new VoltTable[] {table});
    }
}
