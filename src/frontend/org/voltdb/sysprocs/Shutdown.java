/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.DtxnConstants;

/**
 * A wholly improper shutdown. No promise is given to return a result to a client,
 * to finish work queued behind this procedure or to return meaningful errors for
 * those queued transactions.
 *
 * Invoking this procedure immediately attempts to terminate each node in the cluster.
 */
@ProcInfo(singlePartition = false)
public class Shutdown extends VoltSystemProcedure {

    @Override
    public void init() {
        registerPlanFragment(SysProcFragmentId.PF_shutdownCommand);
        registerPlanFragment(SysProcFragmentId.PF_procedureDone);
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                                           long fragmentId,
                                           ParameterSet params,
                                           SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_shutdownCommand) {
            Thread shutdownThread = new Thread() {
                @Override
                public void run() {
                    boolean die = false;
                    try {
                        die = VoltDB.instance().shutdown(this);
                    } catch (InterruptedException e) {
                        new VoltLogger("HOST").error(
                                "Exception while attempting to shutdown VoltDB from shutdown sysproc",
                                e);
                    }
                    if (die) {
                        System.exit(0);
                    }
                    else {
                        try {
                            Thread.sleep(10000);
                        }
                        catch (InterruptedException e) {}
                    }
                }
            };
            shutdownThread.start();
        }
        return null;
    }

    /**
     * Begin an un-graceful shutdown.
     * @param ctx Internal parameter not exposed to the end-user.
     * @return Never returned, no he never returned...
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_shutdownCommand;
        pfs[0].outputDepId = (int) SysProcFragmentId.PF_procedureDone | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        executeSysProcPlanFragments(pfs, (int) SysProcFragmentId.PF_procedureDone);
        return new VoltTable[0];
    }
}
