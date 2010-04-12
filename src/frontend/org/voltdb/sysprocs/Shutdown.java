/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.util.HashMap;
import java.util.List;

import org.voltdb.*;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.VoltLoggerFactory;
import org.apache.log4j.Logger;

/** A wholly improper shutdown. The only guarantee is that a transaction
 * is committed or not committed - never partially committed. However, no
 * promise is given to return a result to a client, to finish work queued
 * behind this procedure or to return meaningful errors for those queued
 * transactions.
 *
 * Invoking this procedure will terminate each node in the cluster.
 */

@ProcInfo(singlePartition = false)

public class Shutdown extends VoltSystemProcedure {

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_shutdownCommand, this);
        site.registerPlanFragment(SysProcFragmentId.PF_procedureDone, this);
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies,
                                           long fragmentId,
                                           ParameterSet params,
                                           ExecutionSite.SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_shutdownCommand) {
            ProcedureProfiler.flushProfile();
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            Thread shutdownThread = new Thread() {
                @Override
                public void run() {
                    try {
                        VoltDB.instance().shutdown(this);
                    } catch (InterruptedException e) {
                        Logger.getLogger("HOST", VoltLoggerFactory.instance()).error(
                                "Exception while attempting to shutdown VoltDB from shutdown sysproc",
                                e);
                    }
                    System.exit(0);
                }
            };
            shutdownThread.start();
        }
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_shutdownCommand;
        pfs[0].outputDepId = (int) SysProcFragmentId.PF_procedureDone | DtxnConstants.MULTINODE_DEPENDENCY;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = true;
        pfs[0].parameters = new ParameterSet();

        executeSysProcPlanFragments(pfs,
                (int) SysProcFragmentId.PF_procedureDone);
        return new VoltTable[0];
    }
}
