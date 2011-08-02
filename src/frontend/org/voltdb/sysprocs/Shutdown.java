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

import java.util.HashMap;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.logging.VoltLogger;

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
            // Choose the lowest site ID on this host to do the global
            // shutdown.  all other sites should just bail out (for now)
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() != lowest_site_id)
            {
                return null;
            }

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
                        new VoltLogger("HOST").error(
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
        pfs[0].parameters = new ParameterSet();

        executeSysProcPlanFragments(pfs,
                (int) SysProcFragmentId.PF_procedureDone);
        return new VoltTable[0];
    }
}
