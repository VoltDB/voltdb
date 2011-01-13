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
import org.voltdb.*;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;

/**
 * This procedure is not available to users. It is not added to
 * the users catalog.  This procedure should not be described
 * in the documentation.
 */
@ProcInfo(singlePartition = false)
public class ProfCtl extends VoltSystemProcedure {

    Database m_db = null;
    static final int DEP_ID = 1 | DtxnConstants.MULTIPARTITION_DEPENDENCY;

    @Override
        public void init(int numberOfPartitions, SiteProcedureConnection site,
                Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_startSampler, this);
        m_db = cluster.getDatabases().get("database");
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {

        VoltTable table = new VoltTable(new ColumnInfo("Result", VoltType.STRING));

        if (params.toArray()[0] != null) {
            String command = (String)params.toArray()[0];
            if (command.equalsIgnoreCase("SAMPLER_START")) {
                VoltDB.instance().startSampler();
                table.addRow("SAMPLER_START");
                return new DependencyPair(DEP_ID, table);
            }
            else if (command.equalsIgnoreCase("GPERF_ENABLE") || command.equalsIgnoreCase("GPERF_DISABLE")) {
                // Choose the lowest site ID on this host to do the work.
                int host_id = context.getExecutionSite().getCorrespondingHostId();
                Integer lowest_site_id =
                    VoltDB.instance().getCatalogContext().siteTracker.
                    getLowestLiveExecSiteIdForHost(host_id);
                if (context.getExecutionSite().getSiteId() != lowest_site_id)
                {
                    table.addRow("GPERF_NOOP");
                    return new DependencyPair(DEP_ID, table);
                }
                if (command.equalsIgnoreCase("GPERF_ENABLE")) {
                    context.getExecutionEngine().toggleProfiler(1);
                    table.addRow("GPERF_ENABLE");
                    return new DependencyPair(DEP_ID, table);
                }
                else {
                    context.getExecutionEngine().toggleProfiler(0);
                    table.addRow("GPERF_DISABLE");
                    return new DependencyPair(DEP_ID, table);
                }

            }
            else {
                table.addRow("Invalid command: " + command);
                return new DependencyPair(DEP_ID, table);
            }
        }
        table.addRow("No command.");
        return new DependencyPair(DEP_ID, table);
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command) {

        SynthesizedPlanFragment spf = new SynthesizedPlanFragment();
        spf.fragmentId = SysProcFragmentId.PF_startSampler;
        spf.outputDepId = DEP_ID;
        spf.inputDepIds = new int[] {};
        spf.multipartition = true;

        ParameterSet params = new ParameterSet();
        params.setParameters(command);
        spf.parameters = params;

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        return executeSysProcPlanFragments(new SynthesizedPlanFragment[] { spf }, DEP_ID);
    }
}
