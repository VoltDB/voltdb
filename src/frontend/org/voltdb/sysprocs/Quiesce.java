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
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;

@ProcInfo(singlePartition = false)

public class Quiesce extends VoltSystemProcedure {

    static final int DEP_SITES = (int) SysProcFragmentId.PF_quiesce_sites | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_NODES = (int) SysProcFragmentId.PF_quiesce_nodes | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_PROCESSED_SITES = (int) SysProcFragmentId.PF_quiesce_processed_sites;
    static final int DEP_PROCESSED_NODES = (int) SysProcFragmentId.PF_quiesce_processed_nodes;

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_quiesce_sites, this);
        site.registerPlanFragment(SysProcFragmentId.PF_quiesce_nodes, this);
        site.registerPlanFragment(SysProcFragmentId.PF_quiesce_processed_sites, this);
        site.registerPlanFragment(SysProcFragmentId.PF_quiesce_processed_nodes, this);
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer,List<VoltTable>> dependencies,
        long fragmentId, ParameterSet params, SystemProcedureExecutionContext context)
    {
        try {
            if (fragmentId == SysProcFragmentId.PF_quiesce_sites) {
                // tell each site to quiesce
                context.getExecutionEngine().quiesce(context.getLastCommittedTxnId());
                VoltTable results = new VoltTable(new ColumnInfo("id", VoltType.INTEGER));
                results.addRow(Integer.parseInt(context.getSite().getTypeName()));
                return new DependencyPair(DEP_SITES, results);
            }
            else if (fragmentId == SysProcFragmentId.PF_quiesce_processed_sites) {
                VoltTable dummy = new VoltTable(new ColumnInfo("status", VoltType.STRING));
                dummy.addRow("okay");
                return new DependencyPair(DEP_PROCESSED_SITES, dummy);
            }

            else if (fragmentId == SysProcFragmentId.PF_quiesce_nodes) {
                // Choose the lowest site ID on this host to do the global
                // quiesce.  All other sites should just claim 'okay'
                int host_id = context.getExecutionSite().getCorrespondingHostId();
                Integer lowest_site_id =
                    VoltDB.instance().getCatalogContext().siteTracker.
                    getLowestLiveExecSiteIdForHost(host_id);
                if (context.getExecutionSite().getSiteId() == lowest_site_id)
                {
                    VoltDB.quiesce();
                }
                VoltTable dummy = new VoltTable(new ColumnInfo("status", VoltType.STRING));
                dummy.addRow("okay");
                return new DependencyPair(DEP_NODES, dummy);
            }
            else if (fragmentId == SysProcFragmentId.PF_quiesce_processed_nodes) {
                VoltTable dummy = new VoltTable(new ColumnInfo("status", VoltType.STRING));
                dummy.addRow("okay");
                return new DependencyPair(DEP_PROCESSED_NODES, dummy);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
            VoltTable[] result = null;

            SynthesizedPlanFragment pfs1[] = new SynthesizedPlanFragment[2];
            pfs1[0] = new SynthesizedPlanFragment();
            pfs1[0].fragmentId = SysProcFragmentId.PF_quiesce_sites;
            pfs1[0].outputDepId = DEP_SITES;
            pfs1[0].inputDepIds = new int[]{};
            pfs1[0].multipartition = true;
            pfs1[0].nonExecSites = false;
            pfs1[0].parameters = new ParameterSet();

            pfs1[1] = new SynthesizedPlanFragment();
            pfs1[1].fragmentId = SysProcFragmentId.PF_quiesce_processed_sites;
            pfs1[1].outputDepId = DEP_PROCESSED_SITES;
            pfs1[1].inputDepIds = new int[] { DEP_SITES };
            pfs1[1].multipartition = false;
            pfs1[1].nonExecSites = false;
            pfs1[1].parameters = new ParameterSet();

            try {
                result = executeSysProcPlanFragments(pfs1, DEP_PROCESSED_SITES);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }

            SynthesizedPlanFragment pfs2[] = new SynthesizedPlanFragment[2];
            pfs2[0] = new SynthesizedPlanFragment();
            pfs2[0].fragmentId = SysProcFragmentId.PF_quiesce_nodes;
            pfs2[0].outputDepId = DEP_NODES;
            pfs2[0].inputDepIds = new int[]{};
            pfs2[0].multipartition = true;
            pfs2[0].nonExecSites = false;
            pfs2[0].parameters = new ParameterSet();

            pfs2[1] = new SynthesizedPlanFragment();
            pfs2[1].fragmentId = SysProcFragmentId.PF_quiesce_processed_nodes;
            pfs2[1].outputDepId = DEP_PROCESSED_NODES;
            pfs2[1].inputDepIds = new int[] { DEP_NODES };
            pfs2[1].multipartition = false;
            pfs2[1].nonExecSites = false;
            pfs2[1].parameters = new ParameterSet();

            try {
                result = executeSysProcPlanFragments(pfs2, DEP_PROCESSED_NODES);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }

            return result;
    }

}
