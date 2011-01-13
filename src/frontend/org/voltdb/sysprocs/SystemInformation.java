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

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;

/*
 *  Execute at each NODE to return a list of key value pairs.
 *  Data is returned in the schema {node_id, key, value}.
 */

@ProcInfo(singlePartition = false)

public class SystemInformation extends VoltSystemProcedure {
    static final int DEP_DISTRIBUTE = 1 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_AGGREGATE = 2;

    /**
     * Accumulate per-host information and return as a table.
     * This function does the real work. Everything else is
     * boilerplate sysproc stuff.
     */
    private VoltTable populateTable(SystemProcedureExecutionContext context) {
        VoltTable vt = new VoltTable(
                new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID,
                               VoltSystemProcedure.CTYPE_ID),
                new ColumnInfo("KEY", VoltType.STRING),
                new ColumnInfo("VALUE", VoltType.STRING));

        // host name and IP address.
        InetAddress addr = org.voltdb.client.ConnectionUtil.getLocalAddress();
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "IPADDRESS", addr.getHostAddress());
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "HOSTNAME", addr.getHostName());

        // build string
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "BUILDSTRING", VoltDB.instance().getBuildString());

        // version
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "VERSION", VoltDB.instance().getVersionString());

        // catalog path
        String path = VoltDB.instance().getConfig().m_pathToCatalog;
        if (!path.startsWith("http"))
            path = (new File(path)).getAbsolutePath();
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "CATALOG", path);

        // deployment path
        path = VoltDB.instance().getConfig().m_pathToDeployment;
        if (!path.startsWith("http"))
            path = (new File(path)).getAbsolutePath();
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "DEPLOYMENT", path);

        return vt;
    }

    @Override
        public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(
                SysProcFragmentId.PF_systemInformation_aggregate, this);
        site.registerPlanFragment(
                SysProcFragmentId.PF_systemInformation_distribute, this);
    }

    @Override
    public DependencyPair executePlanFragment(
            HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {

        if (fragmentId == SysProcFragmentId.PF_systemInformation_distribute) {
            VoltTable result = null;
            // Choose the lowest site ID on this host to do the info gathering
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() == lowest_site_id)
            {
                result = populateTable(context);
            }
            else
            {
                result = new VoltTable(
                                       new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
                                       new ColumnInfo("KEY", VoltType.STRING),
                                       new ColumnInfo("VALUE", VoltType.STRING));
            }
            return new DependencyPair(DEP_DISTRIBUTE, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformation_aggregate) {
            VoltTable result = unionTables(dependencies.get(DEP_DISTRIBUTE));
            return new DependencyPair(DEP_AGGREGATE, result);
        }

        return null;
    }

    /**
     * Retrieve basic management informatino about the cluster.
     * Use this procedure to read the ipaddress, hostname, buildstring
     * and version of each node of the cluster.
     *
     * @param ctx       Internal parameter. Not user visible.
     * @return          A table with three columns:
     *  HOST_ID(INTEGER), KEY(STRING), VALUE(STRING).
     *  Keys are "hostname", "ipaddress", "buildstring" and "version".
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        SynthesizedPlanFragment spf[] = new SynthesizedPlanFragment[2];
        spf[0] = new SynthesizedPlanFragment();
        spf[0].fragmentId = SysProcFragmentId.PF_systemInformation_distribute;
        spf[0].outputDepId = DEP_DISTRIBUTE;
        spf[0].inputDepIds = new int[] {};
        spf[0].multipartition = true;
        spf[0].parameters = new ParameterSet();

        spf[1] = new SynthesizedPlanFragment();
        spf[1] = new SynthesizedPlanFragment();
        spf[1].fragmentId = SysProcFragmentId.PF_systemInformation_aggregate;
        spf[1].outputDepId = DEP_AGGREGATE;
        spf[1].inputDepIds = new int[] { DEP_DISTRIBUTE };
        spf[1].multipartition = false;
        spf[1].parameters = new ParameterSet();

        return executeSysProcPlanFragments(spf, DEP_AGGREGATE);
    }

}


