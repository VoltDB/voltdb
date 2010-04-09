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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

import org.voltdb.*;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;

/*
 *  Execute at each NODE to return a list of key value pairs.
 *  Data is returned in the schema {node_id, key, value}.
 */

@ProcInfo(singlePartition = false)

public class SystemInformation extends VoltSystemProcedure {
    static final int DEP_DISTRIBUTE = 1 | DtxnConstants.MULTINODE_DEPENDENCY;
    static final int DEP_AGGREGATE = 2;

    /**
     * Accumulate per-host information and return as a table.
     * This function does the real work. Everything else is
     * boilerplate sysproc stuff.
     */
    private VoltTable populateTable(SystemProcedureExecutionContext context) {
        VoltTable vt = new VoltTable(
                new ColumnInfo("node_id", VoltType.INTEGER),
                new ColumnInfo("key", VoltType.STRING),
                new ColumnInfo("value", VoltType.STRING));

        // host name and IP address.
        try {
            InetAddress addr = InetAddress.getLocalHost();
            vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                    "ipaddress", addr.getHostAddress());
            vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                    "hostname", addr.getHostName());
        }
        catch (java.net.UnknownHostException ex) {
        }

        // build string
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                "buildstring", VoltDB.instance().getBuildString());

        // version
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "version", VoltDB.instance().getVersionString());

        // node status. currently := {running, idle}
        vt.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                  "operStatus", context.getOperStatus());
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
            VoltTable result = populateTable(context);
            return new DependencyPair(DEP_DISTRIBUTE, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformation_aggregate) {
            VoltTable result = null;

            // copy-paste from stats. should be a volt table utility.
            List<VoltTable> dep = dependencies.get(DEP_DISTRIBUTE);
            VoltTable vt = dep.get(0);
            if (vt != null) {
                VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
                for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                    columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii), vt.getColumnType(ii));
                }
                result = new VoltTable(columns);
                for (Object d : dep) {
                    vt = (VoltTable) (d);
                    while (vt.advanceRow()) {
                        result.add(vt);
                    }
                }
            }
            return new DependencyPair(DEP_AGGREGATE, result);
        }

        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        SynthesizedPlanFragment spf[] = new SynthesizedPlanFragment[2];
        spf[0] = new SynthesizedPlanFragment();
        spf[0].fragmentId = SysProcFragmentId.PF_systemInformation_distribute;
        spf[0].outputDepId = DEP_DISTRIBUTE;
        spf[0].inputDepIds = new int[] {};
        spf[0].multipartition = false;      // not every site
        spf[0].nonExecSites = true;         // but one site per host
        spf[0].parameters = new ParameterSet();

        spf[1] = new SynthesizedPlanFragment();
        spf[1] = new SynthesizedPlanFragment();
        spf[1].fragmentId = SysProcFragmentId.PF_systemInformation_aggregate;
        spf[1].outputDepId = DEP_AGGREGATE;
        spf[1].inputDepIds = new int[] { DEP_DISTRIBUTE };
        spf[1].multipartition = false;
        spf[1].nonExecSites = false;
        spf[1].parameters = new ParameterSet();

        return executeSysProcPlanFragments(spf, DEP_AGGREGATE);
    }

}


