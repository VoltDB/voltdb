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

import org.voltdb.HsqlBackend;
import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltDB;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.log4j.LogManager;

@ProcInfo(singlePartition = false)

/**
 * Execute the supplied XML string using org.apache.log4j.xml.DomConfigurator
 *
 * The first parameter is the string containing the XML configuration and the second parameter is boolean value (0 false, everything else true)
 * indicating whether the logger settings should be updated everywhere or only on this host.
 */
public class UpdateLogging extends VoltSystemProcedure {
    private static final int DEP_loggersUpdated = 4;

    @Override
    public void init(ExecutionSite site, Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_updateLoggers, this);
    };

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
        assert(fragmentId == SysProcFragmentId.PF_updateLoggers) :
            "UpdateLogging system procedure should only ever be asked to execute plan fragment " + SysProcFragmentId.PF_updateLoggers +
            " and was asked to run " + fragmentId;

        DOMConfigurator configurator = new DOMConfigurator();
        StringReader sr = new StringReader((String)params.toArray()[0]);
        configurator.doConfigure( sr, LogManager.getLoggerRepository());
        long allHostsLong = ((Long)params.toArray()[1]).longValue();
        boolean allHosts = allHostsLong != 0 ? true : false;

        /**
         * Propagate to each ExecutionSite's engine.
         */
        for (ExecutionSite site : VoltDB.instance().getLocalSites().values()) {
            // Non-final, not-read-only multi-partition procedure fragment
            // has everything blocked. This is safe even on other sites' EE.
            site.updateBackendLogLevels();
        }

        VoltTable t = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
        t.addRow(0);
        return new DependencyPair(allHosts ? DEP_loggersUpdated | DtxnConstants.MULTINODE_DEPENDENCY : DEP_loggersUpdated, t);
    }

    public VoltTable[] run(String xmlConfig, long allHostsLong) {
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];

        int depId = DEP_loggersUpdated;
        if (allHostsLong != 0) {
            depId |= DtxnConstants.MULTINODE_DEPENDENCY;
        }

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_updateLoggers;
        pfs[0].outputDepId = depId;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = (allHostsLong != 0);
        pfs[0].parameters = new ParameterSet();
        pfs[0].parameters.setParameters(new Object[] { xmlConfig, allHostsLong });

        VoltTable[] retval = executeSysProcPlanFragments(pfs, depId);
        assert(retval != null);
        for (VoltTable t : retval)
            assert(t != null);
        return new VoltTable[0];
    }
}
