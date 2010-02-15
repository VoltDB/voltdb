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
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;

/**
 *
 *
 */
@ProcInfo(singlePartition = false)
public class StartSampler extends VoltSystemProcedure {

    Database m_db = null;
    static final int DEP_ID = 1 | DtxnConstants.MULTIPARTITION_DEPENDENCY;

    @Override
    public void init(ExecutionSite site, Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_startSampler, this);
        m_db = cluster.getDatabases().get("database");
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {

        VoltDB.instance().startSampler();

        VoltTable table = new VoltTable(new ColumnInfo("dummy", VoltType.BIGINT));

        return new DependencyPair(DEP_ID, table);
    }

    public VoltTable[] run() {

        SynthesizedPlanFragment spf = new SynthesizedPlanFragment();
        spf.fragmentId = SysProcFragmentId.PF_startSampler;
        spf.outputDepId = DEP_ID;
        spf.inputDepIds = new int[] {};
        spf.multipartition = true;
        spf.nonExecSites = false;
        spf.parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        return executeSysProcPlanFragments(new SynthesizedPlanFragment[] { spf }, DEP_ID);
    }
}
