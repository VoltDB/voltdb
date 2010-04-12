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

@ProcInfo(
    // partitionInfo = "TABLE.ATTR: 0",
    singlePartition = false
)
public class LastCommittedTransaction extends VoltSystemProcedure {

    static final int DEP_lastCommittedScan = (int)
        SysProcFragmentId.PF_lastCommittedScan |
        DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_lastCommittedResults = (int)
        SysProcFragmentId.PF_lastCommittedResults;

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_lastCommittedScan, this);
        site.registerPlanFragment(SysProcFragmentId.PF_lastCommittedResults,
                                  this);
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies,
                                                  long fragmentId,
                                                  ParameterSet params,
                                                  ExecutionSite.SystemProcedureExecutionContext context)
 {
        // Fulfill the request for lastcommittedtxnid
        // return dependency id #1
        if (fragmentId == SysProcFragmentId.PF_lastCommittedScan) {
            System.out.println("SYSPROC: LastCommitedTransaction PF1");
            VoltTable result = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
            result.addRow(context.getLastCommittedTxnId());
            return new DependencyPair(DEP_lastCommittedScan, result);
        }

        // aggregate the site-number of dependencies together
        // return dependency id #2
        if (fragmentId == SysProcFragmentId.PF_lastCommittedResults) {
            System.out.println("SYSPROC: LastCommittedTransaction PF2");
            long lastcommitted = -1;
            // iterate dependencies from work unit.
            assert (dependencies.size() == 1);
            List<VoltTable> dep = dependencies.get(DEP_lastCommittedScan);
            int i = 0; // for debug.
            for (VoltTable vt : dep) {
                long txnid = vt.asScalarLong();
                System.out.printf("SYSPROC: LastCommittedTransaction PF2 dep %d lctid %d\n", i++, txnid);
                lastcommitted = txnid;
            }
            assert(lastcommitted > -1);
            VoltTable result = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
            result.addRow(lastcommitted);
            return new DependencyPair(DEP_lastCommittedResults, result);
        }

        assert (false);
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        System.out.println("SYSPROC: LastCommittedTransaction.run().");
        VoltTable[] results;

        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];

        // create a work fragment to gather data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_lastCommittedScan;
        pfs[1].outputDepId = DEP_lastCommittedScan;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].nonExecSites = false;
        pfs[1].parameters = new ParameterSet();

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_lastCommittedResults;
        pfs[0].outputDepId = DEP_lastCommittedResults;
        pfs[0].inputDepIds = new int[]{ DEP_lastCommittedScan };
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_lastCommittedResults);


        System.out.printf("SYSPROC: LastCommittedTransaction.run() - processing [1] %d results\n", results.length);
        for (VoltTable vt : results) {
            System.out.println("RESULTS: " + vt);
        }

        return results;
    }

}
