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

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.VoltLoggerFactory;

@ProcInfo(
        singlePartition = false
    )

public class InstanceId extends VoltSystemProcedure {
    private static final Logger HOST_LOG =
        Logger.getLogger("HOST", VoltLoggerFactory.instance());

    static final int DEP_retrieveInstanceId = (int)
        SysProcFragmentId.PF_retrieveInstanceId | DtxnConstants.MULTINODE_DEPENDENCY;
    static final int DEP_retrieveInstanceIdAggregator = (int) SysProcFragmentId.PF_retrieveInstanceIdAggregator;
    static final int DEP_setInstanceId = (int)
        SysProcFragmentId.PF_setInstanceId | DtxnConstants.MULTINODE_DEPENDENCY;
    static final int DEP_setInstanceIdAggregator = (int) SysProcFragmentId.PF_setInstanceIdAggregator;

    /*
     * The instanceId
     */
    static volatile long m_instanceId = -1;

    @Override
    public void init(ExecutionSite site, Procedure catProc,
                     BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_retrieveInstanceId, this);
        site.registerPlanFragment(SysProcFragmentId.PF_retrieveInstanceIdAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_setInstanceId, this);
        site.registerPlanFragment(SysProcFragmentId.PF_setInstanceIdAggregator, this);
    }

    private static final ColumnInfo instanceIdSchema[] = new ColumnInfo[] {
        new ColumnInfo("INSTANCE_ID", VoltType.BIGINT)
    };

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies,
                                                  long fragmentId,
                                                  ParameterSet params,
                                                  ExecutionSite.SystemProcedureExecutionContext context)
    {
        final VoltTable result = new VoltTable(instanceIdSchema);
        //  TABLE statistics
        if (fragmentId == SysProcFragmentId.PF_retrieveInstanceId) {
            result.addRow(m_instanceId);
            return new DependencyPair(DEP_retrieveInstanceId, result);
        } else if (fragmentId == SysProcFragmentId.PF_retrieveInstanceIdAggregator) {
            for (VoltTable t : dependencies.get(DEP_retrieveInstanceId)) {
                while (t.advanceRow()) {
                    result.add(t);
                }
            }
            return new DependencyPair(DEP_retrieveInstanceIdAggregator, result);
        } else if (fragmentId == SysProcFragmentId.PF_setInstanceId) {
            assert(params.toArray() != null);
            assert(params.toArray().length == 1);
            assert(params.toArray()[0] instanceof Long);
            final Long oldValue = m_instanceId;
            m_instanceId = (Long)params.toArray()[0];
            result.addRow(oldValue);
            return new DependencyPair(DEP_setInstanceId, result);
        } else if (fragmentId == SysProcFragmentId.PF_setInstanceIdAggregator) {
            for (VoltTable t : dependencies.get(DEP_setInstanceId)) {
                while (t.advanceRow()) {
                    result.add(t);
                }
            }
            return new DependencyPair(DEP_setInstanceIdAggregator, result);
        }

        assert (false);
        return null;
    }

    VoltTable retrieveInstanceId() {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather initiator data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_retrieveInstanceId;
        pfs[1].outputDepId = DEP_retrieveInstanceId;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = false;
        pfs[1].nonExecSites = true;
        pfs[1].parameters = new ParameterSet();

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_retrieveInstanceIdAggregator;
        pfs[0].outputDepId = DEP_retrieveInstanceIdAggregator;
        pfs[0].inputDepIds = new int[]{DEP_retrieveInstanceId};
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_retrieveInstanceIdAggregator);
        return results[0];
    }

    VoltTable setInstanceId(Long instanceId) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather initiator data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_setInstanceId;
        pfs[1].outputDepId = DEP_setInstanceId;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = false;
        pfs[1].nonExecSites = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters(instanceId);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_setInstanceIdAggregator;
        pfs[0].outputDepId = DEP_setInstanceIdAggregator;
        pfs[0].inputDepIds = new int[]{DEP_setInstanceId};
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_setInstanceIdAggregator);
        return results[0];
    }

    /*
     * Op 0 is retrieve, op 1 is set
     */
    public VoltTable[] run(byte op, long instanceId) throws VoltAbortException {
        final ColumnInfo resultSchema[] = new ColumnInfo[] {
                new ColumnInfo("RESULT", VoltType.STRING),
                new ColumnInfo("ERR_MSG", VoltType.STRING),
                new ColumnInfo("INSTANCE_ID", VoltType.BIGINT)};
        final VoltTable result = new VoltTable(resultSchema);

        if (instanceId < 0 && op == 1) {
            result.addRow("FAILURE", "InstanceId must be greater than 0", m_instanceId);
            return new VoltTable[] { result };
        }

        if (op == 0 || op == 1) {
            VoltTable instanceIds = retrieveInstanceId();
            while (instanceIds.advanceRow()) {
                final Long mInstanceId = instanceIds.getLong(0);
                if (!mInstanceId.equals(m_instanceId)) {
                    result.addRow("FAILURE", "Cluster instanceId is inconsistent, coordinators is " +
                            m_instanceId + " but found a node with " + mInstanceId, m_instanceId);
                }
            }
            if (result.getRowCount() > 0) {
                return new VoltTable[] { result };
            }

            if (op == 1) {
                setInstanceId(instanceId);
                result.addRow("SUCCESS", "", m_instanceId);
            } else {
                result.addRow("SUCCESS", "", m_instanceId);
            }
        } else {
            result.addRow("FAILURE", "Op was not 1 or 0", m_instanceId);
        }

        return new VoltTable[] { result };
    }
}