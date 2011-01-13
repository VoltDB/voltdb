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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.MultiPartitionParticipantTxnState;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;

/**
 * System procedures extend VoltSystemProcedure and use its utility methods to
 * create work in the system. This functionality is not available to standard
 * user procedures (which extend VoltProcedure).
 */
public abstract class VoltSystemProcedure extends VoltProcedure {

    /** Standard column type for host/partition/site id columns */
    protected static VoltType CTYPE_ID = VoltType.INTEGER;

    /** Standard column name for a host id column */
    protected static String CNAME_HOST_ID = "HOST_ID";

    /** Standard column name for a site id column */
    protected static String CNAME_SITE_ID = "SITE_ID";

    /** Standard column name for a partition id column */
    protected static String CNAME_PARTITION_ID = "PARTITION_ID";

    /** Standard schema for sysprocs returning a simple status table */
    public static ColumnInfo STATUS_SCHEMA =
        new ColumnInfo("STATUS", VoltType.BIGINT);   // public to fix javadoc linking warning

    /** Standard success return value for sysprocs returning STATUS_SCHEMA */
    protected static long STATUS_OK = 0L;

    /**
     * Utility to aggregate a list of tables sharing a schema. Common for
     * sysprocs to do this, to aggregate results.
     */
    protected VoltTable unionTables(List<VoltTable> operands) {
        VoltTable result = null;
        VoltTable vt = operands.get(0);
        if (vt != null) {
            VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt
                                                                        .getColumnCount()];
            for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii),
                                                       vt.getColumnType(ii));
            }
            result = new VoltTable(columns);
            for (Object table : operands) {
                vt = (VoltTable) (table);
                while (vt.advanceRow()) {
                    result.add(vt);
                }
            }
        }
        return result;
    }

    /**
     * Allow sysprocs to update m_currentTxnState manually. User procedures are
     * passed this state in call(); sysprocs have other entry points on
     * non-coordinator sites.
     */
    public void setTransactionState(TransactionState txnState) {
        m_currentTxnState = txnState;
    }

    public TransactionState getTransactionState() {
        return m_currentTxnState;
    }

    /** Bundles the data needed to describe a plan fragment. */
    public static class SynthesizedPlanFragment {
        public int siteId = -1;
        public long fragmentId = -1;
        public int outputDepId = -1;
        public int inputDepIds[] = null;
        public ParameterSet parameters = null;
        public boolean multipartition = false;
        /** true if distributes to all executable partitions */
        /**
         * Used to tell the DTXN to suppress duplicate results from sites that
         * replicate a partition. Most system procedures don't want this, but,
         * for example, adhoc queries actually do want duplicate suppression
         * like user sysprocs.
         */
        public boolean suppressDuplicates = false;
    }

    abstract public DependencyPair executePlanFragment(
            HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params,
            ExecutionSite.SystemProcedureExecutionContext context);

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce a
     * single output dependency. This aggregate output is returned as the
     * result.
     *
     * @param pfs
     *            an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId
     *            dependency id produced by the aggregation pf The id of the
     *            table returned as the result of this procedure.
     * @return the resulting VoltTable as a length-one array.
     */
    protected VoltTable[] executeSysProcPlanFragments(
            SynthesizedPlanFragment pfs[], int aggregatorOutputDependencyId) {
        VoltTable[] results = new VoltTable[1];
        executeSysProcPlanFragmentsAsync(pfs);

        // the stack frame drop terminates the recursion and resumes
        // execution of the current stored procedure.
        assert (m_currentTxnState != null);
        assert (m_currentTxnState instanceof MultiPartitionParticipantTxnState);
        m_currentTxnState
                         .setupProcedureResume(
                                               false,
                                               new int[] { aggregatorOutputDependencyId });

        // execute the tasks that just got queued.
        // recursively call recurableRun and don't allow it to shutdown
        Map<Integer, List<VoltTable>> mapResults = m_site
                                                         .recursableRun(m_currentTxnState);

        List<VoltTable> matchingTablesForId = mapResults
                                                        .get(aggregatorOutputDependencyId);
        if (matchingTablesForId == null) {
            assert (mapResults.size() == 0);
            results[0] = null;
        } else {
            results[0] = matchingTablesForId.get(0);
        }

        return results;
    }

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce a
     * single output dependency. This aggregate output is returned as the
     * result.
     *
     * @param pfs
     *            an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId
     *            dependency id produced by the aggregation pf The id of the
     *            table returned as the result of this procedure.
     */
    protected void executeSysProcPlanFragmentsAsync(
            SynthesizedPlanFragment pfs[]) {
        for (SynthesizedPlanFragment pf : pfs) {
            assert (pf.parameters != null);

            // check the output dep id makes sense given the number of sites to
            // run this on
            if (pf.multipartition) {
                assert ((pf.outputDepId & DtxnConstants.MULTIPARTITION_DEPENDENCY) == DtxnConstants.MULTIPARTITION_DEPENDENCY);
            }

            // serialize parameters
            ByteBuffer parambytes = null;
            if (pf.parameters != null) {
                FastSerializer fs = new FastSerializer();
                try {
                    fs.writeObject(pf.parameters);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    assert (false);
                }
                parambytes = fs.getBuffer();
            }

            FragmentTaskMessage task = new FragmentTaskMessage(
                m_currentTxnState.initiatorSiteId,
                m_site.getCorrespondingSiteId(),
                m_currentTxnState.txnId,
                false,
                new long[] { pf.fragmentId },
                new int[] { pf.outputDepId },
                new ByteBuffer[] { parambytes },
                false);
            if (pf.inputDepIds != null) {
                for (int depId : pf.inputDepIds) {
                    task.addInputDepId(0, depId);
                }
            }
            task.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_SITE);
            if (pf.suppressDuplicates) {
                task.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);
            }

            if (pf.multipartition) {
                // create a workunit for every execution site
                m_currentTxnState.createAllParticipatingFragmentWork(task);
            } else {
                // create one workunit for the current site
                if (pf.siteId == -1)
                    m_currentTxnState.createLocalFragmentWork(task, false);
                else
                    m_currentTxnState.createFragmentWork(new int[] { pf.siteId },
                                                         task);
            }
        }
    }
}
