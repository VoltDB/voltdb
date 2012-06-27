/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.util.List;
import java.util.Map;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
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
    public static final VoltType CTYPE_ID = VoltType.INTEGER;

    /** Standard column name for a host id column */
    public static final String CNAME_HOST_ID = "HOST_ID";

    /** Standard column name for a site id column */
    public static final String CNAME_SITE_ID = "SITE_ID";

    /** Standard column name for a partition id column */
    public static final String CNAME_PARTITION_ID = "PARTITION_ID";

    /** Standard schema for sysprocs returning a simple status table */
    public static final ColumnInfo STATUS_SCHEMA =
        new ColumnInfo("STATUS", VoltType.BIGINT);   // public to fix javadoc linking warning

    /** Standard success return value for sysprocs returning STATUS_SCHEMA */
    protected static long STATUS_OK = 0L;

    protected int m_numberOfPartitions;
    protected Procedure m_catProc;
    protected Cluster m_cluster;
    protected SiteProcedureConnection m_site;
    private LoadedProcedureSet m_loadedProcedureSet;
    protected ProcedureRunner m_runner; // overrides private parent var

    @Override
    void init(ProcedureRunner procRunner) {
        super.init(procRunner);
        m_runner = procRunner;
    }

    void initSysProc(int numberOfPartitions, SiteProcedureConnection site,
            LoadedProcedureSet loadedProcedureSet,
            Procedure catProc, Cluster cluster) {

        m_numberOfPartitions = numberOfPartitions;
        m_site = site;
        m_catProc = catProc;
        m_cluster = cluster;
        m_loadedProcedureSet = loadedProcedureSet;

        init();
    }

    /**
     * For Sysproc init tasks like registering plan frags
     */
    abstract public void init();

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

    /** Bundles the data needed to describe a plan fragment. */
    public static class SynthesizedPlanFragment {
        public long siteId = -1;
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
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params,
            SystemProcedureExecutionContext context);

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
    public VoltTable[] executeSysProcPlanFragments(
            SynthesizedPlanFragment pfs[], int aggregatorOutputDependencyId) {

        TransactionState txnState = m_runner.getTxnState();

        VoltTable[] results = new VoltTable[1];
        executeSysProcPlanFragmentsAsync(pfs);

        // the stack frame drop terminates the recursion and resumes
        // execution of the current stored procedure.
        assert (txnState != null);
        txnState.setupProcedureResume(false, new int[] { aggregatorOutputDependencyId });

        // execute the tasks that just got queued.
        // recursively call recurableRun and don't allow it to shutdown
        Map<Integer, List<VoltTable>> mapResults = m_site.recursableRun(txnState);

        List<VoltTable> matchingTablesForId = mapResults.get(aggregatorOutputDependencyId);
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
    public void executeSysProcPlanFragmentsAsync(
            SynthesizedPlanFragment pfs[]) {

        TransactionState txnState = m_runner.getTxnState();

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

            FragmentTaskMessage task = FragmentTaskMessage.createWithOneFragment(
                    txnState.initiatorHSId,
                    m_site.getCorrespondingSiteId(),
                    txnState.txnId,
                    txnState.isReadOnly(),
                    pf.fragmentId,
                    pf.outputDepId,
                    parambytes,
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
                txnState.createAllParticipatingFragmentWork(task);
            } else {
                // create one workunit for the current site
                if (pf.siteId == -1)
                    txnState.createLocalFragmentWork(task, false);
                else
                    txnState.createFragmentWork(new long[] { pf.siteId },
                                                         task);
            }
        }
    }

    // It would be nicer if init() on a sysproc was really "getPlanFragmentIds()"
    // and then the loader could ask for the ids directly instead of stashing
    // its reference here and inverting the relationship between loaded procedure
    // set and system procedure.
    public void registerPlanFragment(long fragmentId) {
        assert(m_runner != null);
        m_loadedProcedureSet.registerPlanFragment(fragmentId, m_runner);
    }
}
