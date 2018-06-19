/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.iv2.MpTransactionState;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.NodeSettings;

import com.google_voltpatches.common.primitives.Longs;

/**
 * System procedures extend VoltProcedure and use its utility methods to
 * create work in the system. This functionality is not available to standard
 * user procedures (which extend VoltProcedure).
 */
public abstract class VoltSystemProcedure extends VoltProcedure {

    private static final VoltLogger log = new VoltLogger("HOST");

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
    public static long STATUS_OK = 0L;
    public static long STATUS_FAILURE = 1L;

    protected Cluster m_cluster;
    protected ClusterSettings m_clusterSettings;
    protected NodeSettings m_nodeSettings;
    protected SiteProcedureConnection m_site;
    protected ProcedureRunner m_runner; // overrides private parent var

    /**
     * Since sysprocs still use long fragids in places (rather than sha1 hashes),
     * provide a utility method to convert between longs and hashes. This method
     * is the inverse of {@link VoltSystemProcedure#fragIdToHash(long)}.
     *
     * @param hash 20bytes of hash value (last 12 bytes ignored)
     * @return 8 bytes of fragid
     */
    public static long hashToFragId(byte[] hash) {
        return Longs.fromByteArray(hash);
    }

    /**
     * Since sysprocs still use long fragids in places (rather than sha1 hashes),
     * provide a utility method to convert between longs and hashes. This method
     * is the inverse of {@link VoltSystemProcedure#hashToFragId(byte[])}.
     *
     * @param fragId 8 bytes of frag id
     * @return 20 bytes of hash padded with 12 empty bytes
     */
    public static byte[] fragIdToHash(long fragId) {
        // use 12 bytes to pad the fake 20-byte sha1 hash after the 8-byte long
        return ArrayUtils.addAll(Longs.toByteArray(fragId), new byte[12]);
    }

    @Override
    void init(ProcedureRunner procRunner) {
        super.init(procRunner);
        m_runner = procRunner;
    }

    void initSysProc(SiteProcedureConnection site,
            Cluster cluster,
            ClusterSettings clusterSettings,
            NodeSettings nodeSettings) {

        m_site = site;
        m_cluster = cluster;
        m_clusterSettings = clusterSettings;
        m_nodeSettings = nodeSettings;
    }

    /**
     * return all SysProc plan fragments that needs to be registered
     */
    abstract public long[] getPlanFragmentIds();

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

        // the stack frame drop terminates the recursion and resumes
        // execution of the current stored procedure.
        assert (txnState != null);
        txnState.setupProcedureResume(new int[] { aggregatorOutputDependencyId });

        final ArrayList<VoltTable> results = new ArrayList<>();
        executeSysProcPlanFragmentsAsync(pfs);

        // execute the tasks that just got queued.
        // recursively call recurableRun and don't allow it to shutdown
        Map<Integer, List<VoltTable>> mapResults = m_site.recursableRun(txnState);

        if (mapResults != null) {
            List<VoltTable> matchingTablesForId = mapResults.get(aggregatorOutputDependencyId);
            if (matchingTablesForId == null) {
                log.error("Sysproc received a stale fragment response message from before the " +
                          "transaction restart.");
                throw new MpTransactionState.FragmentFailureException();
            } else {
                results.add(matchingTablesForId.get(0));
            }
        }

        return results.toArray(new VoltTable[0]);
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

        MpTransactionState txnState = (MpTransactionState)m_runner.getTxnState();
        assert(txnState != null);

        int fragmentIndex = 0;
        for (SynthesizedPlanFragment pf : pfs) {
            assert (pf.parameters != null);

            // check the output dep id makes sense given the number of sites to
            // run this on
            if (pf.multipartition) {
                assert ((pf.outputDepId & DtxnConstants.MULTIPARTITION_DEPENDENCY) == DtxnConstants.MULTIPARTITION_DEPENDENCY);
            }

            FragmentTaskMessage task = FragmentTaskMessage.createWithOneFragment(
                    txnState.initiatorHSId,
                    m_site.getCorrespondingSiteId(),
                    txnState.txnId,
                    txnState.uniqueId,
                    txnState.isReadOnly(),
                    fragIdToHash(pf.fragmentId),
                    pf.outputDepId,
                    pf.parameters,
                    false,
                    txnState.isForReplay(),
                    txnState.isNPartTxn(),
                    txnState.getTimetamp());

            //During @MigratePartitionLeader, a fragment may be mis-routed. fragmentIndex is used to check which fragment is mis-routed and
            //to determine how the follow-up fragments are processed.
            task.setBatch(fragmentIndex++);
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

    protected void noteOperationalFailure(String errMsg) {
        m_runner.m_statusCode = ClientResponse.OPERATIONAL_FAILURE;
        m_runner.m_statusString = errMsg;
    }

    protected void registerUndoAction(UndoAction action) {
        m_runner.getTxnState().registerUndoAction(action);
    }

    protected Long getMasterHSId(int partition) {
        TransactionState txnState = m_runner.getTxnState();
        if (txnState instanceof MpTransactionState) {
            return ((MpTransactionState) txnState).getMasterHSId(partition);
        } else {
            throw new RuntimeException("SP sysproc doesn't support getting the master HSID");
        }
    }
}
