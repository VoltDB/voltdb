/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

    /**
     * return all SysProc plan fragments that needs to be registered
     * These fragments will be replayed in rejoining process.
     */
    public long[] getAllowableSysprocFragIdsInTaskLog() {
        return new long[]{};
    }

    /**
     * @return true if the procedure should not be skipped from TaskLog replay
     */
    public boolean allowableSysprocForTaskLog() {
        return false;
    }

    /** Bundles the data needed to describe a plan fragment. */
    public static class SynthesizedPlanFragment {
        public long siteId = -1;
        public final long fragmentId;
        public final int outputDepId;
        public final ParameterSet parameters;
        /** true if distributes to all executable partitions */
        public final boolean multipartition;

        public SynthesizedPlanFragment(int fragmentId, boolean multipartition) {
            this(fragmentId, multipartition, ParameterSet.emptyParameterSet());
        }

        public SynthesizedPlanFragment(int fragmentId, boolean multipartition, ParameterSet parameters) {
            this(fragmentId, fragmentId, multipartition, parameters);
        }

        public SynthesizedPlanFragment(int fragmentId, int outputDepId, boolean multipartition,
                ParameterSet parameters) {
            this(-1, fragmentId, outputDepId, multipartition, parameters);
        }

        public SynthesizedPlanFragment(long siteId, int fragmentId, int outputDepId, boolean multipartition,
                ParameterSet parameters) {
            this.siteId = siteId;
            this.fragmentId = fragmentId;
            this.outputDepId = outputDepId;
            this.parameters = parameters;
            this.multipartition = multipartition;
        }

        /**
         * Most MP sysprocs use this pattern of MP fragment and a non-MP aggregator fragment. Utility method to create
         * that.
         *
         * @param fragmentId    Id of the MP fragment distributed to sites. This will be the fragment id of the first
         *                      SynthesizedPlanFragment, its output dependency id and input dependency id of the
         *                      aggregator fragment.
         * @param aggFragmentId Id of the aggregator fragment. This will be the fragment id of the second
         *                      SynthesizedPlanFragment and its output dependency id.
         * @param params        {@link ParameterSet} for the first fragment.
         * @return Array of fragments in the correct order
         */
        static SynthesizedPlanFragment[] createFragmentAndAggregator(int fragmentId, int aggFragmentId,
                ParameterSet params) {
            SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];

            pfs[0] = new SynthesizedPlanFragment(fragmentId, true, params);
            pfs[1] = new SynthesizedPlanFragment(aggFragmentId, false, ParameterSet.emptyParameterSet());

            return pfs;
        }
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

        for (SynthesizedPlanFragment pf : pfs) {
            assert (pf.parameters != null);

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

            task.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_SITE);

            if (pf.multipartition) {
                // create a workunit for every execution site
                task.setBatch(txnState.getNextFragmentIndex());
                txnState.createAllParticipatingFragmentWork(task);
            } else {
                // create one workunit for the current site
                if (pf.siteId == -1) {
                    txnState.createLocalFragmentWork(task, false);
                } else {
                    txnState.createFragmentWork(new long[] { pf.siteId },
                                                         task);
                }
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

    /**
     * @param fragmentId    Of the multi partition fragment
     * @param aggFragmentId Fragment ID of the aggeregator fragment
     * @return Resulting {@link VoltTable[]} from executing the fragments
     * @see #createAndExecuteSysProcPlan(int, int, ParameterSet)
     */
    protected VoltTable[] createAndExecuteSysProcPlan(int fragmentId, int aggFragmentId) {
        return createAndExecuteSysProcPlan(fragmentId, aggFragmentId, ParameterSet.emptyParameterSet());
    }

    /**
     * @param fragmentId    Of the multi partition fragment
     * @param aggFragmentId Fragment ID of the aggeregator fragment
     * @param params        Parameters which is to be passed to the multi partition fragment
     * @return Resulting {@link VoltTable[]} from executing the fragments
     * @see #createAndExecuteSysProcPlan(int, int, ParameterSet)
     */
    protected VoltTable[] createAndExecuteSysProcPlan(int fragmentId, int aggFragmentId, Object... params) {
        return createAndExecuteSysProcPlan(fragmentId, aggFragmentId, ParameterSet.fromArrayNoCopy(params));
    }

    /**
     * Create a two {@link SynthesizedPlanFragment} the first one is a multi partition fragment and the second is an
     * aggregator fragment. Then execute the fragments and return the resulting {@link VoltTable[]} from the aggregator
     * fragment.
     *
     * @param fragmentId    Of the multi partition fragment
     * @param aggFragmentId Fragment ID of the aggeregator fragment
     * @param params        {@link ParameterSet} which is to be passed to the multi partition fragment
     * @return Resulting {@link VoltTable[]} from executing the fragments
     */
    protected VoltTable[] createAndExecuteSysProcPlan(int fragmentId, int aggFragmentId, ParameterSet params) {
        return executeSysProcPlanFragments(
                SynthesizedPlanFragment.createFragmentAndAggregator(fragmentId, aggFragmentId, params),
                aggFragmentId);
    }
}
