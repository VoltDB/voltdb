/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.iv2.MpTransactionState;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;

import com.google_voltpatches.common.primitives.Longs;

/**
 * System procedures extend VoltSystemProcedure and use its utility methods to
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
    protected static long STATUS_OK = 0L;
    protected static long STATUS_FAILURE = 1L;

    protected Procedure m_catProc;
    protected Cluster m_cluster;
    protected SiteProcedureConnection m_site;
    private LoadedProcedureSet m_loadedProcedureSet;
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
            LoadedProcedureSet loadedProcedureSet,
            Procedure catProc, Cluster cluster) {

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
        txnState.setupProcedureResume(false, new int[] { aggregatorOutputDependencyId });

        VoltTable[] results = new VoltTable[1];
        executeSysProcPlanFragmentsAsync(pfs);

        // execute the tasks that just got queued.
        // recursively call recurableRun and don't allow it to shutdown
        Map<Integer, List<VoltTable>> mapResults = m_site.recursableRun(txnState);

        if (mapResults != null) {
            List<VoltTable> matchingTablesForId = mapResults.get(aggregatorOutputDependencyId);
            if (matchingTablesForId == null) {
                assert (mapResults.size() == 0);
                results[0] = null;
            } else {
                results[0] = matchingTablesForId.get(0);
            }
        }

        return results;
    }

    /*
     * When restoring replicated tables I found that a single site can receive multiple fragments instructing it to
     * distribute a replicated table. It processes each fragment causing it to enter executeSysProcPlanFragments
     * twice. Each time it enters it has generated a task for some other site, and is waiting on dependencies.
     *
     * The problem is that they will come back out of order, the dependency for the first task comes while
     * the site is waiting for the dependency of the second task. When the second dependency arrives we fail
     * to drop out because of extra/mismatches dependencies.
     *
     * The solution is to recognize unexpected dependencies, stash them away, and then check for them each time
     * we finish running a plan fragment. This doesn't allow you to process the dependencies immediately
     * (Continuations anyone?), but it doesn't deadlock and is good enough for restore.
     */
    private final Map<Integer, List<VoltTable>> m_unexpectedDependencies =
            new HashMap<Integer, List<VoltTable>>();

    /*
     * A helper method for snapshot restore that manages a mailbox run loop and dependency tracking.
     * The mailbox is a dedicated mailbox for snapshot restore. This assumes a very specific plan fragment
     * worklow where fragments 0 - (N - 1) all have a single output dependency that is aggregated
     * by fragment N which uses their output dependencies as it's input dependencies.
     *
     * This matches the workflow of snapshot restore
     *
     * This is not safe to use after restore because it doesn't do failure handling that would deal with
     * dropped plan fragments
     */
    public VoltTable[] executeSysProcPlanFragments(SynthesizedPlanFragment pfs[], Mailbox m) {
        Set<Integer> dependencyIds = new HashSet<Integer>();
        VoltTable results[] = new VoltTable[1];

        /*
         * Iterate the plan fragments and distribute them. Each
         * plan fragment goes to an individual site.
         * The output dependency of each fragment is added to the
         * set of expected dependencies
         */
        for (int ii = 0; ii < pfs.length - 1; ii++) {
            SynthesizedPlanFragment pf = pfs[ii];
            dependencyIds.add(pf.outputDepId);

            log.trace(
                    "Sending fragment " + pf.fragmentId + " dependency " + pf.outputDepId +
                    " from " + CoreUtils.hsIdToString(m.getHSId()) + "-" +
                            CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) + " to " +
                            CoreUtils.hsIdToString(pf.siteId));
            /*
             * The only real data is the fragment id, output dep id,
             * and parameters. Transactions ids, readonly-ness, and finality-ness
             * are unused.
             */
            FragmentTaskMessage ftm =
                    FragmentTaskMessage.createWithOneFragment(
                            0,
                            m.getHSId(),
                            0,
                            0,
                            false,
                            fragIdToHash(pf.fragmentId),
                            pf.outputDepId,
                            pf.parameters,
                            false,
                            m_runner.getTxnState().isForReplay());
            m.send(pf.siteId, ftm);
        }

        /*
         * Track the received dependencies. Stored as a list because executePlanFragment for
         * the aggregator plan fragment expects the tables as a list in the dependency map,
         * but sysproc fragments only every have a single output dependency.
         */
        Map<Integer, List<VoltTable>> receivedDependencyIds = new HashMap<Integer, List<VoltTable>>();

        /*
         * This loop will wait for all the responses to the fragment that was sent out,
         * but will also respond to incoming fragment tasks by executing them.
         */
        while (true) {
            //Lightly spinning makes debugging easier by allowing inspection
            //of stuff on the stack
            VoltMessage vm = m.recvBlocking(1000);
            if (vm == null) continue;

            if (vm instanceof FragmentTaskMessage) {
                FragmentTaskMessage ftm = (FragmentTaskMessage)vm;
                DependencyPair dp =
                        m_runner.executeSysProcPlanFragment(
                                m_runner.getTxnState(),
                                null,
                                hashToFragId(ftm.getPlanHash(0)),
                                ftm.getParameterSetForFragment(0));
                FragmentResponseMessage frm = new FragmentResponseMessage(ftm, m.getHSId());
                frm.addDependency(dp.depId, dp.dependency);
                m.send(ftm.getCoordinatorHSId(), frm);

                if (!m_unexpectedDependencies.isEmpty()) {
                    for (Integer dependencyId : dependencyIds) {
                        if (m_unexpectedDependencies.containsKey(dependencyId)) {
                            receivedDependencyIds.put(dependencyId, m_unexpectedDependencies.remove(dependencyId));
                        }
                    }

                    /*
                     * This predicate exists below in FRM handling, they have to match
                     */
                    if (receivedDependencyIds.size() == dependencyIds.size() &&
                            receivedDependencyIds.keySet().equals(dependencyIds)) {
                        break;
                    }
                }
            } else if (vm instanceof FragmentResponseMessage) {
                FragmentResponseMessage frm = (FragmentResponseMessage)vm;
                final int dependencyId = frm.getTableDependencyIdAtIndex(0);
                if (dependencyIds.contains(dependencyId)) {
                    receivedDependencyIds.put(
                            dependencyId,
                            Arrays.asList(new VoltTable[] {frm.getTableAtIndex(0)}));
                    log.trace("Received dependency at " + CoreUtils.hsIdToString(m.getHSId()) +
                            "-" + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                            " from " + CoreUtils.hsIdToString(frm.m_sourceHSId) +
                            " have " + receivedDependencyIds.size() + " " + receivedDependencyIds.keySet() +
                            " and need " + dependencyIds.size() + " " + dependencyIds);
                    /*
                     * This predicate exists above in FTM handling, they have to match
                     */
                    if (receivedDependencyIds.size() == dependencyIds.size() &&
                            receivedDependencyIds.keySet().equals(dependencyIds)) {
                        break;
                    }
                } else {
                    /*
                     * Stash the dependency intended for a different fragment
                     */
                    if (m_unexpectedDependencies.put(
                            dependencyId,
                            Arrays.asList(new VoltTable[] {frm.getTableAtIndex(0)})) != null) {
                        VoltDB.crashGlobalVoltDB("Received a duplicate dependency", true, null);
                    }
                }
            }
        }

        /*
         * Executing the last aggregator plan fragment in the list produces the result
         */
        results[0] =
                m_runner.executeSysProcPlanFragment(
                        m_runner.getTxnState(),
                        receivedDependencyIds,
                        pfs[pfs.length - 1].fragmentId,
                        pfs[pfs.length - 1].parameters).dependency;
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
                    txnState.isForReplay());
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
