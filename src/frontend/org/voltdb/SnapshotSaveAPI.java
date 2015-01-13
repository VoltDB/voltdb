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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.sysprocs.saverestore.CSVSnapshotWritePlan;
import org.voltdb.sysprocs.saverestore.HashinatorSnapshotData;
import org.voltdb.sysprocs.saverestore.IndexSnapshotWritePlan;
import org.voltdb.sysprocs.saverestore.NativeSnapshotWritePlan;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotWritePlan;
import org.voltdb.sysprocs.saverestore.StreamSnapshotWritePlan;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

/**
 * SnapshotSaveAPI extracts reusuable snapshot production code
 * that can be called from the SnapshotSave stored procedure or
 * directly from an ExecutionSite thread, perhaps has a message
 * or failure action.
 */
public class SnapshotSaveAPI
{
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotSaveAPI.class.getName());
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    // ugh, ick, ugh
    // IV2 does not do any snapshot work on any sites on a rejoining node.
    // The new snapshot planning stuff post-3.0 depends on it.
    public static final AtomicInteger recoveringSiteCount = new AtomicInteger(0);

    /**
     * Global collections populated by snapshot creator, poll'd by individual sites
     */
    // The four items containing createSetup artifacts below are all synchronized on m_createLock.
    private static final Object m_createLock = new Object();
    private static final Map<Long, Deque<SnapshotTableTask>> m_taskListsForHSIds =
        new HashMap<Long, Deque<SnapshotTableTask>>();
    private static final AtomicReference<VoltTable> m_createResult = new AtomicReference<VoltTable>();
    private static final AtomicBoolean m_createSuccess = new AtomicBoolean(false);
    private static ListenableFuture<DeferredSnapshotSetup> m_deferredSetupFuture = null;

    //Protected by SnapshotSiteProcessor.m_snapshotCreateLock when accessed from SnapshotSaveAPI.startSnanpshotting
    private static Map<Integer, Long> m_partitionLastSeenTransactionIds =
            new HashMap<Integer, Long>();

    /*
     * Ugh!, needs to be visible to all the threads doing the snapshot,
     * pbulished under the snapshot create lock.
     */
    private static Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers;

    /**
     * The only public method: do all the work to start a snapshot.
     * Assumes that a snapshot is feasible, that the caller has validated it can
     * be accomplished, that the caller knows this is a consistent or useful
     * transaction point at which to snapshot.
     *
     * @param file_path
     * @param file_nonce
     * @param format
     * @param block
     * @param txnId
     * @param data
     * @param context
     * @param hostname
     * @return VoltTable describing the results of the snapshot attempt
     */
    public VoltTable startSnapshotting(
            final String file_path, final String file_nonce, final SnapshotFormat format, final byte block,
            final long multiPartTxnId, final long partitionTxnId, final long legacyPerPartitionTxnIds[],
            final String data, final SystemProcedureExecutionContext context, final String hostname,
            final HashinatorSnapshotData hashinatorData,
            final long timestamp)
    {
        TRACE_LOG.trace("Creating snapshot target and handing to EEs");
        final VoltTable result = SnapshotUtil.constructNodeResultsTable();
        final int numLocalSites = context.getCluster().getDeployment().get("deployment").getSitesperhost();

        // One site wins the race to create the snapshot targets, populating
        // m_taskListsForSites for the other sites and creating an appropriate
        // number of snapshot permits.
        synchronized (SnapshotSiteProcessor.m_snapshotCreateLock) {

            SnapshotSiteProcessor.m_snapshotCreateSetupBarrierActualAction.set(new Runnable() {
                @Override
                public void run() {
                    Map<Integer, Long>  partitionTransactionIds = new HashMap<Integer, Long>();
                    partitionTransactionIds = m_partitionLastSeenTransactionIds;
                    SNAP_LOG.debug("Last seen partition transaction ids " + partitionTransactionIds);
                    m_partitionLastSeenTransactionIds = new HashMap<Integer, Long>();
                    partitionTransactionIds.put(TxnEgo.getPartitionId(multiPartTxnId), multiPartTxnId);


                    /*
                     * Do a quick sanity check that the provided IDs
                     * don't conflict with currently active partitions. If they do
                     * it isn't fatal we can just skip it.
                     */
                    for (long txnId : legacyPerPartitionTxnIds) {
                        final int legacyPartition = TxnEgo.getPartitionId(txnId);
                        if (partitionTransactionIds.containsKey(legacyPartition)) {
                            SNAP_LOG.warn("While saving a snapshot and propagating legacy " +
                                "transaction ids found an id that matches currently active partition" +
                                partitionTransactionIds.get(legacyPartition));
                        } else {
                            partitionTransactionIds.put( legacyPartition, txnId);
                        }
                    }
                    exportSequenceNumbers = SnapshotSiteProcessor.getExportSequenceNumbers();
                    createSetupIv2(
                            file_path,
                            file_nonce,
                            format,
                            multiPartTxnId,
                            partitionTransactionIds,
                            data,
                            context,
                            result,
                            exportSequenceNumbers,
                            context.getSiteTrackerForSnapshot(),
                            hashinatorData,
                            timestamp);
                }
            });

            // Create a barrier to use with the current number of sites to wait for
            // or if the barrier is already set up check if it is broken and reset if necessary
            SnapshotSiteProcessor.readySnapshotSetupBarriers(numLocalSites);

            //From within this EE, record the sequence numbers as of the start of the snapshot (now)
            //so that the info can be put in the digest.
            SnapshotSiteProcessor.populateExportSequenceNumbersForExecutionSite(context);
            SNAP_LOG.debug("Registering transaction id " + partitionTxnId + " for " +
                    TxnEgo.getPartitionId(partitionTxnId));
            m_partitionLastSeenTransactionIds.put(TxnEgo.getPartitionId(partitionTxnId), partitionTxnId);
        }

        boolean runPostTasks = false;
        VoltTable earlyResultTable = null;
        try {
            SnapshotSiteProcessor.m_snapshotCreateSetupBarrier.await();
            try {
                synchronized (m_createLock) {
                    SNAP_LOG.debug("Found tasks for HSIds: " +
                            CoreUtils.hsIdCollectionToString(m_taskListsForHSIds.keySet()));
                    SNAP_LOG.debug("Looking for local HSID: " +
                            CoreUtils.hsIdToString(context.getSiteId()));
                    Deque<SnapshotTableTask> taskList = m_taskListsForHSIds.remove(context.getSiteId());
                    // If createSetup failed, then the first site to reach here is going
                    // to send the results table generated by createSetup, and then empty out the table.
                    // All other sites to reach here will send the appropriate empty table.
                    // If createSetup was a success but the taskList is null, then we'll use the block
                    // switch to figure out what flavor of empty SnapshotSave result table to return.
                    if (!m_createSuccess.get()) {
                        // There shouldn't be any work for any site if we failed
                        assert(m_taskListsForHSIds.isEmpty());
                        VoltTable finalresult = m_createResult.get();
                        if (finalresult != null) {
                            m_createResult.set(null);
                            earlyResultTable = finalresult;
                        }
                        else {
                            // We returned a non-empty NodeResultsTable with the failures in it,
                            // every other site needs to return a NodeResultsTable as well.
                            earlyResultTable = SnapshotUtil.constructNodeResultsTable();
                        }
                    }
                    else if (taskList == null) {
                        SNAP_LOG.debug("No task for this site, block " + block);
                        // This node is participating in the snapshot but this site has nothing to do.
                        // Send back an appropriate empty table based on the block flag
                        if (block != 0) {
                            runPostTasks = true;
                            earlyResultTable = SnapshotUtil.constructPartitionResultsTable();
                            earlyResultTable.addRow(context.getHostId(), hostname,
                                    CoreUtils.getSiteIdFromHSId(context.getSiteId()), "SUCCESS", "");
                        } else {
                            earlyResultTable = SnapshotUtil.constructNodeResultsTable();
                        }
                    }
                    else {
                        context.getSiteSnapshotConnection().initiateSnapshots(
                                format,
                                taskList,
                                multiPartTxnId,
                                exportSequenceNumbers);
                    }

                    if (m_deferredSetupFuture != null && taskList != null) {
                        // Add a listener to the deferred setup so that it can kick off the snapshot
                        // task once the setup is done.
                        m_deferredSetupFuture.addListener(new Runnable() {
                            @Override
                            public void run()
                            {
                                DeferredSnapshotSetup deferredSnapshotSetup = null;
                                try {
                                    deferredSnapshotSetup = m_deferredSetupFuture.get();
                                } catch (Exception e) {
                                    // it doesn't throw
                                }

                                assert deferredSnapshotSetup != null;
                                context.getSiteSnapshotConnection().startSnapshotWithTargets(
                                        deferredSnapshotSetup.getPlan().getSnapshotDataTargets());
                            }
                        }, CoreUtils.SAMETHREADEXECUTOR);
                    }
                }
            } finally {
                SnapshotSiteProcessor.m_snapshotCreateFinishBarrier.await(120, TimeUnit.SECONDS);
            }
        } catch (TimeoutException e) {
            VoltDB.crashLocalVoltDB(
                    "Timed out waiting 120 seconds for all threads to arrive and start snapshot", true, null);
        } catch (InterruptedException e) {
            result.addRow(
                    context.getHostId(),
                    hostname,
                    "",
                    "FAILURE",
                    CoreUtils.throwableToString(e));
            earlyResultTable = result;
        } catch (BrokenBarrierException e) {
            result.addRow(
                    context.getHostId(),
                    hostname,
                    "",
                    "FAILURE",
                    CoreUtils.throwableToString(e));
            earlyResultTable = result;
        }

        // If earlyResultTable is set, return here
        if (earlyResultTable != null) {
            if (runPostTasks) {
                // Need to run post-snapshot tasks before finishing
                SnapshotSiteProcessor.runPostSnapshotTasks(context);
            }
            return earlyResultTable;
        }

        if (block != 0) {
            HashSet<Exception> failures = Sets.newHashSet();
            String status = "SUCCESS";
            String err = "";
            try {
                // For blocking snapshot, propogate the error from deferred setup back to the client
                final DeferredSnapshotSetup deferredSnapshotSetup = m_deferredSetupFuture.get();
                if (deferredSnapshotSetup != null && deferredSnapshotSetup.getError() != null) {
                    status = "FAILURE";
                    err = deferredSnapshotSetup.getError().toString();
                    failures.add(deferredSnapshotSetup.getError());
                }

                failures.addAll(context.getSiteSnapshotConnection().completeSnapshotWork());
                SnapshotSiteProcessor.runPostSnapshotTasks(context);
            } catch (Exception e) {
                status = "FAILURE";
                err = e.toString();
                failures.add(e);
            }
            final VoltTable blockingResult = SnapshotUtil.constructPartitionResultsTable();

            if (failures.isEmpty()) {
                blockingResult.addRow(
                        context.getHostId(),
                        hostname,
                        CoreUtils.getSiteIdFromHSId(context.getSiteId()),
                        status,
                        err);
            } else {
                status = "FAILURE";
                for (Exception e : failures) {
                    err = e.toString();
                }
                blockingResult.addRow(
                        context.getHostId(),
                        hostname,
                        CoreUtils.getSiteIdFromHSId(context.getSiteId()),
                        status,
                        err);
            }
            return blockingResult;
        }

        return result;
    }

    /**
     * Create the completion node for the snapshot identified by the txnId. It
     * assumes that all hosts will race to call this, so it doesn't fail if the
     * node already exists.
     *
     * @param nonce Nonce of the snapshot
     * @param txnId
     * @param hostId The local host ID
     * @param isTruncation Whether or not this is a truncation snapshot
     * @param truncReqId Optional unique ID fed back to the monitor for identification
     * @return true if the node is created successfully, false if the node already exists.
     */
    public static ZKUtil.StringCallback createSnapshotCompletionNode(String path,
                                                                     String nonce,
                                                                     long txnId,
                                                                     boolean isTruncation,
                                                                     String truncReqId) {
        if (!(txnId > 0)) {
            VoltDB.crashGlobalVoltDB("Txnid must be greather than 0", true, null);
        }

        byte nodeBytes[] = null;
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.key("txnId").value(txnId);
            stringer.key("isTruncation").value(isTruncation);
            stringer.key("didSucceed").value(false);
            stringer.key("hostCount").value(-1);
            stringer.key("path").value(path);
            stringer.key("nonce").value(nonce);
            stringer.key("truncReqId").value(truncReqId);
            stringer.key("exportSequenceNumbers").object().endObject();
            stringer.endObject();
            JSONObject jsonObj = new JSONObject(stringer.toString());
            nodeBytes = jsonObj.toString(4).getBytes(Charsets.UTF_8);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error serializing snapshot completion node JSON", true, e);
        }

        ZKUtil.StringCallback cb = new ZKUtil.StringCallback();
        final String snapshotPath = VoltZK.completed_snapshots + "/" +  txnId;
        VoltDB.instance().getHostMessenger().getZK().create(
                snapshotPath, nodeBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                cb, null);

        return cb;
    }

    /**
     * Once participating host count is set, SnapshotCompletionMonitor can check this ZK node to
     * determine whether the snapshot has finished or not.
     *
     * This should only be called when all participants have responded. It is possible that some
     * hosts finish taking snapshot before the coordinator logs the participating host count. In
     * this case, the host count would have been decremented multiple times already. To make sure
     * finished hosts are logged correctly, this method adds participating host count + 1 to the
     * current host count.
     *
     * @param txnId The snapshot txnId
     * @param participantCount The number of hosts participating in this snapshot
     */
    public static void logParticipatingHostCount(long txnId, int participantCount) {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        final String snapshotPath = VoltZK.completed_snapshots + "/" + txnId;

        boolean success = false;
        while (!success) {
            Stat stat = new Stat();
            byte data[] = null;
            try {
                data = zk.getData(snapshotPath, false, stat);
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NONODE) {
                    // If snapshot creation failed for some reason, the node won't exist. ignore
                    return;
                }
                VoltDB.crashLocalVoltDB("Failed to get snapshot completion node", true, e);
            } catch (InterruptedException e) {
                VoltDB.crashLocalVoltDB("Interrupted getting snapshot completion node", true, e);
            }
            if (data == null) {
                VoltDB.crashLocalVoltDB("Data should not be null if the node exists", false, null);
            }

            try {
                JSONObject jsonObj = new JSONObject(new String(data, Charsets.UTF_8));
                if (jsonObj.getLong("txnId") != txnId) {
                    VoltDB.crashLocalVoltDB("TxnId should match", false, null);
                }

                int hostCount = jsonObj.getInt("hostCount");
                // +1 because hostCount was initialized to -1
                jsonObj.put("hostCount", hostCount + participantCount + 1);
                zk.setData(snapshotPath, jsonObj.toString(4).getBytes(Charsets.UTF_8),
                        stat.getVersion());
            } catch (KeeperException.BadVersionException e) {
                continue;
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("This ZK call should never fail", true, e);
            }

            success = true;
        }
    }

    private void createSetupIv2(
            final String file_path, final String file_nonce, SnapshotFormat format,
            final long txnId, final Map<Integer, Long> partitionTransactionIds,
            String data, final SystemProcedureExecutionContext context,
            final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp)
    {
        JSONObject jsData = null;
        if (data != null && !data.isEmpty()) {
            try {
                jsData = new JSONObject(data);
            }
            catch (JSONException e) {
                SNAP_LOG.error(String.format("JSON exception on snapshot data \"%s\".", data),
                        e);
            }
        }

        SnapshotWritePlan plan;
        if (format == SnapshotFormat.NATIVE) {
            plan = new NativeSnapshotWritePlan();
        }
        else if (format == SnapshotFormat.CSV) {
            plan = new CSVSnapshotWritePlan();
        }
        else if (format == SnapshotFormat.STREAM) {
            plan = new StreamSnapshotWritePlan();
        }
        else if (format == SnapshotFormat.INDEX) {
            plan = new IndexSnapshotWritePlan();
        }
        else {
            throw new RuntimeException("BAD BAD BAD");
        }
        final Callable<Boolean> deferredSetup = plan.createSetup(file_path, file_nonce, txnId,
                partitionTransactionIds, jsData, context, result, exportSequenceNumbers, tracker,
                hashinatorData, timestamp);
        m_deferredSetupFuture =
                VoltDB.instance().submitSnapshotIOWork(
                        new DeferredSnapshotSetup(plan, deferredSetup, txnId, partitionTransactionIds));

        synchronized (m_createLock) {
            //Seems like this should be cleared out just in case
            //Log if there is actually anything to clear since it is unexpected
            if (!m_taskListsForHSIds.isEmpty()) {
                SNAP_LOG.warn("Found lingering snapshot tasks while setting up a snapshot");
            }
            m_taskListsForHSIds.clear();
            m_createSuccess.set(true);
            m_createResult.set(result);

            m_taskListsForHSIds.putAll(plan.getTaskListsForHSIds());

            // HACK HACK HACK.  If the task list is empty, this host has no work to do for
            // this snapshot.  We're going to create an empty list of tasks for one of the sites to do
            // so that we'll have a SnapshotSiteProcessor which will do the logSnapshotCompleteToZK.
            if (m_taskListsForHSIds.isEmpty()) {
                SNAP_LOG.debug("Node had no snapshot work to do.  Creating a null task to drive completion.");
                m_taskListsForHSIds.put(context.getSiteId(), new ArrayDeque<SnapshotTableTask>());
            }
            SNAP_LOG.debug("Planned tasks: " +
                           CoreUtils.hsIdCollectionToString(plan.getTaskListsForHSIds().keySet()));
            SNAP_LOG.debug("Created tasks for HSIds: " +
                           CoreUtils.hsIdCollectionToString(m_taskListsForHSIds.keySet()));
        }
    }
}
