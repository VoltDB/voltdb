/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.rejoin.StreamSnapshotDataTarget;

import org.voltdb.sysprocs.saverestore.CSVSnapshotWritePlan;
import org.voltdb.sysprocs.saverestore.NativeSnapshotWritePlan;
import org.voltdb.sysprocs.saverestore.SnapshotWritePlan;
import org.voltdb.sysprocs.saverestore.StreamSnapshotWritePlan;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotSave;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.CatalogUtil;

import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

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
     * Global collection populated by snapshot creator, poll'd by individual sites
     */
    private static final LinkedList<Deque<SnapshotTableTask>> m_taskListsForSites =
        new LinkedList<Deque<SnapshotTableTask>>();

    // The four items containing createSetup artifacts below are all synchronized on m_createLock.
    private static final Object m_createLock = new Object();
    private static final Map<Long, Deque<SnapshotTableTask>> m_taskListsForHSIds =
        new HashMap<Long, Deque<SnapshotTableTask>>();
    private static final List<SnapshotDataTarget> m_snapshotDataTargets =
        new ArrayList<SnapshotDataTarget>();
    private static final AtomicReference<VoltTable> m_createResult = new AtomicReference<VoltTable>();
    private static final AtomicBoolean m_createSuccess = new AtomicBoolean(false);

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
            final long timestamp)
    {
        TRACE_LOG.trace("Creating snapshot target and handing to EEs");
        final VoltTable result = SnapshotSave.constructNodeResultsTable();
        final SiteTracker st = context.getSiteTrackerForSnapshot();
        final int numLocalSites = st.getLocalSites().length;

        // One site wins the race to create the snapshot targets, populating
        // m_taskListsForSites for the other sites and creating an appropriate
        // number of snapshot permits.
        synchronized (SnapshotSiteProcessor.m_snapshotCreateLock) {

            SnapshotSiteProcessor.m_snapshotCreateSetupBarrierActualAction.set(new Runnable() {
                @Override
                public void run() {
                    Map<Integer, Long>  partitionTransactionIds = new HashMap<Integer, Long>();
                    if (VoltDB.instance().isIV2Enabled()) {
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
                    }
                    exportSequenceNumbers = SnapshotSiteProcessor.getExportSequenceNumbers();
                    if (VoltDB.instance().isIV2Enabled()) {
                        createSetupIv2(
                                file_path,
                                file_nonce,
                                format,
                                multiPartTxnId,
                                partitionTransactionIds,
                                data,
                                context,
                                hostname,
                                result,
                                exportSequenceNumbers,
                                st,
                                timestamp);
                    }
                    else {
                        createSetup(
                                file_path,
                                file_nonce,
                                format,
                                multiPartTxnId,
                                partitionTransactionIds,
                                data,
                                context,
                                hostname,
                                result,
                                exportSequenceNumbers,
                                st,
                                timestamp);
                    }
                }
            });

            // Create a barrier to use with the current number of sites to wait for
            // or if the barrier is already set up check if it is broken and reset if necessary
            SnapshotSiteProcessor.readySnapshotSetupBarriers(numLocalSites);

            //From within this EE, record the sequence numbers as of the start of the snapshot (now)
            //so that the info can be put in the digest.
            SnapshotSiteProcessor.populateExportSequenceNumbersForExecutionSite(context);
            if (VoltDB.instance().isIV2Enabled()) {
                SNAP_LOG.debug("Registering transaction id " + partitionTxnId + " for " +
                        TxnEgo.getPartitionId(partitionTxnId));
                m_partitionLastSeenTransactionIds.put(TxnEgo.getPartitionId(partitionTxnId), partitionTxnId);
            }
        }

        try {
            SnapshotSiteProcessor.m_snapshotCreateSetupBarrier.await();
            try {
                if (VoltDB.instance().isIV2Enabled()) {
                    synchronized (m_createLock) {
                        SNAP_LOG.info("Found tasks for HSIds: " + CoreUtils.hsIdCollectionToString(m_taskListsForHSIds.keySet()));
                        SNAP_LOG.info("Looking for local HSID: " + CoreUtils.hsIdToString(context.getSiteId()));
                        Deque<SnapshotTableTask> taskList = m_taskListsForHSIds.remove(context.getSiteId());
                        List<SnapshotDataTarget> targetList = new ArrayList<SnapshotDataTarget>(m_snapshotDataTargets);
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
                                return finalresult;
                            }
                            else {
                                // We returned a non-empty NodeResultsTable with the failures in it,
                                // every other site needs to return a NodeResultsTable as well.
                                return SnapshotSave.constructNodeResultsTable();
                            }
                        }
                        else if (taskList == null) {
                            // This node is participating in the snapshot but this site has nothing to do.
                            // Send back an appropriate empty table based on the block flag
                            if (block != 0) {
                                return SnapshotSave.constructPartitionResultsTable();
                            }
                            else {
                                return SnapshotSave.constructNodeResultsTable();
                            }
                        }
                        else {
                            context.getSiteSnapshotConnection().initiateSnapshots(
                                    taskList,
                                    targetList,
                                    multiPartTxnId,
                                    context.getSiteTrackerForSnapshot().getAllHosts().size(),
                                    exportSequenceNumbers);
                        }
                    }
                }
                else {
                    /*
                     * The synchronized block doesn't throw IE or BBE, but wrapping
                     * both barriers saves writing the exception handling twice
                     */
                    synchronized (m_taskListsForSites) {
                        final Deque<SnapshotTableTask> m_taskList = m_taskListsForSites.poll();
                        if (m_taskList == null) {
                            return result;
                        } else {
                            context.getSiteSnapshotConnection().initiateSnapshots(
                                    m_taskList,
                                    null,
                                    multiPartTxnId,
                                    context.getSiteTrackerForSnapshot().getAllHosts().size(),
                                    exportSequenceNumbers);
                        }
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
            return result;
        } catch (BrokenBarrierException e) {
            result.addRow(
                    context.getHostId(),
                    hostname,
                    "",
                    "FAILURE",
                    CoreUtils.throwableToString(e));
            return result;
        }

        if (block != 0) {
            HashSet<Exception> failures = null;
            String status = "SUCCESS";
            String err = "";
            try {
                failures = context.getSiteSnapshotConnection().completeSnapshotWork();
            } catch (InterruptedException e) {
                status = "FAILURE";
                err = e.toString();
                failures = new HashSet<Exception>();
                failures.add(e);
            }
            final VoltTable blockingResult = SnapshotSave.constructPartitionResultsTable();

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


    private void logSnapshotStartToZK(long txnId,
            SystemProcedureExecutionContext context, String nonce, String truncReqId) {
        /*
         * Going to send out the requests async to make snapshot init move faster
         */
        ZKUtil.StringCallback cb1 = new ZKUtil.StringCallback();

        /*
         * Log that we are currently snapshotting this snapshot
         */
        try {
            //This node shouldn't already exist... should have been erased when the last snapshot finished
            assert(VoltDB.instance().getHostMessenger().getZK().exists(
                    VoltZK.nodes_currently_snapshotting + "/" + VoltDB.instance().getHostMessenger().getHostId(), false)
                    == null);
            ByteBuffer snapshotTxnId = ByteBuffer.allocate(8);
            snapshotTxnId.putLong(txnId);
            VoltDB.instance().getHostMessenger().getZK().create(
                    VoltZK.nodes_currently_snapshotting + "/" + VoltDB.instance().getHostMessenger().getHostId(),
                    snapshotTxnId.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, cb1, null);
        } catch (NodeExistsException e) {
            SNAP_LOG.warn("Didn't expect the snapshot node to already exist", e);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        String nextTruncationNonce = null;
        boolean isTruncation = false;
        try {
            final byte payloadBytes[] =
                VoltDB.instance().getHostMessenger().getZK().getData(VoltZK.request_truncation_snapshot, false, null);
            //request_truncation_snapshot data may be null when initially created. If that is the case
            //then this snapshot is definitely not a truncation snapshot because
            //the snapshot daemon hasn't gotten around to asking for a truncation snapshot
            if (payloadBytes != null) {
                ByteBuffer payload = ByteBuffer.wrap(payloadBytes);
                nextTruncationNonce = Long.toString(payload.getLong());
            }
        } catch (KeeperException.NoNodeException e) {}
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Getting the nonce should never fail with anything other than no node", true, e);
        }
        if (nextTruncationNonce == null) {
            isTruncation = false;
        } else {
            if (nextTruncationNonce.equals(nonce)) {
                isTruncation = true;
            } else {
                isTruncation = false;
            }
        }

        /*
         * Race with the others to create the place where will count down to completing the snapshot
         */
        if (!createSnapshotCompletionNode(nonce, txnId, context.getHostId(), isTruncation, truncReqId)) {
            // the node already exists, add local host ID to the list
            increaseParticipateHost(txnId, context.getHostId());
        }

        try {
            cb1.get();
        } catch (NodeExistsException e) {
            SNAP_LOG.warn("Didn't expect the snapshot node to already exist", e);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
    }

    /**
     * Add the host to the list of hosts participating in this snapshot.
     *
     * @param txnId The snapshot txnId
     * @param hostId The host ID of the host that's calling this
     */
    public static void increaseParticipateHost(long txnId, int hostId) {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();

        final String snapshotPath = VoltZK.completed_snapshots + "/" + txnId;
        boolean success = false;
        while (!success) {
            Stat stat = new Stat();
            byte data[] = null;
            try {
                data = zk.getData(snapshotPath, false, stat);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("This ZK get should never fail", true, e);
            }
            if (data == null) {
                VoltDB.crashLocalVoltDB("Data should not be null if the node exists", false, null);
            }

            try {
                JSONObject jsonObj = new JSONObject(new String(data, Charsets.UTF_8));
                if (jsonObj.getLong("txnId") != txnId) {
                    VoltDB.crashLocalVoltDB("TxnId should match", false, null);
                }

                boolean hasLocalhost = false;
                JSONArray hosts = jsonObj.getJSONArray("hosts");
                for (int i = 0; i < hosts.length(); i++) {
                    if (hosts.getInt(i) == hostId) {
                        hasLocalhost = true;
                        break;
                    }
                }
                if (!hasLocalhost) {
                    hosts.put(hostId);
                }

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
    public static boolean createSnapshotCompletionNode(String nonce,
                                                          long txnId,
                                                          int hostId,
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
            stringer.key("hosts").array().value(hostId).endArray();
            stringer.key("isTruncation").value(isTruncation);
            stringer.key("hostCount").value(-1);
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

        try {
            cb.get();
            return true;
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unexpected exception logging snapshot completion to ZK", true, e);
        }

        return false;
    }

    /**
     * Freezes participating host count base on the number of hosts in the "hosts" field of the
     * snapshot completion ZK node. Once participating host count is set, SnapshotCompletionMonitor
     * can check this ZK node to determine whether the snapshot has finished or not.
     *
     * This should only be called when all participants have logged themselves in the completion
     * ZK node. It is possible that some hosts finish taking snapshot before the coordinator logs
     * the participating host count. In this case, the host count would have been decremented
     * multiple times already. To make sure finished hosts are logged correctly, this method adds
     * participating host count + 1 to the current host count.
     *
     * @param txnId The snapshot txnId
     */
    public static void logParticipatingHostCount(long txnId) {
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

                JSONArray hosts = jsonObj.getJSONArray("hosts");
                int hostCount = jsonObj.getInt("hostCount");
                // +1 because hostCount was initialized to -1
                jsonObj.put("hostCount", hostCount + hosts.length() + 1);
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
            String file_path, String file_nonce, SnapshotFormat format,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            String data, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker, long timestamp)
    {
        JSONObject jsData = null;
        String truncReqId = "";
        if (data != null && !data.isEmpty()) {
            try {
                jsData = new JSONObject(data);
                if (jsData.has("truncReqId")) {
                    truncReqId = jsData.getString("truncReqId");
                }
            }
            catch (JSONException e) {
                SNAP_LOG.error(String.format("JSON exception on snapshot data \"%s\".", data),
                        e);
            }
        }
        SnapshotWritePlan plan = null;
        if (format == SnapshotFormat.NATIVE) {
            plan = new NativeSnapshotWritePlan();
        }
        else if (format == SnapshotFormat.CSV) {
            plan = new CSVSnapshotWritePlan();
        }
        else if (format == SnapshotFormat.STREAM) {
            plan = new StreamSnapshotWritePlan();
        }
        else {
            throw new RuntimeException("BAD BAD BAD");
        }
        boolean abort = plan.createSetup(file_path, file_nonce, txnId, partitionTransactionIds,
                jsData, context, hostname, result, exportSequenceNumbers, tracker, timestamp);
        synchronized (m_createLock) {
            //Seems like this should be cleared out just in case
            //Log if there is actually anything to clear since it is unexpected
            if (!m_taskListsForHSIds.isEmpty() || !m_snapshotDataTargets.isEmpty()) {
                SNAP_LOG.warn("Found lingering snapshot tasks while setting up a snapshot");
                m_taskListsForHSIds.clear();
                m_snapshotDataTargets.clear();
            }
            m_createSuccess.set(!abort);
            m_createResult.set(result);
            if (!abort) {
                m_taskListsForHSIds.putAll(plan.getTaskListsForHSIds());
                m_snapshotDataTargets.addAll(plan.getSnapshotDataTargets());
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
                /*
                 * Inform the SnapshotCompletionMonitor of what the partition specific txnids for
                 * this snapshot were so it can forward that to completion interests.
                 */
                VoltDB.instance().getSnapshotCompletionMonitor().registerPartitionTxnIdsForSnapshot(
                        txnId, partitionTransactionIds);
                // Provide the truncation request ID so the monitor can recognize a specific snapshot.
                logSnapshotStartToZK( txnId, context, file_nonce, truncReqId);
            }
        }
    }

    // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
    // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING ON THE 3.0 BRANCH
    // UNTIL SUCH TIME AS WE START DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE POST-3.0 NO LONGER
    // USES ANY OF THIS METHOD.  SO DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR WORK TO HAVE
    // ANY MEANING.  MESSAGE REPEATS...
    private void createSetup(
            String file_path, String file_nonce, SnapshotFormat format,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            String data, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker, long timestamp) {
        {
            final int numLocalSites =
                    (tracker.getLocalSites().length - recoveringSiteCount.get());

            // not empty if targeting only one site (used for rejoin)
            // set later from the "data" JSON string
            Map<Integer, Long> streamPairs = new HashMap<Integer, Long>();

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

            MessageDigest digest;
            try {
                digest = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new AssertionError(e);
            }

            /*
             * List of partitions to include if this snapshot is
             * going to be deduped. Attempts to break up the work
             * by seeding and RNG selecting
             * a random replica to do the work. Will not work in failure
             * cases, but we don't use dedupe when we want durability.
             *
             * Originally used the partition id as the seed, but it turns out
             * that nextInt(2) returns a 1 for seeds 0-4095. Now use SHA-1
             * on the txnid + partition id.
             */
            // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
            // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING ON THE 3.0 BRANCH
            // UNTIL SUCH TIME AS WE START DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE POST-3.0 NO LONGER
            // USES ANY OF THIS METHOD.  SO DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR WORK TO HAVE
            // ANY MEANING.  MESSAGE REPEATS...
            List<Integer> partitionsToInclude = new ArrayList<Integer>();
            List<Long> sitesToInclude = new ArrayList<Long>();
            for (long localSite : tracker.getLocalSites()) {
                final int partitionId = tracker.getPartitionForSite(localSite);
                List<Long> sites =
                        new ArrayList<Long>(tracker.getSitesForPartition(tracker.getPartitionForSite(localSite)));
                Collections.sort(sites);

                digest.update(Longs.toByteArray(txnId));
                final long seed = Longs.fromByteArray(Arrays.copyOf( digest.digest(Ints.toByteArray(partitionId)), 8));

                int siteIndex = new java.util.Random(seed).nextInt(sites.size());
                if (localSite == sites.get(siteIndex)) {
                    partitionsToInclude.add(partitionId);
                    sitesToInclude.add(localSite);
                }
            }

            assert(partitionsToInclude.size() == sitesToInclude.size());

            /*
             * Used to close targets on failure
             */
            final ArrayList<SnapshotDataTarget> targets = new ArrayList<SnapshotDataTarget>();
            try {
                final ArrayDeque<SnapshotTableTask> partitionedSnapshotTasks =
                    new ArrayDeque<SnapshotTableTask>();
                final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
                    new ArrayList<SnapshotTableTask>();
                assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

                final List<Table> tables = SnapshotUtil.getTablesToSave(context.getDatabase());
                /*
                 * For a file based snapshot
                 */
                if (format.isFileBased()) {
                    Runnable completionTask = SnapshotUtil.writeSnapshotDigest(
                                                  txnId,
                                                  context.getCatalogCRC(),
                                                  file_path,
                                                  file_nonce,
                                                  tables,
                                                  context.getHostId(),
                                                  exportSequenceNumbers,
                                                  partitionTransactionIds,
                                                  VoltDB.instance().getHostMessenger().getInstanceId(),
                                                  timestamp);
                    if (completionTask != null) {
                        SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
                    }
                    completionTask = SnapshotUtil.writeSnapshotCatalog(file_path, file_nonce);
                    if (completionTask != null) {
                        SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
                    }
                }

                // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
                // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING ON THE 3.0 BRANCH
                // UNTIL SUCH TIME AS WE START DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE POST-3.0 NO LONGER
                // USES ANY OF THIS METHOD.  SO DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR WORK TO HAVE
                // ANY MEANING.  MESSAGE REPEATS...
                final AtomicInteger numTables = new AtomicInteger(tables.size());
                final SnapshotRegistry.Snapshot snapshotRecord =
                    SnapshotRegistry.startSnapshot(
                            txnId,
                            context.getHostId(),
                            file_path,
                            file_nonce,
                            format,
                            tables.toArray(new Table[0]));

                SnapshotDataTarget sdt = null;
                if (!format.isTableBased()) {
                    // table schemas for all the tables we'll snapshot on this partition
                    Map<Integer, byte[]> schemas = new HashMap<Integer, byte[]>();
                    for (final Table table : SnapshotUtil.getTablesToSave(context.getDatabase())) {
                        VoltTable schemaTable = CatalogUtil.getVoltTable(table);
                        schemas.put(table.getRelativeIndex(), schemaTable.getSchemaBytes());
                    }

                    if (format == SnapshotFormat.STREAM && jsData != null) {
                        try {
                            List<Long> localHSIds = tracker.getSitesForHost(context.getHostId());
                            JSONObject sp = jsData.getJSONObject("streamPairs");
                            @SuppressWarnings("unchecked")
                            Iterator<String> it = sp.keys();
                            while (it.hasNext()) {
                                String key = it.next();
                                long sourceHSId = Long.valueOf(key);
                                // See whether this source HSID is a local site, if so, we need
                                // the partition ID
                                if (localHSIds.contains(sourceHSId)) {
                                    int partitionId = tracker.getPartitionForSite(sourceHSId);
                                    Long destHSId = Long.valueOf(sp.getString(key));
                                    streamPairs.put(partitionId, destHSId);
                                }
                            }
                            if (streamPairs.size() > 0) {
                                sdt = new StreamSnapshotDataTarget(streamPairs.values().iterator().next(), schemas);
                            }
                            else {
                                sdt = new DevNullSnapshotTarget();
                            }
                        }
                        catch (JSONException e) {} // Leave sdt as null for later error path
                    }
                }

                // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
                // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING ON THE 3.0 BRANCH
                // UNTIL SUCH TIME AS WE START DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE POST-3.0 NO LONGER
                // USES ANY OF THIS METHOD.  SO DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR WORK TO HAVE
                // ANY MEANING.  MESSAGE REPEATS...
                for (final Table table : SnapshotUtil.getTablesToSave(context.getDatabase()))
                {
                    /*
                     * For a deduped csv snapshot, only produce the replicated tables on the "leader"
                     * host.
                     */
                    if (format == SnapshotFormat.CSV && table.getIsreplicated() && !tracker.isFirstHost()) {
                        snapshotRecord.removeTable(table.getTypeName());
                        continue;
                    }
                    String canSnapshot = "SUCCESS";
                    String err_msg = "";

                    File saveFilePath = null;
                    if (format.isFileBased()) {
                        saveFilePath = SnapshotUtil.constructFileForTable(
                                    table,
                                    file_path,
                                    file_nonce,
                                    format,
                                    context.getHostId());
                    }

                    try {
                        if (format == SnapshotFormat.CSV) {
                            sdt = new SimpleFileSnapshotDataTarget(saveFilePath, !table.getIsreplicated());
                        } else if (format == SnapshotFormat.NATIVE) {
                            sdt =
                                constructSnapshotDataTargetForTable(
                                        context,
                                        saveFilePath,
                                        table,
                                        context.getHostId(),
                                        tracker.m_numberOfPartitions,
                                        txnId,
                                        timestamp,
                                        tracker.getPartitionsForHost(context.getHostId()));
                        }

                        if (sdt == null) {
                            throw new IOException("Unable to create snapshot target");
                        }

                        targets.add(sdt);
                        final SnapshotDataTarget sdtFinal = sdt;
                        final Runnable onClose = new Runnable() {
                            @SuppressWarnings("synthetic-access")
                            @Override
                            public void run() {
                                snapshotRecord.updateTable(table.getTypeName(),
                                        new SnapshotRegistry.Snapshot.TableUpdater() {
                                    @Override
                                    public SnapshotRegistry.Snapshot.Table update(
                                            SnapshotRegistry.Snapshot.Table registryTable) {
                                        return snapshotRecord.new Table(
                                                registryTable,
                                                sdtFinal.getBytesWritten(),
                                                sdtFinal.getLastWriteException());
                                    }
                                });
                                int tablesLeft = numTables.decrementAndGet();
                                if (tablesLeft == 0) {
                                    final SnapshotRegistry.Snapshot completed =
                                        SnapshotRegistry.finishSnapshot(snapshotRecord);
                                    final double duration =
                                        (completed.timeFinished - completed.timeStarted) / 1000.0;
                                    SNAP_LOG.info(
                                            "Snapshot " + snapshotRecord.nonce + " finished at " +
                                             completed.timeFinished + " and took " + duration
                                             + " seconds ");
                                }
                            }
                        };

                        // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
                        // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING
                        // ON THE 3.0 BRANCH UNTIL SUCH TIME AS WE START
                        // DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE
                        // POST-3.0 NO LONGER USES ANY OF THIS METHOD.  SO
                        // DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR
                        // WORK TO HAVE
                        // ANY MEANING.  MESSAGE REPEATS...
                        sdt.setOnCloseHandler(onClose);

                        List<SnapshotDataFilter> filters = new ArrayList<SnapshotDataFilter>();
                        if (format == SnapshotFormat.CSV) {
                            /*
                             * Don't need to do filtering on a replicated table.
                             */
                            if (!table.getIsreplicated()) {
                                filters.add(
                                        new PartitionProjectionSnapshotFilter(
                                                Ints.toArray(partitionsToInclude),
                                                0));
                            }
                            filters.add(new CSVSnapshotFilter(CatalogUtil.getVoltTable(table), ',', null));
                        }

                        if (format == SnapshotFormat.STREAM) {
                            // if this snapshot targets sites on this node
                            if (streamPairs.size() > 0) {
                                filters.add(new PartitionProjectionSnapshotFilter(
                                            com.google.common.primitives.Ints.toArray(streamPairs.keySet()),
                                            sdt.getHeaderSize()));
                            }
                            else {
                                // filter EVERYTHING because the site we want isn't local
                                filters.add(new PartitionProjectionSnapshotFilter(
                                            new int[0], sdt.getHeaderSize()));
                            }
                        }
                        final SnapshotTableTask task =
                            new SnapshotTableTask(
                                    table.getRelativeIndex(),
                                    sdt,
                                    filters.toArray(new SnapshotDataFilter[filters.size()]),
                                    table.getIsreplicated(),
                                    table.getTypeName());

                        if (table.getIsreplicated()) {
                            replicatedSnapshotTasks.add(task);
                        } else {
                            partitionedSnapshotTasks.offer(task);
                        }
                        // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
                        // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING
                        // ON THE 3.0 BRANCH UNTIL SUCH TIME AS WE START
                        // DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE
                        // POST-3.0 NO LONGER USES ANY OF THIS METHOD.  SO
                        // DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR
                        // WORK TO HAVE ANY MEANING.  MESSAGE REPEATS...
                    } catch (IOException ex) {
                        /*
                         * Creation of this specific target failed. Close it if it was created.
                         * Continue attempting the snapshot anyways so that at least some of the data
                         * can be retrieved.
                         */
                        try {
                            if (sdt != null) {
                                targets.remove(sdt);
                                sdt.close();
                            }
                        } catch (Exception e) {
                            SNAP_LOG.error(e);
                        }

                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        ex.printStackTrace(pw);
                        pw.flush();
                        canSnapshot = "FAILURE";
                        err_msg = "SNAPSHOT INITIATION OF " + file_nonce +
                        "RESULTED IN IOException: \n" + sw.toString();
                    }

                    result.addRow(context.getHostId(),
                            hostname,
                            table.getTypeName(),
                            canSnapshot,
                            err_msg);
                }

                synchronized (m_taskListsForSites) {
                    //Seems like this should be cleared out just in case
                    //Log if there is actually anything to clear since it is unexpected
                    if (!m_taskListsForSites.isEmpty()) {
                        SNAP_LOG.warn("Found lingering snapshot tasks while setting up a snapshot");
                        m_taskListsForSites.clear();
                    }
                    boolean aborted = false;
                    if (!partitionedSnapshotTasks.isEmpty() || !replicatedSnapshotTasks.isEmpty()) {
                        for (int ii = 0; ii < numLocalSites; ii++) {
                            m_taskListsForSites.add(new ArrayDeque<SnapshotTableTask>());
                        }
                    } else {
                        SnapshotRegistry.discardSnapshot(snapshotRecord);
                        aborted = true;
                    }

                    /**
                     * Distribute the writing of replicated tables to exactly one partition.
                     */
                    for (int ii = 0; ii < numLocalSites && !partitionedSnapshotTasks.isEmpty(); ii++) {
                        m_taskListsForSites.get(ii).addAll(partitionedSnapshotTasks);
                        if (!format.isTableBased()) {
                            m_taskListsForSites.get(ii).addAll(replicatedSnapshotTasks);
                        }
                    }

                    if (format.isTableBased()) {
                        int siteIndex = 0;
                        for (SnapshotTableTask t : replicatedSnapshotTasks) {
                            m_taskListsForSites.get(siteIndex++ % numLocalSites).offer(t);
                        }
                    }
                    // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
                    // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING ON THE 3.0 BRANCH
                    // UNTIL SUCH TIME AS WE START DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE POST-3.0 NO LONGER
                    // USES ANY OF THIS METHOD.  SO DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR WORK TO HAVE
                    // ANY MEANING.  MESSAGE REPEATS...
                    if (!aborted) {
                        /*
                         * Inform the SnapshotCompletionMonitor of what the partition specific txnids for
                         * this snapshot were so it can forward that to completion interests.
                         */
                        VoltDB.instance().getSnapshotCompletionMonitor().registerPartitionTxnIdsForSnapshot(
                                txnId, partitionTransactionIds);
                        // Provide the truncation request ID so the monitor can recognize a specific snapshot.
                        String truncReqId = "";
                        if (jsData != null && jsData.has("truncReqId")) {
                            truncReqId = jsData.getString("truncReqId");
                        }
                        logSnapshotStartToZK( txnId, context, file_nonce, truncReqId);
                    }
                }
            } catch (Exception ex) {
                /*
                 * Close all the targets to release the threads. Don't let sites get any tasks.
                 */
                m_taskListsForSites.clear();
                for (SnapshotDataTarget sdt : targets) {
                    try {
                        sdt.close();
                    } catch (Exception e) {
                        SNAP_LOG.error(ex);
                    }
                }

                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                pw.flush();
                result.addRow(
                        context.getHostId(),
                        hostname,
                        "",
                        "FAILURE",
                        "SNAPSHOT INITIATION OF " + file_path + file_nonce +
                        "RESULTED IN Exception: \n" + sw.toString());
                SNAP_LOG.error(ex);
                // HEY, YOU!  YES, YOU, ABOUT TO EDIT THIS METHOD!
                // THIS ONLY STILL EXISTS TO KEEP LEGACY MODE WORKING ON THE 3.0 BRANCH
                // UNTIL SUCH TIME AS WE START DELETING LEGACY CODE LIKE MADMEN.  SNAPSHOT SAVE POST-3.0 NO LONGER
                // USES ANY OF THIS METHOD.  SO DON'T CHANGE/EDIT/ADD CODE TO IT AND EXPECT YOUR WORK TO HAVE
                // ANY MEANING.  MESSAGE REPEATS...
            }
        }
    }

    private final SnapshotDataTarget constructSnapshotDataTargetForTable(
            SystemProcedureExecutionContext context,
            File f,
            Table table,
            int hostId,
            int numPartitions,
            long txnId,
            long timestamp,
            List<Integer> partitionsForHost)
    throws IOException
    {
        return new DefaultSnapshotDataTarget(f,
                                             hostId,
                                             context.getCluster().getTypeName(),
                                             context.getDatabase().getTypeName(),
                                             table.getTypeName(),
                                             numPartitions,
                                             table.getIsreplicated(),
                                             partitionsForHost,
                                             CatalogUtil.getVoltTable(table),
                                             txnId,
                                             timestamp);
    }

}
