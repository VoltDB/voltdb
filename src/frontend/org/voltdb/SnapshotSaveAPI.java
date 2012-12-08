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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
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
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotSave;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.CatalogUtil;

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
    private static final VoltLogger HOST_LOG = new VoltLogger("HOST");

    // ugh, ick, ugh
    public static final AtomicInteger recoveringSiteCount = new AtomicInteger(0);

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
            String file_path, String file_nonce, SnapshotFormat format, byte block,
            long multiPartTxnId, long partitionTxnId, long legacyPerPartitionTxnIds[],
            String data, SystemProcedureExecutionContext context, String hostname)
    {
        TRACE_LOG.trace("Creating snapshot target and handing to EEs");
        final VoltTable result = SnapshotSave.constructNodeResultsTable();
        final int numLocalSites = (context.getSiteTrackerForSnapshot().getLocalSites().length -
                recoveringSiteCount.get());

        // One site wins the race to create the snapshot targets, populating
        // m_taskListsForSites for the other sites and creating an appropriate
        // number of snapshot permits.
        synchronized (SnapshotSiteProcessor.m_snapshotCreateLock) {

            // First time use lazy initialization (need to calculate numLocalSites.
            if (SnapshotSiteProcessor.m_snapshotCreateSetupPermit == null) {
                SnapshotSiteProcessor.m_snapshotCreateSetupPermit = new Semaphore(numLocalSites);
            }

            try {
                //From within this EE, record the sequence numbers as of the start of the snapshot (now)
                //so that the info can be put in the digest.
                SnapshotSiteProcessor.populateExportSequenceNumbersForExecutionSite(context);
                SnapshotSiteProcessor.m_snapshotCreateSetupPermit.acquire();
                if (VoltDB.instance().isIV2Enabled()) {
                    SnapshotSiteProcessor.m_partitionLastSeenTransactionIds.add(partitionTxnId);
                }
            } catch (InterruptedException e) {
                result.addRow(
                        context.getHostId(),
                        hostname,
                        "",
                        "FAILURE",
                        e.toString());
                return result;
            }

            if (SnapshotSiteProcessor.m_snapshotCreateSetupPermit.availablePermits() == 0) {
                List<Long>  partitionTransactionIds = new ArrayList<Long>();
                if (VoltDB.instance().isIV2Enabled()) {
                    partitionTransactionIds = SnapshotSiteProcessor.m_partitionLastSeenTransactionIds;
                    SnapshotSiteProcessor.m_partitionLastSeenTransactionIds = new ArrayList<Long>();
                    partitionTransactionIds.add(multiPartTxnId);

                    /*
                     * Do a quick sanity check that the provided IDs
                     * don't conflict with currently active partitions. If they do
                     * it isn't fatal we can just skip it.
                     */
                    for (long txnId : legacyPerPartitionTxnIds) {
                        final int legacyPartition = TxnEgo.getPartitionId(txnId);
                        boolean isDup = false;
                        for (long existingId : partitionTransactionIds) {
                            final int existingPartition = TxnEgo.getPartitionId(existingId);
                            if (existingPartition == legacyPartition) {
                                HOST_LOG.warn("While saving a snapshot and propagating legacy " +
                                        "transaction ids found an id that matches currently active partition" +
                                        existingPartition);
                                isDup = true;
                            }
                        }
                        if (!isDup) {
                            partitionTransactionIds.add(txnId);
                        }
                    }
                }
                exportSequenceNumbers = SnapshotSiteProcessor.getExportSequenceNumbers();
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
                        exportSequenceNumbers);
                // release permits for the next setup, now that is one is complete
                SnapshotSiteProcessor.m_snapshotCreateSetupPermit.release(numLocalSites);
            }
        }

        // All sites wait for a permit to start their individual snapshot tasks
        VoltTable error = acquireSnapshotPermit(context, hostname, result);
        if (error != null) {
            return error;
        }

        synchronized (SnapshotSiteProcessor.m_taskListsForSites) {
            final Deque<SnapshotTableTask> m_taskList = SnapshotSiteProcessor.m_taskListsForSites.poll();
            if (m_taskList == null) {
                return result;
            } else {
                assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() > 0);
                context.getSiteSnapshotConnection().initiateSnapshots(
                        m_taskList,
                        multiPartTxnId,
                        context.getSiteTrackerForSnapshot().getAllHosts().size(),
                        exportSequenceNumbers);
            }
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
            HOST_LOG.warn("Didn't expect the snapshot node to already exist", e);
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
            increaseParticipateHostCount(txnId, context.getHostId());
        }

        try {
            cb1.get();
        } catch (NodeExistsException e) {
            HOST_LOG.warn("Didn't expect the snapshot node to already exist", e);
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
    public static void increaseParticipateHostCount(long txnId, int hostId) {
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
                JSONObject jsonObj = new JSONObject(new String(data, "UTF-8"));
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

                zk.setData(snapshotPath, jsonObj.toString(4).getBytes("UTF-8"), stat.getVersion());
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
            stringer.key("finishedHosts").value(0);
            stringer.key("nonce").value(nonce);
            stringer.key("truncReqId").value(truncReqId);
            stringer.key("exportSequenceNumbers").object().endObject();
            stringer.endObject();
            JSONObject jsonObj = new JSONObject(stringer.toString());
            nodeBytes = jsonObj.toString(4).getBytes("UTF-8");
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

    private void createSetup(
            String file_path, String file_nonce, SnapshotFormat format,
            long txnId, List<Long> partitionTransactionIds,
            String data, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {
        {
            SiteTracker tracker = context.getSiteTrackerForSnapshot();
            final int numLocalSites =
                    (tracker.getLocalSites().length - recoveringSiteCount.get());

            // non-null if targeting only one site (used for rejoin)
            // set later from the "data" JSON string
            Long targetHSid = null;

            JSONObject jsData = null;
            if (data != null && !data.isEmpty()) {
                try {
                    jsData = new JSONObject(data);
                }
                catch (JSONException e) {
                    HOST_LOG.error(String.format("JSON exception on snapshot data \"%s\".", data),
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
                assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() == -1);

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
                                                  VoltDB.instance().getHostMessenger().getInstanceId());
                    if (completionTask != null) {
                        SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
                    }
                    completionTask = SnapshotUtil.writeSnapshotCatalog(file_path, file_nonce);
                    if (completionTask != null) {
                        SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
                    }
                }

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
                        long hsId = jsData.getLong("hsId");

                        // if a target_hsid exists, set it for filtering a snapshot for a specific site
                        try {
                            targetHSid = jsData.getLong("target_hsid");
                        }
                        catch (JSONException e) {} // leave value as null on exception

                        // if this snapshot targets a specific site...
                        if (targetHSid != null) {
                            // get the list of sites on this node
                            List<Long> localHSids = tracker.getSitesForHost(context.getHostId());
                            // if the target site is local to this node...
                            if (localHSids.contains(targetHSid)) {
                                sdt = new StreamSnapshotDataTarget(hsId, schemas);
                            }
                            else {
                                sdt = new DevNullSnapshotTarget();
                            }
                        }
                    }
                }

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
                            sdt = new SimpleFileSnapshotDataTarget(saveFilePath);
                        } else if (format == SnapshotFormat.NATIVE) {
                            sdt =
                                constructSnapshotDataTargetForTable(
                                        context,
                                        saveFilePath,
                                        table,
                                        context.getHostId(),
                                        tracker.m_numberOfPartitions,
                                        txnId);
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
                                    HOST_LOG.info(
                                            "Snapshot " + snapshotRecord.nonce + " finished at " +
                                             completed.timeFinished + " and took " + duration
                                             + " seconds ");
                                }
                            }
                        };

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

                        // if this snapshot targets a specific site...
                        if (targetHSid != null) {
                            // get the list of sites on this node
                            List<Long> localHSids = tracker.getSitesForHost(context.getHostId());
                            // if the target site is local to this node...
                            if (localHSids.contains(targetHSid)) {
                                // ...get its partition id...
                                int partitionId = tracker.getPartitionForSite(targetHSid);
                                // ...and build a filter to only get that partition
                                filters.add(new PartitionProjectionSnapshotFilter(
                                        new int[] { partitionId }, sdt.getHeaderSize()));
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
                            HOST_LOG.error(e);
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

                synchronized (SnapshotSiteProcessor.m_taskListsForSites) {
                    boolean aborted = false;
                    if (!partitionedSnapshotTasks.isEmpty() || !replicatedSnapshotTasks.isEmpty()) {
                        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.set(numLocalSites);
                        for (int ii = 0; ii < numLocalSites; ii++) {
                            SnapshotSiteProcessor.m_taskListsForSites.add(new ArrayDeque<SnapshotTableTask>());
                        }
                    } else {
                        SnapshotRegistry.discardSnapshot(snapshotRecord);
                        aborted = true;
                    }

                    /**
                     * Distribute the writing of replicated tables to exactly one partition.
                     */
                    for (int ii = 0; ii < numLocalSites && !partitionedSnapshotTasks.isEmpty(); ii++) {
                        SnapshotSiteProcessor.m_taskListsForSites.get(ii).addAll(partitionedSnapshotTasks);
                        if (!format.isTableBased()) {
                            SnapshotSiteProcessor.m_taskListsForSites.get(ii).addAll(replicatedSnapshotTasks);
                        }
                    }

                    if (format.isTableBased()) {
                        int siteIndex = 0;
                        for (SnapshotTableTask t : replicatedSnapshotTasks) {
                            SnapshotSiteProcessor.m_taskListsForSites.get(siteIndex++ % numLocalSites).offer(t);
                        }
                    }
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
                SnapshotSiteProcessor.m_taskListsForSites.clear();
                for (SnapshotDataTarget sdt : targets) {
                    try {
                        sdt.close();
                    } catch (Exception e) {
                        HOST_LOG.error(ex);
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
                HOST_LOG.error(ex);
            } finally {
                SnapshotSiteProcessor.m_snapshotPermits.release(numLocalSites);
            }

        }
    }

    private VoltTable acquireSnapshotPermit(SystemProcedureExecutionContext context,
            String hostname, final VoltTable result) {
        try {
            SnapshotSiteProcessor.m_snapshotPermits.acquire();
        } catch (Exception e) {
            result.addRow(context.getHostId(),
                    hostname,
                    "",
                    "FAILURE",
                    e.toString());
            return result;
        }
        return null;
    }


    private final SnapshotDataTarget constructSnapshotDataTargetForTable(
            SystemProcedureExecutionContext context,
            File f,
            Table table,
            int hostId,
            int numPartitions,
            long txnId)
    throws IOException
    {
        return new DefaultSnapshotDataTarget(f,
                                             hostId,
                                             context.getCluster().getTypeName(),
                                             context.getDatabase().getTypeName(),
                                             table.getTypeName(),
                                             numPartitions,
                                             table.getIsreplicated(),
                                             context.getSiteTrackerForSnapshot().getPartitionsForHost(hostId),
                                             CatalogUtil.getVoltTable(table),
                                             txnId);
    }

}
