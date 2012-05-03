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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotSave;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.CatalogUtil;

import com.google.common.primitives.Ints;

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

    /**
     * The only public method: do all the work to start a snapshot.
     * Assumes that a snapshot is feasible, that the caller has validated it can
     * be accomplished, that the caller knows this is a consistent or useful
     * transaction point at which to snapshot.
     *
     * @param file_path
     * @param file_nonce
     * @param block
     * @param startTime
     * @param context
     * @param hostname
     * @return VoltTable describing the results of the snapshot attempt
     */
    public VoltTable startSnapshotting(
            String file_path, String file_nonce, boolean csv, byte block,
            long txnId, SystemProcedureExecutionContext context, String hostname)
    {
        TRACE_LOG.trace("Creating snapshot target and handing to EEs");
        final VoltTable result = SnapshotSave.constructNodeResultsTable();
        final int numLocalSites = VoltDB.instance().getLocalSites().values().size();

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
                createSetup(file_path, file_nonce, csv, txnId, context, hostname, result);
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
                context.getExecutionSite().initiateSnapshots(
                        m_taskList,
                        txnId,
                        context.getExecutionSite().getSiteTracker().getAllHosts().size());
            }
        }

        if (block != 0) {
            HashSet<Exception> failures = null;
            String status = "SUCCESS";
            String err = "";
            try {
                failures = context.getExecutionSite().completeSnapshotWork();
            } catch (InterruptedException e) {
                status = "FAILURE";
                err = e.toString();
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
            SystemProcedureExecutionContext context, String nonce) {
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
        int hosts = context.getExecutionSite().getSiteTracker().getAllHosts().size();
        createSnapshotCompletionNode( nonce, txnId, isTruncation, hosts);

        try {
            cb1.get();
        } catch (NodeExistsException e) {
            HOST_LOG.warn("Didn't expect the snapshot node to already exist", e);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
    }


    /**
     * Create the completion node for the snapshot identified by the txnId. It
     * assumes that all hosts will race to call this, so it doesn't fail if the
     * node already exists.
     *
     * @param txnId
     * @param isTruncation Whether or not this is a truncation snapshot
     * @param hosts The total number of live hosts
     */
    public static void createSnapshotCompletionNode(String nonce,
                                                    long txnId,
                                                    boolean isTruncation,
                                                    int hosts) {
        if (hosts == 0) {
            VoltDB.crashGlobalVoltDB("Hosts must be greater than 0", true, null);
        }
        if (!(txnId > 0)) {
            VoltDB.crashGlobalVoltDB("Txnid must be greather than 0", true, null);
        }

        byte  nodeBytes[] = null;
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.key("txnId").value(txnId);
            stringer.key("hosts").value(hosts);
            stringer.key("isTruncation").value(isTruncation);
            stringer.key("finishedHosts").value(0);
            stringer.key("nonce").value(nonce);
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
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unexpected exception logging snapshot completion to ZK", true, e);
        }
    }


    @SuppressWarnings("unused")
    private void createSetup(
            String file_path, String file_nonce, boolean csv,
            long txnId, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result) {
        {
            final int numLocalSites = VoltDB.instance().getLocalSites().values().size();
            SiteTracker tracker = context.getExecutionSite().m_tracker;

            /*
             * List of partitions to include if this snapshot is
             * going to be deduped. Attempts to break up the work
             * by seeding an RNG with the partition id and then selecting
             * a random replica to do the work. Will not work in failure
             * cases, but we don't use dedupe when we want durability.
             */
            List<Integer> partitionsToInclude = new ArrayList<Integer>();
            List<Long> sitesToInclude = new ArrayList<Long>();
            for (long localSite : tracker.getLocalSites()) {
                final int partitionId = tracker.getPartitionForSite(localSite);
                List<Long> sites =
                        new ArrayList<Long>(tracker.getSitesForPartition(tracker.getPartitionForSite(localSite)));
                Collections.sort(sites);
                int siteIndex = new java.util.Random(partitionId).nextInt(sites.size());
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

                Runnable completionTask = SnapshotUtil.writeSnapshotDigest(
                        txnId,
                        context.getExecutionSite().m_context.getCatalogCRC(),
                        file_path,
                        file_nonce,
                        tables,
                        context.getExecutionSite().getCorrespondingHostId(),
                        SnapshotSiteProcessor.getExportSequenceNumbers());
                if (completionTask != null) {
                    SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
                }
                completionTask = SnapshotUtil.writeSnapshotCatalog(file_path, file_nonce);
                if (completionTask != null) {
                    SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
                }
                final AtomicInteger numTables = new AtomicInteger(tables.size());
                final SnapshotRegistry.Snapshot snapshotRecord =
                    SnapshotRegistry.startSnapshot(
                            txnId,
                            context.getExecutionSite().getCorrespondingHostId(),
                            file_path,
                            file_nonce,
                            csv,
                            tables.toArray(new Table[0]));
                for (final Table table : SnapshotUtil.getTablesToSave(context.getDatabase()))
                {
                    /*
                     * For a deduped csv snapshot, only produce the replicated tables on the "leader"
                     * host.
                     */
                    if (csv && table.getIsreplicated() && !tracker.isFirstHost()) {
                        continue;
                    }
                    String canSnapshot = "SUCCESS";
                    String err_msg = "";
                    final File saveFilePath =
                            SnapshotUtil.constructFileForTable(
                                    table,
                                    file_path,
                                    file_nonce,
                                    csv ? ".csv" : ".vpt",
                                    context.getHostId());
                    SnapshotDataTarget sdt = null;
                    try {
                        if (csv) {
                            sdt = new SimpleFileSnapshotDataTarget(saveFilePath);
                        } else {
                            sdt =
                                constructSnapshotDataTargetForTable(
                                        context,
                                        saveFilePath,
                                        table,
                                        context.getHostId(),
                                        context.getSiteTracker().m_numberOfPartitions,
                                        txnId);
                        }
                        targets.add(sdt);
                        final SnapshotDataTarget sdtFinal = sdt;
                        final Runnable onClose = new Runnable() {
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
                                        (completed.timeFinished - org.voltdb.TransactionIdManager.getTimestampFromTransactionId(completed.txnId)) / 1000.0;
                                    HOST_LOG.info(
                                            "Snapshot " + snapshotRecord.nonce + " finished at " +
                                             completed.timeFinished + " and took " + duration
                                             + " seconds ");
                                }
                            }
                        };

                        sdt.setOnCloseHandler(onClose);

                        List<SnapshotDataFilter> filters = new ArrayList<SnapshotDataFilter>();
                        if (csv) {
                            filters.add(
                                    new PartitionProjectionSnapshotFilter(
                                            Ints.toArray(partitionsToInclude),
                                            0));
                            filters.add(new CSVSnapshotFilter(CatalogUtil.getVoltTable(table), ',', null));
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
                        err_msg = "SNAPSHOT INITIATION OF " + saveFilePath +
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
                        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.set(
                                VoltDB.instance().getLocalSites().values().size());
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
                    }

                    int siteIndex = 0;
                    for (SnapshotTableTask t : replicatedSnapshotTasks) {
                        SnapshotSiteProcessor.m_taskListsForSites.get(siteIndex++ % numLocalSites).offer(t);
                    }
                    if (!aborted) {
                        logSnapshotStartToZK( txnId, context, file_nonce);
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
                                             context.getSiteTracker().getPartitionsForHost(hostId),
                                             CatalogUtil.getVoltTable(table),
                                             txnId);
    }

}
