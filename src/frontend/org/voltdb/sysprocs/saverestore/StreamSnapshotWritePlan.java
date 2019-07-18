/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.rejoin.StreamSnapshotAckReceiver;
import org.voltdb.rejoin.StreamSnapshotDataTarget;
import org.voltdb.sysprocs.SnapshotRegistry;

import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Multimap;

/**
 * Create a snapshot write plan for snapshots streamed to other sites
 * (specified in the jsData).  Each source site specified in the streamPairs
 * key will write all of its tables, partitioned and replicated, to a target
 * per-site.
 */
public class StreamSnapshotWritePlan extends SnapshotWritePlan
{
    private int m_siteIndex = 0;

    @Override
    public Callable<Boolean> createSetup(
            String file_path, String pathType, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            final VoltTable result,
            ExtensibleSnapshotDigestData extraSnapshotData,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp)
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        final StreamSnapshotRequestConfig config =
            new StreamSnapshotRequestConfig(jsData, context.getDatabase());
        final List<StreamSnapshotRequestConfig.Stream> localStreams =
                filterRemoteStreams(config.streams, tracker.getSitesForHost(context.getHostId()));
        final Map<Integer, Set<Long>> destsByHostId = collectTargetSitesByHostId(config.streams);

        /*
         * The snapshot (if config.shouldTruncate) will only contain existing partitions. Write the new partition count
         * down in the digest so that we can check if enough command log is collected on
         * replay.
         *
         * When calculating the new number of partitions
         * Exploit the fact that when adding partitions the highest partition id will
         * always be the number of partitions - 1. This works around the issue
         * where previously we were always incrementing by the number of new partitions
         * which when we failed a join resulted in an inaccurate large number of partitions
         * because there was no corresponding decrement when we cleaned out and then re-added
         * the partitions that were being joined. Now it will increment once and stay incremented
         * after the first attempt which is less than ideal because it means you are stuck trying
         * to restart with the additional partitions even if you may have operated for a long time without
         * them.
         *
         */
        Integer newPartitionCount = config.newPartitionCount;
        Callable<Boolean> deferredSetup = null;
        // Coalesce a truncation snapshot if shouldTruncate is true
        if (config.shouldTruncate) {
            assert newPartitionCount != null;
            deferredSetup = coalesceTruncationSnapshotPlan(file_path, pathType, file_nonce, txnId, partitionTransactionIds,
                                           context, result,
                                           extraSnapshotData,
                                           tracker,
                                           hashinatorData,
                                           timestamp,
                                           newPartitionCount);
        } else if (newPartitionCount != null) {
            // Create post snapshot update hashinator work
            createUpdatePartitionCountTasksForSites(tracker, context, newPartitionCount);
        }

        // Mark snapshot start in registry
        final AtomicInteger numTables = new AtomicInteger(config.tables.length);
        m_snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.STREAM,
                    config.tables);

        // table schemas for all the tables we'll snapshot on this partition
        Map<Integer, Pair<Boolean, byte[]>> schemas = new HashMap<>();
        for (final Table table : config.tables) {
            VoltTable schemaTable = HiddenColumnFilter.NONE.createSchema(context.getCluster(), table);
            schemas.put(table.getRelativeIndex(),
                        Pair.of(table.getIsreplicated(), PrivateVoltTableFactory.getSchemaBytes(schemaTable)));
        }

        List<DataTargetInfo> sdts = createDataTargets(localStreams, destsByHostId, hashinatorData, schemas);

        // If there's no work to do on this host, just claim success, return an empty plan,
        // and things will sort themselves out properly

        // For each table, create tasks where each task has a data target.
        // reset siteId to 0 for placing replicated Task from site 0
        m_siteIndex = 0;
        for (final Table table : config.tables) {
            createTasksForTable(table, sdts, numTables, m_snapshotRecord, tracker.getSitesForHost(context.getHostId()));
            result.addRow(context.getHostId(), CoreUtils.getHostnameOrAddress(), table.getTypeName(), "SUCCESS", "");
        }

        return deferredSetup;
    }

    private static boolean haveAnyStreamPairs(List<StreamSnapshotRequestConfig.Stream> localStreams) {
        if (localStreams != null && !localStreams.isEmpty()) {
            for (StreamSnapshotRequestConfig.Stream stream : localStreams) {
                if (stream != null && stream.streamPairs != null && !stream.streamPairs.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<DataTargetInfo> createDataTargets(List<StreamSnapshotRequestConfig.Stream> localStreams,
                                                   Map<Integer, Set<Long>> destsByHostId,
                                                   HashinatorSnapshotData hashinatorData,
                                                   Map<Integer, Pair<Boolean, byte[]>> schemas)
    {
        byte[] hashinatorConfig = null;
        if (hashinatorData != null) {
            ByteBuffer hashinatorConfigBuf = ByteBuffer.allocate(8 + hashinatorData.m_serData.length);
            hashinatorConfigBuf.putLong(hashinatorData.m_version);
            hashinatorConfigBuf.put(hashinatorData.m_serData);
            hashinatorConfig =  hashinatorConfigBuf.array();
        }

        List<DataTargetInfo> sdts = Lists.newArrayList();

        if (haveAnyStreamPairs(localStreams) && !schemas.isEmpty()) {
            Mailbox mb = VoltDB.instance().getHostMessenger().createMailbox();
            StreamSnapshotDataTarget.SnapshotSender sender = new StreamSnapshotDataTarget.SnapshotSender(mb);
            StreamSnapshotAckReceiver ackReceiver = new StreamSnapshotAckReceiver(mb);
            new Thread(sender, "Stream Snapshot Sender").start();
            new Thread(ackReceiver, "Stream Snapshot Ack Receiver").start();
            // The mailbox will be removed after all snapshot data targets are finished
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(createCompletionTask(mb));

            // Create data target for each source HSID in each stream
            for (StreamSnapshotRequestConfig.Stream stream : localStreams) {
                SNAP_LOG.debug("Sites to stream from: " +
                               CoreUtils.hsIdCollectionToString(stream.streamPairs.keySet()));
                for (Entry<Long, Long> entry : stream.streamPairs.entries()) {
                    long srcHSId = entry.getKey();
                    long destHSId = entry.getValue();

                    DataTargetInfo nextTarget =
                            new DataTargetInfo(stream,
                                               srcHSId,
                                               destHSId,
                                               new StreamSnapshotDataTarget(destHSId,
                                                                            (destHSId == stream.lowestSiteSinkHSId),
                                                                            destsByHostId.get(CoreUtils.getHostIdFromHSId(destHSId)),
                                                                            hashinatorConfig, schemas, sender, ackReceiver));
                    sdts.add(nextTarget);
                }
            }
        }

        return sdts;
    }

    /**
     * Remove the mailbox from the host messenger after all data targets are done.
     */
    private Runnable createCompletionTask(final Mailbox mb)
    {
        return new Runnable() {
            @Override
            public void run() {
                VoltDB.instance().getHostMessenger().removeMailbox(mb.getHSId());
            }
        };
    }

    // The truncation snapshot will always have all the tables regardless of what tables are requested
    // in the stream snapshot. Passing null to the JSON config below will cause the
    // NativeSnapshotWritePlan to include all tables.
    private Callable<Boolean> coalesceTruncationSnapshotPlan(String file_path, String pathType, String file_nonce, long txnId,
                                                             Map<Integer, Long> partitionTransactionIds,
                                                             SystemProcedureExecutionContext context,
                                                             VoltTable result,
                                                             ExtensibleSnapshotDigestData extraSnapshotData,
                                                             SiteTracker tracker,
                                                             HashinatorSnapshotData hashinatorData,
                                                             long timestamp,
                                                             int newPartitionCount)
    {
        final NativeSnapshotWritePlan plan = new NativeSnapshotWritePlan();
        final Callable<Boolean> deferredTruncationSetup =
                plan.createSetupInternal(file_path, pathType, file_nonce, txnId, partitionTransactionIds,
                        new SnapshotRequestConfig(newPartitionCount, context.getDatabase()), context, result,
                        extraSnapshotData, tracker, hashinatorData, timestamp);
        m_taskListsForHSIds.putAll(plan.m_taskListsForHSIds);

        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception
            {
                final Boolean retval = deferredTruncationSetup.call();
                m_targets.addAll(plan.m_targets);
                return retval;
            }
        };
    }

    private List<StreamSnapshotRequestConfig.Stream>
    filterRemoteStreams(List<StreamSnapshotRequestConfig.Stream> streams, Collection<Long> localHSIds)
    {
        List<StreamSnapshotRequestConfig.Stream> localStreams = Lists.newArrayList();

        // This is the list of streams for a specific target partition when using ElasticJoin
        // For Rejoin, there will only be one element in this list
        for (StreamSnapshotRequestConfig.Stream stream : streams) {
            ArrayListMultimap<Long, Long> streamPairs = ArrayListMultimap.create();
            for (Long localHsId : localHSIds) {
                Collection<Long> destinations = stream.streamPairs.get(localHsId);
                if (!destinations.isEmpty()) {
                    streamPairs.putAll(localHsId, destinations);
                }
            }

            localStreams.add(new StreamSnapshotRequestConfig.Stream(streamPairs,
                                                                    stream.lowestSiteSinkHSId));
        }

        return localStreams;
    }

    private Map<Integer, Set<Long>> collectTargetSitesByHostId(List<StreamSnapshotRequestConfig.Stream> streams)
    {
        Map<Integer, Set<Long>> targetHSIdsByHostId = new HashMap<>();

        // This is the list of streams for a specific target partition when using ElasticJoin
        // For Rejoin, there will only be one element in this list
        for (StreamSnapshotRequestConfig.Stream stream : streams) {
            for (Long targetHSId : stream.streamPairs.values()) {
                Integer hostId = CoreUtils.getHostIdFromHSId(targetHSId);
                Set<Long> targetSet = targetHSIdsByHostId.get(hostId);
                if (targetSet == null) {
                    targetSet = new HashSet<>();
                    targetHSIdsByHostId.put(hostId, targetSet);
                }
                targetSet.add(targetHSId);
            }
        }

        return targetHSIdsByHostId;
    }

    private SnapshotTableTask createSingleTableTask(Table table,
                                                    DataTargetInfo targetInfo,
                                                    AtomicInteger numTables,
                                                    SnapshotRegistry.Snapshot snapshotRecord) {
        final Runnable onClose = new TargetStatsClosure(targetInfo.dataTarget,
                table.getTypeName(),
                numTables,
                snapshotRecord);
        targetInfo.dataTarget.setOnCloseHandler(onClose);

        final SnapshotTableTask task =
                new SnapshotTableTask(table,
                                      new SnapshotDataFilter[0], // This task no longer needs partition filtering
                                      null,
                                      false);
        task.setTarget(targetInfo.dataTarget);
        m_targets.add(targetInfo.dataTarget);
        return task;
    }

    /**
     * For each site, generate a task for each target it has for this table.
     */
    private void createTasksForTable(Table table,
                                     List<DataTargetInfo> dataTargets,
                                     AtomicInteger numTables,
                                     SnapshotRegistry.Snapshot snapshotRecord,
                                     List<Long> hsids)
    {
        // srcHSId -> tasks
        Multimap<Long, SnapshotTableTask> tasks = ArrayListMultimap.create();
        for (DataTargetInfo targetInfo : dataTargets) {
            if (table.getIsreplicated() && !targetInfo.dataTarget.isReplicatedTableTarget()) {
                // For replicated tables only the lowest site's dataTarget actually does any work.
                // The other dataTargets just need to be tracked so we send EOF when all streams have finished.
                m_targets.add(targetInfo.dataTarget);
                continue;
            }
            final SnapshotTableTask task = createSingleTableTask(table, targetInfo, numTables, snapshotRecord);

            SNAP_LOG.debug("ADDING TASK for streamSnapshot: " + task);
            tasks.put(targetInfo.srcHSId, task);
        }

        placeTasksForTable(table, tasks, hsids);
    }

    private void placeTasksForTable(Table table, Multimap<Long, SnapshotTableTask> tasks, List<Long> hsids)
    {
        for (Entry<Long, Collection<SnapshotTableTask>> tasksEntry : tasks.asMap().entrySet()) {
            // Stream snapshots need to write all partitioned tables to all selected partitions
            // and all replicated tables to across all the sites on every host
            if (table.getIsreplicated()) {
                placeReplicatedTasks(tasksEntry.getValue(), hsids);
            } else {
                placePartitionedTasks(tasksEntry.getValue(), Arrays.asList(tasksEntry.getKey()));
            }
        }
    }

    /**
     * Encapsulates the information about a data target so that when we generate task for this
     * table target, we can create the predicate associated with it.
     */
    private static class DataTargetInfo {
        @SuppressWarnings("unused")
        public final StreamSnapshotRequestConfig.Stream stream;
        public final long srcHSId;
        @SuppressWarnings("unused")
        public final long dstHSId;
        public final StreamSnapshotDataTarget dataTarget;

        public DataTargetInfo(StreamSnapshotRequestConfig.Stream stream,
                              long srcHSId,
                              long dstHSId,
                              StreamSnapshotDataTarget dataTarget)
        {
            this.stream = stream;
            this.srcHSId = srcHSId;
            this.dstHSId = dstHSId;
            this.dataTarget = dataTarget;
        }
    }

    @Override
    protected void placeReplicatedTasks(Collection<SnapshotTableTask> tasks, List<Long> hsids)
    {
        SNAP_LOG.debug("Placing replicated tasks at sites: " + CoreUtils.hsIdCollectionToString(hsids));
        // Round-robin the placement of replicated table tasks across the provided HSIds
        for (SnapshotTableTask task : tasks) {
            ArrayList<Long> robin = new ArrayList<Long>();
            robin.add(hsids.get(m_siteIndex));
            placeTask(task, robin);
            m_siteIndex = (m_siteIndex +1) % hsids.size();
        }
    }
}
