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

package org.voltdb.sysprocs.saverestore;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import org.voltdb.PostSnapshotTask;
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
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.primitives.Longs;

/**
 * Create a snapshot write plan for snapshots streamed to other sites
 * (specified in the jsData).  Each source site specified in the streamPairs
 * key will write all of its tables, partitioned and replicated, to a target
 * per-site.
 */
public class StreamSnapshotWritePlan extends SnapshotWritePlan
{
    @Override
    public Callable<Boolean> createSetup(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp)
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        final StreamSnapshotRequestConfig config =
            new StreamSnapshotRequestConfig(jsData, context.getDatabase());
        final List<StreamSnapshotRequestConfig.Stream> localStreams =
            filterRemoteStreams(config.streams, Longs.asList(tracker.getLocalSites()));
        final Set<Integer> partitionsToAdd = getPartitionsToAdd(localStreams);

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
        final int newPartitionCount = partitionsToAdd.isEmpty() ? context.getNumberOfPartitions() : Collections.max(partitionsToAdd) + 1;
        Callable<Boolean> deferredSetup = null;
        // Coalesce a truncation snapshot if shouldTruncate is true
        if (config.shouldTruncate) {
            deferredSetup = coalesceTruncationSnapshotPlan(file_path, file_nonce, txnId, partitionTransactionIds,
                                           jsData, context, result,
                                           exportSequenceNumbers, tracker,
                                           hashinatorData,
                                           timestamp,
                                           newPartitionCount);
        }

        // Create post snapshot update hashinator work
        List<Integer> localPartitions = tracker.getPartitionsForHost(context.getHostId());
        if (!partitionsToAdd.isEmpty()) {
            createUpdatePartitionCountTasksForSites(localPartitions, newPartitionCount);
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
        Map<Integer, byte[]> schemas = new HashMap<Integer, byte[]>();
        for (final Table table : config.tables) {
            VoltTable schemaTable = CatalogUtil.getVoltTable(table);
            schemas.put(table.getRelativeIndex(), PrivateVoltTableFactory.getSchemaBytes(schemaTable));
        }

        List<DataTargetInfo> sdts = createDataTargets(localStreams, hashinatorData, schemas);

        // If there's no work to do on this host, just claim success, return an empty plan,
        // and things will sort themselves out properly

        // For each table, create tasks where each task has a data target.
        for (final Table table : config.tables) {
            createTasksForTable(table, sdts, numTables, m_snapshotRecord);
            result.addRow(context.getHostId(), CoreUtils.getHostnameOrAddress(), table.getTypeName(), "SUCCESS", "");
        }

        return deferredSetup;
    }

    private static boolean haveAnyStreamPairs(List<StreamSnapshotRequestConfig.Stream> localStreams) {
        boolean haveAny = false;
        if (localStreams != null && !localStreams.isEmpty()) {
            int pairCount = 0;
            Iterator<StreamSnapshotRequestConfig.Stream> itr = localStreams.iterator();
            while (itr.hasNext() && pairCount == 0) {
                StreamSnapshotRequestConfig.Stream stream = itr.next();
                if (stream != null && stream.streamPairs != null) {
                    pairCount = stream.streamPairs.size();
                }
            }
            haveAny = pairCount > 0;
        }
        return haveAny;
    }

    private List<DataTargetInfo> createDataTargets(List<StreamSnapshotRequestConfig.Stream> localStreams,
                                                   HashinatorSnapshotData hashinatorData,
                                                   Map<Integer, byte[]> schemas)
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

                    sdts.add(new DataTargetInfo(stream,
                                                srcHSId,
                                                destHSId,
                                                new StreamSnapshotDataTarget(destHSId, hashinatorConfig,
                                                                             schemas, sender, ackReceiver)));
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

    private Callable<Boolean> coalesceTruncationSnapshotPlan(String file_path, String file_nonce, long txnId,
                                                             Map<Integer, Long> partitionTransactionIds,
                                                             JSONObject jsData,
                                                             SystemProcedureExecutionContext context,
                                                             VoltTable result,
                                                             Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
                                                             SiteTracker tracker,
                                                             HashinatorSnapshotData hashinatorData,
                                                             long timestamp,
                                                             int newPartitionCount)
    {
        final NativeSnapshotWritePlan plan = new NativeSnapshotWritePlan();
        final Callable<Boolean> deferredTruncationSetup =
                plan.createSetupInternal(file_path, file_nonce, txnId, partitionTransactionIds,
                        jsData, context, result, exportSequenceNumbers,
                        tracker, hashinatorData, timestamp, newPartitionCount);
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

        for (StreamSnapshotRequestConfig.Stream stream : streams) {
            ArrayListMultimap<Long, Long> streamPairs = ArrayListMultimap.create();

            for (Entry<Long, Long> streamPair : stream.streamPairs.entries()) {
                // Only include entries where the sourceHSId is a local HSID
                if (localHSIds.contains(streamPair.getKey())) {
                    streamPairs.put(streamPair.getKey(), streamPair.getValue());
                }
            }

            localStreams.add(new StreamSnapshotRequestConfig.Stream(streamPairs,
                                                                    stream.newPartition));
        }

        return localStreams;
    }

    /**
     * For each site, generate a task for each target it has for this table.
     */
    private void createTasksForTable(Table table,
                                     List<DataTargetInfo> dataTargets,
                                     AtomicInteger numTables,
                                     SnapshotRegistry.Snapshot snapshotRecord)
    {
        // srcHSId -> tasks
        Multimap<Long, SnapshotTableTask> tasks = ArrayListMultimap.create();
        for (DataTargetInfo targetInfo : dataTargets) {
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

            tasks.put(targetInfo.srcHSId, task);
            m_targets.add(targetInfo.dataTarget);
        }

        placeTasksForTable(table, tasks);
    }

    private void placeTasksForTable(Table table, Multimap<Long, SnapshotTableTask> tasks)
    {
        for (Entry<Long, Collection<SnapshotTableTask>> tasksEntry : tasks.asMap().entrySet()) {
            // Stream snapshots need to write all partitioned tables to all selected partitions
            // and all replicated tables to all selected partitions
            if (table.getIsreplicated()) {
                placeReplicatedTasks(tasksEntry.getValue(), Arrays.asList(tasksEntry.getKey()));
            } else {
                placePartitionedTasks(tasksEntry.getValue(), Arrays.asList(tasksEntry.getKey()));
            }
        }
    }

    /**
     * Look at all streams and consolidate the ranges of all streams.
     * @return A map of tokens to partition IDs
     */
    private static Set<Integer> getPartitionsToAdd(Collection<StreamSnapshotRequestConfig.Stream> streams)
    {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        for (StreamSnapshotRequestConfig.Stream stream : streams) {
            if (stream.newPartition != null) {
                builder.add(stream.newPartition);
            }
        }
        return builder.build();
    }

    private static void createUpdatePartitionCountTasksForSites(Collection<Integer> localPartitions,
                                                                Integer newPartitionCount)
    {
        Preconditions.checkNotNull(newPartitionCount);
        PostSnapshotTask task = new UpdatePartitionCount(newPartitionCount);
        assert !localPartitions.isEmpty();
        Iterator<Integer> iter = localPartitions.iterator();
        while (iter.hasNext()) {
            int partition = iter.next();
            SnapshotSiteProcessor.m_siteTasksPostSnapshotting.put(partition, task);
        }
    }

    /**
     * A post-snapshot site task that updates the partition count on each site.
     */
    private static class UpdatePartitionCount implements PostSnapshotTask {
        private final int m_newPartitionCount;

        public UpdatePartitionCount(int newPartitionCount)
        {
            m_newPartitionCount = newPartitionCount;
        }

        @Override
        public void run(SystemProcedureExecutionContext context)
        {
            if (SNAP_LOG.isDebugEnabled()) {
                SNAP_LOG.debug("P" + context.getPartitionId() +
                               " updating partition count to: " + m_newPartitionCount);
            }

            // Update partition count stored on this site
            context.setNumberOfPartitions(m_newPartitionCount);
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
}
