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

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.PostSnapshotTask;
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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Longs;

/**
 * Create a snapshot write plan for snapshots streamed to other sites
 * (specified in the jsData).  Each source site specified in the streamPairs
 * key will write all of its tables, partitioned and replicated, to a target
 * per-site.
 */
public class StreamSnapshotWritePlan extends SnapshotWritePlan
{
    @Override
    protected boolean createSetupInternal(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp) throws IOException
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        final StreamSnapshotRequestConfig config =
            new StreamSnapshotRequestConfig(jsData, context.getDatabase());
        final List<StreamSnapshotRequestConfig.Stream> localStreams =
            filterRemoteStreams(config.streams, Longs.asList(tracker.getLocalSites()));
        final Set<Integer> partitionsToAdd = getPartitionsToAdd(localStreams);

        // Coalesce a truncation snapshot if shouldTruncate is true
        if (config.shouldTruncate) {
            /*
             * The snapshot will only contain existing partitions. Write the new partition count
             * down in the digest so that we can check if enough command log is collected on
             * replay.
             */
            final int newPartitionCount = context.getNumberOfPartitions() + partitionsToAdd.size();
            coalesceTruncationSnapshotPlan(file_path, file_nonce, txnId, partitionTransactionIds,
                                           jsData, context, hostname, result,
                                           exportSequenceNumbers, tracker,
                                           hashinatorData,
                                           timestamp,
                                           newPartitionCount);
        }

        // Create post snapshot update hashinator work
        List<Integer> localPartitions = tracker.getPartitionsForHost(context.getHostId());
        if (!partitionsToAdd.isEmpty()) {
            createUpdatePartitionCountTasksForSites(localPartitions, partitionsToAdd);
        }

        // Mark snapshot start in registry
        final AtomicInteger numTables = new AtomicInteger(config.tables.length);
        final SnapshotRegistry.Snapshot snapshotRecord =
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
            schemas.put(table.getRelativeIndex(), schemaTable.getSchemaBytes());
        }

        List<DataTargetInfo> sdts = createDataTargets(localStreams, hashinatorData, schemas);

        // If there's no work to do on this host, just claim success, return an empty plan,
        // and things will sort themselves out properly

        // For each table, create tasks where each task has a data target.
        for (final Table table : config.tables) {
            createTasksForTable(table, sdts, numTables, snapshotRecord);
            result.addRow(context.getHostId(), hostname, table.getTypeName(), "SUCCESS", "");
        }

        return false;
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

        if (!localStreams.isEmpty() && !schemas.isEmpty()) {
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

    private void coalesceTruncationSnapshotPlan(String file_path, String file_nonce, long txnId,
                                                Map<Integer, Long> partitionTransactionIds,
                                                JSONObject jsData,
                                                SystemProcedureExecutionContext context,
                                                String hostname, VoltTable result,
                                                Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
                                                SiteTracker tracker,
                                                HashinatorSnapshotData hashinatorData,
                                                long timestamp,
                                                int newPartitionCount)
        throws IOException
    {
        NativeSnapshotWritePlan plan = new NativeSnapshotWritePlan();
        plan.createSetupInternal(file_path, file_nonce, txnId, partitionTransactionIds,
                                 jsData, context, hostname, result, exportSequenceNumbers,
                                 tracker, hashinatorData, timestamp, newPartitionCount);
        m_targets.addAll(plan.m_targets);
        m_taskListsForHSIds.putAll(plan.m_taskListsForHSIds);
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
                                      targetInfo.dataTarget,
                                      new SnapshotDataFilter[0], // This task no longer needs partition filtering
                                      null,
                                      false);

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
                                                                Set<Integer> partitionsToAdd)
    {
        PostSnapshotTask task = new UpdatePartitionCount(partitionsToAdd);
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
        private final Set<Integer> m_newPartitions;

        public UpdatePartitionCount(Set<Integer> newPartitions)
        {
            m_newPartitions = newPartitions;
        }

        @Override
        public void run(SystemProcedureExecutionContext context)
        {
            if (SNAP_LOG.isDebugEnabled()) {
                SNAP_LOG.debug("P" + context.getPartitionId() +
                               " updating partition count with new partitions: " + m_newPartitions);
            }

            // Update partition count stored on this site
            context.setNumberOfPartitions(context.getNumberOfPartitions() + m_newPartitions.size());
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
