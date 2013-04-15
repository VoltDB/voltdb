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

import java.util.ArrayList;

import java.util.Collection;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import org.json_voltpatches.JSONObject;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

import org.voltdb.PostSnapshotTask;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.Table;

import org.voltdb.dtxn.SiteTracker;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.HashRangeExpression;
import org.voltdb.rejoin.StreamSnapshotDataTarget;

import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;

import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.SystemProcedureExecutionContext;

import org.voltdb.utils.CatalogUtil;
import org.voltdb.VoltTable;

/**
 * Create a snapshot write plan for snapshots streamed to other sites
 * (specified in the jsData).  Each source site specified in the streamPairs
 * key will write all of its tables, partitioned and replicated, to a target
 * per-site.
 */
public class StreamSnapshotWritePlan extends SnapshotWritePlan
{
    protected boolean createSetupInternal(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker, long timestamp) throws IOException
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        final List<Long> localHSIds = Longs.asList(tracker.getLocalSites());
        final StreamSnapshotRequestConfig config =
            new StreamSnapshotRequestConfig(jsData, context.getDatabase(), localHSIds);

        List<Integer> localPartitions = tracker.getPartitionsForHost(context.getHostId());
        Map<Long, Integer> tokensToAdd = createTokensToAdd(config.partitionsToAdd);
        createUpdateHashinatorTasksForSites(localPartitions, tokensToAdd, txnId);

        final AtomicInteger numTables = new AtomicInteger(config.tables.size());
        final SnapshotRegistry.Snapshot snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.STREAM,
                    config.tables.toArray(new Table[0]));

        // table schemas for all the tables we'll snapshot on this partition
        Map<Integer, byte[]> schemas = new HashMap<Integer, byte[]>();
        for (final Table table : config.tables) {
            VoltTable schemaTable = CatalogUtil.getVoltTable(table);
            schemas.put(table.getRelativeIndex(), schemaTable.getSchemaBytes());
        }

        Map<Long, SnapshotDataTarget> sdts = new HashMap<Long, SnapshotDataTarget>();
        if (config.streamPairs.size() > 0) {
            SNAP_LOG.debug("Sites to stream from: " +
                    CoreUtils.hsIdCollectionToString(config.streamPairs.keySet()));
            for (Entry<Long, Long> entry : config.streamPairs.entrySet()) {
                sdts.put(entry.getKey(), new StreamSnapshotDataTarget(entry.getValue(), schemas));
            }
        }
        else
        {
            // There's no work to do on this host, just claim success, return an empty plan, and things
            // will sort themselves out properly
            return false;
        }

        for (Entry<Long, SnapshotDataTarget> entry : sdts.entrySet()) {
            final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
                new ArrayList<SnapshotTableTask>();
            final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
                new ArrayList<SnapshotTableTask>();
            SnapshotDataTarget sdt = entry.getValue();
            m_targets.add(sdt);
            for (final Table table : config.tables)
            {
                final Runnable onClose = new TargetStatsClosure(sdt, table.getTypeName(),
                        numTables, snapshotRecord);
                sdt.setOnCloseHandler(onClose);
                AbstractExpression predicate = null;
                boolean deleteTuples = false;
                if (!table.getIsreplicated()) {
                    predicate = createPredicateForTable(table, config);
                    deleteTuples = true;
                }

                final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table.getRelativeIndex(),
                            sdt,
                            new SnapshotDataFilter[0], // This task no longer needs partition filtering
                            predicate,
                            deleteTuples,
                            table.getIsreplicated(),
                            table.getTypeName());

                if (table.getIsreplicated()) {
                    replicatedSnapshotTasks.add(task);
                } else {
                    partitionedSnapshotTasks.add(task);
                }
                result.addRow(context.getHostId(),
                        hostname,
                        table.getTypeName(),
                        "SUCCESS",
                        "");
            }

            // Stream snapshots need to write all partitioned tables to all
            // selected partitions and all replicated tables to all selected
            // partitions
            List<Long> thisOne = new ArrayList<Long>();
            thisOne.add(entry.getKey());
            placePartitionedTasks(partitionedSnapshotTasks, thisOne);
            placeReplicatedTasks(replicatedSnapshotTasks, thisOne);
        }
        return false;
    }

    private static AbstractExpression createPredicateForTable(Table table,
                                                              StreamSnapshotRequestConfig config)
    {
        HashRangeExpression predicate = null;

        if (!config.partitionsToAdd.isEmpty()) {
            Map<Long, Long> ranges = new TreeMap<Long, Long>();
            for (Entry<Integer, SortedMap<Long, Long>> entry : config.partitionsToAdd.entrySet()) {
                int partition = entry.getKey();
                SortedMap<Long, Long> pRanges = entry.getValue();
                if (SNAP_LOG.isTraceEnabled()) {
                    SNAP_LOG.trace("Partition " + partition + " has ranges " + pRanges);
                }
                ranges.putAll(pRanges);
            }
            predicate = new HashRangeExpression();
            predicate.setRanges(ranges);
            predicate.setHashColumnIndex(table.getPartitioncolumn().getIndex());
        }

        return predicate;
    }

    private static Map<Long, Integer> createTokensToAdd(Map<Integer, SortedMap<Long, Long>> newPartitions)
    {
        ImmutableMap.Builder<Long, Integer> tokenBuilder = ImmutableMap.builder();
        for (Entry<Integer, SortedMap<Long, Long>> entry : newPartitions.entrySet()) {
            int partition = entry.getKey();
            SortedMap<Long, Long> ranges = entry.getValue();
            for (long token : ranges.keySet()) {
                tokenBuilder.put(token, partition);
            }
        }
        return tokenBuilder.build();
    }

    private static void createUpdateHashinatorTasksForSites(Collection<Integer> localPartitions,
                                                            Map<Long, Integer> tokensToPartitions,
                                                            long txnId)
    {
        byte[] configBytes = TheHashinator.addPartitions(tokensToPartitions);
        PostSnapshotTask task = new UpdateHashinator(ImmutableSet.copyOf(tokensToPartitions.values()),
                                                     txnId, configBytes);
        assert !localPartitions.isEmpty();
        Iterator<Integer> iter = localPartitions.iterator();
        while (iter.hasNext()) {
            int partition = iter.next();
            SnapshotSiteProcessor.m_siteTasksPostSnapshotting.put(partition, task);
        }
    }

    /**
     * A post-snapshot site task that updates the hashinator in both Java and EE,
     * runs on all sites. Only one site will succeed in updating the Java hashinator.
     */
    private static class UpdateHashinator implements PostSnapshotTask {
        private final Set<Integer> m_newPartitions;
        // txnId of the snapshot MP txn, used for hashinator update
        private final long m_txnId;
        // This site should update the Java hashinator if this is not null
        private final byte[] m_javaHashinatorConfig;

        public UpdateHashinator(Set<Integer> newPartitions,
                                long txnId,
                                byte[] javaHashinatorConfig)
        {
            m_newPartitions = newPartitions;
            m_txnId = txnId;
            m_javaHashinatorConfig = javaHashinatorConfig;
        }

        @Override
        public void run(SystemProcedureExecutionContext context)
        {
            SNAP_LOG.debug("P" + context.getPartitionId() +
                               " updating Java hashinator with new partitions: " +
                               m_newPartitions);
            // Update the Java hashinator, sites will race to do this, only one will succeed
            TheHashinator.updateHashinator(TheHashinator.getConfiguredHashinatorType().hashinatorClass,
                                           m_txnId,
                                           m_javaHashinatorConfig);

            if (SNAP_LOG.isDebugEnabled()) {
                SNAP_LOG.debug("P" + context.getPartitionId() +
                               " updated the hashinator with new partitions: " + m_newPartitions);
            }
            // Update EE hashinator
            Pair<TheHashinator.HashinatorType, byte[]> currentConfig = TheHashinator.getCurrentConfig();
            context.updateHashinator(currentConfig);
        }
    }
}
