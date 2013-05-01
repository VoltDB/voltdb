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
import org.voltdb.utils.MiscUtils;

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
        if (!config.partitionsToAdd.isEmpty()) {
            Map<Long, Integer> tokensToAdd = createTokensToAdd(config.partitionsToAdd);
            createUpdateHashinatorTasksForSites(localPartitions, tokensToAdd, txnId);
        }

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

        Map<Long, List<SnapshotDataTarget>> sdts = new HashMap<Long, List<SnapshotDataTarget>>();
        if (config.streamPairs.size() > 0) {
            SNAP_LOG.debug("Sites to stream from: " +
                    CoreUtils.hsIdCollectionToString(config.streamPairs.keySet()));
            for (Entry<Long, List<Long>> entry : config.streamPairs.entrySet()) {
                long srcHSId = entry.getKey();
                List<Long> destHSIds = entry.getValue();

                for (long destHSId : destHSIds) {
                    MiscUtils.multimapPut(sdts, srcHSId,
                                          new StreamSnapshotDataTarget(destHSId, schemas));
                }
            }
        } else {
            // There's no work to do on this host, just claim success, return an empty plan, and things
            // will sort themselves out properly
            return false;
        }

        // For each table, create tasks where each task has a data target.
        for (final Table table : config.tables) {
            createTasksForTable(table, sdts, config, numTables, snapshotRecord);
            result.addRow(context.getHostId(), hostname, table.getTypeName(), "SUCCESS", "");
        }

        return false;
    }

    private void createTasksForTable(Table table,
                                     Map<Long, List<SnapshotDataTarget>> dataTargets,
                                     StreamSnapshotRequestConfig config,
                                     AtomicInteger numTables,
                                     SnapshotRegistry.Snapshot snapshotRecord)
    {
        final List<SnapshotTableTask> tasksForThisTable = new ArrayList<SnapshotTableTask>();
        final List<Long> srcHSIds = new ArrayList<Long>(dataTargets.keySet());

        AbstractExpression predicate = null;
        boolean deleteTuples = false;
        if (!table.getIsreplicated()) {
            predicate = createPredicateForTable(table, config);
            // Only delete tuples if there is a predicate, e.g. elastic join
            if (predicate != null) {
                deleteTuples = true;
            }
        }

        /*
         * There can be multiple data targets for a single site. Iterate through all data targets
         * and create a task for each one.
         */
        for (List<SnapshotDataTarget> targets : dataTargets.values()) {
            m_targets.addAll(targets);

            for (SnapshotDataTarget target : targets) {
                final Runnable onClose = new TargetStatsClosure(target, table.getTypeName(),
                                                                numTables, snapshotRecord);
                target.setOnCloseHandler(onClose);

                final SnapshotTableTask task =
                    new SnapshotTableTask(table,
                                          target,
                                          new SnapshotDataFilter[0], // This task no longer needs partition filtering
                                          predicate,
                                          deleteTuples);

                tasksForThisTable.add(task);
            }
        }

        // Stream snapshots need to write all partitioned tables to all
        // selected partitions and all replicated tables to all selected
        // partitions
        if (table.getIsreplicated()) {
            placeReplicatedTasks(tasksForThisTable, srcHSIds);
        } else {
            placePartitionedTasks(tasksForThisTable, srcHSIds);
        }
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
