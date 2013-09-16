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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.Pair;
import org.voltdb.DevNullSnapshotTarget;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.HashRangeExpression;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.utils.CatalogUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexSnapshotWritePlan extends SnapshotWritePlan {
    @Override
    protected boolean createSetupInternal(String file_path, String file_nonce, long txnId,
                                          Map<Integer, Long> partitionTransactionIds,
                                          JSONObject jsData,
                                          SystemProcedureExecutionContext context, String hostname,
                                          VoltTable result,
                                          Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
                                          SiteTracker tracker, long timestamp) throws IOException
    {
        assert SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty();

        final IndexSnapshotRequestConfig config =
            new IndexSnapshotRequestConfig(jsData, context.getDatabase());
        final Map<Integer, Long> pidToLocalHSIds = findLocalSources(config.partitionRanges, tracker);

        // only create index on partitioned tables
        List<Table> tables = CatalogUtil.getNormalTables(context.getDatabase(), false);

        // mark snapshot start in registry
        final AtomicInteger numTables = new AtomicInteger(tables.size());
        final SnapshotRegistry.Snapshot snapshotRecord =
            SnapshotRegistry.startSnapshot(txnId,
                                           context.getHostId(),
                                           file_path,
                                           file_nonce,
                                           SnapshotFormat.INDEX,
                                           tables.toArray(new Table[0]));

        // create table tasks
        for (Table table : tables) {
            createTasksForTable(table,
                                config.partitionRanges,
                                pidToLocalHSIds,
                                numTables,
                                snapshotRecord);
            result.addRow(context.getHostId(), hostname, table.getTypeName(), "SUCCESS", "");
        }

        return false;
    }

    private static Map<Integer, Long>
    findLocalSources(Collection<IndexSnapshotRequestConfig.PartitionRanges> partitionRanges,
                     SiteTracker tracker)
    {
        Set<Integer> partitions = Sets.newHashSet();
        for (IndexSnapshotRequestConfig.PartitionRanges partitionRange : partitionRanges) {
            partitions.add(partitionRange.partitionId);
        }

        Map<Integer, Long> pidToLocalHSId = Maps.newHashMap();
        List<Long> localSites = Longs.asList(tracker.getLocalSites());

        for (long hsId : localSites) {
            int pid = tracker.getPartitionForSite(hsId);
            if (partitions.contains(pid)) {
                pidToLocalHSId.put(pid, hsId);
            }
        }

        return pidToLocalHSId;
    }

    private static AbstractExpression
    createPredicateForTable(Table table, IndexSnapshotRequestConfig.PartitionRanges partitionRanges)
    {
        if (SNAP_LOG.isTraceEnabled()) {
            SNAP_LOG.trace("Partition " + partitionRanges.partitionId + " has ranges " +
                           partitionRanges.ranges);
        }
        HashRangeExpression predicate = new HashRangeExpression();
        predicate.setRanges(partitionRanges.ranges);
        predicate.setHashColumnIndex(table.getPartitioncolumn().getIndex());

        return predicate;
    }

    /**
     * For each site, generate a task for each target it has for this table.
     */
    private void createTasksForTable(Table table,
                                     Collection<IndexSnapshotRequestConfig.PartitionRanges> partitionRanges,
                                     Map<Integer, Long> pidToLocalHSIDs,
                                     AtomicInteger numTables,
                                     SnapshotRegistry.Snapshot snapshotRecord)
    {
        // no work on this node
        if (pidToLocalHSIDs.isEmpty()) {
            return;
        }

        // create a null data target
        final DevNullSnapshotTarget dataTarget = new DevNullSnapshotTarget();
        final Runnable onClose = new TargetStatsClosure(dataTarget,
                                                        table.getTypeName(),
                                                        numTables,
                                                        snapshotRecord);
        dataTarget.setOnCloseHandler(onClose);
        m_targets.add(dataTarget);

        // go over all local sites, create a task for each source site
        for (IndexSnapshotRequestConfig.PartitionRanges partitionRange : partitionRanges) {
            Long localHSId = pidToLocalHSIDs.get(partitionRange.partitionId);

            // The partition may not exist on this node. If so, keep calm and carry on
            if (localHSId != null) {
                // based on the source partition, the predicate is different
                final SnapshotTableTask task =
                    new SnapshotTableTask(table,
                                          dataTarget,
                                          new SnapshotDataFilter[0],
                                          createPredicateForTable(table, partitionRange),
                                          false);

                placeTask(task, Arrays.asList(localHSId));
            }
        }
    }
}
