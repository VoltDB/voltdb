/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DevNullSnapshotTarget;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.HashRangeExpression;
import org.voltdb.sysprocs.SnapshotRegistry;

import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.primitives.Longs;

public class IndexSnapshotWritePlan extends SnapshotWritePlan<IndexSnapshotRequestConfig> {

    @Override
    public void setConfiguration(SystemProcedureExecutionContext context, JSONObject jsData) {
        m_config = new IndexSnapshotRequestConfig(jsData, context.getDatabase());
    }

    @Override
    public Callable<Boolean> createSetup(String file_path, String pathType, String file_nonce, long txnId,
                                         Map<Integer, Long> partitionTransactionIds,
                                         SystemProcedureExecutionContext context,
                                         VoltTable result,
                                         ExtensibleSnapshotDigestData extraSnapshotData,
                                         SiteTracker tracker,
                                         HashinatorSnapshotData hashinatorData,
                                         long timestamp)
    {
        assert SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty();

        final Map<Integer, Long> pidToLocalHSIds = findLocalSources(m_config.partitionRanges, tracker);

        // mark snapshot start in registry
        final AtomicInteger numTables = new AtomicInteger(m_config.tables.size());
        m_snapshotRecord =
            SnapshotRegistry.startSnapshot(txnId,
                                           context.getHostId(),
                                           file_path,
                                           file_nonce,
                                           SnapshotFormat.INDEX,
                                           m_config.tables);

        // create table tasks
        for (SnapshotTableInfo table : m_config.tables) {
            createTasksForTable(table,
                                m_config.partitionRanges,
                                pidToLocalHSIds,
                                numTables,
                                m_snapshotRecord);
            result.addRow(context.getHostId(), CoreUtils.getHostnameOrAddress(), table.getName(), "SUCCESS", "");
        }

        return null;
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

    /**
     * Create the expression used to build elastic index for a given table.
     * @param table     The table to build the elastic index on
     * @param ranges    The hash ranges that the index should include
     */
    public static AbstractExpression createIndexExpressionForTable(SnapshotTableInfo table,
            Map<Integer, Integer> ranges)
    {
        HashRangeExpression predicate = new HashRangeExpression();
        predicate.setRanges(ranges);
        predicate.setHashColumnIndex(table.getPartitionColumn());

        return predicate;
    }

    /**
     * For each site, generate a task for each target it has for this table.
     */
    private void createTasksForTable(SnapshotTableInfo table,
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
                                                        Arrays.asList(table.getName()),
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
                                          new SnapshotDataFilter[0],
                                          createIndexExpressionForTable(table, partitionRange.ranges),
                                          false);
                task.setTarget(dataTarget);

                placeTask(task, Arrays.asList(localHSId));
            }
        }
    }
}
