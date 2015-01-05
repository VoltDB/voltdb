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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.Pair;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.collect.Maps;

/**
 * Create a snapshot write plan for a native snapshot.  This will attempt to
 * write every table at every site on every node to disk, with one file per
 * table per node.  Replicated tables are written on every node, but the
 * responsibility for writing them is spread round-robin across the sites on a
 * node.  Partitioned tables are written to the same target per table by every
 * site on a node.
 */
public class NativeSnapshotWritePlan extends SnapshotWritePlan
{
    @Override
    public Callable<Boolean> createSetup(String file_path,
                                            String file_nonce,
                                            long txnId,
                                            Map<Integer, Long> partitionTransactionIds,
                                            JSONObject jsData,
                                            SystemProcedureExecutionContext context,
                                            final VoltTable result,
                                            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
                                            SiteTracker tracker,
                                            HashinatorSnapshotData hashinatorData,
                                            long timestamp)
    {
        return createSetupInternal(file_path, file_nonce, txnId, partitionTransactionIds, jsData, context,
                result, exportSequenceNumbers, tracker, hashinatorData, timestamp, context.getNumberOfPartitions());
    }

    Callable<Boolean> createSetupInternal(String file_path,
                                                    String file_nonce,
                                                    long txnId,
                                                    Map<Integer, Long> partitionTransactionIds,
                                                    JSONObject jsData,
                                                    SystemProcedureExecutionContext context,
                                                    final VoltTable result,
                                                    Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
                                                    SiteTracker tracker,
                                                    HashinatorSnapshotData hashinatorData,
                                                    long timestamp,
                                                    int newPartitionCount)
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        if (TheHashinator.getConfiguredHashinatorType() == HashinatorType.ELASTIC && hashinatorData == null) {
            throw new RuntimeException("No hashinator data provided for elastic hashinator type.");
        }

        final List<Table> tables = SnapshotUtil.getTablesToSave(context.getDatabase());
        m_snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.NATIVE,
                    tables.toArray(new Table[0]));

        final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        for (final Table table : tables) {
            final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table,
                            new SnapshotDataFilter[0],
                            null,
                            false);

            SNAP_LOG.debug("ADDING TASK: " + task);

            if (table.getIsreplicated()) {
                replicatedSnapshotTasks.add(task);
            } else {
                partitionedSnapshotTasks.add(task);
            }

            result.addRow(context.getHostId(),
                    CoreUtils.getHostnameOrAddress(),
                    table.getTypeName(),
                    "SUCCESS",
                    "");
        }

        if (!tables.isEmpty() && replicatedSnapshotTasks.isEmpty() && partitionedSnapshotTasks.isEmpty()) {
            SnapshotRegistry.discardSnapshot(m_snapshotRecord);
        }

        // Native snapshots place the partitioned tasks on every site and round-robin the
        // replicated tasks across all the sites on every host
        placePartitionedTasks(partitionedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));
        placeReplicatedTasks(replicatedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));

        // All IO work will be deferred and be run on the dedicated snapshot IO thread
        return createDeferredSetup(file_path, file_nonce, txnId, partitionTransactionIds, context,
                exportSequenceNumbers, tracker, hashinatorData, timestamp,
                newPartitionCount, tables, m_snapshotRecord, partitionedSnapshotTasks,
                replicatedSnapshotTasks);
    }

    private Callable<Boolean> createDeferredSetup(final String file_path,
                                                  final String file_nonce,
                                                  final long txnId,
                                                  final Map<Integer, Long> partitionTransactionIds,
                                                  final SystemProcedureExecutionContext context,
                                                  final Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
                                                  final SiteTracker tracker,
                                                  final HashinatorSnapshotData hashinatorData,
                                                  final long timestamp,
                                                  final int newPartitionCount,
                                                  final List<Table> tables,
                                                  final SnapshotRegistry.Snapshot snapshotRecord,
                                                  final ArrayList<SnapshotTableTask> partitionedSnapshotTasks,
                                                  final ArrayList<SnapshotTableTask> replicatedSnapshotTasks)
    {
        return new Callable<Boolean>() {
            private final HashMap<Integer, SnapshotDataTarget> m_createdTargets = Maps.newHashMap();

            @Override
            public Boolean call() throws Exception
            {
                final AtomicInteger numTables = new AtomicInteger(tables.size());

                NativeSnapshotWritePlan.createFileBasedCompletionTasks(file_path, file_nonce,
                        txnId, partitionTransactionIds, context, exportSequenceNumbers,
                        hashinatorData,
                        timestamp,
                        newPartitionCount);

                for (SnapshotTableTask task : replicatedSnapshotTasks) {
                    SnapshotDataTarget target = getSnapshotDataTarget(numTables, task);
                    task.setTarget(target);
                }

                for (SnapshotTableTask task : partitionedSnapshotTasks) {
                    SnapshotDataTarget target = getSnapshotDataTarget(numTables, task);
                    task.setTarget(target);
                }

                return true;
            }

            private SnapshotDataTarget getSnapshotDataTarget(AtomicInteger numTables, SnapshotTableTask task)
                    throws IOException
            {
                SnapshotDataTarget target = m_createdTargets.get(task.m_table.getRelativeIndex());
                if (target == null) {
                    target = createDataTargetForTable(file_path, file_nonce, task.m_table, txnId,
                            context.getHostId(), context.getCluster().getTypeName(),
                            context.getDatabase().getTypeName(), context.getNumberOfPartitions(),
                            tracker, timestamp, numTables, snapshotRecord);
                    m_createdTargets.put(task.m_table.getRelativeIndex(), target);
                }
                return target;
            }
        };
    }

    private SnapshotDataTarget createDataTargetForTable(String file_path,
                                                        String file_nonce,
                                                        Table table,
                                                        long txnId,
                                                        int hostId,
                                                        String clusterName,
                                                        String databaseName,
                                                        int partitionCount,
                                                        SiteTracker tracker,
                                                        long timestamp,
                                                        AtomicInteger numTables,
                                                        SnapshotRegistry.Snapshot snapshotRecord)
            throws IOException
    {
        SnapshotDataTarget sdt;

        File saveFilePath = SnapshotUtil.constructFileForTable(
                table,
                file_path,
                file_nonce,
                SnapshotFormat.NATIVE,
                hostId);

        sdt = new DefaultSnapshotDataTarget(saveFilePath,
                hostId,
                clusterName,
                databaseName,
                table.getTypeName(),
                partitionCount,
                table.getIsreplicated(),
                tracker.getPartitionsForHost(hostId),
                CatalogUtil.getVoltTable(table),
                txnId,
                timestamp);

        m_targets.add(sdt);
        final Runnable onClose = new TargetStatsClosure(sdt, table.getTypeName(), numTables, snapshotRecord);
        sdt.setOnCloseHandler(onClose);

        return sdt;
    }

    static void createFileBasedCompletionTasks(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            SystemProcedureExecutionContext context,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            HashinatorSnapshotData hashinatorData,
            long timestamp, int newPartitionCount) throws IOException
    {
        final List<Table> tables = SnapshotUtil.getTablesToSave(context.getDatabase());
        InstanceId instId = VoltDB.instance().getHostMessenger().getInstanceId();
        Runnable completionTask = SnapshotUtil.writeSnapshotDigest(
                txnId,
                context.getCatalogCRC(),
                file_path,
                file_nonce,
                tables,
                context.getHostId(),
                exportSequenceNumbers,
                partitionTransactionIds,
                instId,
                timestamp,
                newPartitionCount);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        if (hashinatorData != null) {
            completionTask = SnapshotUtil.writeHashinatorConfig(
                    instId, file_path, file_nonce, context.getHostId(), hashinatorData);
            if (completionTask != null) {
                SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
            }
        }
        completionTask = SnapshotUtil.writeSnapshotCatalog(file_path, file_nonce);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        completionTask = SnapshotUtil.writeSnapshotCompletion(file_path, file_nonce, context.getHostId(), SNAP_LOG);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
    }
}
