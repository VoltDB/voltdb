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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.InstanceId;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.NativeSnapshotDataTarget;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotRegistry;

import com.google_voltpatches.common.collect.Maps;

/**
 * Create a snapshot write plan for a native snapshot.  This will attempt to
 * write every table at every site on every node to disk, with one file per
 * table per node.  Replicated tables are written on every node, but the
 * responsibility for writing them is spread round-robin across the sites on a
 * node.  Partitioned tables are written to the same target per table by every
 * site on a node.
 */
public class NativeSnapshotWritePlan extends SnapshotWritePlan<SnapshotRequestConfig>
{
    @Override
    public void setConfiguration(SystemProcedureExecutionContext context, JSONObject jsData) {
        m_config = new SnapshotRequestConfig(jsData, context.getDatabase());
    }

    @Override
    public Callable<Boolean> createSetup(String file_path, String pathType,
                                            String file_nonce,
                                            long txnId,
                                            Map<Integer, Long> partitionTransactionIds,
                                            SystemProcedureExecutionContext context,
                                            final VoltTable result,
                                            ExtensibleSnapshotDigestData extraSnapshotData,
                                            SiteTracker tracker,
                                            HashinatorSnapshotData hashinatorData,
                                            long timestamp)
    {
        return createSetupInternal(file_path, pathType, file_nonce, txnId, partitionTransactionIds,
                m_config, context, result, extraSnapshotData, tracker,
                hashinatorData, timestamp);
    }

    Callable<Boolean> createSetupInternal(String file_path, String pathType,
                                                    String file_nonce,
                                                    long txnId,
                                                    Map<Integer, Long> partitionTransactionIds,
                                                    SnapshotRequestConfig config,
                                                    SystemProcedureExecutionContext context,
                                                    final VoltTable result,
                                                    ExtensibleSnapshotDigestData extraSnapshotData,
                                                    SiteTracker tracker,
                                                    HashinatorSnapshotData hashinatorData,
                                                    long timestamp)
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());
        if (hashinatorData == null) {
            throw new RuntimeException("No hashinator data provided for elastic hashinator type.");
        }

        // TRAIL [SnapSave:5] - 3.2 [1 site/host] Get list of tables to save and create tasks for them.
        final List<SnapshotTableInfo> tables = config.tables;

        final int newPartitionCount;
        final int partitionCount;
        if (config.newPartitionCount != null) {
            newPartitionCount = config.newPartitionCount;
            partitionCount = Math.min(newPartitionCount, context.getNumberOfPartitions());
        } else {
            partitionCount = newPartitionCount = context.getNumberOfPartitions();
        }

        if (newPartitionCount != context.getNumberOfPartitions()) {
            createUpdatePartitionCountTasksForSites(tracker, context, newPartitionCount);
        }

        m_snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.NATIVE,
                        tables);

        final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        for (final SnapshotTableInfo table : tables) {
            final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table,
                            new SnapshotDataFilter[0],
                            null,
                            false);

            SNAP_LOG.debug("ADDING TASK for nativeSnapshot: " + task);

            context.getSiteSnapshotConnection();

            if (table.isReplicated()) {
                replicatedSnapshotTasks.add(task);
            } else {
                partitionedSnapshotTasks.add(task);
            }

            result.addRow(context.getHostId(),
                    CoreUtils.getHostnameOrAddress(),
                    table.getName(),
                    "SUCCESS",
                    "");
        }

        if (!tables.isEmpty() && replicatedSnapshotTasks.isEmpty() && partitionedSnapshotTasks.isEmpty()) {
            SnapshotRegistry.discardSnapshot(m_snapshotRecord);
        }

        // Native snapshots place the partitioned tasks on every site and round-robin the
        // replicated tasks across all the sites on every host
        List<Long> sitesOnThisHost = tracker.getSitesForHost(context.getHostId());
        placePartitionedTasks(partitionedSnapshotTasks, sitesOnThisHost);
        placeReplicatedTasks(replicatedSnapshotTasks, sitesOnThisHost);

        // Update the total task count, which used in snapshot progress tracking
        int totalPartTasks = partitionedSnapshotTasks.size() * sitesOnThisHost.size();
        int totalRepTasks = replicatedSnapshotTasks.size();
        m_snapshotRecord.setTotalTasks(totalPartTasks + totalRepTasks);

        /*
         * Force this to act like a truncation snaphsot when there is no config or the data config has a partition
         * count. This is primarily used by elastic join and remove for the truncation snapshots which they perform.
         * This doesn't actually do a full truncation snapshot since that is a different request path which should be
         * fixed at some point.
         */
        boolean isTruncationSnapshot = config.emptyConfig || config.newPartitionCount != null
                || config.truncationRequestId != null;

        // All IO work will be deferred and be run on the dedicated snapshot IO thread
        return createDeferredSetup(file_path, pathType, file_nonce, txnId, partitionTransactionIds,
                context, extraSnapshotData, tracker, hashinatorData, timestamp,
                partitionCount, newPartitionCount, tables, m_snapshotRecord, partitionedSnapshotTasks,
                replicatedSnapshotTasks, isTruncationSnapshot, config);
    }

    private Callable<Boolean> createDeferredSetup(final String file_path,
                                                  final String pathType,
                                                  final String file_nonce,
                                                  final long txnId,
                                                  final Map<Integer, Long> partitionTransactionIds,
                                                  final SystemProcedureExecutionContext context,
                                                  final ExtensibleSnapshotDigestData extraSnapshotData,
                                                  final SiteTracker tracker,
                                                  final HashinatorSnapshotData hashinatorData,
                                                  final long timestamp,
                                                  final int partitionCount,
                                                  final int newPartitionCount,
                                                  final List<SnapshotTableInfo> tables,
                                                  final SnapshotRegistry.Snapshot snapshotRecord,
                                                  final ArrayList<SnapshotTableTask> partitionedSnapshotTasks,
                                                  final ArrayList<SnapshotTableTask> replicatedSnapshotTasks,
                                                  final boolean isTruncationSnapshot,
                                                  final SnapshotRequestConfig config)
    {
        return new Callable<Boolean>() {
            private final HashMap<Integer, SnapshotDataTarget> m_createdTargets = Maps.newHashMap();
            final int hostId = context.getHostId();

            @Override
            public Boolean call() throws Exception
            {
                // TRAIL [SnapSave:6]  - 3.3 [1 site/host] Create completion tasks
                final AtomicInteger numTables = new AtomicInteger(tables.size());

                NativeSnapshotDataTarget.Factory factory = NativeSnapshotDataTarget.getFactory(file_path, pathType, file_nonce,
                        hostId, context.getCluster().getTypeName(), context.getDatabase().getTypeName(),
                        partitionCount, tracker.getPartitionsForHost(hostId), txnId, timestamp, isTruncationSnapshot);

                NativeSnapshotWritePlan.createFileBasedCompletionTasks(file_path, pathType, file_nonce,
                        txnId, partitionTransactionIds, context, extraSnapshotData,
                        hashinatorData,
                        timestamp,
                        newPartitionCount,
                        tables,
                        isTruncationSnapshot);

                for (SnapshotTableTask task : replicatedSnapshotTasks) {
                    SnapshotDataTarget target = getSnapshotDataTarget(factory, numTables, task);
                    task.setTarget(target);
                }

                for (SnapshotTableTask task : partitionedSnapshotTasks) {
                    SnapshotDataTarget target = getSnapshotDataTarget(factory, numTables, task);
                    task.setTarget(target);
                }

                if (isTruncationSnapshot) {
                    // Only sync the DR Log on Native Snapshots
                    SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(new Runnable() {
                        @Override
                        public void run()
                        {
                            context.forceAllDRNodeBuffersToDisk(false);
                        }
                    });
                }
                // Sync export buffer for all types of snapshot
                SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(new Runnable() {
                    @Override
                    public void run()
                    {
                        VoltDB.getExportManager().sync();
                    }
                });

                return true;
            }

            private SnapshotDataTarget getSnapshotDataTarget(NativeSnapshotDataTarget.Factory factory,
                    AtomicInteger numTables, SnapshotTableTask task) throws IOException {
                SnapshotDataTarget target = m_createdTargets.get(task.m_tableInfo.getTableId());
                if (target == null) {
                    String saveFile = SnapshotUtil.constructFilenameForTable(task.m_tableInfo, file_nonce,
                            SnapshotFormat.NATIVE, hostId);

                    target = factory.create(saveFile, task.m_tableInfo);

                    m_targets.add(target);
                    final Runnable onClose = new TargetStatsClosure(target, Arrays.asList(task.m_tableInfo.getName()),
                            numTables, snapshotRecord);
                    target.setOnCloseHandler(onClose);
                    final Runnable inProgress = new TargetStatsProgress(snapshotRecord);
                    target.setInProgressHandler(inProgress);

                    m_createdTargets.put(task.m_tableInfo.getTableId(), target);
                }
                return target;
            }
        };
    }

    static void createFileBasedCompletionTasks(
            String file_path, String pathType, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            SystemProcedureExecutionContext context,
            ExtensibleSnapshotDigestData extraSnapshotData,
            HashinatorSnapshotData hashinatorData,
            long timestamp, int newPartitionCount,
            List<SnapshotTableInfo> tables,
            boolean isTruncationSnapshot) throws IOException
    {
        InstanceId instId = VoltDB.instance().getHostMessenger().getInstanceId();
        Runnable completionTask = SnapshotUtil.writeSnapshotDigest(
                txnId,
                context.getCatalogCRC(),
                file_path,
                pathType,
                file_nonce,
                tables,
                context.getHostId(),
                partitionTransactionIds,
                extraSnapshotData,
                instId,
                timestamp,
                newPartitionCount,
                context.getClusterId(),
                isTruncationSnapshot);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        if (hashinatorData != null) {
            completionTask = SnapshotUtil.writeHashinatorConfig(
                    instId, file_path, pathType, file_nonce, context.getHostId(), hashinatorData, isTruncationSnapshot);
            if (completionTask != null) {
                SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
            }
        }
        completionTask = SnapshotUtil.writeSnapshotCatalog(file_path, pathType, file_nonce, isTruncationSnapshot);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        completionTask = SnapshotUtil.writeSnapshotCompletion(file_path, pathType, file_nonce, context.getHostId(), SNAP_LOG, isTruncationSnapshot);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        if (extraSnapshotData.getTerminus() != 0L) {
            completionTask = SnapshotUtil.writeTerminusMarker(file_nonce, context.getPaths(), SNAP_LOG);
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
    }
}
