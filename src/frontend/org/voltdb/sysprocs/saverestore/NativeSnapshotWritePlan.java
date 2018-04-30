/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.InstanceId;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.export.ExportManager;
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
    public Callable<Boolean> createSetup(String file_path, String pathType,
                                            String file_nonce,
                                            long txnId,
                                            Map<Integer, Long> partitionTransactionIds,
                                            JSONObject jsData,
                                            SystemProcedureExecutionContext context,
                                            final VoltTable result,
                                            ExtensibleSnapshotDigestData extraSnapshotData,
                                            SiteTracker tracker,
                                            HashinatorSnapshotData hashinatorData,
                                            long timestamp)
    {
        return createSetupInternal(file_path, pathType, file_nonce, txnId, partitionTransactionIds,
                jsData, context, result, extraSnapshotData, tracker, hashinatorData,
                timestamp, context.getNumberOfPartitions());
    }

    Callable<Boolean> createSetupInternal(String file_path, String pathType,
                                                    String file_nonce,
                                                    long txnId,
                                                    Map<Integer, Long> partitionTransactionIds,
                                                    JSONObject jsData,
                                                    SystemProcedureExecutionContext context,
                                                    final VoltTable result,
                                                    ExtensibleSnapshotDigestData extraSnapshotData,
                                                    SiteTracker tracker,
                                                    HashinatorSnapshotData hashinatorData,
                                                    long timestamp,
                                                    int newPartitionCount)
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        if (hashinatorData == null) {
            throw new RuntimeException("No hashinator data provided for elastic hashinator type.");
        }

        final SnapshotRequestConfig config = new SnapshotRequestConfig(jsData, context.getDatabase());
        final Table[] tableArray;
        if (config.tables.length == 0 && (jsData == null || !jsData.has("tables"))) {
            tableArray = SnapshotUtil.getTablesToSave(context.getDatabase()).toArray(new Table[0]);
        } else {
            tableArray = config.tables;
        }

        m_snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.NATIVE,
                    tableArray);

        final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        for (final Table table : tableArray) {
            final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table,
                            new SnapshotDataFilter[0],
                            null,
                            false);

            SNAP_LOG.debug("ADDING TASK for nativeSnapshot: " + task);

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

        if (tableArray.length > 0 && replicatedSnapshotTasks.isEmpty() && partitionedSnapshotTasks.isEmpty()) {
            SnapshotRegistry.discardSnapshot(m_snapshotRecord);
        }

        // Native snapshots place the partitioned tasks on every site and round-robin the
        // replicated tasks across all the sites on every host
        placePartitionedTasks(partitionedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));
        placeReplicatedTasks(replicatedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));

        boolean isTruncationSnapshot = true;
        if (jsData != null) {
            isTruncationSnapshot = jsData.has("truncReqId");
        }

        // All IO work will be deferred and be run on the dedicated snapshot IO thread
        return createDeferredSetup(file_path, pathType, file_nonce, txnId, partitionTransactionIds,
                context, extraSnapshotData, tracker, hashinatorData, timestamp,
                newPartitionCount, tableArray, m_snapshotRecord, partitionedSnapshotTasks,
                replicatedSnapshotTasks, isTruncationSnapshot);
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
                                                  final int newPartitionCount,
                                                  final Table[] tables,
                                                  final SnapshotRegistry.Snapshot snapshotRecord,
                                                  final ArrayList<SnapshotTableTask> partitionedSnapshotTasks,
                                                  final ArrayList<SnapshotTableTask> replicatedSnapshotTasks,
                                                  final boolean isTruncationSnapshot)
    {
        return new Callable<Boolean>() {
            private final HashMap<Integer, SnapshotDataTarget> m_createdTargets = Maps.newHashMap();

            @Override
            public Boolean call() throws Exception
            {
                final AtomicInteger numTables = new AtomicInteger(tables.length);

                NativeSnapshotWritePlan.createFileBasedCompletionTasks(file_path, pathType, file_nonce,
                        txnId, partitionTransactionIds, context, extraSnapshotData,
                        hashinatorData,
                        timestamp,
                        newPartitionCount,
                        tables,
                        isTruncationSnapshot);

                for (SnapshotTableTask task : replicatedSnapshotTasks) {
                    SnapshotDataTarget target = getSnapshotDataTarget(numTables, task);
                    task.setTarget(target);
                }

                for (SnapshotTableTask task : partitionedSnapshotTasks) {
                    SnapshotDataTarget target = getSnapshotDataTarget(numTables, task);
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
                        ExportManager.sync(false);
                    }
                });

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
                                                      DrRoleType.XDCR.value().equals(context.getCluster().getDrrole()),
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
                                                        boolean isActiveActiveDRed,
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

        if (isActiveActiveDRed && table.getIsdred()) {
            sdt = new DefaultSnapshotDataTarget(saveFilePath,
                    hostId,
                    clusterName,
                    databaseName,
                    table.getTypeName(),
                    partitionCount,
                    table.getIsreplicated(),
                    tracker.getPartitionsForHost(hostId),
                    CatalogUtil.getVoltTable(table, CatalogUtil.DR_HIDDEN_COLUMN_INFO),
                    txnId,
                    timestamp);
        }
        else {
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
        }

        m_targets.add(sdt);
        final Runnable onClose = new TargetStatsClosure(sdt, table.getTypeName(), numTables, snapshotRecord);
        sdt.setOnCloseHandler(onClose);

        return sdt;
    }

    static void createFileBasedCompletionTasks(
            String file_path, String pathType, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            SystemProcedureExecutionContext context,
            ExtensibleSnapshotDigestData extraSnapshotData,
            HashinatorSnapshotData hashinatorData,
            long timestamp, int newPartitionCount,
            Table[] tables,
            boolean isTruncationSnapshot) throws IOException
    {
        InstanceId instId = VoltDB.instance().getHostMessenger().getInstanceId();
        Runnable completionTask = SnapshotUtil.writeSnapshotDigest(
                txnId,
                context.getCatalogCRC(),
                file_path,
                pathType,
                file_nonce,
                Arrays.asList(tables),
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
                    instId, file_path, file_nonce, context.getHostId(), hashinatorData, isTruncationSnapshot);
            if (completionTask != null) {
                SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
            }
        }
        completionTask = SnapshotUtil.writeSnapshotCatalog(file_path, file_nonce, isTruncationSnapshot);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        completionTask = SnapshotUtil.writeSnapshotCompletion(file_path, file_nonce, context.getHostId(), SNAP_LOG, isTruncationSnapshot);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        if (extraSnapshotData.getTerminus() != 0L) {
            completionTask = SnapshotUtil.writeTerminusMarker(file_nonce, context.getPaths(), SNAP_LOG);
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
    }
}
