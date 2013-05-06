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

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONObject;

import org.voltcore.utils.Pair;

import org.voltdb.catalog.Table;

import org.voltdb.DefaultSnapshotDataTarget;

import org.voltdb.dtxn.SiteTracker;

import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;

import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.SystemProcedureExecutionContext;

import org.voltdb.utils.CatalogUtil;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;

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
    protected boolean createSetupInternal(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker, long timestamp) throws IOException
    {
        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        NativeSnapshotWritePlan.createFileBasedCompletionTasks(file_path, file_nonce,
                txnId, partitionTransactionIds, context, exportSequenceNumbers, timestamp);

        final List<Table> tables = SnapshotUtil.getTablesToSave(context.getDatabase());
        final AtomicInteger numTables = new AtomicInteger(tables.size());
        final SnapshotRegistry.Snapshot snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.NATIVE,
                    tables.toArray(new Table[0]));

        SnapshotDataTarget sdt = null;
        // If no targets were successfully created, that's our cue to abort.
        boolean noTargetsCreated = true;

        final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        for (final Table table : tables)
        {
            try {
                File saveFilePath = null;
                saveFilePath = SnapshotUtil.constructFileForTable(
                        table,
                        file_path,
                        file_nonce,
                        SnapshotFormat.NATIVE,
                        context.getHostId());

                sdt =
                    constructSnapshotDataTargetForTable(
                            context,
                            saveFilePath,
                            table,
                            context.getHostId(),
                            tracker.m_numberOfPartitions,
                            txnId,
                            timestamp,
                            tracker.getPartitionsForHost(context.getHostId()));

                if (sdt == null) {
                    throw new IOException("Unable to create snapshot target");
                }

                m_targets.add(sdt);
                final Runnable onClose = new TargetStatsClosure(sdt, table.getTypeName(),
                        numTables, snapshotRecord);
                sdt.setOnCloseHandler(onClose);

                final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table,
                            sdt,
                            new SnapshotDataFilter[0],
                            null,
                            false);

                SNAP_LOG.debug("ADDING TASK: " + task);

                if (table.getIsreplicated()) {
                    replicatedSnapshotTasks.add(task);
                } else {
                    partitionedSnapshotTasks.add(task);
                }

                noTargetsCreated = false;
                result.addRow(context.getHostId(),
                        hostname,
                        table.getTypeName(),
                        "SUCCESS",
                        "");
            } catch (IOException ex) {
                handleTargetCreationError(sdt, context, file_nonce, hostname, table.getTypeName(),
                        ex, result);
            }
        }

        if (noTargetsCreated) {
            SnapshotRegistry.discardSnapshot(snapshotRecord);
        }

        // Native snapshots place the partitioned tasks on every site and round-robin the
        // replicated tasks across all the sites on every host
        placePartitionedTasks(partitionedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));
        placeReplicatedTasks(replicatedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));
        return noTargetsCreated;
    }

    private final SnapshotDataTarget constructSnapshotDataTargetForTable(
            SystemProcedureExecutionContext context,
            File f,
            Table table,
            int hostId,
            int numPartitions,
            long txnId,
            long timestamp,
            List<Integer> partitionsForHost)
        throws IOException
    {
        return new DefaultSnapshotDataTarget(f,
                hostId,
                context.getCluster().getTypeName(),
                context.getDatabase().getTypeName(),
                table.getTypeName(),
                numPartitions,
                table.getIsreplicated(),
                partitionsForHost,
                CatalogUtil.getVoltTable(table),
                txnId,
                timestamp);
    }

    static void createFileBasedCompletionTasks(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            SystemProcedureExecutionContext context,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            long timestamp) throws IOException
    {
        final List<Table> tables = SnapshotUtil.getTablesToSave(context.getDatabase());
        Runnable completionTask = SnapshotUtil.writeSnapshotDigest(
                txnId,
                context.getCatalogCRC(),
                file_path,
                file_nonce,
                tables,
                context.getHostId(),
                exportSequenceNumbers,
                partitionTransactionIds,
                VoltDB.instance().getHostMessenger().getInstanceId(),
                timestamp);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
        completionTask = SnapshotUtil.writeSnapshotCatalog(file_path, file_nonce);
        if (completionTask != null) {
            SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.offer(completionTask);
        }
    }
}
