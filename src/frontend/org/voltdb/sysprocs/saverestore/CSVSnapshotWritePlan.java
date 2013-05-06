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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONObject;

import org.voltcore.utils.Pair;

import org.voltdb.catalog.Table;

import org.voltdb.CSVSnapshotFilter;

import org.voltdb.dtxn.SiteTracker;

import org.voltdb.SimpleFileSnapshotDataTarget;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;

import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.SystemProcedureExecutionContext;

import org.voltdb.utils.CatalogUtil;
import org.voltdb.VoltTable;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * Create a snapshot write plan for a CSV snapshot.  This will attempt to write
 * every table only once across the entire cluster.  Replicated tables are only
 * written at the 'first host', which is the lowest host ID currently in the
 * cluster, and at that host the responsibility for writing them is round-robin
 * across all the sites on that node.  Partitioned tables are written by only
 * one of the replicas of each partition, chosen according to a random
 * selection which is seeded such that each node in the cluster will reach the
 * same conclusion about whether or not it is writing a given partition.  Each
 * partitioned table is written to the same target per table by each selected
 * site on a node. */
public class CSVSnapshotWritePlan extends SnapshotWritePlan
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

        /*
         * List of partitions to include if this snapshot is
         * going to be deduped. Attempts to break up the work
         * by seeding and RNG selecting
         * a random replica to do the work. Will not work in failure
         * cases, but we don't use dedupe when we want durability.
         */
        List<Long> sitesToInclude = CSVSnapshotWritePlan.computeDedupedLocalSites(txnId, tracker);
        // If there's no work to do on this host, just claim success and get out:
        if (sitesToInclude.isEmpty() && !tracker.isFirstHost()) {
            return false;
        }

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
                    SnapshotFormat.CSV,
                    tables.toArray(new Table[0]));

        SnapshotDataTarget sdt = null;
        boolean noTargetsCreated = true;

        final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        for (final Table table : tables)
        {
            /*
             * For a deduped csv snapshot, only produce the replicated tables on the "leader"
             * host.
             */
            if (table.getIsreplicated() && !tracker.isFirstHost()) {
                snapshotRecord.removeTable(table.getTypeName());
                continue;
            }

            File saveFilePath = null;
            saveFilePath = SnapshotUtil.constructFileForTable(
                    table,
                    file_path,
                    file_nonce,
                    SnapshotFormat.CSV,
                    context.getHostId());

            try {
                sdt = new SimpleFileSnapshotDataTarget(saveFilePath, !table.getIsreplicated());

                m_targets.add(sdt);
                final Runnable onClose = new TargetStatsClosure(sdt, table.getTypeName(),
                        numTables, snapshotRecord);
                sdt.setOnCloseHandler(onClose);

                List<SnapshotDataFilter> filters = new ArrayList<SnapshotDataFilter>();
                filters.add(new CSVSnapshotFilter(CatalogUtil.getVoltTable(table), ',', null));

                final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table,
                            sdt,
                            filters.toArray(new SnapshotDataFilter[filters.size()]),
                            null,
                            false);

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

        // CSV snapshots do the partitioned work only on the specified sites for de-duping,
        // but since we've pre-filtered the replicated task list to only contain entries on
        // one node, we can go ahead and distribute them across all of the sites on that node.
        placePartitionedTasks(partitionedSnapshotTasks, sitesToInclude);
        placeReplicatedTasks(replicatedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));
        return noTargetsCreated;
    }

    static private List<Long> computeDedupedLocalSites(long txnId, SiteTracker tracker)
    {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }

        /*
         * List of partitions to include if this snapshot is
         * going to be deduped. Attempts to break up the work
         * by seeding and RNG selecting
         * a random replica to do the work. Will not work in failure
         * cases, but we don't use dedupe when we want durability.
         *
         * Originally used the partition id as the seed, but it turns out
         * that nextInt(2) returns a 1 for seeds 0-4095. Now use SHA-1
         * on the txnid + partition id.
         */
        List<Long> sitesToInclude = new ArrayList<Long>();
        for (long localSite : tracker.getLocalSites()) {
            final int partitionId = tracker.getPartitionForSite(localSite);
            List<Long> sites =
                new ArrayList<Long>(tracker.getSitesForPartition(tracker.getPartitionForSite(localSite)));
            Collections.sort(sites);

            digest.update(Longs.toByteArray(txnId));
            final long seed = Longs.fromByteArray(Arrays.copyOf( digest.digest(Ints.toByteArray(partitionId)), 8));

            int siteIndex = new java.util.Random(seed).nextInt(sites.size());
            if (localSite == sites.get(siteIndex)) {
                sitesToInclude.add(localSite);
            }
        }
        return sitesToInclude;
    }
}
