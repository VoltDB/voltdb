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

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CSVSnapshotFilter;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.SimpleFileSnapshotDataTarget;
import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotRegistry;

import com.google_voltpatches.common.primitives.Ints;
import com.google_voltpatches.common.primitives.Longs;

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
public class CSVSnapshotWritePlan extends SnapshotWritePlan<CsvSnapshotRequestConfig>
{
    @Override
    public void setConfiguration(SystemProcedureExecutionContext context, JSONObject jsData) {
        m_config = new CsvSnapshotRequestConfig(jsData, context.getDatabase());
    }

    @Override
    public Callable<Boolean> createSetup(
            String file_path, String pathType, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            SystemProcedureExecutionContext context,
            final VoltTable result,
            ExtensibleSnapshotDigestData extraSnapshotData,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp)
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
            return null;
        }

        final AtomicInteger numTables = new AtomicInteger(m_config.tables.size());
        final SnapshotRegistry.Snapshot snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.CSV,
                    m_config.tables);

        boolean noTargetsCreated = true;

        final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
            new ArrayList<SnapshotTableTask>();
        for (final SnapshotTableInfo table : m_config.tables)
        {
            /*
             * For a deduped csv snapshot, only produce the replicated tables on the "leader"
             * host.
             */
            if (table.isReplicated() && !tracker.isFirstHost()) {
                snapshotRecord.removeTable(table.getName());
                // We'll expect one less table in the global table count
                // in order to be done, too (ENG-4802)
                numTables.decrementAndGet();
                continue;
            }

            List<SnapshotDataFilter> filters = new ArrayList<SnapshotDataFilter>();
            filters.add(new CSVSnapshotFilter(table.getSchema(), ',', null));

            final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table,
                            filters.toArray(new SnapshotDataFilter[filters.size()]),
                            null,
                            false);

            if (table.isReplicated()) {
                replicatedSnapshotTasks.add(task);
            } else {
                partitionedSnapshotTasks.add(task);
            }

            noTargetsCreated = false;
            result.addRow(context.getHostId(),
                    CoreUtils.getHostnameOrAddress(),
                    table.getName(),
                    "SUCCESS",
                    "");
        }

        if (noTargetsCreated) {
            SnapshotRegistry.discardSnapshot(snapshotRecord);
        }

        // CSV snapshots do the partitioned work only on the specified sites for de-duping,
        // but since we've pre-filtered the replicated task list to only contain entries on
        // one node, we can go ahead and distribute them across all of the sites on that node.
        placePartitionedTasks(partitionedSnapshotTasks, sitesToInclude);
        placeReplicatedTasks(replicatedSnapshotTasks, tracker.getSitesForHost(context.getHostId()));
        snapshotRecord.setTotalTasks(partitionedSnapshotTasks.size() * sitesToInclude.size() + replicatedSnapshotTasks.size());

        // All IO work will be deferred and be run on the dedicated snapshot IO thread
        return createDeferredSetup(file_path, pathType, file_nonce, m_config.tables, txnId, partitionTransactionIds,
                context, extraSnapshotData, timestamp, numTables, snapshotRecord,
                partitionedSnapshotTasks, replicatedSnapshotTasks);
    }

    private Callable<Boolean> createDeferredSetup(final String file_path,
                                                  final String pathType,
                                                  final String file_nonce,
                                                  final List<SnapshotTableInfo> tables,
                                                  final long txnId,
                                                  final Map<Integer, Long> partitionTransactionIds,
                                                  final SystemProcedureExecutionContext context,
                                                  final ExtensibleSnapshotDigestData extraSnapshotData,
                                                  final long timestamp,
                                                  final AtomicInteger numTables,
                                                  final SnapshotRegistry.Snapshot snapshotRecord,
                                                  final ArrayList<SnapshotTableTask> partitionedSnapshotTasks,
                                                  final ArrayList<SnapshotTableTask> replicatedSnapshotTasks)
    {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception
            {
                NativeSnapshotWritePlan.createFileBasedCompletionTasks(file_path, pathType, file_nonce,
                        txnId, partitionTransactionIds, context, extraSnapshotData, null, timestamp,
                        context.getNumberOfPartitions(), tables, false);

                for (SnapshotTableTask task : replicatedSnapshotTasks) {
                    final SnapshotDataTarget target = createDataTargetForTable(file_path, file_nonce,
                            context.getHostId(), numTables, snapshotRecord, task.m_tableInfo);
                    task.setTarget(target);
                }

                for (SnapshotTableTask task : partitionedSnapshotTasks) {
                    final SnapshotDataTarget target = createDataTargetForTable(file_path, file_nonce,
                            context.getHostId(), numTables, snapshotRecord, task.m_tableInfo);
                    task.setTarget(target);
                }

                return true;
            }
        };
    }

    private SnapshotDataTarget createDataTargetForTable(String file_path,
                                                        String file_nonce,
                                                        int hostId,
                                                        AtomicInteger numTables,
                                                        SnapshotRegistry.Snapshot snapshotRecord,
                                                        SnapshotTableInfo table)
            throws IOException
    {
        SnapshotDataTarget sdt;
        File saveFilePath = SnapshotUtil.constructFileForTable(
                table,
                file_path,
                file_nonce,
                SnapshotFormat.CSV,
                hostId);

        sdt = new SimpleFileSnapshotDataTarget(saveFilePath, !table.isReplicated());

        m_targets.add(sdt);
        final Runnable onClose = new TargetStatsClosure(sdt, Arrays.asList(table.getName()), numTables, snapshotRecord);
        sdt.setOnCloseHandler(onClose);
        final Runnable inProgress = new TargetStatsProgress(snapshotRecord);
        sdt.setInProgressHandler(inProgress);

        return sdt;
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

        if (sitesToInclude.isEmpty()) {
            SNAP_LOG.info("This host was not selected to write CSV data for any partition");
        }

        return sitesToInclude;
    }
}
