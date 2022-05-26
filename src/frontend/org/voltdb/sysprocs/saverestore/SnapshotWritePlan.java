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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DevNullSnapshotTarget;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.PostSnapshotTask;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotStatus.SnapshotResult;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotRegistry;

import com.google_voltpatches.common.collect.Maps;

/**
 * This class was a crude initial effort to tease apart the code in
 * SnapshotSaveAPI.createSetup() into something readable in order to make
 * changes to how snapshot save works.
 *
 * Previously, the snapshot task model was that we would generate a list of
 * SnapshotTableTasks for every site on a node, where each site could
 * potentially run any of the lists (so they would just grab the next list in
 * the queue), and then if we needed any per-partition filtering, a
 * SnapshotDataTarget would filter out all of the data except the partition(s)
 * required.  SnapshotDataTargets could either be specific to a table or to the
 * entire node, but specifying a SnapshotDataTarget per partition was nigh
 * impossible, which makes streaming more than one partition's snapshot data
 * from a node for rejoin (or, in the future, repartitioning or long-running
 * queries) also very difficult.
 *
 * The new snapshot task model is that we create a specific list of
 * SnapshotTableTasks for each node, with foreknowledge of per-partition
 * filtering.  Each site on a node looks up the task list specifically
 * generated for it.  In some cases a site will not have any tasks to perform
 * for a snapshot, unlike the previous model.
 *
 * Subclasses of SnapshotWritePlan are intended to generate the lists of
 * SnapshotTableTasks for each site based on the type of snapshot being
 * performed, and the list of SnapshotDataTargets being used for said snapshot.
 *
 * Hopefully, as we go forward and want to take advantage of the scan that
 * occurs for a snapshot to fulfil multiple different requests for data, we can
 * generate SnapshotWritePlans for each thing we want to do and then do some
 * sane unioning of those plans.
 */
public abstract class SnapshotWritePlan<C extends SnapshotRequestConfig>
{
    static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    static class TargetStatsClosure implements Runnable
    {
        final private List<String> m_tableNames;
        final private SnapshotDataTarget m_sdt;
        final private AtomicInteger m_numTables;
        final private SnapshotRegistry.Snapshot m_snapshotRecord;

        TargetStatsClosure(SnapshotDataTarget sdt, List<String> tableNames,
                AtomicInteger numTables,
                SnapshotRegistry.Snapshot snapshotRecord)
        {
            m_sdt = sdt;
            m_tableNames = tableNames;
            m_numTables = numTables;
            m_snapshotRecord = snapshotRecord;
        }

        @Override
        public void run() {
            for (String tableName : m_tableNames) {
                m_snapshotRecord.updateTable(tableName,
                    new SnapshotRegistry.Snapshot.TableUpdater() {
                        @Override
                        public SnapshotRegistry.Snapshot.Table update(SnapshotRegistry.Snapshot.Table registryTable) {
                            return m_snapshotRecord.new Table(
                                registryTable,
                                m_sdt.getBytesWritten(), /* Bytes written is shared between multiple stream snapshot tables */
                                m_sdt.getLastWriteException(),
                                m_sdt.getSerializationException());
                            }
                    });
                int tablesLeft = m_numTables.decrementAndGet();
                if (tablesLeft == 0) {
                    final SnapshotRegistry.Snapshot completed =
                        SnapshotRegistry.finishSnapshot(m_snapshotRecord);
                    if (completed.result == SnapshotResult.SUCCESS) {
                        final double duration =
                            (completed.timeFinished - completed.timeStarted) / 1000.0;
                        SNAP_LOG.info(
                                "Snapshot " + m_snapshotRecord.nonce + " finished at " +
                                completed.timeFinished + " and took " + duration
                                + " seconds ");
                    } else {
                        SNAP_LOG.info(
                                "Failed to take snapshot " + m_snapshotRecord.nonce);
                    }
                }
            }
        }
    }

    class TargetStatsProgress implements Runnable
    {
        final private SnapshotRegistry.Snapshot m_snapshotRecord;

        TargetStatsProgress(SnapshotRegistry.Snapshot snapshotRecord)
        {
            m_snapshotRecord = snapshotRecord;
        }

        @Override
        public void run() {
            m_snapshotRecord.taskFinished();
        }
    }

    protected final Map<Long, Deque<SnapshotTableTask>> m_taskListsForHSIds =
        new HashMap<Long, Deque<SnapshotTableTask>>();

    protected List<SnapshotDataTarget> m_targets = new ArrayList<SnapshotDataTarget>();
    protected SnapshotRegistry.Snapshot m_snapshotRecord = null;
    protected C m_config;

    /**
     * Given the giant list of inputs, generate the snapshot write plan
     * artifacts.  Will dispatch to a subclass appropriate method call based on
     * the snapshot type.  Returns a callable for deferred setup, null if there
     * is nothing to do for deferred setup.
     * @return returns a Callable
     */
    abstract public Callable<Boolean> createSetup(
            String file_path, String pathType, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            SystemProcedureExecutionContext context,
            final VoltTable result,
            ExtensibleSnapshotDigestData extraSnapshotData,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp);

    public abstract void setConfiguration(SystemProcedureExecutionContext context, JSONObject jsData);

    public C getConfiguration() {
        return m_config;
    }

    /**
     * Get the task lists for each site.  Will only be useful after
     * createSetup() is called.
     */
    public Map<Long, Deque<SnapshotTableTask>> getTaskListsForHSIds()
    {
        return m_taskListsForHSIds;
    }

    /**
     * Get the SnapshotDataTargets used on this site.  Will only be useful after createSetup() is called.
     */
    public List<SnapshotDataTarget> getSnapshotDataTargets()
    {
        return m_targets;
    }

    /**
     * In case the deferred setup phase fails, some data targets may have not been created yet.
     * This method will close all existing data targets and replace all with DevNullDataTargets
     * so that snapshot can be drained.
     */
    public void createAllDevNullTargets(Exception lastWriteException)
    {
        Map<Integer, SnapshotDataTarget> targets = Maps.newHashMap();
        final AtomicInteger numTargets = new AtomicInteger();

        for (Deque<SnapshotTableTask> tasksForSite : m_taskListsForHSIds.values()) {
            for (SnapshotTableTask task : tasksForSite) {
                // Close any created targets and replace them with DevNull, go web-scale
                if (task.getTarget(true) != null) {
                    try {
                        task.getTarget().close();
                    } catch (Exception e) {
                        SNAP_LOG.error("Failed closing data target after error", e);
                    }
                }

                SnapshotDataTarget target = targets.get(task.m_tableInfo.getTableId());
                if (target == null) {
                    target = new DevNullSnapshotTarget(lastWriteException);
                    final Runnable onClose = new TargetStatsClosure(target,
                            Arrays.asList(task.m_tableInfo.getName()),
                            numTargets,
                            m_snapshotRecord);
                    target.setOnCloseHandler(onClose);

                    targets.put(task.m_tableInfo.getTableId(), target);
                    m_targets.add(target);
                    numTargets.incrementAndGet();
                }

                task.setTarget(target);
            }
        }
    }

    protected void placePartitionedTasks(Collection<SnapshotTableTask> tasks, List<Long> hsids)
    {
        SNAP_LOG.debug("Placing partitioned tasks at sites: " + CoreUtils.hsIdCollectionToString(hsids));
        for (SnapshotTableTask task : tasks) {
            placeTask(task, hsids);
        }
    }

    protected void placeTask(SnapshotTableTask task, List<Long> hsids)
    {
        for (Long hsid : hsids) {
            Deque<SnapshotTableTask> tasks = m_taskListsForHSIds.get(hsid);
            if (tasks == null) {
                tasks = new ArrayDeque<SnapshotTableTask>();
                m_taskListsForHSIds.put(hsid, tasks);
            }
            tasks.add(task);
        }
    }

    protected void placeReplicatedTasks(Collection<SnapshotTableTask> tasks, List<Long> hsids)
    {
        SNAP_LOG.debug("Placing replicated tasks at sites: " + CoreUtils.hsIdCollectionToString(hsids));
        int siteIndex = 0;
        // Round-robin the placement of replicated table tasks across the provided HSIds
        for (SnapshotTableTask task : tasks) {
            ArrayList<Long> robin = new ArrayList<Long>();
            robin.add(hsids.get(siteIndex));
            placeTask(task, robin);
            siteIndex = (siteIndex + 1) % hsids.size();
        }
    }

    protected static void createUpdatePartitionCountTasksForSites(SiteTracker tracker,
            SystemProcedureExecutionContext context, int newPartitionCount) {
        PostSnapshotTask task = new UpdatePartitionCount(newPartitionCount);
        List<Integer> localPartitions = tracker.getPartitionsForHost(context.getHostId());
        assert !localPartitions.isEmpty();
        for (Integer partition : localPartitions) {
            SnapshotSiteProcessor.m_siteTasksPostSnapshotting.put(partition, task);
        }
    }

    /**
     * A post-snapshot site task that updates the partition count on each site.
     */
    private static class UpdatePartitionCount implements PostSnapshotTask {
        private final int m_newPartitionCount;

        public UpdatePartitionCount(int newPartitionCount) {
            m_newPartitionCount = newPartitionCount;
        }

        @Override
        public void run(SystemProcedureExecutionContext context) {
            if (SNAP_LOG.isDebugEnabled()) {
                SNAP_LOG.debug("P" + context.getPartitionId() + " updating partition count to: " + m_newPartitionCount);
            }

            // Update partition count stored on this site
            context.setNumberOfPartitions(m_newPartitionCount);
        }
    }
}
