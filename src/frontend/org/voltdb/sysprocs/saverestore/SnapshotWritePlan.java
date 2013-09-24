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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotRegistry;

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
public abstract class SnapshotWritePlan
{
    static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    class TargetStatsClosure implements Runnable
    {
        final private String m_tableName;
        final private SnapshotDataTarget m_sdt;
        final private AtomicInteger m_numTables;
        final private SnapshotRegistry.Snapshot m_snapshotRecord;

        TargetStatsClosure(SnapshotDataTarget sdt, String tableName,
                AtomicInteger numTables,
                SnapshotRegistry.Snapshot snapshotRecord)
        {
            m_sdt = sdt;
            m_tableName = tableName;
            m_numTables = numTables;
            m_snapshotRecord = snapshotRecord;
        }

        @Override
        public void run() {
            m_snapshotRecord.updateTable(m_tableName,
                    new SnapshotRegistry.Snapshot.TableUpdater() {
                        @Override
                        public SnapshotRegistry.Snapshot.Table update(
                            SnapshotRegistry.Snapshot.Table registryTable) {
                            return m_snapshotRecord.new Table(
                                registryTable,
                                m_sdt.getBytesWritten(),
                                m_sdt.getLastWriteException());
                            }
                    });
            int tablesLeft = m_numTables.decrementAndGet();
            if (tablesLeft == 0) {
                final SnapshotRegistry.Snapshot completed =
                    SnapshotRegistry.finishSnapshot(m_snapshotRecord);
                final double duration =
                    (completed.timeFinished - completed.timeStarted) / 1000.0;
                SNAP_LOG.info(
                        "Snapshot " + m_snapshotRecord.nonce + " finished at " +
                        completed.timeFinished + " and took " + duration
                        + " seconds ");
            }
        }
    }

    protected final Map<Long, Deque<SnapshotTableTask>> m_taskListsForHSIds =
        new HashMap<Long, Deque<SnapshotTableTask>>();

    protected List<SnapshotDataTarget> m_targets = new ArrayList<SnapshotDataTarget>();

    /**
     * Given the giant list of inputs, generate the snapshot write plan
     * artifacts.  Will dispatch to a subclass appropriate method call based on
     * the snapshot type.  Returns true if the setup aborted, false if it was
     * successful (yes, unfortunately, but this was the polarity of things
     * before I refactored them and I was too lazy to flip it).
     */
    public boolean createSetup(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp)
    {
        try {
            boolean aborted = createSetupInternal(file_path, file_nonce,
                    txnId, partitionTransactionIds, jsData, context,
                    hostname, result, exportSequenceNumbers,
                    tracker, hashinatorData, timestamp);
            return aborted;
        }
        catch (Exception ex) {
            /*
             * Close all the targets to release the threads. Don't let sites get any tasks.
             */
            m_taskListsForHSIds.clear();
            for (SnapshotDataTarget sdt : m_targets) {
                try {
                    sdt.close();
                } catch (Exception e) {
                    SNAP_LOG.error("Failed to create snapshot write plan: " + ex.getMessage(), ex);
                    SNAP_LOG.error("Failed closing data target after error: " + e.getMessage(), e);
                }
            }

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            pw.flush();
            result.addRow(
                    context.getHostId(),
                    hostname,
                    "",
                    "FAILURE",
                    "SNAPSHOT INITIATION OF " + file_path + file_nonce +
                    "RESULTED IN Exception: \n" + sw.toString());
            SNAP_LOG.error("Failed to create snapshot write plan: " + ex.getMessage(), ex);
            return true;
        }
    }

    /**
     * Overridden by subclasses.  Do the appropriate work for this type of snapshot to create the plan artifacts.
     * Returns true on abort, false on success.
     *
     * Abort means that none of the snapshot data targets that should have been
     * created was successfully created.  Since we attempt to support partial
     * snapshots (meaning that if we, for some reason, can't write one table,
     * we'll still try to write every other table), a single failure won't cause us to abort.
     */
    abstract protected boolean createSetupInternal(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker,
            HashinatorSnapshotData hashinatorData,
            long timestamp) throws IOException;

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
            siteIndex = siteIndex++ % hsids.size();
        }
    }

    protected void handleTargetCreationError(SnapshotDataTarget sdt,
            SystemProcedureExecutionContext context, String file_nonce, String hostname,
            String tableName, Exception ex, final VoltTable result)
    {
        SNAP_LOG.warn("Failure creating snapshot target", ex);
        /*
         * Creation of this specific target failed. Close it if it was created.
         * Continue attempting the snapshot anyways so that at least some of the data
         * can be retrieved.
         */
        try {
            if (sdt != null) {
                m_targets.remove(sdt);
                sdt.close();
            }
        } catch (Exception e) {
            SNAP_LOG.error("Failed to close snapshot data target after error: " + e.getMessage(), e);
        }

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        pw.flush();
        result.addRow(context.getHostId(),
                hostname,
                tableName,
                "FAILURE",
                "SNAPSHOT INITIATION OF " + file_nonce +
                "RESULTED IN IOException: \n" + sw.toString());
    }

}
