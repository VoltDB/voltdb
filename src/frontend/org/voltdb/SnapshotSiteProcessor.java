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

package org.voltdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NoNodeException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.iv2.SiteTaskerQueue;
import org.voltdb.iv2.SnapshotTask;
import org.voltdb.rejoin.StreamSnapshotDataTarget.StreamSnapshotTimeoutException;
import org.voltdb.sysprocs.saverestore.SnapshotPredicates;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ListMultimap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

/**
 * Encapsulates the state needed to manage an ongoing snapshot at the
 * per-execution site level. Also contains some static global snapshot
 * counters. This class requires callers to maintain thread safety;
 * generally (exclusively?) it is driven by ExecutionSite, each of
 * which has a SnapshotSiteProcessor.
 */
public class SnapshotSiteProcessor {

    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    /** Global count of execution sites on this node performing snapshot */
    public static final Set<Object> ExecutionSitesCurrentlySnapshotting =
            Collections.synchronizedSet(new HashSet<Object>());
    /**
     * Ensure only one thread running the setup fragment does the creation
     * of the targets and the distribution of the work.
     */
    public static final Object m_snapshotCreateLock = new Object();
    public static CyclicBarrier m_snapshotCreateSetupBarrier = null;
    public static CyclicBarrier m_snapshotCreateFinishBarrier = null;
    public static final Runnable m_snapshotCreateSetupBarrierAction = new Runnable() {
        @Override
        public void run() {
            Runnable r = SnapshotSiteProcessor.m_snapshotCreateSetupBarrierActualAction.getAndSet(null);
            if (r != null) {
                r.run();
            }
        }
    };
    public static AtomicReference<Runnable> m_snapshotCreateSetupBarrierActualAction =
            new AtomicReference<Runnable>();

    public static void readySnapshotSetupBarriers(int numSites) {
        synchronized (SnapshotSiteProcessor.m_snapshotCreateLock) {
            if (SnapshotSiteProcessor.m_snapshotCreateSetupBarrier == null) {
                SnapshotSiteProcessor.m_snapshotCreateFinishBarrier = new CyclicBarrier(numSites);
                SnapshotSiteProcessor.m_snapshotCreateSetupBarrier =
                        new CyclicBarrier(numSites, SnapshotSiteProcessor.m_snapshotCreateSetupBarrierAction);
            } else if (SnapshotSiteProcessor.m_snapshotCreateSetupBarrier.isBroken()) {
                SnapshotSiteProcessor.m_snapshotCreateSetupBarrier.reset();
                SnapshotSiteProcessor.m_snapshotCreateFinishBarrier.reset();
            }
        }
    }

    /**
     * Sequence numbers for export tables. This is repopulated before each snapshot by each execution site
     * that reaches the snapshot.
     */
    private static final Map<String, Map<Integer, Pair<Long, Long>>> m_exportSequenceNumbers =
        new HashMap<String, Map<Integer, Pair<Long, Long>>>();

    /**
     * This field is the same values as m_exportSequenceNumbers once they have been extracted
     * in SnapshotSaveAPI.createSetup and then passed back in to SSS.initiateSnapshots. The only
     * odd thing is that setting up a snapshot can fail in which case values will have been populated into
     * m_exportSequenceNumbers and kept until the next snapshot is started in which case they are repopulated.
     * Decoupling them seems like a good idea in case snapshot code is every re-organized.
     */
    private Map<String, Map<Integer, Pair<Long,Long>>> m_exportSequenceNumbersToLogOnCompletion;

    /*
     * Do some random tasks that are deferred to the snapshot termination thread.
     * The two I know about are syncing/closing the digest file and catalog copy
     */
    public static final ConcurrentLinkedQueue<Runnable> m_tasksOnSnapshotCompletion =
        new ConcurrentLinkedQueue<Runnable>();

    /*
     * Random tasks performed on each site after the snapshot tasks are finished but
     * before the snapshot transaction is finished.
     */
    public static final Map<Integer, PostSnapshotTask> m_siteTasksPostSnapshotting =
            Collections.synchronizedMap(new HashMap<Integer, PostSnapshotTask>());

    /**
     * Pick a buffer length that is big enough to store at least one of the largest size tuple supported
     * in the system (2 megabytes). Add a fudge factor for metadata.
     */
    public static final int m_snapshotBufferLength = (1024 * 1024 * 2) + Short.MAX_VALUE;
    public static final int m_snapshotBufferCompressedLen =
        CompressionService.maxCompressedLength(m_snapshotBufferLength);

    /**
     * Limit the number of buffers that are outstanding at any given time
     */
    private static final AtomicInteger m_availableSnapshotBuffers = new AtomicInteger(16);

    /**
     * The last EE out has to shut off the lights. Cache a list
     * of targets in case this EE ends up being the one that needs
     * to close each target.
     */
    private volatile ArrayList<SnapshotDataTarget> m_snapshotTargets = null;

    /**
     * Map of tasks for tables that still need to be snapshotted.
     * Once a table has finished serializing stuff, it's removed from the map.
     * Making it a TreeMap so that it works through one table at time, easier to debug.
     */
    private ListMultimap<Integer, SnapshotTableTask> m_snapshotTableTasks = null;
    private Map<Integer, TableStreamer> m_streamers = null;

    private long m_lastSnapshotTxnId;
    private final int m_snapshotPriority;

    private boolean m_lastSnapshotSucceded = true;

    /**
     * List of threads to join to block on snapshot completion
     * when using completeSnapshotWork().
     */
    private ArrayList<Thread> m_snapshotTargetTerminators = null;

    /**
     * When a buffer is returned to the pool a new snapshot task will be offered to the queue
     * to ensure the EE wakes up and does any potential snapshot work with that buffer
     */
    private final SiteTaskerQueue m_siteTaskerQueue;

    private final Random m_random = new Random();

    /*
     * Interface that will be checked when scheduling snapshot work in IV2.
     * Reports whether the site is "idle" for whatever definition that may be.
     * If the site is idle then work will be scheduled immediately instead of being
     * throttled
     */
    public interface IdlePredicate {
        public boolean idle(long now);
    }

    private final IdlePredicate m_idlePredicate;

    /*
     * Synchronization is handled by SnapshotSaveAPI.startSnapshotting
     * Store the export sequence numbers for every table and partition. This will
     * be called by every execution site before the snapshot starts. Then the execution
     * site that gets the setup permit will  use getExportSequenceNumbers to retrieve the full
     * set and reset the contents.
     */
    public static void populateExportSequenceNumbersForExecutionSite(SystemProcedureExecutionContext context) {
        Database database = context.getDatabase();
        for (Table t : database.getTables()) {
            if (!CatalogUtil.isTableExportOnly(database, t))
                continue;

            Map<Integer, Pair<Long,Long>> sequenceNumbers = m_exportSequenceNumbers.get(t.getTypeName());
            if (sequenceNumbers == null) {
                sequenceNumbers = new HashMap<Integer, Pair<Long, Long>>();
                m_exportSequenceNumbers.put(t.getTypeName(), sequenceNumbers);
            }

            long[] ackOffSetAndSequenceNumber =
                context.getSiteProcedureConnection().getUSOForExportTable(t.getSignature());
            sequenceNumbers.put(
                            context.getPartitionId(),
                            Pair.of(
                                ackOffSetAndSequenceNumber[0],
                                ackOffSetAndSequenceNumber[1]));
        }
    }

    public static Map<String, Map<Integer, Pair<Long, Long>>> getExportSequenceNumbers() {
        HashMap<String, Map<Integer, Pair<Long, Long>>> sequenceNumbers =
            new HashMap<String, Map<Integer, Pair<Long, Long>>>(m_exportSequenceNumbers);
        m_exportSequenceNumbers.clear();
        return sequenceNumbers;
    }

    private long m_quietUntil = 0;

    public SnapshotSiteProcessor(SiteTaskerQueue siteQueue, int snapshotPriority) {
        this(siteQueue, snapshotPriority, new IdlePredicate() {
            @Override
            public boolean idle(long now) {
                throw new UnsupportedOperationException();
            }
        });
    }

    public SnapshotSiteProcessor(SiteTaskerQueue siteQueue, int snapshotPriority, IdlePredicate idlePredicate) {
        m_siteTaskerQueue = siteQueue;
        m_snapshotPriority = snapshotPriority;
        m_idlePredicate = idlePredicate;
    }

    public void shutdown() throws InterruptedException {
        m_snapshotCreateSetupBarrier = null;
        m_snapshotCreateFinishBarrier = null;
        if (m_snapshotTargetTerminators != null) {
            for (Thread t : m_snapshotTargetTerminators) {
                t.join();
            }
        }
    }


    public static boolean isSnapshotInProgress()
    {
        final int numSitesSnapshotting = SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.size();
        if (numSitesSnapshotting > 0) {
            if (SNAP_LOG.isDebugEnabled()) {
                SNAP_LOG.debug("Snapshot in progress, " + numSitesSnapshotting + " sites are still snapshotting");
            }
            return true;
        }
        return false;
    }

    private BBContainer createNewBuffer(final BBContainer origin, final boolean noSchedule)
    {
        return new BBContainer(origin.b()) {
            @Override
            public void discard() {
                checkDoubleFree();
                origin.discard();
                m_availableSnapshotBuffers.incrementAndGet();

                if (!noSchedule) {
                    rescheduleSnapshotWork();
                }
            }
        };
    }

    private void rescheduleSnapshotWork() {
        /*
         * If IV2 is enabled, don't run the potential snapshot work jigger
         * until the quiet period restrictions have been met. In IV2 doSnapshotWork
         * is always called with ignoreQuietPeriod and the scheduling is instead done
         * via the STPE in RealVoltDB.
         *
         * The goal of the quiet period is to spread snapshot work out over time and minimize
         * the impact on latency
         *
         * If snapshot priority is 0 then running the jigger immediately is the specified
         * policy anyways. 10 would be the largest delay
         */
        if (m_snapshotPriority > 0) {
            final long now = System.currentTimeMillis();
            //Ask if the site is idle, and if it is queue the work immediately
            if (m_idlePredicate.idle(now)) {
                m_siteTaskerQueue.offer(new SnapshotTask());
                return;
            }

            //Cache the value locally, the dirty secret is that in edge cases multiple threads
            //will read/write briefly, but it isn't a big deal since the scheduling can be wrong
            //briefly. Caching it locally will make the logic here saner because it can't change
            //as execution progresses
            final long quietUntil = m_quietUntil;

                    /*
                     * If the current time is > than quietUntil then the quiet period is over
                     * and the snapshot work should be done immediately
                     *
                     * Otherwise it needs to be scheduled in the future and the next quiet period
                     * needs to be calculated
                     */
            if (now > quietUntil) {
                m_siteTaskerQueue.offer(new SnapshotTask());
                //Now push the quiet period further into the future,
                //generally no threads will be racing to do this
                //since the execution site only interacts with one snapshot data target at a time
                //except when it is switching tables. It doesn't really matter if it is wrong
                //it will just result in a little extra snapshot work being done close together
                m_quietUntil =
                        System.currentTimeMillis() +
                                (5 * m_snapshotPriority) + ((long)(m_random.nextDouble() * 15));
            } else {
                //Schedule it to happen after the quiet period has elapsed
                VoltDB.instance().schedulePriorityWork(
                        new Runnable() {
                            @Override
                            public void run()
                            {
                                m_siteTaskerQueue.offer(new SnapshotTask());
                            }
                        },
                        quietUntil - now,
                        0,
                        TimeUnit.MILLISECONDS);

                        /*
                         * This is the same calculation as above except the future is not based
                         * on the current time since the quiet period was already in the future
                         * and we need to move further past it since we just scheduled snapshot work
                         * at the end of the current quietUntil value
                         */
                m_quietUntil =
                        quietUntil +
                                (5 * m_snapshotPriority) + ((long)(m_random.nextDouble() * 15));
            }
        } else {
            m_siteTaskerQueue.offer(new SnapshotTask());
        }
    }

    public void initiateSnapshots(
            SystemProcedureExecutionContext context,
            SnapshotFormat format,
            Deque<SnapshotTableTask> tasks,
            long txnId,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers)
    {
        ExecutionSitesCurrentlySnapshotting.add(this);
        final long now = System.currentTimeMillis();
        m_quietUntil = now + 200;
        m_lastSnapshotSucceded = true;
        m_lastSnapshotTxnId = txnId;
        m_snapshotTableTasks = MiscUtils.sortedArrayListMultimap();
        m_streamers = Maps.newHashMap();
        m_snapshotTargetTerminators = new ArrayList<Thread>();
        m_exportSequenceNumbersToLogOnCompletion = exportSequenceNumbers;

        // Table doesn't implement hashCode(), so use the table ID as key
        for (Map.Entry<Integer, byte[]> tablePredicates : makeTablesAndPredicatesToSnapshot(tasks).entrySet()) {
            int tableId = tablePredicates.getKey();
            TableStreamer streamer =
                new TableStreamer(tableId, format.getStreamType(), m_snapshotTableTasks.get(tableId));
            if (!streamer.activate(context, tablePredicates.getValue())) {
                VoltDB.crashLocalVoltDB("Failed to activate snapshot stream on table " +
                                        CatalogUtil.getTableNameFromId(context.getDatabase(), tableId), false, null);
            }
            m_streamers.put(tableId, streamer);
        }

        /*
         * Resize the buffer pool to contain enough buffers for the number of tasks. The buffer
         * pool will be cleaned up at the end of the snapshot.
         *
         * For the general case of only one snapshot at a time, this will have the same behavior
         * as before, 5 buffers per snapshot.
         *
         * TODO: This is not a good algorithm for general snapshot coalescing. Rate limiting
         * won't work as expected with this approach. For general snapshot coalescing,
         * a better approach like pool per output target should be used.
         */
        int maxTableTaskSize = 0;
        for (Collection<SnapshotTableTask> perTableTasks : m_snapshotTableTasks.asMap().values()) {
            maxTableTaskSize = Math.max(maxTableTaskSize, perTableTasks.size());
        }
    }

    /**
     * This is called from the snapshot IO thread when the deferred setup is finished. It sets
     * the data targets and queues a snapshot task onto the site thread.
     */
    public void startSnapshotWithTargets(Collection<SnapshotDataTarget> targets, long now)
    {
        //Basically asserts that there are no tasks with null targets at this point
        //getTarget checks and crashes
        for (SnapshotTableTask t : m_snapshotTableTasks.values()) {
            t.getTarget();
        }

        ArrayList<SnapshotDataTarget> targetsToClose = Lists.newArrayList();
        for (final SnapshotDataTarget target : targets) {
            if (target.needsFinalClose()) {
                targetsToClose.add(target);
            }
        }
        m_snapshotTargets = targetsToClose;

        // Queue the first snapshot task
        VoltDB.instance().schedulePriorityWork(
                new Runnable() {
                    @Override
                    public void run()
                    {
                        m_siteTaskerQueue.offer(new SnapshotTask());
                    }
                },
                (m_quietUntil + (5 * m_snapshotPriority) - now),
                0,
                TimeUnit.MILLISECONDS);
        m_quietUntil += 5 * m_snapshotPriority;
    }

    private Map<Integer, byte[]>
    makeTablesAndPredicatesToSnapshot(Collection<SnapshotTableTask> tasks) {
        Map<Integer, SnapshotPredicates> tablesAndPredicates = Maps.newHashMap();
        Map<Integer, byte[]> predicateBytes = Maps.newHashMap();

        for (SnapshotTableTask task : tasks) {
            SNAP_LOG.debug("Examining SnapshotTableTask: " + task);

            // Add the task to the task list for the given table
            m_snapshotTableTasks.put(task.m_table.getRelativeIndex(), task);

            // Make sure there is a predicate object for each table, the predicate could contain
            // empty expressions. So activateTableStream() doesn't have to do a null check.
            SnapshotPredicates predicates = tablesAndPredicates.get(task.m_table.getRelativeIndex());
            if (predicates == null) {
                predicates = new SnapshotPredicates(task.m_table.getRelativeIndex());
                tablesAndPredicates.put(task.m_table.getRelativeIndex(), predicates);
            }

            predicates.addPredicate(task.m_predicate, task.m_deleteTuples);
        }

        for (Map.Entry<Integer, SnapshotPredicates> e : tablesAndPredicates.entrySet()) {
            predicateBytes.put(e.getKey(), e.getValue().toBytes());
        }

        return predicateBytes;
    }

    /**
     * Create an output buffer for each task.
     * @return null if there aren't enough buffers left in the pool.
     */
    private List<BBContainer> getOutputBuffers(Collection<SnapshotTableTask> tableTasks, boolean noSchedule)
    {
        final int desired = tableTasks.size();
        while (true) {
            int available = m_availableSnapshotBuffers.get();

            //Limit the number of buffers used concurrently
            if (desired > available) {
                return null;
            }
            if (m_availableSnapshotBuffers.compareAndSet(available, available - desired)) break;
        }

        List<BBContainer> outputBuffers = new ArrayList<BBContainer>(tableTasks.size());

        for (int ii = 0; ii < tableTasks.size(); ii++) {
            final BBContainer origin = DBBPool.allocateDirectAndPool(m_snapshotBufferLength);
            outputBuffers.add(createNewBuffer(origin, noSchedule));
        }

        return outputBuffers;
    }

    private void asyncTerminateReplicatedTableTasks(Collection<SnapshotTableTask> tableTasks)
    {
        for (final SnapshotTableTask tableTask : tableTasks) {
            /**
             * Replicated tables are assigned to a single ES on each site and that ES
             * is responsible for closing the data target. Done in a separate
             * thread so the EE can continue working.
             */
            if (tableTask.m_table.getIsreplicated() &&
                tableTask.m_target.getFormat().canCloseEarly()) {
                final Thread terminatorThread =
                    new Thread("Replicated SnapshotDataTarget terminator ") {
                        @Override
                        public void run() {
                            try {
                                tableTask.m_target.close();
                            } catch (IOException e) {
                                m_lastSnapshotSucceded = false;
                                throw new RuntimeException(e);
                            } catch (InterruptedException e) {
                                m_lastSnapshotSucceded = false;
                                throw new RuntimeException(e);
                            }

                        }
                    };
                m_snapshotTargetTerminators.add(terminatorThread);
                terminatorThread.start();
            }
        }
    }

    /*
     * No schedule means don't try and schedule snapshot work because this is a blocking
     * task from completeSnapshotWork. This avoids creating thousands of task objects.
     */
    public Future<?> doSnapshotWork(SystemProcedureExecutionContext context, boolean noSchedule) {
        ListenableFuture<?> retval = null;

        /*
         * This thread will null out the reference to m_snapshotTableTasks when
         * a snapshot is finished. If the snapshot buffer is loaned out that means
         * it is pending I/O somewhere so there is no work to do until it comes back.
         */
        if (m_snapshotTableTasks == null) {
            return retval;
        }

        if (m_snapshotTargets == null) {
            return null;
        }

        /*
         * Try to serialize a block from a table, if the table is finished,
         * remove the tasks from the task map and move on to the next table. If a block is
         * successfully serialized, break out of the loop and release the site thread for more
         * transaction work.
         */
        Iterator<Map.Entry<Integer, Collection<SnapshotTableTask>>> taskIter =
            m_snapshotTableTasks.asMap().entrySet().iterator();
        while (taskIter.hasNext()) {
            Map.Entry<Integer, Collection<SnapshotTableTask>> taskEntry = taskIter.next();
            final int tableId = taskEntry.getKey();
            final Collection<SnapshotTableTask> tableTasks = taskEntry.getValue();

            final List<BBContainer> outputBuffers = getOutputBuffers(tableTasks, noSchedule);
            if (outputBuffers == null) {
                // Not enough buffers available
                if (!noSchedule) {
                    rescheduleSnapshotWork();
                }
                break;
            }

            // Stream more and add a listener to handle any failures
            Pair<ListenableFuture, Boolean> streamResult =
                    m_streamers.get(tableId).streamMore(context, outputBuffers, null);
            if (streamResult.getFirst() != null) {
                final ListenableFuture writeFutures = streamResult.getFirst();
                writeFutures.addListener(new Runnable() {
                    @Override
                    public void run()
                    {
                        try {
                            writeFutures.get();
                        } catch (Throwable t) {
                            if (m_lastSnapshotSucceded) {
                                if (t instanceof StreamSnapshotTimeoutException ||
                                        t.getCause() instanceof StreamSnapshotTimeoutException) {
                                    //This error is already logged by the watchdog when it generates the exception
                                } else {
                                    SNAP_LOG.error("Error while attempting to write snapshot data", t);
                                }
                                m_lastSnapshotSucceded = false;
                            }
                        }
                    }
                }, CoreUtils.SAMETHREADEXECUTOR);
            }

            /**
             * The table streamer will return false when there is no more data left to pull from that table. The
             * enclosing loop ensures that the next table is then addressed.
             */
            if (!streamResult.getSecond()) {
                asyncTerminateReplicatedTableTasks(tableTasks);
                // XXX: Guava's multimap will clear the tableTasks collection when the entry is
                // removed from the containing map, so don't use the collection after removal!
                taskIter.remove();
                SNAP_LOG.debug("Finished snapshot tasks for table " + tableId +
                               ": " + tableTasks);
            } else {
                break;
            }
        }

        /**
         * If there are no more tasks then this particular EE is finished doing snapshot work
         * Check the AtomicInteger to find out if this is the last one.
         */
        if (m_snapshotTableTasks.isEmpty()) {
            SNAP_LOG.debug("Finished with tasks");
            // In case this is a non-blocking snapshot, do the post-snapshot tasks here.
            runPostSnapshotTasks(context);
            final ArrayList<SnapshotDataTarget> snapshotTargets = m_snapshotTargets;
            m_snapshotTargets = null;
            m_snapshotTableTasks = null;
            boolean IamLast = false;
            synchronized (ExecutionSitesCurrentlySnapshotting) {
                if (!ExecutionSitesCurrentlySnapshotting.contains(this)) {
                    VoltDB.crashLocalVoltDB(
                            "Currently snapshotting site didn't find itself in set of snapshotting sites", true, null);
                }
                IamLast = ExecutionSitesCurrentlySnapshotting.size() == 1;
                if (!IamLast) {
                    ExecutionSitesCurrentlySnapshotting.remove(this);
                }
            }

            /**
             * If this is the last one then this EE must close all the SnapshotDataTargets.
             * Done in a separate thread so the EE can go and do other work. It will
             * sync every file descriptor and that may block for a while.
             */
            if (IamLast) {
                SNAP_LOG.debug("I AM LAST!");
                final long txnId = m_lastSnapshotTxnId;
                final Map<String, Map<Integer, Pair<Long,Long>>> exportSequenceNumbers =
                        m_exportSequenceNumbersToLogOnCompletion;
                m_exportSequenceNumbersToLogOnCompletion = null;
                final Thread terminatorThread =
                    new Thread("Snapshot terminator") {
                    @Override
                    public void run() {
                        try {
                            /*
                             * Be absolutely sure the snapshot is finished
                             * and synced to disk before another is started
                             */
                            for (Thread t : m_snapshotTargetTerminators){
                                if (t == this) {
                                    continue;
                                }
                                try {
                                    t.join();
                                } catch (InterruptedException e) {
                                    return;
                                }
                            }
                            for (final SnapshotDataTarget t : snapshotTargets) {
                                try {
                                    t.close();
                                } catch (IOException e) {
                                    m_lastSnapshotSucceded = false;
                                    throw new RuntimeException(e);
                                } catch (InterruptedException e) {
                                    m_lastSnapshotSucceded = false;
                                    throw new RuntimeException(e);
                                }
                            }

                            Runnable r = null;
                            while ((r = m_tasksOnSnapshotCompletion.poll()) != null) {
                                try {
                                    r.run();
                                } catch (Exception e) {
                                    SNAP_LOG.error("Error running snapshot completion task", e);
                                }
                            }
                        } finally {
                            // Caching the value here before the site removes itself from the
                            // ExecutionSitesCurrentlySnapshotting set, so
                            // logSnapshotCompletionToZK() will not see incorrect values
                            // from the next snapshot
                            final boolean snapshotSucceeded = m_lastSnapshotSucceded;

                            try {
                                VoltDB.instance().getHostMessenger().getZK().delete(
                                        VoltZK.nodes_currently_snapshotting + "/" + VoltDB.instance().getHostMessenger().getHostId(), -1);
                            } catch (NoNodeException e) {
                                SNAP_LOG.warn("Expect the snapshot node to already exist during deletion", e);
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                            } finally {
                                /**
                                 * Remove this last site from the set here after the terminator has run
                                 * so that new snapshots won't start until
                                 * everything is on disk for the previous snapshot. This prevents a really long
                                 * snapshot initiation procedure from occurring because it has to contend for
                                 * filesystem resources
                                 *
                                 * Do this before logSnapshotCompleteToZK() because the ZK operations are slow,
                                 * and they can trigger snapshot completion interests to fire before this site
                                 * removes itself from the set. The next snapshot request may come in and see
                                 * this snapshot is still in progress.
                                 */
                                ExecutionSitesCurrentlySnapshotting.remove(SnapshotSiteProcessor.this);
                            }

                            logSnapshotCompleteToZK(txnId, snapshotSucceeded, exportSequenceNumbers);
                        }
                    }
                };

                m_snapshotTargetTerminators.add(terminatorThread);
                terminatorThread.start();
            }
        }
        return retval;
    }

    public static void runPostSnapshotTasks(SystemProcedureExecutionContext context)
    {
        SNAP_LOG.debug("Running post-snapshot tasks");
        PostSnapshotTask postSnapshotTask = m_siteTasksPostSnapshotting.remove(context.getPartitionId());
        if (postSnapshotTask != null) {
            postSnapshotTask.run(context);
        }
    }

    private static void logSnapshotCompleteToZK(
            long txnId,
            boolean snapshotSuccess,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();

        // Timeout after 10 minutes
        final long endTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        final String snapshotPath = VoltZK.completed_snapshots + "/" + txnId;
        boolean success = false;
        while (!success) {
            if (System.currentTimeMillis() > endTime) {
                VoltDB.crashLocalVoltDB("Timed out logging snapshot completion to ZK");
            }

            Stat stat = new Stat();
            byte data[] = null;
            try {
                data = zk.getData(snapshotPath, false, stat);
            } catch (NoNodeException e) {
                // The MPI creates the snapshot completion node asynchronously,
                // if the node doesn't exist yet, retry
                continue;
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("This ZK get should never fail", true, e);
            }
            if (data == null) {
                VoltDB.crashLocalVoltDB("Data should not be null if the node exists", false, null);
            }

            try {
                JSONObject jsonObj = new JSONObject(new String(data, "UTF-8"));
                if (jsonObj.getLong("txnId") != txnId) {
                    VoltDB.crashLocalVoltDB("TxnId should match", false, null);
                }
                int remainingHosts = jsonObj.getInt("hostCount") - 1;
                jsonObj.put("hostCount", remainingHosts);
                jsonObj.put("didSucceed", snapshotSuccess);
                if (!snapshotSuccess) {
                    jsonObj.put("isTruncation", false);
                }
                mergeExportSequenceNumbers(jsonObj, exportSequenceNumbers);
                zk.setData(snapshotPath, jsonObj.toString(4).getBytes("UTF-8"), stat.getVersion());
            } catch (KeeperException.BadVersionException e) {
                continue;
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("This ZK call should never fail", true, e);
            }
            success = true;
        }

        /*
         * If we are running without command logging there will be no consumer for
         * the completed snapshot messages. Consume them here to bound space usage in ZK.
         */
        try {
            TreeSet<String> snapshots = new TreeSet<String>(zk.getChildren(VoltZK.completed_snapshots, false));
            while (snapshots.size() > 30) {
                try {
                    zk.delete(VoltZK.completed_snapshots + "/" + snapshots.first(), -1);
                } catch (NoNodeException e) {}
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB(
                            "Deleting a snapshot completion record from ZK should only fail with NoNodeException", true, e);
                }
                snapshots.remove(snapshots.first());
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Retrieving list of completed snapshots from ZK should never fail", true, e);
        }
    }

    /*
     * When recording snapshot completion we also record export sequence numbers
     * as JSON. Need to merge our sequence numbers with existing numbers
     * since multiple replicas will submit the sequence number
     */
    private static void mergeExportSequenceNumbers(JSONObject jsonObj,
            Map<String, Map<Integer, Pair<Long,Long>>> exportSequenceNumbers) throws JSONException {
        JSONObject tableSequenceMap;
        if (jsonObj.has("exportSequenceNumbers")) {
            tableSequenceMap = jsonObj.getJSONObject("exportSequenceNumbers");
        } else {
            tableSequenceMap = new JSONObject();
            jsonObj.put("exportSequenceNumbers", tableSequenceMap);
        }

        for (Map.Entry<String, Map<Integer, Pair<Long, Long>>> tableEntry : exportSequenceNumbers.entrySet()) {
            JSONObject sequenceNumbers;
            final String tableName = tableEntry.getKey();
            if (tableSequenceMap.has(tableName)) {
                sequenceNumbers = tableSequenceMap.getJSONObject(tableName);
            } else {
                sequenceNumbers = new JSONObject();
                tableSequenceMap.put(tableName, sequenceNumbers);
            }

            for (Map.Entry<Integer, Pair<Long, Long>> partitionEntry : tableEntry.getValue().entrySet()) {
                final Integer partitionId = partitionEntry.getKey();
                final String partitionIdString = partitionId.toString();
                final Long ackOffset = partitionEntry.getValue().getFirst();
                final Long partitionSequenceNumber = partitionEntry.getValue().getSecond();

                /*
                 * Check that the sequence number is the same everywhere and log if it isn't.
                 * Not going to crash because we are worried about poison pill transactions.
                 */
                if (sequenceNumbers.has(partitionIdString)) {
                    JSONObject existingEntry = sequenceNumbers.getJSONObject(partitionIdString);
                    Long existingSequenceNumber = existingEntry.getLong("sequenceNumber");
                    if (!existingSequenceNumber.equals(partitionSequenceNumber)) {
                        SNAP_LOG.error("Found a mismatch in export sequence numbers while recording snapshot metadata " +
                                " for partition " + partitionId +
                                " the sequence number should be the same at all replicas, but one had " +
                                existingSequenceNumber
                                + " and another had " + partitionSequenceNumber);
                    }
                    existingEntry.put(partitionIdString, Math.max(existingSequenceNumber, partitionSequenceNumber));

                    Long existingAckOffset = existingEntry.getLong("ackOffset");
                    existingEntry.put("ackOffset", Math.max(ackOffset, existingAckOffset));
                } else {
                    JSONObject newObj = new JSONObject();
                    newObj.put("sequenceNumber", partitionSequenceNumber);
                    newObj.put("ackOffset", ackOffset);
                    sequenceNumbers.put(partitionIdString, newObj);
                }
            }
        }
    }

    /**
     * Is the EE associated with this SnapshotSiteProcessor currently
     * snapshotting?
     *
     * No thread safety here, but assuming single-threaded access from
     * the IV2 site.
     */
    public boolean isEESnapshotting() {
        return m_snapshotTableTasks != null;
    }

    /**
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the fsync() and close() of snapshot data targets has completed.
     */
    public HashSet<Exception> completeSnapshotWork(SystemProcedureExecutionContext context)
        throws InterruptedException {
        HashSet<Exception> retval = new HashSet<Exception>();
        //Set to 10 gigabytes/sec, basically unlimited
        //Does nothing if rate limiting is not enabled
        DefaultSnapshotDataTarget.setRate(1024 * 10);
        try {
            while (m_snapshotTableTasks != null) {
                Future<?> result = doSnapshotWork(context, true);
                if (result != null) {
                    try {
                        result.get();
                    } catch (ExecutionException e) {
                        final boolean added = retval.add((Exception)e.getCause());
                        assert(added);
                    } catch (Exception e) {
                        final boolean added = retval.add((Exception)e.getCause());
                        assert(added);
                    }
                }
            }
        } finally {
            //Request default rate again
            DefaultSnapshotDataTarget.setRate(null);
        }

        /**
         * Block until the sync has actually occurred in the forked threads.
         * The threads are spawned even in the blocking case to keep it simple.
         */
        if (m_snapshotTargetTerminators != null) {
            for (final Thread t : m_snapshotTargetTerminators) {
                t.join();
            }
            m_snapshotTargetTerminators = null;
        }

        return retval;
    }
}
