/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NoNodeException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.CatalogUtil;

import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Encapsulates the state needed to manage an ongoing snapshot at the
 * per-execution site level. Also contains some static global snapshot
 * counters. This class requires callers to maintain thread safety;
 * generally (exclusively?) it is driven by ExecutionSite, each of
 * which has a SnapshotSiteProcessor.
 */
public class SnapshotSiteProcessor {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /** Global count of execution sites on this node performing snapshot */
    public static final AtomicInteger ExecutionSitesCurrentlySnapshotting =
        new AtomicInteger(-1);

    /**
     * Ensure only one thread running the setup fragment does the creation
     * of the targets and the distribution of the work.
     */
    public static final Object m_snapshotCreateLock = new Object();
    public static Semaphore m_snapshotCreateSetupPermit = null;

    //Protected by SnapshotSiteProcessor.m_snapshotCreateLock when accessed from SnapshotSaveAPI.startSnanpshotting
    public static ArrayList<Long> m_partitionLastSeenTransactionIds =
            new ArrayList<Long>();

    /**
     * Only proceed once permits are available after setup completes
     */
    public static Semaphore m_snapshotPermits = new Semaphore(0);

    /**
     * Global collection populated by snapshot creator, poll'd by individual sites
     */
    public static final LinkedList<Deque<SnapshotTableTask>> m_taskListsForSites =
        new LinkedList<Deque<SnapshotTableTask>>();

    /**
     * Sequence numbers for export tables. This is repopulated before each snapshot by each execution site
     * that reaches the snapshot.
     */
    private static final Map<String, List<Pair<Integer, Long>>> m_exportSequenceNumbers =
        new HashMap<String, List<Pair<Integer, Long>>>();

    /*
     * Do some random tasks that are deferred to the snapshot termination thread.
     * The two I know about are syncing/closing the digest file and catalog copy
     */
    public static final ConcurrentLinkedQueue<Runnable> m_tasksOnSnapshotCompletion =
        new ConcurrentLinkedQueue<Runnable>();


    /** Number of snapshot buffers to keep */
    static final int m_numSnapshotBuffers = 5;

    /**
     * Pick a buffer length that is big enough to store at least one of the largest size tuple supported
     * in the system (2 megabytes). Add a fudge factor for metadata.
     */
    public static final int m_snapshotBufferLength = (1024 * 1024 * 2) + Short.MAX_VALUE;
    private final ArrayList<BBContainer> m_snapshotBufferOrigins =
        new ArrayList<BBContainer>();
    /**
     * Set to true when the buffer is sent to a SnapshotDataTarget for I/O
     * and back to false when the container is discarded.
     * A volatile allows the EE to check for the buffer without
     * synchronization when the snapshot is done online.
     */
    private final ConcurrentLinkedQueue<BBContainer> m_availableSnapshotBuffers
        = new ConcurrentLinkedQueue<BBContainer>();

    /**
     * The last EE out has to shut off the lights. Cache a list
     * of targets in case this EE ends up being the one that needs
     * to close each target.
     */
    private ArrayList<SnapshotDataTarget> m_snapshotTargets = null;

    /**
     * Queue of tasks for tables that still need to be snapshotted.
     * This is polled from until there are no more tasks.
     */
    private ArrayDeque<SnapshotTableTask> m_snapshotTableTasks = null;

    private long m_lastSnapshotTxnId;
    private int m_lastSnapshotNumHosts;
    private final int m_snapshotPriority;

    private boolean m_lastSnapshotSucceded = true;

    /**
     * List of threads to join to block on snapshot completion
     * when using completeSnapshotWork().
     */
    private ArrayList<Thread> m_snapshotTargetTerminators = null;

    /**
     * When a buffer is returned to the pool this is invoked to ensure the EE wakes up
     * and does any potential snapshot work with that buffer
     */
    private final Runnable m_onPotentialSnapshotWork;

    private final boolean m_isIV2Enabled = VoltDB.instance().isIV2Enabled();

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

            List<Pair<Integer, Long>> sequenceNumbers = m_exportSequenceNumbers.get(t.getTypeName());
            if (sequenceNumbers == null) {
                sequenceNumbers = new ArrayList<Pair<Integer,Long>>();
                m_exportSequenceNumbers.put(t.getTypeName(), sequenceNumbers);
            }

            long[] ackOffSetAndSequenceNumber =
                context.getSiteProcedureConnection().getUSOForExportTable(t.getSignature());
            sequenceNumbers.add(
                    Pair.of(
                            context.getPartitionId(),
                            ackOffSetAndSequenceNumber[1]));
        }
    }

    public static Map<String, List<Pair<Integer, Long>>> getExportSequenceNumbers() {
        HashMap<String, List<Pair<Integer,Long>>> sequenceNumbers =
            new HashMap<String, List<Pair<Integer, Long>>>(m_exportSequenceNumbers);
        m_exportSequenceNumbers.clear();
        return sequenceNumbers;
    }

    private long m_quietUntil = 0;

    private boolean inQuietPeriod() {
        if (m_isIV2Enabled) {
            return false;
        } else {
            return org.voltcore.utils.EstTime.currentTimeMillis() < m_quietUntil;
        }
    }

    public SnapshotSiteProcessor(Runnable onPotentialSnapshotWork, int snapshotPriority) {
        this(onPotentialSnapshotWork, snapshotPriority, new IdlePredicate() {
            @Override
            public boolean idle(long now) {
                throw new UnsupportedOperationException();
            }
        });
    }

    public SnapshotSiteProcessor(Runnable onPotentialSnapshotWork, int snapshotPriority, IdlePredicate idlePredicate) {
        m_onPotentialSnapshotWork = onPotentialSnapshotWork;
        m_snapshotPriority = snapshotPriority;
        initializeBufferPool();
        m_idlePredicate = idlePredicate;
    }

    public void shutdown() throws InterruptedException {
        for (BBContainer c : m_snapshotBufferOrigins ) {
            c.discard();
        }
        m_snapshotBufferOrigins.clear();
        m_availableSnapshotBuffers.clear();
        m_snapshotCreateSetupPermit = null;
        if (m_snapshotTargetTerminators != null) {
            for (Thread t : m_snapshotTargetTerminators) {
                t.join();
            }
        }
    }

    void initializeBufferPool() {
        for (int ii = 0; ii < SnapshotSiteProcessor.m_numSnapshotBuffers; ii++) {
            final BBContainer origin = org.voltcore.utils.DBBPool.allocateDirect(m_snapshotBufferLength);
            m_snapshotBufferOrigins.add(origin);
            long snapshotBufferAddress = 0;
            if (VoltDB.getLoadLibVOLTDB()) {
                snapshotBufferAddress = org.voltcore.utils.DBBPool.getBufferAddress(origin.b);
            }
            m_availableSnapshotBuffers.offer(new BBContainer(origin.b, snapshotBufferAddress) {
                @Override
                public void discard() {
                    m_availableSnapshotBuffers.offer(this);

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
                    if (m_isIV2Enabled && m_snapshotPriority > 0) {
                        final long now = System.currentTimeMillis();
                        //Ask if the site is idle, and if it is queue the work immediately
                        if (m_idlePredicate.idle(now)) {
                            m_onPotentialSnapshotWork.run();
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
                            m_onPotentialSnapshotWork.run();
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
                            VoltDB.instance().scheduleWork(
                                    m_onPotentialSnapshotWork,
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
                        m_onPotentialSnapshotWork.run();
                    }
                }
            });
        }
    }

    public void initiateSnapshots(ExecutionEngine ee, Deque<SnapshotTableTask> tasks, long txnId, int numHosts) {
        final long now = System.currentTimeMillis();
        m_quietUntil = now + 200;
        m_lastSnapshotSucceded = true;
        m_lastSnapshotTxnId = txnId;
        m_lastSnapshotNumHosts = numHosts;
        m_snapshotTableTasks = new ArrayDeque<SnapshotTableTask>(tasks);
        m_snapshotTargets = new ArrayList<SnapshotDataTarget>();
        m_snapshotTargetTerminators = new ArrayList<Thread>();
        for (final SnapshotTableTask task : tasks) {
            if ((!task.m_isReplicated) || (!task.m_target.getFormat().isTableBased())) {
                assert(task != null);
                assert(m_snapshotTargets != null);
                m_snapshotTargets.add(task.m_target);
            }
            /*
             * Why do the extra work for a /dev/null target
             * Check if it is dev null and don't activate COW
             */
            if (!task.m_isDevNull) {
                if (!ee.activateTableStream(task.m_tableId, TableStreamType.SNAPSHOT )) {
                    hostLog.error("Attempted to activate copy on write mode for table "
                            + task.m_name + " and failed");
                    hostLog.error(task);
                    VoltDB.crashLocalVoltDB("No additional info", false, null);
                }
            }
        }
        /*
         * Kick off the initial snapshot tasks. They will continue to
         * requeue themselves as the snapshot progresses. See intializeBufferPool
         * and the discard method of BBContainer for how requeuing works.
         */
        if (m_isIV2Enabled) {
            for (int ii = 0; ii < m_availableSnapshotBuffers.size(); ii++) {
                VoltDB.instance().scheduleWork(
                        m_onPotentialSnapshotWork,
                        (m_quietUntil + (5 * m_snapshotPriority) - now),
                        0,
                        TimeUnit.MILLISECONDS);
                m_quietUntil += 5 * m_snapshotPriority;
            }
        }
    }

    private void quietPeriodSet(boolean ignoreQuietPeriod) {
        if (!m_isIV2Enabled && !ignoreQuietPeriod && m_snapshotPriority > 0) {
            m_quietUntil = System.currentTimeMillis() + (5 * m_snapshotPriority) + ((long)(m_random.nextDouble() * 15));
        }
    }
    public Future<?> doSnapshotWork(ExecutionEngine ee, boolean ignoreQuietPeriod) {
        ListenableFuture<?> retval = null;

        /*
         * This thread will null out the reference to m_snapshotTableTasks when
         * a snapshot is finished. If the snapshot buffer is loaned out that means
         * it is pending I/O somewhere so there is no work to do until it comes back.
         */
        if (m_snapshotTableTasks == null ||
                m_availableSnapshotBuffers.isEmpty() ||
                (!ignoreQuietPeriod && inQuietPeriod())) {
            return retval;
        }

        /*
         * There definitely is snapshot work to do. There should be a task
         * here. If there isn't something is wrong because when the last task
         * is polled cleanup and nulling should occur.
         */
        while (!m_snapshotTableTasks.isEmpty()) {
            final SnapshotTableTask currentTask = m_snapshotTableTasks.peek();
            assert(currentTask != null);
            final int headerSize = currentTask.m_target.getHeaderSize();
            int serialized = 0;
            final BBContainer snapshotBuffer = m_availableSnapshotBuffers.poll();
            assert(snapshotBuffer != null);
            snapshotBuffer.b.clear();
            snapshotBuffer.b.position(headerSize);

            /*
             * For a dev null target don't do the work. The table wasn't
             * put in COW mode anyway so this will fail
             */
            if (!currentTask.m_isDevNull) {
                serialized =
                    ee.tableStreamSerializeMore(
                        snapshotBuffer,
                        currentTask.m_tableId,
                        TableStreamType.SNAPSHOT);
                if (serialized < 0) {
                    VoltDB.crashLocalVoltDB("Failure while serialize data from a table for COW snapshot", false, null);
                }
            }

            /**
             * The EE will return 0 when there is no more data left to pull from that table.
             * The enclosing loop ensures that the next table is then addressed.
             */
            if (serialized == 0) {
                final SnapshotTableTask t = m_snapshotTableTasks.poll();
                /**
                 * Replicated tables are assigned to a single ES on each site and that ES
                 * is responsible for closing the data target. Done in a separate
                 * thread so the EE can continue working.
                 */
                if (t.m_isReplicated && t.m_target.getFormat().isTableBased()) {
                    final Thread terminatorThread =
                        new Thread("Replicated SnapshotDataTarget terminator ") {
                        @Override
                        public void run() {
                            try {
                                t.m_target.close();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }

                        }
                    };
                    m_snapshotTargetTerminators.add(terminatorThread);
                    terminatorThread.start();
                }
                m_availableSnapshotBuffers.offer(snapshotBuffer);
                continue;
            }

            /**
             * The block from the EE will contain raw tuple data with no length prefix etc.
             */
            snapshotBuffer.b.limit(headerSize + serialized);
            snapshotBuffer.b.position(0);
            Callable<BBContainer> valueForTarget = Callables.returning(snapshotBuffer);
            for (SnapshotDataFilter filter : currentTask.m_filters) {
                valueForTarget = filter.filter(valueForTarget);
            }
            retval = currentTask.m_target.write(valueForTarget, currentTask);
            if (retval != null) {
                final ListenableFuture<?> retvalFinal = retval;
                retvalFinal.addListener(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            retvalFinal.get();
                        } catch (Throwable t) {
                            if (m_lastSnapshotSucceded) {
                                hostLog.error("Error while attempting to write snapshot data to file " +
                                        currentTask.m_target, t);
                                m_lastSnapshotSucceded = false;
                            }
                        }
                    }
                }, MoreExecutors.sameThreadExecutor());
            }
            quietPeriodSet(ignoreQuietPeriod);
            break;
        }

        /**
         * If there are no more tasks then this particular EE is finished doing snapshot work
         * Check the AtomicInteger to find out if this is the last one.
         */
        if (m_snapshotTableTasks.isEmpty()) {
            final ArrayList<SnapshotDataTarget> snapshotTargets = m_snapshotTargets;
            m_snapshotTargets = null;
            m_snapshotTableTasks = null;
            final int result = ExecutionSitesCurrentlySnapshotting.decrementAndGet();

            /**
             * If this is the last one then this EE must close all the SnapshotDataTargets.
             * Done in a separate thread so the EE can go and do other work. It will
             * sync every file descriptor and that may block for a while.
             */
            if (result == 0) {
                final long txnId = m_lastSnapshotTxnId;
                final int numHosts = m_lastSnapshotNumHosts;
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
                                    hostLog.error("Error running snapshot completion task", e);
                                }
                            }
                        } finally {
                            try {
                                logSnapshotCompleteToZK(txnId, numHosts, m_lastSnapshotSucceded);
                            } finally {
                                /**
                                 * Set it to -1 indicating the system is ready to perform another snapshot.
                                 * Changed to wait until all the previous snapshot work has finished so
                                 * that snapshot initiation doesn't wait on the file system
                                 */
                                ExecutionSitesCurrentlySnapshotting.decrementAndGet();
                            }
                        }
                    }
                };

                m_snapshotTargetTerminators.add(terminatorThread);
                terminatorThread.start();
            }
        }
        return retval;
    }


    private static void logSnapshotCompleteToZK(long txnId, int numHosts, boolean snapshotSuccess) {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();

        final String snapshotPath = VoltZK.completed_snapshots + "/" + txnId;
        boolean success = false;
        while (!success) {
            Stat stat = new Stat();
            byte data[] = null;
            try {
                data = zk.getData(snapshotPath, false, stat);
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
                int numHostsFinished = jsonObj.getInt("finishedHosts") + 1;
                jsonObj.put("finishedHosts", numHostsFinished);
                if (!snapshotSuccess) {
                    hostLog.error("Snapshot failed at this node, snapshot will not be viable for log truncation");
                    jsonObj.put("isTruncation", false);
                }

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

        try {
            VoltDB.instance().getHostMessenger().getZK().delete(
                    VoltZK.nodes_currently_snapshotting + "/" + VoltDB.instance().getHostMessenger().getHostId(), -1);
        } catch (NoNodeException e) {
            hostLog.warn("Expect the snapshot node to already exist during deletion", e);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
    }

    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the fsync() and close() of snapshot data targets has completed.
     */
    public HashSet<Exception> completeSnapshotWork(ExecutionEngine ee) throws InterruptedException {
        HashSet<Exception> retval = new HashSet<Exception>();
        while (m_snapshotTableTasks != null) {
            Future<?> result = doSnapshotWork(ee, true);
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
