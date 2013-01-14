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

package org.voltdb;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NoNodeException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
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

    //Protected by SnapshotSiteProcessor.m_snapshotCreateLock when accessed from SnapshotSaveAPI.startSnanpshotting
    public static Map<Integer, Long> m_partitionLastSeenTransactionIds =
            new HashMap<Integer, Long>();

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
        m_snapshotCreateSetupBarrier = null;
        m_snapshotCreateFinishBarrier = null;
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
                            VoltDB.instance().schedulePriorityWork(
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

    public void initiateSnapshots(
            ExecutionEngine ee,
            Deque<SnapshotTableTask> tasks,
            long txnId,
            int numHosts,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {
        ExecutionSitesCurrentlySnapshotting.add(this);
        final long now = System.currentTimeMillis();
        m_quietUntil = now + 200;
        m_lastSnapshotSucceded = true;
        m_lastSnapshotTxnId = txnId;
        m_lastSnapshotNumHosts = numHosts;
        m_snapshotTableTasks = new ArrayDeque<SnapshotTableTask>(tasks);
        m_snapshotTargets = new ArrayList<SnapshotDataTarget>();
        m_snapshotTargetTerminators = new ArrayList<Thread>();
        m_exportSequenceNumbersToLogOnCompletion = exportSequenceNumbers;
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
                VoltDB.instance().schedulePriorityWork(
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
            snapshotBuffer.b.limit(serialized + headerSize);
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
                final long txnId = m_lastSnapshotTxnId;
                final int numHosts = m_lastSnapshotNumHosts;
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
                                    hostLog.error("Error running snapshot completion task", e);
                                }
                            }
                        } finally {
                            try {
                                logSnapshotCompleteToZK(
                                        txnId,
                                        numHosts,
                                        m_lastSnapshotSucceded,
                                        exportSequenceNumbers);
                            } finally {
                                /**
                                 * Remove this last site from the set here after the terminator has run
                                 * so that new snapshots won't start until
                                 * everything is on disk for the previous snapshot. This prevents a really long
                                 * snapshot initiation procedure from occurring because it has to contend for
                                 * filesystem resources
                                 */
                                ExecutionSitesCurrentlySnapshotting.remove(SnapshotSiteProcessor.this);
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


    private static void logSnapshotCompleteToZK(
            long txnId,
            int numHosts,
            boolean snapshotSuccess,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {
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
                int remainingHosts = jsonObj.getInt("hostCount") - 1;
                jsonObj.put("hostCount", remainingHosts);
                if (!snapshotSuccess) {
                    hostLog.error("Snapshot failed at this node, snapshot will not be viable for log truncation");
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
                        hostLog.error("Found a mismatch in export sequence numbers while recording snapshot metadata " +
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
