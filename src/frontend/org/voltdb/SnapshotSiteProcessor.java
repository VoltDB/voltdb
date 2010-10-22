/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.jni.ExecutionEngine;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.DBBPool.BBContainer;

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

    /**
     * Only proceed once permits are available after setup completes
     */
    public static Semaphore m_snapshotPermits = new Semaphore(0);

    /**
     * Global collection populated by snapshot creator, poll'd by individual sites
     */
    public static final LinkedList<Deque<SnapshotTableTask>> m_taskListsForSites =
        new LinkedList<Deque<SnapshotTableTask>>();


    /** Number of snapshot buffers to keep */
    static final int m_numSnapshotBuffers = 3;

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

    /**
     * A class identifying a table that should be snapshotted as well as the destination
     * for the resulting tuple blocks
     */
    public static class SnapshotTableTask {
        private final int m_tableId;
        private final SnapshotDataTarget m_target;
        private final boolean m_isReplicated;
        private final String m_name;

        public SnapshotTableTask(
                final int tableId,
                final SnapshotDataTarget target,
                boolean isReplicated,
                final String tableName) {
            m_tableId = tableId;
            m_target = target;
            m_isReplicated = isReplicated;
            m_name = tableName;
        }

        @Override
        public String toString() {
            return ("SnapshotTableTask for " + m_name + " replicated " + m_isReplicated);
        }
    }

    SnapshotSiteProcessor(Runnable onPotentialSnapshotWork) {
        m_onPotentialSnapshotWork = onPotentialSnapshotWork;
        initializeBufferPool();
    }

    public void shutdown() {
        for (BBContainer c : m_snapshotBufferOrigins ) {
            c.discard();
        }
        m_snapshotBufferOrigins.clear();
        m_availableSnapshotBuffers.clear();
    }

    void initializeBufferPool() {
        for (int ii = 0; ii < SnapshotSiteProcessor.m_numSnapshotBuffers; ii++) {
            final BBContainer origin = org.voltdb.utils.DBBPool.allocateDirect(m_snapshotBufferLength);
            m_snapshotBufferOrigins.add(origin);
            long snapshotBufferAddress = 0;
            if (VoltDB.getLoadLibVOLTDB()) {
                snapshotBufferAddress = org.voltdb.utils.DBBPool.getBufferAddress(origin.b);
            }
            m_availableSnapshotBuffers.offer(new BBContainer(origin.b, snapshotBufferAddress) {
                @Override
                public void discard() {
                    m_availableSnapshotBuffers.offer(this);
                    m_onPotentialSnapshotWork.run();
                }
            });
        }
    }

    public void initiateSnapshots(ExecutionEngine ee, Deque<SnapshotTableTask> tasks) {
        m_snapshotTableTasks = new ArrayDeque<SnapshotTableTask>(tasks);
        m_snapshotTargets = new ArrayList<SnapshotDataTarget>();
        m_snapshotTargetTerminators = new ArrayList<Thread>();
        for (final SnapshotTableTask task : tasks) {
            if (!task.m_isReplicated) {
                assert(task != null);
                assert(m_snapshotTargets != null);
                m_snapshotTargets.add(task.m_target);
            }
            if (!ee.activateTableStream(task.m_tableId, TableStreamType.SNAPSHOT )) {
                hostLog.error("Attempted to activate copy on write mode for table "
                        + task.m_name + " and failed");
                hostLog.error(task);
                VoltDB.crashVoltDB();
            }
        }
    }

    public Future<?> doSnapshotWork(ExecutionEngine ee) {
        Future<?> retval = null;

        /*
         * This thread will null out the reference to m_snapshotTableTasks when
         * a snapshot is finished. If the snapshot buffer is loaned out that means
         * it is pending I/O somewhere so there is no work to do until it comes back.
         */
        if (m_snapshotTableTasks == null || m_availableSnapshotBuffers.isEmpty()) {
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
            final BBContainer snapshotBuffer = m_availableSnapshotBuffers.poll();
            assert(snapshotBuffer != null);
            snapshotBuffer.b.clear();
            snapshotBuffer.b.position(headerSize);
            final int serialized =
                ee.tableStreamSerializeMore(
                    snapshotBuffer,
                    currentTask.m_tableId,
                    TableStreamType.SNAPSHOT);

            if (serialized < 0) {
                hostLog.error("Failure while serialize data from a table for COW snapshot");
                VoltDB.crashVoltDB();
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
                if (t.m_isReplicated) {
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
            retval = currentTask.m_target.write(snapshotBuffer);
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

                final Thread terminatorThread =
                    new Thread("Snapshot terminator") {
                    @Override
                    public void run() {
                        try {
                            for (final SnapshotDataTarget t : snapshotTargets) {
                                try {
                                    t.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
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
                        } finally {
                            /**
                             * Set it to -1 indicating the system is ready to perform another snapshot.
                             * Changed to wait until all the previous snapshot work has finished so
                             * that snapshot initiation doesn't wait on the file system
                             */
                            ExecutionSitesCurrentlySnapshotting.decrementAndGet();
                        }
                    }
                };

                m_snapshotTargetTerminators.add(terminatorThread);
                terminatorThread.start();
            }
        }
        return retval;
    }

    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the fsync() and close() of snapshot data targets has completed.
     */
    public HashSet<Exception> completeSnapshotWork(ExecutionEngine ee) throws InterruptedException {
        HashSet<Exception> retval = new HashSet<Exception>();
        while (m_snapshotTableTasks != null) {
            Future<?> result = doSnapshotWork(ee);
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
