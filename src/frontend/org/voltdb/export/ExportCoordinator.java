/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.zk.SynchronizedStatesManager;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;


/**
 * @author rdykiel
 *
 * An {@code ExportCoordinator} instance determines whether the associated
 * {@code ExportDataSource} instance should be an export master. It coordinates
 * the transfer of export mastership between the replicas of an export partition.
 *
 * Synchronization: the ExportCoordinator instance is confined to the thread-of the
 * monothread executor used by the associated ExportDataSource. All access to the
 * state variables must be through Runnable executed on the ExportDataSource executor.
 *
 * Terminology:
 *
 *  - Partition Leader is the host where the SPI leader resides.
 *
 *  - Export Master is the host currently exporting rows: usually
 *    the partition leader, unless another replica is filling a gap.
 *
 * Every {@code ExportCoordinator} share the same view of the following information:
 *
 *  - who is the partition leader
 *  - what are the gaps that were present in every node
 *
 *  Therefore, every instance can reach the same decision as to which node should be the
 *  Export Master of any row being exported (identified by its sequence number, or seqNo).
 */
public class ExportCoordinator {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    public static final String s_coordinatorTaskName = "coordinator";

    // ExportSequenceNumberTracker uses closed ranges and using
    // Long.MAX_VALUE throws IllegalStateException in Range.java.
    public static final long MAX_TRACKER_SEQNO = Long.MAX_VALUE - 1;

    private final Integer m_hostId;
    private final ExportDataSource m_eds;

    private final SynchronizedStatesManager m_ssm;
    private final ExportCoordinationTask m_task;

    private Integer m_leaderHostId = Integer.MIN_VALUE;

    // Map of state machine id to hostId
    private Map<String, Integer> m_hosts = new HashMap<>();

    // Map of hostId to ExportSequenceNumberTracker
    private TreeMap<Integer, ExportSequenceNumberTracker> m_trackers = new TreeMap<>();

    private boolean m_isMaster = false;
    private long m_safePoint = 0L;

    private void resetCoordinator(boolean resetLeader) {
        if (resetLeader) {
            m_leaderHostId = Integer.MIN_VALUE;
        }
        m_hosts.clear();
        m_trackers.clear();
        m_isMaster = false;
        m_safePoint = 0L;

    }

    /**
     * @author rdykiel
     *
     * This class is used to synchronize tasks and state changes for a topic/partition
     * across all the nodes in a cluster. Some methods are callbacks invoked from the
     * SSM daemon thread, others are methods invoked from ExportDataSource runnables.
     * Any access or update to state of the enclosing {@code ExportCoordinator} must be
     * done through Runnable executed on the ExportDataSource executor.
     *
     * The task maintains a queue of invocations; only one invocation can be outstanding
     * in the state machine. There are 2 kinds of invocation:
     *
     *  - Request a state change to a new partition leader
     *  - Start a task to collect the {@code ExportSequenceNumberTracker} from all the nodes
     */
    private class ExportCoordinationTask extends SynchronizedStatesManager.StateMachineInstance {

        private ByteBuffer m_startingState;
        private ByteBuffer m_currentState;

        // A queue of invocation runnables: each invocation needs the distributed lock
        private ConcurrentLinkedQueue<Runnable> m_invocations = new ConcurrentLinkedQueue<>();
        private AtomicBoolean m_pending = new AtomicBoolean(false);

        public ExportCoordinationTask(SynchronizedStatesManager ssm) {
            ssm.super(s_coordinatorTaskName, exportLog);
        }

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            // FIXME: should we crash voltdb
            exportLog.error("State machine was reset");
            resetCoordinator(true);
            return null;
        }

        @Override
        protected void setInitialState(ByteBuffer initialState) {
            m_startingState = initialState;
        }

        @Override
        protected String stateToString(ByteBuffer state) {
            state.rewind();
            Integer leaderHostId = state.getInt();
            StringBuilder sb = new StringBuilder("Leader hostId: ").append(leaderHostId);
            return sb.toString();
        }

        @Override
        protected String taskToString(ByteBuffer task) {
            return "ExportCoordinationTask";
        }

        /**
         * Queue a new invocation
         * @param runnable
         */
        void invoke(Runnable runnable) {
            m_invocations.add(runnable);
            invokeNext();
        }

        /**
         * Request lock for next queued invocation.
         */
        private void invokeNext() {
            if (m_invocations.isEmpty()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("No invocations pending on " + m_eds);
                }
                return;
            }
            if (!m_pending.compareAndSet(false, true)) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Invocation already pending on " + m_eds);
                }
                return;
            }
            if (requestLock()) {
                m_eds.getExecutorService().execute(m_invocations.poll());
            }
        }

        /**
         * End current invocation: only pertinent for the host that started the invocation.
         */
        private void endInvocation() {
            if (!m_pending.compareAndSet(true, false)) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("No invocation was pending on " + m_eds);
                }
            }
            invokeNext();
        }

        /**
         * lockRequestCompleted()
         * We got the distributed lock, run the first queued invocation.
         */
        @Override
        protected void lockRequestCompleted()
        {
            Runnable runnable = m_invocations.poll();
            if (runnable == null) {
                exportLog.warn("No runnable to invoke, canceling lock");
                cancelLockRequest();
                m_pending.set(false);
                return;
            }
            try {
                m_eds.getExecutorService().execute(runnable);

            } catch (RejectedExecutionException ex) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Execution rejected (shutdown?) on " + m_eds);
                    m_pending.set(false);
                }
            } catch (Exception ex) {
                // FIXME: should we crash voltdb
                exportLog.error("Failed to execute runnable: " + ex);
                m_pending.set(false);
            }
        }

        @Override
        protected void proposeStateChange(ByteBuffer proposedState) {
            super.proposeStateChange(proposedState);
        }

        /**
         * stateChangeProposed()
         * Always accept the proposed state change identifying a new partition leader.
         */
        @Override
        public void stateChangeProposed(ByteBuffer newState) {
            requestedStateChangeAcceptable(true);
        }

        /**
         * proposedStateResolved()
         * On success, transition the local host to partition leader, and if no trackers are present,
         * start a task to request them.
         */
        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {

            m_eds.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {

                    // Process change of partition leadership
                    try {
                        Integer newLeaderHostId = proposedState.getInt();
                        if (!success) {
                            exportLog.warn("Rejected change to new leader host: " + newLeaderHostId);
                            return;
                        }
                        m_leaderHostId = newLeaderHostId;
                        if (exportLog.isDebugEnabled()) {
                            StringBuilder sb = new StringBuilder("Host ")
                                    .append(m_leaderHostId)
                                    .append(isLeader() ? " (localHost) " : " ")
                                    .append("is the new leader");
                            exportLog.debug(sb.toString());
                        }

                    } catch (Exception e) {
                        exportLog.error("Failed to change to new leader: " + e);
                    }

                    // If leader and maps empty request {@code ExportSequenceNumberTracker} from all nodes.
                    // Note: cannot initiate a coordinator task directly from here, must go
                    // through another runnable and the invocation path.
                    if (isLeader() && m_trackers.isEmpty()) {
                        requestTrackers();
                    }
                }
            });
            endInvocation();
        }

        @Override
        protected void initiateCoordinatedTask(boolean correlated, ByteBuffer proposedTask) {
            super.initiateCoordinatedTask(correlated, proposedTask);
        }

        /**
         * taskRequested()
         * Reply with a copy of our tracker, truncated to our last released seqNo.
         * Task response = m_hostId (4) + serialized ExportSequenceNumberTracker.
         */
        @Override
        protected void taskRequested(ByteBuffer proposedTask) {

            m_eds.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {
                    ByteBuffer response = null;
                    try {
                        // Get the EDS tracker (note, this is a duplicate)
                        ExportSequenceNumberTracker tracker = m_eds.getTracker();
                        long lastReleasedSeqNo = m_eds.getLastReleaseSeqNo();
                        if (!tracker.isEmpty() && lastReleasedSeqNo > tracker.getFirstSeqNo()) {
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Truncating coordination tracker: " + tracker
                                        + ", to seqNo: " + lastReleasedSeqNo);
                            }
                            tracker.truncate(lastReleasedSeqNo);
                        }
                        int bufSize = tracker.getSerializedSize() + 4;
                        response = ByteBuffer.allocate(bufSize);
                        response.putInt(m_hostId);
                        tracker.serialize(response);

                    } catch (Exception e) {
                        exportLog.error("Failed to serialize coordination tracker: " + e);
                        response = ByteBuffer.allocate(0);
                    }
                    finally {
                        requestedTaskComplete(response);
                    }
                }
            });
        }

        /**
         * correlatedTaskCompleted()
         * Reset the coordinator with the new trackers included in task results.
         * Normalize the trackers to align any gaps occurring at the end.
         */
        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest,
                Map<String, ByteBuffer> results) {

            m_eds.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        resetCoordinator(false);
                        for(Map.Entry<String, ByteBuffer> entry : results.entrySet()) {
                            if (!entry.getValue().hasRemaining()) {
                                exportLog.warn("Received empty response from: " + entry.getKey());
                            }
                            else {
                                ByteBuffer buf = entry.getValue();
                                int host = buf.getInt();
                                try {
                                    ExportSequenceNumberTracker tracker = ExportSequenceNumberTracker.deserialize(buf);
                                    if (exportLog.isDebugEnabled()) {
                                        exportLog.debug("Received tracker from " + host + ": " + tracker);
                                    }

                                    m_hosts.put(entry.getKey(), host);
                                    m_trackers.put(host, tracker);

                                } catch (Exception e) {
                                    exportLog.error("Failed to deserialize coordination tracker from : "
                                            + host + ", got: " + e);
                                }
                            }
                        }
                        normalizeTrackers();
                        dumpTrackers();

                    } catch (Exception e) {
                        exportLog.error("Failed to handle coordination trackers: " + e);
                        resetCoordinator(false);
                    }
                }
            });
            endInvocation();
        }

        /**
         * membershipChanged()
         * When members are added, the partition leader requests the trackers from all hosts.
         * Otherwise, each host removes the removed host and resets the coordinator so that
         * the Export Mastership gets reevaluated.
         */
        @Override
        protected void membershipChanged(Set<String> addedMembers, Set<String> removedMembers) {

            m_eds.getExecutorService().execute(new Runnable() {
                @Override
                public void run() {

                    try {
                    // Added members require collecting the trackers: this is only initiated by the leader.
                    // Note that by the time the distributed lock is acquired for this invocation, this host
                    // may have lost leadership. But this is not important as the goal is to start the task
                    // from one host. Note also that the request goes through another EDS runnable.
                    if (!addedMembers.isEmpty()) {
                        if (isLeader()) {
                            exportLog.info("Leader requests trackers, added members: " + addedMembers);
                            requestTrackers();

                        } else if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Expecting new trackers, for added members: " + addedMembers);
                        }

                    } else if (!removedMembers.isEmpty()){
                        exportLog.info("Removing members: " + removedMembers);

                        for (String memberId: removedMembers) {
                            Integer hostId = m_hosts.get(memberId);
                            if (hostId == null) {
                                throw new IllegalStateException("Unknown memberId: " + memberId);
                            }
                            ExportSequenceNumberTracker tracker = m_trackers.remove(hostId);
                            if (tracker == null) {
                                throw new IllegalStateException("Unmapped tracker for memberId: " + memberId
                                        + ", hostId: " + hostId);
                            }
                            tracker = null;
                        }
                        resetCoordinator(false);
                    }
                    } catch (Exception e) {
                        exportLog.error("Failed to handle membership change (added: " + addedMembers
                                + ", removed: " + removedMembers + "), got: " + e);
                    }
                }
            });
        }
    }

    public ExportCoordinator(ZooKeeper zk, Integer hostId, ExportDataSource eds) {

        m_hostId = hostId;
        m_eds = eds;

        SynchronizedStatesManager ssm = null;
        ExportCoordinationTask task = null;
        try {
            ZKUtil.addIfMissing(zk, VoltZK.exportCoordination, CreateMode.PERSISTENT, null);

            // Set up a SynchronizedStateManager for the topic/partition
            String topicName = eds.getTableName() + "_" + eds.getPartitionId();
            ssm = new SynchronizedStatesManager(
                    zk,
                    VoltZK.exportCoordination,
                    topicName,
                    m_hostId.toString(),
                    1);

            task = new ExportCoordinationTask(ssm);
            ByteBuffer initialState = ByteBuffer.allocate(4);
            initialState.putInt(m_leaderHostId);
            initialState.flip();
            task.setInitialState(initialState);
            task.registerStateMachineWithManager(initialState);

            exportLog.info("Created ssm for topic " + topicName + ", and hostId " + m_hostId
                    + ", leaderHostId: " + m_leaderHostId);

        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to create ExportCoordinator state machine", true, e);
        } finally {
            m_ssm = ssm;
            m_task = task;
        }
    }

    public void shutdown() throws InterruptedException {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Shutting down...");
        }
        m_ssm.ShutdownSynchronizedStatesManager();
    }

    public void becomeLeader() {
        if (m_hostId.equals(m_leaderHostId)) {
            exportLog.warn(m_eds + " is already the partition leader");
        }

        m_task.invoke(new Runnable() {
            @Override
            public void run() {
                ByteBuffer changeState = ByteBuffer.allocate(4);
                changeState.putInt(m_hostId);
                changeState.flip();
                m_task.proposeStateChange(changeState);
            }});
    }

    boolean isLeader() {
        return m_hostId.equals(m_leaderHostId);
    }

    void requestTrackers() {
        exportLog.info("Host: " + m_hostId + " requesting export trackers");

        m_task.invoke(new Runnable() {
            @Override
            public void run() {
                try {
                    ByteBuffer task = ByteBuffer.allocate(0);
                    m_task.initiateCoordinatedTask(true, task);

                } catch (Exception e) {
                    exportLog.error("Failed to initiate a request for trackers: " + e);
                }
            }});
    }

    /**
     * Normalize the trackers to account for any host having a gap at the end; one
     * of the other hosts will have a higher sequence number.
     */
    void normalizeTrackers() {

        long highestSeqNo = 0L;
        long lowestSeqNo = Long.MAX_VALUE;

        for (ExportSequenceNumberTracker tracker : m_trackers.values()) {
            if (tracker.isEmpty()) {
                continue;
            }
            lowestSeqNo = Math.min(lowestSeqNo, tracker.getFirstSeqNo());
            highestSeqNo = Math.max(highestSeqNo, tracker.getLastSeqNo());
        }
        if (lowestSeqNo == Long.MAX_VALUE) {
            lowestSeqNo = 1L;
        }
        for (ExportSequenceNumberTracker tracker : m_trackers.values()) {
            if (tracker.isEmpty()) {
                tracker.append(lowestSeqNo, MAX_TRACKER_SEQNO);
            } else {
                tracker.append(highestSeqNo, MAX_TRACKER_SEQNO);
            }
        }
    }

    void dumpTrackers() {
        if (!exportLog.isDebugEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder("Export Cooordination Trackers:\n");
        m_trackers.forEach((k, v) -> sb.append(k).append(":\t").append(v).append("\n"));
        exportLog.debug(sb.toString());
    }
}
