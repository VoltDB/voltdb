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
import org.voltcore.utils.Pair;
import org.voltcore.zk.SynchronizedStatesManager;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB;


/**
 * An {@code ExportCoordinator} instance determines whether the associated
 * {@code ExportDataSource} instance should be an export master. It coordinates
 * the transfer of export mastership between the replicas of an export partition.
 *
 * Synchronization: the ExportCoordinator instance is confined to the thread-of the
 * monothread executor used by the associated ExportDataSource. All access to the state
 * variables must be through a Runnable executed on the ExportDataSource executor.
 *
 * Likewise, all the public methods MUST be invoked through a Runnable executed on
 * the ExportDataSource executor.
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

    /**
     * This class is used to synchronize tasks and state changes for a topic/partition
     * across all the nodes in a cluster. Some methods are callbacks invoked from the
     * SSM daemon thread, others are methods invoked from ExportDataSource runnables.
     * Any access or update to state of the enclosing {@code ExportCoordinator} must be
     * done through Runnable executed on the ExportDataSource executor.
     *
     * The state machine maintains a queue of invocations; only one invocation can be
     * outstanding in the state machine. There are 2 kinds of invocation:
     *
     *  - Request a state change to a new partition leader
     *  - Start a task to collect the {@code ExportSequenceNumberTracker} from all the nodes
     *
     *  Notes:
     *
     *  - the {@code ExportDataSource} may be shut down asynchronously to the callbacks
     *    invoked by the SSM daemon thread
     *
     *  - we want to catch any exceptions occurring in the SSM daemon callbacks in order
     *    to preserve the integrity of the SSM.
     */
    private class ExportCoordinationStateMachine extends SynchronizedStatesManager.StateMachineInstance {

        // A queue of invocation runnables: each invocation needs the distributed lock
        private ConcurrentLinkedQueue<Runnable> m_invocations = new ConcurrentLinkedQueue<>();

        private AtomicBoolean m_pending = new AtomicBoolean(false);
        private AtomicBoolean m_shutdown = new AtomicBoolean(false);

        public ExportCoordinationStateMachine(SynchronizedStatesManager ssm) {
            ssm.super(s_coordinatorTaskName, exportLog);
        }

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            exportLog.error("State machine was reset");
            resetCoordinator(true, true);
            return null;
        }

        @Override
        protected void setInitialState(ByteBuffer initialState) {
            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, ignore initial state proposed on " + m_eds);
                }
                return;
            }
            try {
                m_eds.getExecutorService().execute(new Runnable() {
                    @Override
                    public void run() {
                        // Handle initial partition leadership
                        try {
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Set initial state on host: " + m_hostId);
                            }
                            Integer newLeaderHostId = initialState.getInt();
                            m_leaderHostId = newLeaderHostId;
                            StringBuilder sb = new StringBuilder("Initialized export coordinator: host ")
                                    .append(m_leaderHostId)
                                    .append(isPartitionLeader() ? " (localHost) " : " ")
                                    .append("is the leader at initial state");
                            exportLog.info(sb.toString());
                            setCoordinatorInitialized();
                            invokeNext();

                        } catch (Exception e) {
                            exportLog.error("Failed to change to initial state leader: " + e);
                        }
                    }
                });
            } catch (RejectedExecutionException rej) {
                // This callback may be racing with a shut down EDS
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Initial state rejected by: " + m_eds);
                }
            } catch (Exception e) {
                exportLog.error("Failed to handle initial state: " + e);
            }
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
            Long lastReleasedSeqNo = task.getLong();
            return "ExportCoordinationTask (last released seqNo: " + lastReleasedSeqNo + ")";
        }

        void shutdownCoordinationTask() {
            m_shutdown.set(true);
        }

        /**
         * Queue a new invocation
         * @param runnable
         */
        void invoke(Runnable runnable) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Queue invocation: " + runnable);
            }
            m_invocations.add(runnable);
            invokeNext();
        }

        /**
         * Clear the queued invocations (on shutdown).
         */
        void clearInvocations() {
            m_invocations.clear();
        }

        /**
         * @return the count of invocations currently queued
         */
        int invocationCount() {
            return m_invocations.size();
        }

        /**
         * Request lock for next queued invocation.
         */
        private void invokeNext() {

            // Do not try to acquire the distributed lock until
            // completely initialized
            if (!isCoordinatorInitialized()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Uninitialized, skip invocation");
                }
                return;
            }

            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, ignore invocation on " + m_eds);
                }
                return;
            }
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
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Request lock for: " + m_invocations.peek());
            }
            if (requestLock()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Immediate execution of: " + m_invocations.peek());
                }
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
            try {
                if (m_shutdown.get()) {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Shutdown, ignore lock request on " + m_eds);
                    }
                    return;
                }
                Runnable runnable = m_invocations.poll();
                if (runnable == null) {
                    exportLog.warn("No runnable to invoke, canceling lock");
                    cancelLockRequest();
                    m_pending.set(false);
                    return;
                }
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Deferred execution of: " + runnable);
                }
                m_eds.getExecutorService().execute(runnable);

            } catch (RejectedExecutionException ex) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Execution rejected (shutdown?) on " + m_eds);
                }
                m_pending.set(false);
            } catch (Exception ex) {
                exportLog.error("Failed to execute runnable: " + ex);
                m_pending.set(false);
            }

        }

        @Override
        protected void proposeStateChange(ByteBuffer proposedState) {
            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, ignore proposing state change on " + m_eds);
                }
                return;
            }
            super.proposeStateChange(proposedState);
        }

        /**
         * stateChangeProposed()
         * Always accept the proposed state change identifying a new partition leader.
         */
        @Override
        public void stateChangeProposed(ByteBuffer newState) {
            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, proposed state change on " + m_eds);
                }
                return;
            }
            requestedStateChangeAcceptable(true);
        }

        /**
         * proposedStateResolved()
         * On success, transition the local host to partition leader, and if no trackers are present,
         * start a task to request them.
         */
        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {

            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, ignore proposed state resolved on " + m_eds);
                }
                return;
            }
            try {
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
                            StringBuilder sb = new StringBuilder("Host ")
                                    .append(m_leaderHostId)
                                    .append(isPartitionLeader() ? " (localHost) " : " ")
                                    .append("is the new leader");
                            exportLog.info(sb.toString());

                            // If leader and maps empty request ExportSequenceNumberTracker from all nodes.
                            // Note: cannot initiate a coordinator task directly from here, must go
                            // through another runnable and the invocation path.
                            if (isPartitionLeader() && m_trackers.isEmpty()) {
                                requestTrackers();
                            }

                        } catch (Exception e) {
                            exportLog.error("Failed to change to new leader: " + e);

                        } finally {
                            // End the current invocation and do the next
                            if (ourProposal) {
                                endInvocation();
                            }
                        }
                    }
                });
            } catch (RejectedExecutionException rej) {
                // This callback may be racing with a shut down EDS
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("State resolution rejected by: " + m_eds);
                }
            } catch (Exception e) {
                exportLog.error("Failed to handle state resolution: " + e);
            }
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

            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, ignore task requested on " + m_eds);
                }
                return;
            }
            try {
                m_eds.getExecutorService().execute(new Runnable() {
                    @Override
                    public void run() {
                        ByteBuffer response = null;
                        try {
                            Long lastReleasedSeqNo = proposedTask.getLong();
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Task requested with leader acked to seqNo: " + lastReleasedSeqNo);
                            }

                            // If not the leader, check if the leader is behind or ahead on acks.
                            if (!isPartitionLeader()) {
                                if (lastReleasedSeqNo < m_eds.getLastReleaseSeqNo()) {
                                    // Leader is behind on acks, ask the ExportDataSource to resend
                                    // an ack to all the other hosts.
                                    StringBuilder sb = new StringBuilder("Leader host ")
                                            .append(m_leaderHostId)
                                            .append(" released sequence number (")
                                            .append(lastReleasedSeqNo)
                                            .append(") is behind the local released sequence number (")
                                            .append(m_eds.getLastReleaseSeqNo())
                                            .append(")");
                                    exportLog.warn(sb.toString());
                                    m_eds.forwardAckToOtherReplicas();

                                } else if (m_eds.getLastReleaseSeqNo() < lastReleasedSeqNo) {
                                    // Leader is ahead on acks, do a local release
                                    StringBuilder sb = new StringBuilder("Leader host ")
                                            .append(m_leaderHostId)
                                            .append(" released sequence number (")
                                            .append(lastReleasedSeqNo)
                                            .append(") is ahead of the local released sequence number (")
                                            .append(m_eds.getLastReleaseSeqNo())
                                            .append(")");
                                    exportLog.warn(sb.toString());
                                    m_eds.localAck(lastReleasedSeqNo, lastReleasedSeqNo);
                                }
                            }

                            // Get the EDS tracker (note, this is a duplicate)
                            ExportSequenceNumberTracker tracker = m_eds.getTracker();
                            lastReleasedSeqNo = m_eds.getLastReleaseSeqNo();
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

                        } finally {
                            requestedTaskComplete(response);
                        }
                    }
                });
            } catch (RejectedExecutionException rej) {
                // This callback may be racing with a shut down EDS
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Task request rejected by: " + m_eds);
                }
            } catch (Exception e) {
                exportLog.error("Failed to handle task request: " + e);
            }
        }

        /**
         * correlatedTaskCompleted()
         * Reset the coordinator with the new trackers included in task results.
         * Normalize the trackers to align any gaps occurring at the end.
         * Kick any pending poll on the EDS.
         */
        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest,
                Map<String, ByteBuffer> results) {

            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, ignore task completed on " + m_eds);
                }
                return;
            }
            try {
                m_eds.getExecutorService().execute(new Runnable() {
                    @Override
                    public void run() {

                        try {
                            boolean requestAgain = false;
                            resetCoordinator(false, true);
                            for(Map.Entry<String, ByteBuffer> entry : results.entrySet()) {
                                ByteBuffer buf = entry.getValue();
                                if ((buf == null)) {
                                    if (ourTask) {
                                        exportLog.warn("No response from: " + entry.getKey() + ", request trackers again");
                                    } else if (exportLog.isDebugEnabled()) {
                                        exportLog.warn("No response from: " + entry.getKey() + ", wait for complete trackers");
                                    }
                                    requestAgain = true;
                                    break;
                                }
                                else if (!buf.hasRemaining()) {
                                    exportLog.warn("Received empty response from: " + entry.getKey());
                                }
                                else {
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
                            if (requestAgain) {
                                // On incomplete trackers make sure to avoid
                                // deciding who is the export master
                                resetCoordinator(false, true);
                                if (ourTask) {
                                    requestTrackers();
                                }
                            } else {
                                normalizeTrackers();
                                dumpTrackers();

                                // JUnit test synchronization
                                if (m_testReady != null) {
                                    m_testReady.set(true);
                                }

                                m_eds.resumePolling();
                            }

                        } catch (Exception e) {
                            exportLog.error("Failed to handle coordination trackers: " + e);
                            resetCoordinator(false, true);

                        } finally {
                            // End the current invocation and do the next
                            if (ourTask) {
                                endInvocation();
                            }
                        }
                    }
                });
            } catch (RejectedExecutionException rej) {
                // This callback may be racing with a shut down EDS
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Task completion rejected by: " + m_eds);
                }
            } catch (Exception e) {
                exportLog.error("Failed to handle task completion: " + e);
            }
        }

        /**
         * membershipChanged()
         * When members are added, the partition leader requests the trackers from all hosts.
         * Otherwise, each host removes the removed host and resets the coordinator so that
         * the Export Mastership gets reevaluated.
         */
        @Override
        protected void membershipChanged(Set<String> addedMembers, Set<String> removedMembers) {

            if (m_shutdown.get()) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Shutdown, ignore membership changed on " + m_eds);
                }
                return;
            }
            try {
                m_eds.getExecutorService().execute(new Runnable() {
                    @Override
                    public void run() {

                        try {
                            // Added members require collecting the trackers: this is only initiated by the leader.
                            // Note that by the time the distributed lock is acquired for this invocation, this host
                            // may have lost leadership. But this is not important as the goal is to start the task
                            // from one host. Note also that the request goes through another EDS runnable.
                            if (!addedMembers.isEmpty()) {
                                if (isPartitionLeader()) {
                                    exportLog.info("Leader requests trackers for added members: " + addedMembers);
                                    requestTrackers();

                                } else if (exportLog.isDebugEnabled()) {
                                    exportLog.debug("Expecting new trackers for added members: " + addedMembers);
                                }

                            } else if (!removedMembers.isEmpty()){
                                exportLog.info("Removing members: " + removedMembers);

                                for (String memberId: removedMembers) {
                                    Integer hostId = m_hosts.remove(memberId);
                                    if (hostId == null) {
                                        if (exportLog.isDebugEnabled()) {
                                            exportLog.debug("Ignore removal of unknown memberId: " + memberId);
                                        }
                                        continue;
                                    }
                                    ExportSequenceNumberTracker tracker = m_trackers.remove(hostId);
                                    if (tracker == null) {
                                        throw new IllegalStateException("Unmapped tracker for memberId: " + memberId
                                                + ", hostId: " + hostId);
                                    }
                                    tracker = null;
                                    if (m_leaderHostId == hostId) {
                                        // If the leader went away, wait for the next leader
                                        exportLog.warn("Lost leader host " + hostId + " reset coordinator");
                                        resetCoordinator(true, true);
                                        return;
                                    }
                                }

                                // After removing members, we want to reevaluate Export Mastership
                                resetSafePoint();
                            }
                        } catch (Exception e) {
                            exportLog.error("Failed to handle membership change (added: " + addedMembers
                                    + ", removed: " + removedMembers + "), got: " + e);
                        }
                    }
                });
            } catch (RejectedExecutionException rej) {
                // This callback may be racing with a shut down EDS
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Membership change rejected by: " + m_eds);
                }
            } catch (Exception e) {
                exportLog.error("Failed to handle membership change: " + e);
            }
        }
    }

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    public static final String s_coordinatorTaskName = "coordinator";

    // ExportSequenceNumberTracker uses closed ranges and using
    // Long.MAX_VALUE throws IllegalStateException in Range.java.
    private static final long INFINITE_SEQNO = Long.MAX_VALUE - 1;

    private final ZooKeeper m_zk;
    private final String m_rootPath;
    private final Integer m_hostId;
    private final ExportDataSource m_eds;

    // State machine will only be instantiated on initialize()
    private final SynchronizedStatesManager m_ssm;
    private final ExportCoordinationStateMachine m_stateMachine;

    private Integer m_leaderHostId = NO_HOST_ID;
    private static final int NO_HOST_ID =  -1;

    // Map of state machine id to hostId
    private Map<String, Integer> m_hosts = new HashMap<>();

    // Map of hostId to ExportSequenceNumberTracker
    private TreeMap<Integer, ExportSequenceNumberTracker> m_trackers = new TreeMap<>();

    enum State {
        CREATED,
        REPLICATED,
        INITIALIZING,
        INITIALIZED
    }
    private State m_state = State.CREATED;
    private boolean m_isMaster = false;
    private long m_safePoint = 0L;

    // For JUnit test support
    private AtomicBoolean m_testReady;

    private void resetCoordinator(boolean resetLeader, boolean resetTrackers) {
        if (resetLeader) {
            m_leaderHostId = NO_HOST_ID;
        }
        if (resetTrackers) {
            m_hosts.clear();
            m_trackers.clear();
        }
        resetSafePoint();
    }

    // The safe point is reset with mastership back to partition leader
    private void resetSafePoint() {
        m_isMaster = isPartitionLeader();
        m_safePoint = 0L;
    }

    /**
     * JUnit test constructor
     */
    ExportCoordinator(ZooKeeper zk, String rootPath, Integer hostId, ExportDataSource eds, boolean setTestReady) {
        this(zk, rootPath, hostId, eds);
        m_testReady = new AtomicBoolean(false);
    }

    // This method only for JUnit tests
    boolean isTestReady() {
        return m_testReady != null && m_testReady.get();
    }

    /**
     * Constructor - note that it does not initialize the state machine...
     *
     * @param zk
     * @param rootPath
     * @param hostId
     * @param eds
     */
    public ExportCoordinator(ZooKeeper zk, String rootPath, Integer hostId, ExportDataSource eds) {

        m_zk = zk;
        m_rootPath = rootPath;
        m_hostId = hostId;
        m_eds = eds;

        SynchronizedStatesManager ssm = null;
        ExportCoordinationStateMachine task = null;
        try {
            ZKUtil.addIfMissing(m_zk, m_rootPath, CreateMode.PERSISTENT, null);

            // Set up a SynchronizedStateManager for the topic/partition
            String topicName = getTopicName(m_eds.getTableName(), m_eds.getPartitionId());
            ssm = new SynchronizedStatesManager(
                    m_zk,
                    m_rootPath,
                    topicName,
                    m_hostId.toString(),
                    1);

            task = new ExportCoordinationStateMachine(ssm);

            exportLog.info("Created export coordinator for topic " + topicName + ", and hostId " + m_hostId
                    + ", leaderHostId: " + m_leaderHostId);

        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to initialize ExportCoordinator state machine", true, e);
        } finally {
            m_ssm = ssm;
            m_stateMachine = task;
        }
    }

    private String getTopicName(String tableName, int partitionId) {
        return tableName + "_" + partitionId;

    }

    // Coordinator must be shut down whenever it has started initializing; the
    // shutdown will occur only after the coordinator is completely initialized.
    private boolean mustBeShutdown() {
        return m_state == State.INITIALIZED || m_state == State.INITIALIZING;
    }

    /**
     * @return true if coordinator is initialized
     *
     * Note: avoid conflicting with "isInitialized" in {
     */
    public boolean isCoordinatorInitialized() {
        return m_state == State.INITIALIZED;
    }

    private void setCoordinatorInitialized() {
        m_state = State.INITIALIZED;
    }

    private boolean isReplicated() {
        return m_state == State.REPLICATED;
    }

    /**
     * Called by EDS when notified it is ready for polling.
     *
     * @param replicatedPartition true if the partition is replicated
     */
    public void initialize(boolean replicatedPartition) {

        if (replicatedPartition) {
            exportLog.debug("Export coordinator initialized in replicated mode for " + m_eds);
            m_state = State.REPLICATED;
            return;
        }
        else if (m_state == State.INITIALIZING) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Export coordinator initializing for " + m_eds);
            }
            return;
        }
        else if (m_state == State.INITIALIZED) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Export coordinator already initialized for " + m_eds);
            }
            return;
        }
        try {
            m_state = State.INITIALIZING;
            ByteBuffer initialState = ByteBuffer.allocate(4);
            initialState.putInt(m_leaderHostId);
            initialState.flip();
            m_stateMachine.registerStateMachineWithManager(initialState);

            String topicName = getTopicName(m_eds.getTableName(), m_eds.getPartitionId());
            exportLog.info("Initializing export coordinator for topic " + topicName + ", and hostId " + m_hostId
                    + ", leaderHostId: " + m_leaderHostId);

        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to initialize ExportCoordinator state machine", true, e);
        }
    }

    public void initialize() {
        initialize(false);
    }

    /**
     * Shutdown this coordinator.
     *
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {

        if (!mustBeShutdown()) {
            // Nothing to shut down, notify EDS it can shut down
            m_eds.onCoordinatorShutdown();
            return;
        }
        exportLog.info("Export coordinator requesting shutdown: clearing pending invocations");
        m_stateMachine.clearInvocations();

        // We want to shutdown after we get the lock
        m_stateMachine.invoke(new Runnable() {
            @Override
            public void run() {
                try {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Export coordinator shutting down...");
                    }
                    m_stateMachine.shutdownCoordinationTask();
                    m_ssm.ShutdownSynchronizedStatesManager();

                } catch (Exception e) {
                    exportLog.error("Failed to initiate a coordinator shutdown: " + e);
                } finally {
                    m_eds.onCoordinatorShutdown();
                }
            }
            @Override
            public String toString() {
                return "Coordinator shutdown for host:" + m_hostId;
            }
        });
    }

    /**
     * Initiate a state change to make this host the partition leader.
     *
     * NOTE: we accept this invocation even if not initialized yet, but
     * since we may be called multiple times in that state, we ensure we don't
     * queue more than one invocation.
     */
    public void becomeLeader() {

        if (m_state == State.REPLICATED) {
            return;
        }
        if (m_hostId.equals(m_leaderHostId)) {
            exportLog.warn(m_eds + " is already the partition leader");
            return;
        }

        int count = m_stateMachine.invocationCount();
        if (!isCoordinatorInitialized() &&  count >= 1) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug(count + " invocations already pending to become leader");
            }
            return;
        }

        m_stateMachine.invoke(new Runnable() {
            @Override
            public void run() {
                ByteBuffer changeState = ByteBuffer.allocate(4);
                changeState.putInt(m_hostId);
                changeState.flip();
                m_stateMachine.proposeStateChange(changeState);
            }
            @Override
            public String toString() {
                return "becomeLeader request for host:" + m_hostId;
            }
        });
    }

    /**
     * @return true if this host is the partition leader.
     */
    public boolean isPartitionLeader() {
        return m_hostId.equals(m_leaderHostId);
    }

    /**
     * Return true if this host is the export master.
     * @return
     */
    public boolean isMaster() {
        return m_isMaster;
    }

    /**
     * Returns true if the acked sequence number passes the safe point.
     *
     * @param ackedSeqNo the acked sequence number
     * @return true if this passed the safe point
     */
    public boolean isSafePoint(long ackedSeqNo) {

        if (isReplicated()) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Replicated table, skip checking safe point at " + ackedSeqNo);
            }
            return false;
        }
        if (!isCoordinatorInitialized()) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Uninitialized, skip checking safe point at " + ackedSeqNo);
            }
            return false;
        }

        // Always truncate the trackers to the acked seqNo
        m_trackers.forEach((k, v) -> v.truncate(ackedSeqNo));

        if (m_safePoint == 0L || m_safePoint > ackedSeqNo) {
            // Not waiting for safe point or not reached safe point
            return false;
        }
        resetSafePoint();
        return true;
    }

    /**
     * Return true if this host is the Export Master for this sequence number
     *
     * @param exportSeqNo the sequence number we want to export
     * @return if we are the Export Master
     */
    public boolean isExportMaster(long exportSeqNo) {

        if (isReplicated()) {
            // Always true for replicated tables
            return true;
        }
        if (!isCoordinatorInitialized()) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Uninitialized, not export master at " + exportSeqNo);
            }
            return false;
        }

        // No leader, no master.
        if (m_leaderHostId == NO_HOST_ID) {
            return false;
        }

        // First reset the safe point if we're polling past it
        if (isSafePoint(exportSeqNo - 1)) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Polling passed safe point at " + (exportSeqNo - 1));
            }
        }

        // If we're beneath the safe point return the current mastership
        if (m_safePoint > 0L) {
            assert(exportSeqNo <= m_safePoint);
            return m_isMaster;
        }

        // Now we need to re-evaluate export mastership
        assert(m_safePoint == 0);

        // If the leader isn't in a gap the leader is master
        ExportSequenceNumberTracker leaderTracker = m_trackers.get(m_leaderHostId);
        if (leaderTracker == null) {
            // This means that the leadership has been resolved but the
            // trackers haven't been gathered
            return m_isMaster;
        }

        // Note: the trackers are truncated so the seqNo should not be past the first gap
        Pair<Long, Long> gap = leaderTracker.getFirstGap();
        assert (gap == null || exportSeqNo <= gap.getSecond());
        if (gap == null || exportSeqNo < (gap.getFirst() - 1)) {

            m_isMaster = isPartitionLeader();
            if (gap == null) {
                m_safePoint = INFINITE_SEQNO;
            } else {
                m_safePoint = gap.getFirst() - 1;
            }

            exportLog.info("Leader host " + m_leaderHostId + " is Export Master until safe point " + m_safePoint);
            return m_isMaster;
        }

        // Leader is not master
        if (isPartitionLeader()) {
            m_isMaster = false;
        }

        // Return the lowest hostId that can fill the gap
        assert (gap != null);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Leader host " + m_leaderHostId + " at seqNo " + exportSeqNo
                    + ", hits gap [" + gap.getFirst() + ", " + gap.getSecond()
                    + "], look for candidate replicas");
        }

        Integer replicaId = NO_HOST_ID;
        long leaderNextSafePoint = gap.getSecond();
        long  replicaSafePoint = 0L;

        for (Integer hostId : m_trackers.keySet()) {

            if (m_leaderHostId.equals(hostId)) {
                continue;
            }
            Pair<Long, Long> rgap = m_trackers.get(hostId).getFirstGap();
            if (rgap != null) {
                assert (exportSeqNo <= rgap.getSecond());
            }
            if (rgap == null || exportSeqNo < (rgap.getFirst() - 1)) {
                replicaId = hostId;
                if (rgap == null) {
                    replicaSafePoint = INFINITE_SEQNO;
                } else {
                    // The next safe point of the replica is the last before the
                    // replica gap
                    replicaSafePoint = rgap.getFirst() -1;
                }
                break;
            }
        }

        if (!replicaId.equals(NO_HOST_ID)) {
            m_isMaster = m_hostId.equals(replicaId);
            m_safePoint = Math.min(leaderNextSafePoint, replicaSafePoint);
            exportLog.info("Replica host " + replicaId + " fills gap [" + gap.getFirst()
            + ", " + gap.getSecond() + "], until safe point " + m_safePoint);
            return m_isMaster;
        }

        // If no replicas were found, the leader is Export Master and will become BLOCKED
        m_safePoint = gap.getFirst();
        m_isMaster = isPartitionLeader();
        exportLog.info("Leader host " + m_leaderHostId
                + " is Export Master and will be blocked at safe point " + m_safePoint);
        return m_isMaster;
    }

    /**
     * Start a task requesting export trackers from all nodes.
     * Only the partition leader should initiate this.
     * The request carries the leader's last released sequence number.
     */
    private void requestTrackers() {
        exportLog.info("Host: " + m_hostId + " requesting export trackers");

        m_stateMachine.invoke(new Runnable() {
            @Override
            public void run() {
                try {
                    ByteBuffer task = ByteBuffer.allocate(8);
                    task.putLong(m_eds.getLastReleaseSeqNo());
                    task.flip();
                    m_stateMachine.initiateCoordinatedTask(true, task);

                } catch (Exception e) {
                    exportLog.error("Failed to initiate a request for trackers: " + e);
                }
            }
            @Override
            public String toString() {
                return "requestTrackers for host:" + m_hostId + ", leader: " + m_leaderHostId;
            }
        });
    }

    /**
     * Normalize the trackers to account for any host having a gap at the end; one
     * of the other hosts will have a higher sequence number.
     *
     * Normalize the trackers for gaps at the beginning.
     */
    private void normalizeTrackers() {

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
                tracker.append(lowestSeqNo, INFINITE_SEQNO);
            } else {
                tracker.append(highestSeqNo + 1, INFINITE_SEQNO);
                if (tracker.getFirstSeqNo() > lowestSeqNo) {
                    tracker.addRange(lowestSeqNo, lowestSeqNo);
                }
            }
        }
    }

    private void dumpTrackers() {
        if (!exportLog.isDebugEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder("Export Cooordination Trackers:\n");
        m_trackers.forEach((k, v) -> sb.append(k).append(":\t").append(v).append("\n"));
        exportLog.debug(sb.toString());
    }
}
