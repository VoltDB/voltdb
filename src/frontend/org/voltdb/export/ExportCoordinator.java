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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
 * the transfer of export mastership between the replicas of a an export partition.
 *
 * Synchronization: the ExportCoordinator instance is thread-confined to the
 * monothread executor used by the associated ExportDataSource.
 */
public class ExportCoordinator {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    public static final String s_coordinatorTaskName = "coordinator";

    private final Integer m_hostId;
    private final ExportDataSource m_eds;

    private final SynchronizedStatesManager m_ssm;
    private final ExportCoordinationTask m_task;

    private Integer m_leaderHostId = Integer.MIN_VALUE;
    private TreeMap<Integer, ExportSequenceNumberTracker> m_trackers = new TreeMap<>();

    private boolean m_isMaster = false;
    private long m_safePoint = Long.MIN_VALUE;

    /**
     * @author rdykiel
     *
     * This class is used to synchronize tasks and state changes for a topic/partition
     * across all the nodes in a cluster.
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
            // FIXME: behavior TBD
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

        // FIXME: handle exceptions - add a queue of invocations
        void invoke(Runnable runnable) {
            m_invocations.add(runnable);
            invokeNext();
        }

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

        private void endInvocation() {
            if (!m_pending.compareAndSet(true, false)) {
                exportLog.warn("No invocation was pending on " + m_eds);
            }
            invokeNext();
        }

        @Override
        protected void proposeStateChange(ByteBuffer proposedState) {
            super.proposeStateChange(proposedState);
        }

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
            // FIXME: exceptions?
            m_eds.getExecutorService().execute(runnable);
        }

        @Override
        public void stateChangeProposed(ByteBuffer newState) {
            // We always accept the proposed state change.
            requestedStateChangeAcceptable(true);
        }

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

                    } catch (Exception e) {
                        exportLog.error("Failed to change to new leader: " + e);
                    }

                    // If leader request {@code ExportSequenceNumberTracker} from all nodes.
                    // Note: cannot initiate a coordinator task directly from here, must go
                    // through invocation path.
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
                    ExportSequenceNumberTracker tracker = m_eds.getTracker();
                    long lastReleasedSeqNo = m_eds.getLastReleaseSeqNo();
                    if (!tracker.isEmpty() && lastReleasedSeqNo > tracker.getFirstSeqNo()) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Truncating coordination tracker: " + tracker
                                    + ", to seqNo: " + lastReleasedSeqNo);
                        }
                        tracker.truncate(lastReleasedSeqNo);
                    }
                    int bufSize = 0;
                    ByteBuffer response = null;
                    try {
                        bufSize = tracker.getSerializedSize() + 4;
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

        // FIXME: process results in EDS runnable
        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest,
                Map<String, ByteBuffer> results) {

            for(Map.Entry<String, ByteBuffer> entry : results.entrySet()) {
                if (!entry.getValue().hasRemaining()) {
                    exportLog.warn("Received empty response from: " + entry.getKey());
                }
                else {
                    ByteBuffer buf = entry.getValue();
                    int host = buf.getInt();
                    try {
                        ExportSequenceNumberTracker tracker = ExportSequenceNumberTracker.deserialize(buf);
                        exportLog.info("Received responses from: " + host + ", tracker: " + tracker);

                    } catch (Exception e) {
                        exportLog.error("Failed to deserialize coordination tracker from : "
                                + host + ", got: " + e);
                    }
                }
            }
            endInvocation();
        }

        @Override
        protected void membershipChanged(Set<String> addedMembers, Set<String> removedMembers) {
            exportLog.info("YYY Added members: " + addedMembers + ", removed members: " + removedMembers);
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

            exportLog.info("XXX Created ssm for topic " + topicName + ", and hostId " + m_hostId
                    + ", leaderHostId: " + m_leaderHostId);

        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to create ExportCoordinator state machine", true, e);
        } finally {
            m_ssm = ssm;
            m_task = task;
        }
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
        exportLog.info("XXX Host: " + m_hostId + " requesting export trackers");

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
}
