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
    private boolean m_isMaster = false;
    private long m_safePoint = Long.MAX_VALUE;

    /**
     * @author rdykiel
     *
     * This class is used to synchronize tasks and state changes for a topic/partition
     * across all the nodes in a cluster.
     */
    private class ExportCoordinationTask extends SynchronizedStatesManager.StateMachineInstance {

        private ByteBuffer m_startingState;
        private ByteBuffer m_currentState;

        private Runnable m_invocation;

        public ExportCoordinationTask(SynchronizedStatesManager ssm) {
            ssm.super(s_coordinatorTaskName, exportLog);
        }

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            // TODO Auto-generated method stub
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

        @Override
        protected void lockRequestCompleted()
        {
            Runnable runnable = m_invocation;   // FIXME: atomicRef?
            if (runnable == null) {
                exportLog.warn("XXX No runnable to invoke, canceling lock");
                cancelLockRequest();
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
                    // FIXME: exceptions?
                    Integer newLeaderHostId = proposedState.getInt();
                    if (!success) {
                        exportLog.warn("XXX Rejected change to new leader host: " + newLeaderHostId);
                        return;
                    }
                    exportLog.info("XXX Host: " + m_hostId + " accepting new leader host: " + newLeaderHostId);
                    m_leaderHostId = newLeaderHostId;
                }
            });
        }


        // FIXME: handle exceptions
        void invoke(Runnable runnable) {
            if (m_invocation != null) {
                throw new IllegalStateException("Reentrant invocation");
            }
            if (requestLock()) {
                m_eds.getExecutorService().execute(runnable);
            } else {
                m_invocation = runnable;
            }
        }

        @Override
        protected void proposeStateChange(ByteBuffer proposedState) {
            super.proposeStateChange(proposedState);
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
}
