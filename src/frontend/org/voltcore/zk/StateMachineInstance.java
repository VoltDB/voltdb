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
/*
 * Rules for each StateMachineInstance:
 * 1. All access to the state machine is serialized by a per state machine local lock.
 * 2. State changes may only be initiated by the oldest ephemeral lock node under the
 *    LOCK_CONTENDERS directory. This represents the distributed lock used to
 *    synchronize proposals by the members of the state machine.
 * 3. When a new member joins an operational state machine, it must grab the lock node
 *    (2) before it can determine the current agreed state. However since it has to
 *    participate and benignly generate null results for all proposals, it can glean
 *    the state from the outcome of the next request it fully participates in. Note
 *    that an operation in progress during initialization of the new member is not
 *    used by the new member because the proposal initiator may have accepted the
 *    results and released the distributed lock before the new member can evaluate the
 *    results to determine the current state.
 * 4. A member holding the distributed lock can propose a state change by clearing out
 *    the reported result nodes under the BARRIER_RESULTS node, updating the
 *    BARRIER_RESULTS node with the old and proposed state, and adding an ephemeral
 *    unique node under BARRIER_PARTICIPANTS to trigger the state machine members.
 *    Each proposal in the BARRIER_RESULTS node has a version number that is used
 *    by other members to distinguish a new proposal from a proposal it has already
 *    processed.
 * 5. When a member detects a transition in the BARRIER_PARTICIPANTS node and a new
 *    version of the BARRIER_RESULTS node, it adds an ephemeral node for itself under
 *    BARRIER_PARTICIPANTS and copies the old and proposed state from the BARRIER_RESULTS
 *    node.
 * 6. Each member evaluates the proposed state and inserts a SUCCESS or FAILURE indication
 *    in a persistent unique node under the BARRIER_RESULTS path.
 * 7. A member may also request that an action be performed by all fully participating
 *    members (including itself) by assigning the existing state and a task proposal
 *    in the BARRIER_RESULTS node with a new version id and then adding a node under the
 *    BARRIER_PARTICIPANTS directory.
 * 8. All members detecting the task request perform the task specified in the task
 *    proposal and inserting a result node under the BARRIER_RESULTS directory.
 * 9. When the set of BARRIER_RESULTS nodes is a superset of all members currently in the
 *    state machine, all members are considered to have reported in and each member can
 *    independently determine the outcome. If the proposal was a state change request,
 *    and all BARRIER_RESULTS nodes report SUCCESS each member applies the new state
 *    and then removes itself from BARRIER_PARTICIPANTS. If the proposal was a task
 *    request, then members remove themselves from BARRIER_PARTICIPANTS to acknowledge
 *    receipt of all task results.
 *10. Membership in the state machine can change as members join or fail. Membership is
 *    monitored by the SynchronizedStatesManager and changes are reported to each state
 *    machine instance. While members have joined but have not determined the current
 *    state, they report a NULL result for each state change or task proposal made by
 *    other members.
 *11. If any member reports FAILURE node under the BARRIER_RESULTS directory after a
 *    state change request proposal, the new state is not applied by any member.
 *12. Each member that has processed BARRIER_RESULTS removes its ephemeral node from the
 *    BARRIER_PARTICIPANTS directory node.
 *13. The initiator of a state change or task releases the lock node when 9 is satisfied.
 *14. A member is not told they have the lock node until the last member releases the
 *    lock node AND all ephemeral nodes under the BARRIER_PARTICIPANTS node are gone,
 *    indicating that all members waiting on the outcome of the last state change or
 *    task results have processed the previous outcome
 */
package org.voltcore.zk;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.logging.VoltLogger;

public abstract class StateMachineInstance {

    private enum REQUEST_TYPE {
        INITIALIZING,
        LAST_CHANGE_OUTCOME_REQUEST,
        STATE_CHANGE_REQUEST,
        CORRELATED_COORDINATED_TASK,
        UNCORRELATED_COORDINATED_TASK
    };

    private enum RESULT_CONCENSUS {
        AGREE,
        DISAGREE,
        NO_QUORUM
    }

    protected final String m_stateMachineId;
    private Set<String> m_knownMembers;
    private boolean m_membershipChangePending = false;
    public final String m_statePath;
    private final String m_lockPath;
    private final String m_barrierResultsPath;
    private final String m_myResultPath;
    private final String m_barrierParticipantsPath;
    private final String m_myPartiticpantsPath;
    private boolean m_stateChangeInitiator = false;
    private String m_ourDistributedLockName = null;
    private String m_lockWaitingOn = null;
    private boolean m_holdingDistributedLock = false;
    protected final VoltLogger m_log;
    private ByteBuffer m_requestedInitialState = ByteBuffer.allocate(0);
    private ByteBuffer m_synchronizedState = null;
    private ByteBuffer m_pendingProposal = null;
    private REQUEST_TYPE m_currentRequestType = REQUEST_TYPE.INITIALIZING;
    private int m_currentParticipants = 0;
    private Set<String> m_memberResults = null;
    private int m_lastProposalVersion = 0;

    private final Lock m_mutex = new ReentrantLock();
    private int m_mutexLockedCnt = 0;
    private final ThreadLocal<Boolean> m_mutexLocked = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return new Boolean(false);
        }
    };

    private final ZooKeeper m_zk;
    private final String m_ssmRootNode;
    private final String m_stateMachineRoot;
    private final String m_stateMachineMemberPath;
    private final String m_memberId;
    private final AtomicBoolean m_done;
    private final ListeningExecutorService m_shared_es;
    private final SynchronizedStatesManager m_mgr;

    private boolean debugVerifyLockAcquire() {
        m_mutexLocked.set(new Boolean(true));
        return m_mutexLockedCnt++ == 0;
    }

    private boolean debugVerifyLockRelease() {
        m_mutexLocked.set(new Boolean(false));
        return (m_mutexLockedCnt-- == 1);
    }

    protected boolean debugIsLocalStateLocked() {
        return m_mutexLocked.get();
    }

    private void lockLocalState() {
        m_mutex.lock();
        assert (debugVerifyLockAcquire());
    }

    // This wrapper resolves getStackTrace correctly when debugging locking
    private void lockLocalStateForLockRunner() {
        lockLocalState();
    }

    // This wrapper resolves getStackTrace correctly when debugging locking
    private void lockLocalStateForResultsRunner() {
        lockLocalState();
    }

    // This wrapper resolves getStackTrace correctly when debugging locking
    private void lockLocalStateForParticipantRunner() {
        lockLocalState();
    }

    private void unlockLocalState() {
        assert (debugVerifyLockRelease());
        m_mutex.unlock();
    }

    private class LockWatcher implements Watcher {

        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_done.get()) {
                    m_shared_es.submit(HandlerForDistributedLockEvent);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    }

    private final LockWatcher m_lockWatcher = new LockWatcher();

    private class BarrierParticipantsWatcher implements Watcher {

        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_done.get()) {
                    m_shared_es.submit(HandlerForBarrierPartiticpantsEvent);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    }

    class StateChangeRequest {

        public final REQUEST_TYPE m_requestType;
        public final ByteBuffer m_previousState;
        public final ByteBuffer m_proposal;

        private StateChangeRequest(REQUEST_TYPE requestType, ByteBuffer previousState, ByteBuffer proposedState) {
            m_requestType = requestType;
            m_previousState = previousState;
            m_proposal = proposedState;
        }
    }

    private final BarrierParticipantsWatcher m_barrierParticipantsWatcher = new BarrierParticipantsWatcher();

    private class BarrierResultsWatcher implements Watcher {

        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_done.get()) {
                    m_shared_es.submit(HandlerForBarrierResultsEvent);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    };

    private final BarrierResultsWatcher m_barrierResultsWatcher = new BarrierResultsWatcher();

//    // Used only for Mocking StateMachineInstance
//    public StateMachineInstance() {
//        m_statePath = "MockInstanceStatePath";
//        m_stateMachineId = "MockInstanceId";
//        m_barrierResultsPath = "MockBarrierResultsPath";
//        m_myResultPath = "MockMyResultPath";
//        m_barrierParticipantsPath = "MockParticipantsPath";
//        m_myPartiticpantsPath = "MockMyParticipantPath";
//        m_lockPath = "MockLockPath";
//        m_log = null;
//    }

    public StateMachineInstance(SynchronizedStatesManager mgr, String instanceName, VoltLogger logger) throws RuntimeException {
        if (instanceName.equals(SynchronizedStatesManager.MEMBERS)) {
            throw new RuntimeException("State machine name may not be named " + SynchronizedStatesManager.MEMBERS);
        }
        assert (!instanceName.equals(SynchronizedStatesManager.MEMBERS));
        m_ssmRootNode = mgr.m_ssmRootNode;
        m_zk = mgr.m_zk;
        m_stateMachineRoot = mgr.m_stateMachineRoot;
        m_stateMachineMemberPath = mgr.m_stateMachineMemberPath;
        m_memberId = mgr.getMemberId();
        m_done = mgr.m_done;
        m_shared_es = SynchronizedStatesManager.m_shared_es;
        m_mgr = mgr;

        m_statePath = ZKUtil.joinZKPath(m_stateMachineRoot, instanceName);
        m_lockPath = ZKUtil.joinZKPath(m_statePath, "LOCK_CONTENDERS");
        m_barrierResultsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_RESULTS");
        m_myResultPath = ZKUtil.joinZKPath(m_barrierResultsPath, m_memberId);
        m_barrierParticipantsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_PARTICIPANTS");
        m_myPartiticpantsPath = ZKUtil.joinZKPath(m_barrierParticipantsPath, m_memberId);
        m_log = logger;
        m_stateMachineId = "SMI " + m_ssmRootNode + "/" + instanceName + "/" + m_memberId;
        m_log.debug(m_stateMachineId + " created.");
    }

    public void registerStateMachineWithManager(ByteBuffer requestedInitialState) throws InterruptedException {
        assert (requestedInitialState != null);
        assert (requestedInitialState.remaining() < Short.MAX_VALUE);
        m_requestedInitialState = requestedInitialState;

        m_mgr.registerStateMachine(this);
    }

    public void initializeStateMachine(Set<String> knownMembers) throws KeeperException, InterruptedException {
        ZKUtil.addIfMissing(m_zk, m_statePath, CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(m_zk, m_lockPath, CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(m_zk, m_barrierParticipantsPath, CreateMode.PERSISTENT, null);
        lockLocalState();
        boolean ownDistributedLock = requestDistributedLock();
        ByteBuffer startStates = buildProposal(REQUEST_TYPE.INITIALIZING,
                m_requestedInitialState.asReadOnlyBuffer(), m_requestedInitialState.asReadOnlyBuffer());
        ZKUtil.addIfMissing(m_zk, m_barrierResultsPath, CreateMode.PERSISTENT, startStates.array());
        boolean stateMachineNodeCreated = false;
        if (ownDistributedLock) {
                // Only the very first initializer of the state machine will both get the lock and successfully
            // allocate "STATE_INITIALIZED". This guarantees that only one node will assign the initial state.
            stateMachineNodeCreated = ZKUtil.addIfMissing(m_zk, ZKUtil.joinZKPath(m_statePath, "STATE_INITIALIZED"),
                    CreateMode.PERSISTENT, null);
        }

        if (m_membershipChangePending) {
            getLatestMembership();
        } else {
            m_knownMembers = knownMembers;
        }
            // We need to always monitor participants so that if we are initialized we can add ourselves and insert
        // our results and if we are not initialized, we can always auto-insert a null result.
        if (stateMachineNodeCreated) {
            assert (ownDistributedLock);
            m_synchronizedState = m_requestedInitialState;
            m_requestedInitialState = null;
            m_lastProposalVersion = getProposalVersion();
            ByteBuffer readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
            // Add an acceptable result so the next initializing member recognizes an immediate quorum.
            byte result[] = new byte[1];
            result[0] = (byte) (1);
            addResultEntry(result);
            m_lockWaitingOn = "bogus"; // Avoids call to notifyDistributedLockWaiter
            m_log.info(m_stateMachineId + ": Initialized (first member) with State "
                    + stateToString(m_synchronizedState.asReadOnlyBuffer()));
            cancelDistributedLock();
            checkForBarrierParticipantsChange();
            // Notify the derived object that we have a stable state
            setInitialState(readOnlyResult);
        } else {
                // To get a stable result set, we need to get the lock for this state machine. If someone else has the
            // lock they can clear the stale results out from under us.
            if (ownDistributedLock) {
                initializeFromActiveCommunity();
            } else {
                    // This means we will ignore the current update if there is one in progress.
                // Note that if we are not the next waiter for the lock, we will blindly
                // accept the next proposal and use the outcome to set our initial state.
                addResultEntry(null);
                Stat nodeStat = new Stat();
                boolean resultNodeFound = false;
                {
                    try {
                        m_zk.getData(m_barrierResultsPath, false, nodeStat);
                        resultNodeFound = true;
                    } catch (KeeperException.NoNodeException noNode) {
                            // This is a race between another node who got the Distributed lock but
                        // Has not set up the result path yet. Keep Retrying.
                    }
                }
                while (!resultNodeFound);
                m_lastProposalVersion = nodeStat.getVersion();
                checkForBarrierParticipantsChange();
            }
            assert (!debugIsLocalStateLocked());
        }
    }

    /*
     * This state machine and all other state machines under this manager are being removed
     */
    public void disableMembership() throws InterruptedException {
        lockLocalState();
        try {
            m_zk.delete(m_myPartiticpantsPath, -1);
            if (m_ourDistributedLockName != null) {
                m_zk.delete(m_ourDistributedLockName, -1);
            }
        } catch (KeeperException e) {
        }
        unlockLocalState();
    }

    private int getProposalVersion() {
        int proposalVersion = -1;
        try {
            Stat nodeStat = new Stat();
            m_zk.getData(m_barrierResultsPath, null, nodeStat);
            proposalVersion = nodeStat.getVersion();
        } catch (KeeperException.SessionExpiredException e) {
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in getProposalVersion");
        } catch (KeeperException.ConnectionLossException e) {
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in getProposalVersion");
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in getProposalVersion");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
        return proposalVersion;
    }

    private ByteBuffer buildProposal(REQUEST_TYPE requestType, ByteBuffer existingState, ByteBuffer proposedState) {
        ByteBuffer states = ByteBuffer.allocate(proposedState.remaining() + existingState.remaining() + 4);
        states.putShort((short) requestType.ordinal());
        states.putShort((short) existingState.remaining());
        states.put(existingState);
        states.put(proposedState);
        states.flip();
        return states;
    }

    private StateChangeRequest getExistingAndProposedBuffersFromResultsNode(byte oldAndNewState[]) {
        // Either the barrier or the state node should have the current state
        assert (oldAndNewState != null);
        ByteBuffer states = ByteBuffer.wrap(oldAndNewState);
        // Maximum state size is 64K
        REQUEST_TYPE requestType = REQUEST_TYPE.values()[states.getShort()];
        int oldStateSize = states.getShort();
        states.position(oldStateSize + 4);
        ByteBuffer proposedState = states.slice();
        states.flip();      // just the old state
        states.position(4);
        states.limit(oldStateSize + 4);
        return new StateChangeRequest(requestType, states, proposedState);
    }

    private void checkForBarrierParticipantsChange() {
        assert (debugIsLocalStateLocked());
        try {
            Set<String> children = ImmutableSet.copyOf(m_zk.getChildren(m_barrierParticipantsPath, m_barrierParticipantsWatcher));
            Stat nodeStat = new Stat();
                // inspect the m_barrierResultsPath and get the new and old states and version
            // At some point this can be optimized to not examine the proposal all the time
            byte statePair[] = m_zk.getData(m_barrierResultsPath, false, nodeStat);
            int proposalVersion = nodeStat.getVersion();
            if (proposalVersion != m_lastProposalVersion) {
                m_lastProposalVersion = proposalVersion;
                m_currentParticipants = children.size();
                if (!m_stateChangeInitiator) {
                    assert (m_pendingProposal == null);

                    // This is an indication that a new state change is being requested
                    StateChangeRequest existingAndProposedStates = getExistingAndProposedBuffersFromResultsNode(statePair);
                    m_currentRequestType = existingAndProposedStates.m_requestType;
                    if (m_requestedInitialState != null) {
                        // Since we have not initialized yet, we acknowledge this proposal with an empty result.
                        addResultEntry(null);
                        unlockLocalState();
                    } else {
                        // Don't add ourselves as a participant because we don't care about the results
                        REQUEST_TYPE type = m_currentRequestType;
                        if (type == REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST) {
                            // The request is from a new member who found the results in an ambiguous state.
                            byte result[] = new byte[1];
                            if (existingAndProposedStates.m_proposal.equals(m_synchronizedState)) {
                                result[0] = (byte) 1;
                                addResultEntry(result);
                            } else {
                                assert (existingAndProposedStates.m_previousState.equals(m_synchronizedState));
                                result[0] = (byte) 0;
                                addResultEntry(result);
                            }
                            unlockLocalState();
                        } else {
                                // We track the number of people waiting on the results so we know when the result is stale and
                            // the next lock holder can initiate a new state proposal.
                            m_zk.create(m_myPartiticpantsPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                            m_pendingProposal = existingAndProposedStates.m_proposal;
                            ByteBuffer proposedState = m_pendingProposal.asReadOnlyBuffer();
                            assert (existingAndProposedStates.m_previousState.equals(m_synchronizedState));
                            if (m_log.isDebugEnabled()) {
                                if (type == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                                    m_log.debug(m_stateMachineId + ": Received new State proposal "
                                            + stateToString(proposedState.asReadOnlyBuffer()));
                                } else {
                                    m_log.debug(m_stateMachineId + ": Received new Task request "
                                            + taskToString(proposedState.asReadOnlyBuffer()));
                                }
                            }
                            unlockLocalState();
                            if (type == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                                stateChangeProposed(proposedState);
                            } else {
                                taskRequested(proposedState);
                            }
                        }
                    }
                } else {
                    // We initiated a proposal and if it is a TASK we need to call our derived object to kick it off
                    assert (m_pendingProposal != null);
                    if (m_currentRequestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK
                            || m_currentRequestType == REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK) {
                            // This is a Task request made by us so notify the derived state machine to perform the task
                        // provide a result.
                        ByteBuffer taskRequest = m_pendingProposal.asReadOnlyBuffer();
                        unlockLocalState();
                        taskRequested(taskRequest);
                    } else {
                        unlockLocalState();
                    }
                }
            } else {
                m_currentParticipants = children.size();
                if (m_ourDistributedLockName != null && m_ourDistributedLockName == m_lockWaitingOn && children.size() == 0) {
                    // We can finally notify the lock waiter because everyone is finished evaluating the previous state proposal
                    notifyDistributedLockWaiter();
                } else {
                    unlockLocalState();
                }
            }
        } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
            // means zk shutdown without the elector being shutdown.
            // ignore.
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in checkForBarrierParticipantsChange");
            unlockLocalState();
        } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
            // means shutdown without the elector being
            // shutdown; ignore.
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in checkForBarrierParticipantsChange");
            unlockLocalState();
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in checkForBarrierParticipantsChange");
            unlockLocalState();
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
        assert (!debugIsLocalStateLocked());
    }

    private void monitorParticipantChanges() {
            // always start checking for participation changes after the result notifications
        // or initialization notifications to ensure these notifications happen before lock
        // ownership notifications.
        lockLocalState();
        checkForBarrierParticipantsChange();
    }

    private RESULT_CONCENSUS resultsAgreeOnSuccess(Set<String> memberList) throws Exception {
        boolean agree = false;
        for (String memberId : memberList) {
            byte result[];
            try {
                result = m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, memberId), false, null);
                if (result != null) {
                    if (result[0] == 0) {
                        return RESULT_CONCENSUS.DISAGREE;
                    }
                    agree = true;
                }
            } catch (KeeperException.NoNodeException ke) {
                    // This can happen when a new member joins and other members detect the new member before
                // it's initialization code is called and a Null result is supplied to treat this as a null result.
            }
        }
        if (agree) {
            return RESULT_CONCENSUS.AGREE;
        }
        return RESULT_CONCENSUS.NO_QUORUM;
    }

    private ArrayList<ByteBuffer> getUncorrelatedResults(ByteBuffer taskRequest, Set<String> memberList) {
        // Treat ZooKeeper failures as empty result
        ArrayList<ByteBuffer> results = new ArrayList<ByteBuffer>();
        try {
            for (String memberId : memberList) {
                byte result[];
                result = m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, memberId), false, null);
                if (result != null) {
                    ByteBuffer bb = ByteBuffer.wrap(result);
                    results.add(bb);
                    m_log.debug(m_stateMachineId + ":    " + memberId + " reports Result "
                            + taskResultToString(taskRequest.asReadOnlyBuffer(), bb.asReadOnlyBuffer()));
                } else {
                    m_log.debug(m_stateMachineId + ":    " + memberId + " did not supply a Task Result");
                }
            }
            // Remove ourselves from the participants list to unblock the next distributed lock waiter
            m_zk.delete(m_myPartiticpantsPath, -1);
        } catch (KeeperException.SessionExpiredException e) {
            results = new ArrayList<ByteBuffer>();
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in getUncorrelatedResults");
        } catch (KeeperException.ConnectionLossException e) {
            results = new ArrayList<ByteBuffer>();
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in getUncorrelatedResults");
        } catch (InterruptedException e) {
            results = new ArrayList<ByteBuffer>();
            m_log.debug(m_stateMachineId + ": Received InterruptedException in getUncorrelatedResults");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
        return results;
    }

    private Map<String, ByteBuffer> getCorrelatedResults(ByteBuffer taskRequest, Set<String> memberList) {
        // Treat ZooKeeper failures as empty result
        Map<String, ByteBuffer> results = new HashMap<String, ByteBuffer>();
        try {
            for (String memberId : memberList) {
                byte result[];
                result = m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, memberId), false, null);
                if (result != null) {
                    ByteBuffer bb = ByteBuffer.wrap(result);
                    results.put(memberId, bb);
                    m_log.debug(m_stateMachineId + ":    " + memberId + " reports Result "
                            + taskResultToString(taskRequest.asReadOnlyBuffer(), bb.asReadOnlyBuffer()));
                } else {
                    m_log.debug(m_stateMachineId + ":    " + memberId + " did not supply a Task Result");
                }
            }
            // Remove ourselves from the participants list to unblock the next distributed lock waiter
            m_zk.delete(m_myPartiticpantsPath, -1);
        } catch (KeeperException.SessionExpiredException e) {
            results = new HashMap<String, ByteBuffer>();
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in getCorrelatedResults");
        } catch (KeeperException.ConnectionLossException e) {
            results = new HashMap<String, ByteBuffer>();
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in getCorrelatedResults");
        } catch (InterruptedException e) {
            results = new HashMap<String, ByteBuffer>();
            m_log.debug(m_stateMachineId + ": Received InterruptedException in getCorrelatedResults");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
        return results;
    }

    // The number of results is a superset of the membership so analyze the results
    private void processResultQuorum(Set<String> memberList) {
        assert (m_currentRequestType != REQUEST_TYPE.INITIALIZING);
        m_memberResults = null;
        if (m_requestedInitialState != null) {
            // We can now initialize this state machine instance
            assert (m_holdingDistributedLock);
            try {
                assert (m_synchronizedState == null);
                Stat versionInfo = new Stat();
                byte oldAndProposedState[] = m_zk.getData(m_barrierResultsPath, false, versionInfo);
                assert (versionInfo.getVersion() == m_lastProposalVersion);
                StateChangeRequest existingAndProposedStates
                        = getExistingAndProposedBuffersFromResultsNode(oldAndProposedState);
                m_currentRequestType = existingAndProposedStates.m_requestType;

                    // We are waiting to initialize. We are here either because we are piggy-backing
                // another member's proposal or because we initiated a LAST_CHANGE_OUTCOME_REQUEST
                if (m_currentRequestType == REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST) {
                    // We don't have a result yet but it is available
                    RESULT_CONCENSUS result = resultsAgreeOnSuccess(memberList);
                    if (result == RESULT_CONCENSUS.NO_QUORUM) {
                            // Bad news. There is no definitive state so if we are the distributed
                        // lock owner we will assign our initial state as our initial state
                        if (m_stateChangeInitiator) {
                            m_synchronizedState = m_requestedInitialState;
                            ByteBuffer stableState = buildProposal(REQUEST_TYPE.INITIALIZING,
                                    m_synchronizedState.asReadOnlyBuffer(), m_synchronizedState.asReadOnlyBuffer());
                            Stat newProposalStat = m_zk.setData(m_barrierResultsPath, stableState.array(), -1);
                            m_lastProposalVersion = newProposalStat.getVersion();
                        }
                    } else {
                        // There was at least one member that knows the definitive state
                        if (result == RESULT_CONCENSUS.AGREE) {
                            m_synchronizedState = existingAndProposedStates.m_proposal;
                        } else {
                            m_synchronizedState = existingAndProposedStates.m_previousState;
                        }
                    }
                } else if (m_currentRequestType == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                    // We don't have a result yet but it is available
                    RESULT_CONCENSUS result = resultsAgreeOnSuccess(memberList);
                    if (result == RESULT_CONCENSUS.AGREE) {
                        m_synchronizedState = existingAndProposedStates.m_proposal;
                    } else {
                        assert (result == RESULT_CONCENSUS.DISAGREE);
                        m_synchronizedState = existingAndProposedStates.m_previousState;
                    }
                } else {
                    // If we are processing a TASK or a updated STABLE result, we don't need a quorum
                    m_synchronizedState = existingAndProposedStates.m_previousState;
                }

                // Remove ourselves from the participants list to unblock the next distributed lock waiter
                m_zk.delete(m_myPartiticpantsPath, -1);
            } catch (KeeperException.SessionExpiredException e) {
                m_log.debug(m_stateMachineId + ": Received SessionExpiredException in processResultQuorum");
            } catch (KeeperException.ConnectionLossException e) {
                m_log.debug(m_stateMachineId + ": Received ConnectionLossException in processResultQuorum");
            } catch (InterruptedException e) {
                m_log.debug(m_stateMachineId + ": Received InterruptedException in processResultQuorum");
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            if (m_stateChangeInitiator) {
                m_stateChangeInitiator = false;
                cancelDistributedLock();
            }
            if (m_synchronizedState != null) {
                ByteBuffer readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
                // We were not yet participating in the state machine so but now we can
                m_requestedInitialState = null;
                m_pendingProposal = null;
                m_log.info(m_stateMachineId + ": Initialized (concensus) with State "
                        + stateToString(m_synchronizedState.asReadOnlyBuffer()));
                unlockLocalState();
                setInitialState(readOnlyResult);

                    // If we are ready to provide an initial state to the derived state machine, add us to
                // participants watcher so we can see the next request
                monitorParticipantChanges();
            } else {
                unlockLocalState();
            }
        } else {
            assert (m_currentRequestType != REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST);
            boolean initiator = m_stateChangeInitiator;
            boolean success = false;
            if (m_currentRequestType == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                // Treat ZooKeeper failures as unsuccessful proposal
                ByteBuffer attemptedChange = m_pendingProposal.asReadOnlyBuffer();
                try {
                    // We don't have a result yet but it is available
                    RESULT_CONCENSUS result = resultsAgreeOnSuccess(memberList);
                    // Now that we have a result we can remove ourselves from the participants list.
                    m_zk.delete(m_myPartiticpantsPath, -1);
                    if (result == RESULT_CONCENSUS.AGREE) {
                        success = true;
                        m_synchronizedState = m_pendingProposal;
                    } else {
                        assert (result == RESULT_CONCENSUS.DISAGREE);
                    }
                    m_pendingProposal = null;
                    if (m_stateChangeInitiator) {
                        // Since we don't care if we are the last to go away, remove ourselves from the participant list
                        assert (m_holdingDistributedLock);
                        m_stateChangeInitiator = false;
                        cancelDistributedLock();
                    }
                } catch (KeeperException.SessionExpiredException e) {
                    success = false;
                    m_log.debug(m_stateMachineId + ": Received SessionExpiredException in processResultQuorum");
                } catch (KeeperException.ConnectionLossException e) {
                    success = false;
                    m_log.debug(m_stateMachineId + ": Received ConnectionLossException in processResultQuorum");
                } catch (InterruptedException e) {
                    success = false;
                    m_log.debug(m_stateMachineId + ": Received InterruptedException in processResultQuorum");
                } catch (Exception e) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Unexepected failure in StateMachine.", true, e);
                    success = false;
                }
                m_log.info(m_stateMachineId + ": Proposed state " + (success ? "succeeded " : "failed ")
                        + stateToString(attemptedChange.asReadOnlyBuffer()));
                unlockLocalState();
                // Notify the derived state machine engine of the current state
                proposedStateResolved(initiator, attemptedChange, success);
                monitorParticipantChanges();
            } else {
                // Process the results of a TASK request
                ByteBuffer taskRequest = m_pendingProposal.asReadOnlyBuffer();
                m_pendingProposal = null;
                m_log.info(m_stateMachineId + ": All members completed task " + taskToString(taskRequest.asReadOnlyBuffer()));
                if (m_currentRequestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK) {
                    Map<String, ByteBuffer> results = getCorrelatedResults(taskRequest, memberList);
                    if (m_stateChangeInitiator) {
                        // Since we don't care if we are the last to go away, remove ourselves from the participant list
                        assert (m_holdingDistributedLock);
                        m_stateChangeInitiator = false;
                        cancelDistributedLock();
                    }
                    unlockLocalState();
                    correlatedTaskCompleted(initiator, taskRequest, results);
                } else {
                    ArrayList<ByteBuffer> results = getUncorrelatedResults(taskRequest, memberList);
                    if (m_stateChangeInitiator) {
                        // Since we don't care if we are the last to go away, remove ourselves from the participant list
                        assert (m_holdingDistributedLock);
                        m_stateChangeInitiator = false;
                        cancelDistributedLock();
                    }
                    unlockLocalState();
                    uncorrelatedTaskCompleted(initiator, taskRequest, results);
                }
                monitorParticipantChanges();
            }
        }
    }

    private void checkForBarrierResultsChanges() {
        assert (debugIsLocalStateLocked());
        if (m_pendingProposal == null) {
            // Don't check for barrier results until we notice the participant list change.
            unlockLocalState();
            return;
        }
        Set<String> membersWithResults;
        try {
            membersWithResults = ImmutableSet.copyOf(m_zk.getChildren(m_barrierResultsPath,
                    m_barrierResultsWatcher));
        } catch (KeeperException.SessionExpiredException e) {
            membersWithResults = new TreeSet<String>(Arrays.asList("We died so avoid Quorum path"));
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in checkForBarrierResultsChanges");
        } catch (KeeperException.ConnectionLossException e) {
            membersWithResults = new TreeSet<String>(Arrays.asList("We died so avoid Quorum path"));
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in checkForBarrierResultsChanges");
        } catch (InterruptedException e) {
            membersWithResults = new TreeSet<String>(Arrays.asList("We died so avoid Quorum path"));
            m_log.debug(m_stateMachineId + ": Received InterruptedException in checkForBarrierResultsChanges");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
            membersWithResults = new TreeSet<String>();
        }
        if (Sets.difference(m_knownMembers, membersWithResults).isEmpty()) {
            processResultQuorum(membersWithResults);
            assert (!debugIsLocalStateLocked());
        } else {
            m_memberResults = membersWithResults;
            unlockLocalState();
        }
    }

    /*
     * Assumes this state machine owns the distributed lock and can either interrogate the existing state
     * or request the current state from the community (if the existing state is ambiguous)
     */
    private void initializeFromActiveCommunity() {
        ByteBuffer readOnlyResult = null;
        ByteBuffer staleTask = null;
        byte oldAndProposedState[];
        try {
            Stat lastProposal = new Stat();
            oldAndProposedState = m_zk.getData(m_barrierResultsPath, false, lastProposal);
            StateChangeRequest existingAndProposedStates
                    = getExistingAndProposedBuffersFromResultsNode(oldAndProposedState);
            if (existingAndProposedStates.m_requestType == REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST) {
                RESULT_CONCENSUS result = resultsAgreeOnSuccess(m_knownMembers);
                if (result == RESULT_CONCENSUS.AGREE) {
                    // Fall through to set up the initial state and provide notification of initialization
                    existingAndProposedStates = new StateChangeRequest(REQUEST_TYPE.INITIALIZING,
                            existingAndProposedStates.m_proposal.asReadOnlyBuffer(),
                            existingAndProposedStates.m_proposal.asReadOnlyBuffer());
                } else if (result == RESULT_CONCENSUS.DISAGREE) {
                    // Fall through to set up the initial state and provide notification of initialization
                    existingAndProposedStates = new StateChangeRequest(REQUEST_TYPE.INITIALIZING,
                            existingAndProposedStates.m_previousState.asReadOnlyBuffer(),
                            existingAndProposedStates.m_previousState.asReadOnlyBuffer());
                } else {
                        // Another members outcome request was completed but a subsequent proposing member died
                    // between the time the results of this last request were removed and the new proposal
                    // was made. Fall through and propose a new LAST_CHANGE_OUTCOME_REQUEST.
                    existingAndProposedStates = new StateChangeRequest(REQUEST_TYPE.STATE_CHANGE_REQUEST,
                            existingAndProposedStates.m_previousState.asReadOnlyBuffer(),
                            existingAndProposedStates.m_proposal.asReadOnlyBuffer());
                }
            }

            if (existingAndProposedStates.m_requestType == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                // The last lock owner died before completing a state change or determining the current state
                m_stateChangeInitiator = true;
                m_pendingProposal = m_requestedInitialState;
                m_currentRequestType = REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST;
                ByteBuffer stateChange = buildProposal(REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST,
                        existingAndProposedStates.m_previousState, existingAndProposedStates.m_proposal);
                m_lastProposalVersion = wakeCommunityWithProposal(stateChange.array());
                addResultEntry(null);
                checkForBarrierResultsChanges();
            } else {
                assert (existingAndProposedStates.m_requestType == REQUEST_TYPE.INITIALIZING
                        || existingAndProposedStates.m_requestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK
                        || existingAndProposedStates.m_requestType == REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK);
                m_synchronizedState = existingAndProposedStates.m_previousState;
                m_requestedInitialState = null;
                readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
                m_lastProposalVersion = lastProposal.getVersion();
                m_pendingProposal = null;
                m_log.info(m_stateMachineId + ": Initialized (existing) with State "
                        + stateToString(m_synchronizedState.asReadOnlyBuffer()));
                if (existingAndProposedStates.m_requestType != REQUEST_TYPE.INITIALIZING) {
                    staleTask = existingAndProposedStates.m_proposal.asReadOnlyBuffer();
                }
                cancelDistributedLock();
                // Add an acceptable result so the next initializing member recognizes an immediate quorum.
                m_lockWaitingOn = "bogus"; // Avoids call to notifyDistributedLockWaiter below
                checkForBarrierParticipantsChange();
            }
        } catch (KeeperException.SessionExpiredException e) {
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in initializeFromActiveCommunity");
        } catch (KeeperException.ConnectionLossException e) {
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in initializeFromActiveCommunity");
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in initializeFromActiveCommunity");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
        if (readOnlyResult != null) {
            // Notify the derived object that we have a stable state
            setInitialState(readOnlyResult);
        }
        if (staleTask != null) {
            staleTaskRequestNotification(staleTask);
        }
    }

    private int wakeCommunityWithProposal(byte[] proposal) {
        assert (m_holdingDistributedLock);
        assert (m_currentParticipants == 0);
        int newProposalVersion = -1;
        try {
            List<String> results = m_zk.getChildren(m_barrierResultsPath, false);
            for (String resultNode : results) {
                m_zk.delete(ZKUtil.joinZKPath(m_barrierResultsPath, resultNode), -1);
            }
            Stat newProposalStat = m_zk.setData(m_barrierResultsPath, proposal, -1);
            m_zk.create(m_myPartiticpantsPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            newProposalVersion = newProposalStat.getVersion();
        } catch (KeeperException.SessionExpiredException e) {
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in wakeCommunityWithProposal");
        } catch (KeeperException.ConnectionLossException e) {
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in wakeCommunityWithProposal");
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in wakeCommunityWithProposal");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
        return newProposalVersion;
    }

    private final Runnable HandlerForBarrierPartiticpantsEvent = new Runnable() {
        @Override
        public void run() {
            lockLocalStateForParticipantRunner();
            checkForBarrierParticipantsChange();
            assert (!debugIsLocalStateLocked());
        }
    };

    private final Runnable HandlerForBarrierResultsEvent = new Runnable() {
        @Override
        public void run() {
            lockLocalStateForResultsRunner();
            checkForBarrierResultsChanges();
            assert (!debugIsLocalStateLocked());
        }
    };

    private String getNextLockNodeFromList() throws Exception {
        List<String> lockRequestors = m_zk.getChildren(m_lockPath, false);
        Collections.sort(lockRequestors);
        ListIterator<String> iter = lockRequestors.listIterator();
        String currRequestor = null;
        while (iter.hasNext()) {
            currRequestor = ZKUtil.joinZKPath(m_lockPath, iter.next());
            if (currRequestor.equals(m_ourDistributedLockName)) {
                break;
            }
        }
        assert (currRequestor != null); // We should be able to find ourselves
        //Back on currRequestor
        iter.previous();
        String nextLower = null;
        //Until we have previous nodes and we set a watch on previous node.
        while (iter.hasPrevious()) {
            //Process my lower nodes and put a watch on whats live
            String previous = ZKUtil.joinZKPath(m_lockPath, iter.previous());
            if (m_zk.exists(previous, m_lockWatcher) != null) {
                m_log.debug(m_stateMachineId + ": " + m_ourDistributedLockName + " waiting on " + previous);
                nextLower = previous;
                break;
            }
        }
        //If we could not watch any lower node we are lowest and must own the lock.
        if (nextLower == null) {
            return m_ourDistributedLockName;
        }
        return nextLower;
    }

    private final Runnable HandlerForDistributedLockEvent = new Runnable() {
        @Override
        public void run() {
            lockLocalStateForLockRunner();
            if (m_ourDistributedLockName != null) {
                try {
                    m_lockWaitingOn = getNextLockNodeFromList();
                } catch (KeeperException.SessionExpiredException e) {
                    m_log.debug(m_stateMachineId + ": Received SessionExpiredException in HandlerForDistributedLockEvent");
                    m_lockWaitingOn = "We died so we can't ever get the distributed lock";
                } catch (KeeperException.ConnectionLossException e) {
                    m_log.debug(m_stateMachineId + ": Received ConnectionLossException in HandlerForDistributedLockEvent");
                    m_lockWaitingOn = "We died so we can't ever get the distributed lock";
                } catch (InterruptedException e) {
                    m_log.debug(m_stateMachineId + ": Received InterruptedException in HandlerForDistributedLockEvent");
                    m_lockWaitingOn = "We died so we can't ever get the distributed lock";
                } catch (Exception e) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Unexepected failure in StateMachine.", true, e);
                }
                if (m_lockWaitingOn.equals(m_ourDistributedLockName) && m_currentParticipants == 0) {
                    // There are no more members still processing the last result
                    notifyDistributedLockWaiter();
                } else {
                    unlockLocalState();
                }
            } else {
                // lock was cancelled
                unlockLocalState();
            }
            assert (!debugIsLocalStateLocked());
        }
    };

    private void cancelDistributedLock() {
        assert (debugIsLocalStateLocked());
        m_log.debug(m_stateMachineId + ": cancelLockRequest for " + m_ourDistributedLockName);
        assert (m_holdingDistributedLock);
        try {
            m_zk.delete(m_ourDistributedLockName, -1);
        } catch (KeeperException.SessionExpiredException e) {
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in cancelDistributedLock");
        } catch (KeeperException.ConnectionLossException e) {
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in cancelDistributedLock");
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in cancelDistributedLock");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in ", true, e);
        }
        m_ourDistributedLockName = null;
        m_holdingDistributedLock = false;
    }

    /*
     * Warns about membership change notification waiting in the executor thread
     */
    public void checkMembership() {
        lockLocalState();
        m_membershipChangePending = true;
        unlockLocalState();
    }

    /*
     * Callback notification from executor thread of membership change
     */
    public void membershipChanged(Set<String> knownHosts, Set<String> addedMembers, Set<String> removedMembers) {
        lockLocalState();
        // Even though we got a direct update, membership could have changed again between the
        m_knownMembers = knownHosts;
        m_membershipChangePending = false;
        boolean notInitializing = m_requestedInitialState == null;
        if (m_pendingProposal != null && m_memberResults != null
                && Sets.difference(m_knownMembers, m_memberResults).isEmpty()) {
            // We can stop watching for results since we have a quorum.
            processResultQuorum(m_memberResults);
        } else {
            unlockLocalState();
        }
        if (notInitializing) {
            membershipChanged(addedMembers, removedMembers);
        }
        assert (!debugIsLocalStateLocked());
    }

    /*
     * Retrieves member set from ZooKeeper
     */
    private void getLatestMembership() {
        try {
            m_knownMembers = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, null));
        } catch (KeeperException.SessionExpiredException e) {
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in getLatestMembership");
        } catch (KeeperException.ConnectionLossException e) {
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in getLatestMembership");
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in getLatestMembership");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in ", true, e);
        }
    }

    private void notifyDistributedLockWaiter() {
        assert (debugIsLocalStateLocked());
        assert (m_currentParticipants == 0);
        m_holdingDistributedLock = true;
        if (m_membershipChangePending) {
            getLatestMembership();
        }
        if (m_requestedInitialState != null) {
                // We are still waiting to initialize the state. Now that we have have the state lock we can
            // look at the last set of results. We need to set proposedState so results will be checked.
            assert (m_pendingProposal == null);
            m_pendingProposal = m_requestedInitialState;
            initializeFromActiveCommunity();
            assert (!debugIsLocalStateLocked());
        } else {
            m_log.debug(m_stateMachineId + ": Granted lockRequest for " + m_ourDistributedLockName);
            m_lockWaitingOn = null;
            unlockLocalState();
            // Notify the derived class that the lock is available
            lockRequestCompleted();
        }
    }

    private boolean requestDistributedLock() {
        try {
            assert (m_ourDistributedLockName == null);
            assert (debugIsLocalStateLocked());
            m_ourDistributedLockName = m_zk.create(ZKUtil.joinZKPath(m_lockPath, "lock_"), null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            m_lockWaitingOn = getNextLockNodeFromList();
            if (m_lockWaitingOn.equals(m_ourDistributedLockName) && m_currentParticipants == 0) {
                // Prevents a second notification.
                m_log.debug(m_stateMachineId + ": requestLock successful for " + m_ourDistributedLockName);
                m_lockWaitingOn = null;
                m_holdingDistributedLock = true;
                if (m_membershipChangePending) {
                    getLatestMembership();
                }
                return true;
            }
        } catch (KeeperException.SessionExpiredException e) {
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in requestDistributedLock");
        } catch (KeeperException.ConnectionLossException e) {
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in requestDistributedLock");
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in requestDistributedLock");
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
        return false;
    }

    private void addResultEntry(byte result[]) {
        try {
            m_zk.create(m_myResultPath, result, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.SessionExpiredException e) {
            m_log.debug(m_stateMachineId + ": Received SessionExpiredException in addResultEntry");
        } catch (KeeperException.ConnectionLossException e) {
            m_log.debug(m_stateMachineId + ": Received ConnectionLossException in addResultEntry");
        } catch (InterruptedException e) {
            m_log.debug(m_stateMachineId + ": Received InterruptedException in addResultEntry");
        } catch (KeeperException.NodeExistsException e) {
                // There is a race during initialization where a null result is assigned once so a current proposal
            // is not hung. However, if a new proposal is submitted by another instance between that assignment
            // and the call to checkForBarrierParticipantsChange, a second null result is assigned.
            if (m_requestedInitialState == null || result != null) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine; Two results created for one proposal.", true, e);
            }
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Unexepected failure in StateMachine.", true, e);
        }
    }

    private void assignStateChangeAgreement(boolean acceptable) {
        assert (debugIsLocalStateLocked());
        assert (m_pendingProposal != null);
        assert (m_currentRequestType == REQUEST_TYPE.STATE_CHANGE_REQUEST);
        byte result[] = new byte[1];
        result[0] = (byte) (acceptable ? 1 : 0);
        addResultEntry(result);
        if (acceptable) {
            // Since we are only interested in the results when we agree with the proposal
            checkForBarrierResultsChanges();
        } else {
            try {
                // Since we don't care about the outcome remove ourself from the participant list
                m_zk.delete(m_myPartiticpantsPath, -1);
            } catch (KeeperException.SessionExpiredException e) {
                m_log.debug(m_stateMachineId + ": Received SessionExpiredException in assignStateChangeAgreement");
            } catch (KeeperException.ConnectionLossException e) {
                m_log.debug(m_stateMachineId + ": Received ConnectionLossException in assignStateChangeAgreement");
            } catch (InterruptedException e) {
                m_log.debug(m_stateMachineId + ": Received InterruptedException in assignStateChangeAgreement");
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            unlockLocalState();
        }
    }

    /*
     * Returns true when this state machine is a full participant of the community and has the
     * current state of the community
     */
    protected boolean isInitialized() {
        boolean initialized;
        lockLocalState();
        initialized = m_requestedInitialState == null;
        unlockLocalState();
        return initialized;
    }

    /*
     * Notifies of full participation and the current agreed state
     * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected abstract void setInitialState(ByteBuffer currentAgreedState);

    /*
     * Attempts to get the distributed lock. Returns true if the lock was acquired
     */
    protected boolean requestLock() {
        lockLocalState();
        boolean rslt = requestDistributedLock();
        unlockLocalState();
        return rslt;
    }

    /*
     * Cancels an outstanding lock request. Should only be used if a previously desirable
     * proposal is now obsolete (ie. use when lockRequestCompleted is called and a proposal
     * is no longer necessary).
     */
    protected void cancelLockRequest() {
        lockLocalState();
        assert (m_pendingProposal == null);
        cancelDistributedLock();
        unlockLocalState();
    }

    /*
     * Notifies of the successful completion of a previous lock request
     */
    protected void lockRequestCompleted() {
    }

    /*
     * Propose a new state. Only call after successful acquisition of the distributed lock.
     */
    protected void proposeStateChange(ByteBuffer proposedState) {
        assert (proposedState != null);
        assert (proposedState.remaining() < Short.MAX_VALUE);
        lockLocalState();
        // Only the lock owner can initiate a barrier request
        assert (m_requestedInitialState == null);
        if (proposedState.position() == 0) {
            m_pendingProposal = proposedState;
        } else {
            // Move to a new 0 aligned buffer
            m_pendingProposal = ByteBuffer.allocate(proposedState.remaining());
            m_pendingProposal.put(proposedState.array(),
                    proposedState.arrayOffset() + proposedState.position(), proposedState.remaining());
            m_pendingProposal.flip();
        }
        m_log.debug(m_stateMachineId + ": Proposing new state " + stateToString(m_pendingProposal.asReadOnlyBuffer()));
        m_stateChangeInitiator = true;
        m_currentRequestType = REQUEST_TYPE.STATE_CHANGE_REQUEST;
        ByteBuffer stateChange = buildProposal(REQUEST_TYPE.STATE_CHANGE_REQUEST,
                m_synchronizedState.asReadOnlyBuffer(), m_pendingProposal.asReadOnlyBuffer());
        m_lastProposalVersion = wakeCommunityWithProposal(stateChange.array());
        assignStateChangeAgreement(true);
    }

    /*
     * Notification of a new proposed state change by another member.
     * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected void stateChangeProposed(ByteBuffer proposedState) {
    }

    /*
     * Called to accept or reject a new proposed state change by another member.
     */
    protected void requestedStateChangeAcceptable(boolean acceptable) {
        lockLocalState();
        assert (!m_stateChangeInitiator);
        m_log.debug(m_stateMachineId + (acceptable ? ": Agrees with State proposal"
                : ": Disagrees with State proposal"));
        assignStateChangeAgreement(acceptable);
    }

    /*
     * Notification of the outcome of a previous state change proposal.
     * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
    }

    /*
     * Propose a new task. Only call after successful acquisition of the distributed lock.
     */
    protected void initiateCoordinatedTask(boolean correlated, ByteBuffer proposedTask) {
        assert (proposedTask != null);
        assert (proposedTask.remaining() < Short.MAX_VALUE);
        lockLocalState();
        // Only the lock owner can initiate a barrier request
        assert (m_requestedInitialState == null);
        if (proposedTask.position() == 0) {
            m_pendingProposal = proposedTask;
        } else {
            // Move to a new 0 aligned buffer
            m_pendingProposal = ByteBuffer.allocate(proposedTask.remaining());
            m_pendingProposal.put(proposedTask.array(),
                    proposedTask.arrayOffset() + proposedTask.position(), proposedTask.remaining());
            m_pendingProposal.flip();
        }
        if (m_log.isDebugEnabled()) {
            if (m_pendingProposal.hasRemaining()) {
                m_log.debug(m_stateMachineId + ": Requested new Task " + taskToString(m_pendingProposal.asReadOnlyBuffer()));
            } else {
                m_log.debug(m_stateMachineId + ": Requested unspecified new Task");

            }
        }
        m_stateChangeInitiator = true;
        m_currentRequestType = correlated
                ? REQUEST_TYPE.CORRELATED_COORDINATED_TASK
                : REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK;
        ByteBuffer taskProposal = buildProposal(m_currentRequestType,
                m_synchronizedState.asReadOnlyBuffer(), proposedTask.asReadOnlyBuffer());
        m_pendingProposal = proposedTask;
        // Since we don't update m_lastProposalVersion, we will wake ourselves up
        wakeCommunityWithProposal(taskProposal.array());
        unlockLocalState();
    }

    /*
     * Notification of a new task request by this member or another member.
     * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected void taskRequested(ByteBuffer proposedTask) {
    }

    /*
     * Notification of a task request for newly joined members.
     * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected void staleTaskRequestNotification(ByteBuffer proposedTask) {
    }

    /*
     * Called to accept or reject a new proposed state change by another member.
     */
    protected void requestedTaskComplete(ByteBuffer result) {
        lockLocalState();
        assert (m_pendingProposal != null);
        if (m_log.isDebugEnabled()) {
            if (result.hasRemaining()) {
                m_log.debug(m_stateMachineId + ": Local Task completed with result "
                        + taskResultToString(m_pendingProposal.asReadOnlyBuffer(), result.asReadOnlyBuffer()));
            } else {
                m_log.debug(m_stateMachineId + ": Local Task completed with empty result");
            }
        }
        addResultEntry(result.array());
        checkForBarrierResultsChanges();
    }

    /*
     * Notification of the outcome of the task by all members sorted by memberId.
     * warning: The ByteBuffer taskRequest is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results) {
    }

    /*
     * Notification of the outcome of the task by all members.
     * warning: The ByteBuffer taskRequest is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, List<ByteBuffer> results) {
    }

    /*
     * Returns the current State. This request ignores pending proposals or outcomes. And may well be invalid as soon
     * as the result is returned if a new proposed state is pending. To avoid this get the Distributed Lock first.
     * warning: The ByteBuffer taskRequest is not guaranteed to start at position 0 (avoid rewind, flip, ...)
     */
    protected ByteBuffer getCurrentState() {
        lockLocalState();
        ByteBuffer currentState = m_synchronizedState.asReadOnlyBuffer();
        unlockLocalState();
        return currentState;
    }

    protected void membershipChanged(Set<String> addedMembers, Set<String> removedMembers) {
    }

    protected Set<String> getCurrentMembers() {
        lockLocalState();
        if (m_membershipChangePending) {
            getLatestMembership();
        }
        Set<String> latestAndGreatest = ImmutableSet.copyOf(m_knownMembers);
        unlockLocalState();

        return latestAndGreatest;
    }

    protected String getMemberId() {
        return m_memberId;
    }

    /*
     * Provides the version of the last received proposal or task.
     * Note that the version number is not the number of state transitions but counts the total number of
     * successful state transitions, failed state transitions, task requests, and last change outcome requests
     */
    protected int getCurrentStateVersion() {
        return m_lastProposalVersion;
    }

    protected boolean holdingDistributedLock() {
        return m_holdingDistributedLock;
    }

    protected abstract String stateToString(ByteBuffer state);

    protected String taskToString(ByteBuffer task) {
        return "";
    }

    protected String taskResultToString(ByteBuffer task, ByteBuffer taskResult) {
        return "";
    }
}
