package org.voltcore.zk;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.ListeningScheduledExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

/*
 * This state machine coexists with other state machines under a single system node used to track
 * the participant count. When the
 */
public class SynchronizedStatesManager {
    private final AtomicBoolean m_done = new AtomicBoolean(false);
    private Set<String> m_members = new HashSet<String>();
    private final StateMachineInstance m_registeredStateMachines [];
    private int m_registeredStateMachineInstances = 0;
    private static final ScheduledThreadPoolExecutor m_esBase =
            new ScheduledThreadPoolExecutor(1,
                    CoreUtils.getThreadFactory(null, "SMI Daemon", CoreUtils.SMALL_STACK_SIZE, false, null),
                                            new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());
    private static final ListeningScheduledExecutorService m_shared_es = MoreExecutors.listeningDecorator(m_esBase);
    // We assume that we are far enough along that the HostMessenger is up and running. Otherwise add to constructor.
    private final ZooKeeper m_zk;
    private final String m_stateMachineRoot;
    private final String m_stateMachineMemberPath;
    private final String m_memberId;

    protected static boolean addIfMissing(ZooKeeper zk, String absolutePath, CreateMode createMode, byte[] data) {
        try {
            Stat s = zk.exists(absolutePath, false);
            if (s == null) {
                zk.create(absolutePath, data, Ids.OPEN_ACL_UNSAFE, createMode);
                return true;
            }
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            VoltDB.crashGlobalVoltDB("Failed to create Zookeeper node: " + e.getMessage(),
                    false, e);
        }
        return false;
    }

    protected boolean addIfMissing(String absolutePath, CreateMode createMode, byte[] data) {
        return addIfMissing(m_zk, absolutePath, createMode, data);
    }

    protected String getMemberId() {
        return m_memberId;
    }

    /*
     * Rules for each StateMachineInstance:
     * 1. All access to the state machine is serialized by a per state machine local lock.
     * 2. When a new member joins an operational state machine, it must grab the lock node
     *    before it can determine the current agreed state.
     * 3. State changes may only be initiated by the holder of the lock node.
     * 4. A member holding the lock can propose a state change by clearing out the reported
     *    result nodes under the BARRIER_RESULTS node, updating the BARRIER_RESULTS node
     *    with the old and proposed state, and adding an ephemeral unique node under
     *    BARRIER_PARTICIPANTS to trigger the state machine members.
     * 5. When a StateMachineInstance detects a transition in the BARRIER_PARTICIPANTS node
     *    count from 0 to 1 or more, it adds an ephemeral node for itself under
     *    BARRIER_PARTICIPANTS and copies the old and proposed state from the BARRIER_RESULTS
     *    node.
     * 6. Each Host evaluates the proposed state and inserts a SUCCESS or FAILURE indication
     *    in a persistent unique node under the BARRIER_RESULTS path.
     * 7. When the set of BARRIER_RESULTS nodes is a superset of all members currently in the
     *    state machine, all members are considered to have reported in and each member can
     *    independently determine the outcome. If all BARRIER_RESULTS nodes report SUCCESS
     *    each member applies the new state and then removes itself from BARRIER_PARTICIPANTS.
     * 8. Membership in the state machine can change as members join or fail. Membership is
     *    monitored by the SynchronizedStatesManager and changes are reported to each state
     *    machine instance. While members have joined but have not determined the current
     *    state because they have not gotten the lock node, they report SUCCESS for each
     *    state change proposal made by other members.
     * 9. If any node under BARRIER_RESULTS reports failure, the new state is not applied by
     *    any member.
     *10. Each member that has processed BARRIER_RESULTS removes its ephemeral node from the
     *    below the BARRIER_PARTICIPANTS node.
     *11. If the initiator of a state change releases the lock node when 7 is satisfied.
     *12. A member is not told they have the lock node until the last member releases the
     *    lock node AND all ephemeral nodes under the BARRIER_PARTICIPANTS node are gone,
     *    indicating that all members waiting on the outcome of the last state change have
     *    processed the previous outcome
     */

    abstract class StateMachineInstance {
        private final String m_stateMachineId;
        private Set<String> m_knownMembers;
        private final String m_statePath;
        private final String m_lockPath;
        private final String m_barrierResultsPath;
        private final String m_myResultPath;
        private final String m_barrierParticipantsPath;
        private final String m_myPartiticpantsPath;
        private boolean m_stateChangeInitiator = false;
        private String m_currlockName = null;
        private String m_lockWaitingOn = null;
        private final VoltLogger m_log;
        private ByteBuffer m_requestedInitialState = null;
        private ByteBuffer m_synchronizedState = null;
        private ByteBuffer m_proposedState = null;
        private int m_currentParticipants = 0;
        private final Lock m_mutex = new ReentrantLock();
        private boolean m_mutexLocked = false;
        private Set<String> m_resultsSet = null;

        private void lockLocalState() {
            m_mutex.lock();
            assert(!isLocalStateLocked());
            m_mutexLocked = true;
        }

        private void unlockLocalState() {
            assert(isLocalStateLocked());
            m_mutexLocked = false;
            m_mutex.unlock();
        }

        protected boolean isLocalStateLocked() {
            return m_mutexLocked;
        }

        // This Lock can be canceled if someone doesn't want to wait any longer
        private class LockWatcher extends ZKUtil.CancellableWatcher {

            public LockWatcher() {
                super(m_shared_es);
            }

            @Override
            public void pProcess(final WatchedEvent event) {
                if (event.getState() == KeeperState.Disconnected) return;
                try {
                    m_shared_es.submit(LockEventHandler);
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
                        m_shared_es.submit(BarrierPartiticpantsEventHandler);
                    }
                } catch (RejectedExecutionException e) {
                }
            }
        }

        private final BarrierParticipantsWatcher m_barrierParticipantsWatcher = new BarrierParticipantsWatcher();

        private class BarrierResultsWatcher extends ZKUtil.CancellableWatcher {

            public BarrierResultsWatcher() {
                super(m_shared_es);
            }

            @Override
            public void pProcess(final WatchedEvent event) {
                if (event.getState() == KeeperState.Disconnected) return;
                try {
                    if (!m_done.get()) {
                        m_shared_es.submit(BarrierResultsEventHandler);
                    }
                } catch (RejectedExecutionException e) {
                }
            }
        };

        private final BarrierResultsWatcher m_barrierResultsWatcher = new BarrierResultsWatcher();

        public StateMachineInstance(String instanceName, ByteBuffer requestedInitialState, VoltLogger logger) {
            m_statePath = ZKUtil.joinZKPath(m_stateMachineRoot, instanceName);
            m_lockPath = ZKUtil.joinZKPath(m_statePath, "LOCK_CONTENDERS");
            m_barrierResultsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_RESULTS");
            m_myResultPath = ZKUtil.joinZKPath(m_barrierResultsPath, m_memberId);
            m_barrierParticipantsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_PARTICIPANTS");
            m_myPartiticpantsPath = ZKUtil.joinZKPath(m_barrierParticipantsPath, m_memberId);
            m_log = logger;
            m_requestedInitialState = requestedInitialState;
            m_stateMachineId = "SMI " + m_memberId + "/" + instanceName;
            m_log.debug(m_stateMachineId + " created.");
        }

        private void initializeStateMachine(Set<String> knownMembers) {
            addIfMissing(m_statePath, CreateMode.PERSISTENT, null);
            addIfMissing(m_lockPath, CreateMode.PERSISTENT, null);
            addIfMissing(m_barrierParticipantsPath, CreateMode.PERSISTENT, null);
            ByteBuffer startStates = buildResultPair(m_requestedInitialState, m_requestedInitialState);
            boolean stateMachineNodeCreated = addIfMissing(m_barrierResultsPath, CreateMode.PERSISTENT, startStates.array());
            lockLocalState();
            m_knownMembers = knownMembers;
            // We need to always monitor participants so that if we are initialized we can add ourselves and insert
            // our results and if we are not initialized, we can always auto-insert agreement.
            if (stateMachineNodeCreated) {
                m_synchronizedState = m_requestedInitialState;
                m_requestedInitialState = null;
                ByteBuffer readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
                m_lockWaitingOn = "bogus"; // Avoids call to checkForBarrierResultschanges
                assignStateChangeAgreement(true);
                lockLocalState();
                checkForBarrierParticipantsChange();
                // Notify the derived object that we have a stable state
                m_log.debug(m_stateMachineId + " initialized (first member).");
                setInitialState(readOnlyResult);
            }
            else {
                // To get a stable result set, we need to get the lock for this state machine. If someone else has the
                // lock they can clear the stale results out from under us.
                if (requestLockUnderLocalLock()) {
                    // Set this to access the current results without being in the participants list.
                    m_proposedState = m_requestedInitialState;
                    assignStateChangeAgreement(true);
                }
                else {
                    // This means we will ignore the current update if there is one in progress.
                    m_currentParticipants = 1;
                    checkForBarrierParticipantsChange();
                }
                assert(!isLocalStateLocked());
            }
        }

        private ByteBuffer buildResultPair(ByteBuffer existingState, ByteBuffer proposedState) {
            ByteBuffer states = ByteBuffer.allocate(proposedState.remaining() + existingState.remaining() + 2);
            states.putShort((short)existingState.remaining());
            states.put(existingState);
            existingState.rewind();
            states.put(proposedState);
            proposedState.rewind();
            states.flip();
            return states;
        }

        private Pair<ByteBuffer, ByteBuffer> getExistingAndProposedBuffersFromResultsNode(byte oldAndNewState[]) {
            // Either the barrier or the state node should have the current state
            assert(oldAndNewState != null);
            ByteBuffer states = ByteBuffer.wrap(oldAndNewState);
            // Maximum state size is 64K
            int oldStateSize = states.getShort();
            states.position(oldStateSize+2);
            ByteBuffer proposedState = states.slice();
            states.flip();      // just the old state
            states.position(2);
            return new Pair<ByteBuffer, ByteBuffer> (states, proposedState);
        }

        private void checkForBarrierParticipantsChange() {
            assert(isLocalStateLocked());
            try {
                Set<String> children = ImmutableSet.copyOf(m_zk.getChildren(m_barrierParticipantsPath, m_barrierParticipantsWatcher));
                if (m_currentParticipants == 0 && children.size() > 0 && m_proposedState == null) {
                    if (m_requestedInitialState != null) {
                        // This member just joined and has not yet been initialized so use the update path to initialize

                        // Since we have not initialized yet, we agree with all changes.
                        requestedStateChangeAcceptable(true);

                        m_currentParticipants = children.size();
                        unlockLocalState();
                    }
                    else {
                        // We track the number of people waiting on the results so we know when the result is stale and the next lock holder
                        // can initiate a new state proposal.
                        m_zk.create(m_myPartiticpantsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        // inspect the m_barrierResultsPath and get the new and old states
                        byte statePair[] = m_zk.getData(m_barrierResultsPath, false, null);

                        // This is an indication that a new state change is being requested
                        Pair<ByteBuffer, ByteBuffer> existingAndProposedStates = getExistingAndProposedBuffersFromResultsNode(statePair);
                        m_proposedState = existingAndProposedStates.getSecond();
                        ByteBuffer proposed = m_proposedState.asReadOnlyBuffer();
                        assert(existingAndProposedStates.getFirst().equals(m_synchronizedState));
                        m_currentParticipants = children.size();
                        m_log.debug(m_stateMachineId + " new state proposed.");
                        unlockLocalState();
                        stateChangeProposed(proposed);
                    }
                }
                else {
                    m_currentParticipants = children.size();
                    if (m_currlockName != null && m_currlockName == m_lockWaitingOn && children.size() == 0) {
                        // We can finally notify the lock waiter because everyone is finished evaluating the previous state proposal
                        notifyLockWaiter();
                    }
                    else {
                        unlockLocalState();
                    }
                }
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
                unlockLocalState();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
                unlockLocalState();
            } catch (InterruptedException e) {
                e.printStackTrace();
                unlockLocalState();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
        }

        private boolean resultsAgreeOnSuccess(Set<String> resultSet) {
            for (String child : resultSet) {
                byte result[];
                try {
                    result = m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, child), false, null);
                    assert(result != null);
                    if (result[0] == 0) {
                        return false;
                    }
                } catch (KeeperException.SessionExpiredException e) {
                    // lost the full connection. some test cases do this...
                    // means zk shutdown without the elector being shutdown.
                    // ignore.
                    e.printStackTrace();
                } catch (KeeperException.ConnectionLossException e) {
                    // lost the full connection. some test cases do this...
                    // means shutdown without the elector being
                    // shutdown; ignore.
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Unexepected failure in StateMachine.", true, e);
                }
            }
            return true;
        }

        private void processResultQuorum(Set<String> resultNodes) throws Exception {
            // We don't have a result yet but it is available
            boolean success = resultsAgreeOnSuccess(resultNodes);
            if (m_requestedInitialState != null) {
                // We are not yet participating in the state machine so wait for a stable result before
                byte oldAndProposedState[] = m_zk.getData(m_barrierResultsPath, false, null);
                Pair<ByteBuffer, ByteBuffer> existingAndProposedStates =
                        getExistingAndProposedBuffersFromResultsNode(oldAndProposedState);
                if (success) {
                    m_synchronizedState = existingAndProposedStates.getSecond();
                }
                else {
                    m_synchronizedState = existingAndProposedStates.getFirst();
                }
                m_requestedInitialState = null;
                m_proposedState = null;
                // If we are ready to provide a result to the derived state machine, add ourselves as participants
                m_zk.getChildren(m_barrierParticipantsPath, m_barrierParticipantsWatcher);
                cancelLockUnderLocalLock();
                ByteBuffer readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
                unlockLocalState();
                m_log.debug(m_stateMachineId + " initialized.");
                setInitialState(readOnlyResult);
            }
            else {
                // Now that we have a result we can remove ourselves from the participants list.
                m_zk.delete(m_myPartiticpantsPath, -1);
                if (success) {
                    m_synchronizedState = m_proposedState;
                }
                ByteBuffer attemptedChange = m_proposedState.asReadOnlyBuffer();
                m_proposedState = null;
                if (m_stateChangeInitiator) {
                    // Since we don't care if we are the last to go away, remove ourselves from the participant list
                    m_stateChangeInitiator = false;
                    cancelLockUnderLocalLock();
                    unlockLocalState();
                    proposedStateResolved(true, attemptedChange, success);
                }
                else {
                    unlockLocalState();
                    // Notify the derived state machine engine of the current state
                    proposedStateResolved(false, attemptedChange, success);
                }
            }
            m_resultsSet = null;
        }

        private void checkForBarrierResultsChanges() {
            assert(isLocalStateLocked());
            try {
                if (m_proposedState == null) {
                    // Don't check for barrier results until we see the participant list change.
                    unlockLocalState();
                    return;
                }
                m_barrierResultsWatcher.canceled = false;
                Set<String> children = ImmutableSet.copyOf(m_zk.getChildren(m_barrierResultsPath, m_barrierResultsWatcher));
                if (Sets.difference(m_knownMembers, children).isEmpty()) {
                    processResultQuorum(children);
                }
                else {
                    m_resultsSet = children;
                    unlockLocalState();
                }
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
                unlockLocalState();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
                unlockLocalState();
            } catch (InterruptedException e) {
                e.printStackTrace();
                unlockLocalState();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
        }

        private final Runnable BarrierPartiticpantsEventHandler = new Runnable() {
            @Override
            public void run() {
                lockLocalState();
                checkForBarrierParticipantsChange();
                assert(!isLocalStateLocked());
            }
        };

        private final Runnable BarrierResultsEventHandler = new Runnable() {
            @Override
            public void run() {
                lockLocalState();
                checkForBarrierResultsChanges();
                assert(!isLocalStateLocked());
            }
        };

        private String getNextLockNodeFromList() throws Exception {
            List<String> lockRequestors = m_zk.getChildren(m_lockPath, false);
            Collections.sort(lockRequestors);
            ListIterator<String> iter = lockRequestors.listIterator();
            String currRequestor = null;
            while (iter.hasNext()) {
                currRequestor = ZKUtil.joinZKPath(m_lockPath, iter.next());
                if (currRequestor.equals(m_currlockName)) {
                    break;
                }
            }
            assert (currRequestor != null); // We should be able to find ourselves
            //Back on currRequestor
            iter.previous();
            String nextlower = null;
            //Until we have previous nodes and we set a watch on previous node.
            while (iter.hasPrevious()) {
                //Process my lower nodes and put a watch on whats live
                String previous = ZKUtil.joinZKPath(m_lockPath, iter.previous());
                m_lockWatcher.canceled = false;
                if (m_zk.exists(previous, m_lockWatcher) != null) {
                    m_log.info("Waiting for " + previous);
                    nextlower = previous;
                    break;
                }
            }
            //If we could not watch any lower node we are lowest and must own the lock.
            if (nextlower == null) {
                return m_currlockName;
            }
            return nextlower;
        }

        private final Runnable LockEventHandler = new Runnable() {
            @Override
            public void run() {
                lockLocalState();
                try {
                    m_lockWaitingOn = getNextLockNodeFromList();
                    if (m_lockWaitingOn.equals(m_currlockName) && m_currentParticipants == 0) {
                        // There are no more members still processing the last result
                        notifyLockWaiter();
                    }
                    else {
                        unlockLocalState();
                    }
                } catch (KeeperException.SessionExpiredException e) {
                    // lost the full connection. some test cases do this...
                    // means zk shutdown without the elector being shutdown.
                    // ignore.
                    e.printStackTrace();
                    unlockLocalState();
                } catch (KeeperException.ConnectionLossException e) {
                    // lost the full connection. some test cases do this...
                    // means shutdown without the elector being
                    // shutdown; ignore.
                    e.printStackTrace();
                    unlockLocalState();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    unlockLocalState();
                } catch (Exception e) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Unexepected failure in StateMachine.", true, e);
                }
            }
        };

        private void topologyChanged(Set<String> knownHosts, Set<String> addedHosts, Set<String> removedHosts) {
            lockLocalState();
            m_knownMembers = knownHosts;
            if (m_resultsSet != null && Sets.difference(m_knownMembers, m_resultsSet).isEmpty()) {
                try {
                    // We can stop watching for results since we have a quorum.
                    m_barrierResultsWatcher.cancel();
                    processResultQuorum(m_resultsSet);
                } catch (KeeperException.SessionExpiredException e) {
                    // lost the full connection. some test cases do this...
                    // means zk shutdown without the elector being shutdown.
                    // ignore.
                    e.printStackTrace();
                } catch (KeeperException.ConnectionLossException e) {
                    // lost the full connection. some test cases do this...
                    // means shutdown without the elector being
                    // shutdown; ignore.
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Unexepected failure in StateMachine.", true, e);
                }
            }
            if (m_requestedInitialState == null) {
                unlockLocalState();
                membershipChanged(addedHosts, removedHosts);
            }
            else {
                // Don't notify derived class of membership changes during initialization
                unlockLocalState();
            }
        }

        protected abstract void membershipChanged(Set<String> addedHosts, Set<String> removedHosts);

        protected abstract void setInitialState(ByteBuffer currentAgreedState);

        private void notifyLockWaiter() {
            assert(isLocalStateLocked());
            if (m_requestedInitialState != null) {
                // We are still waiting to set the initial state. Now that we have have the state lock we can
                // look at the last set of results. We need to set proposedState so results will be checked.
                m_proposedState = m_requestedInitialState;
                m_currentParticipants = 0;
                checkForBarrierResultsChanges();
                assert(!isLocalStateLocked());
            }
            else {
                m_lockWaitingOn = null;
                unlockLocalState();
                // Notify the derived class that the lock is available
                lockRequestCompleted();
            }
        }

        private boolean requestLockUnderLocalLock() {
            try {
                assert(m_currlockName == null);
                assert(isLocalStateLocked());
                m_currlockName = m_zk.create(ZKUtil.joinZKPath(m_lockPath,"lock_"), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                m_lockWaitingOn = getNextLockNodeFromList();
                if (m_lockWaitingOn.equals(m_currlockName) && m_currentParticipants == 0) {
                    // Prevents a second notification.
                    m_log.info("RequesetLock returned successfully");
                    m_lockWaitingOn = null;
                    return true;
                }
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            return false;
        }

        protected boolean requestLock() {
            lockLocalState();
            boolean rslt = requestLockUnderLocalLock();
            unlockLocalState();
            return rslt;
        }

        private void cancelLockUnderLocalLock() {
            assert(isLocalStateLocked());
            m_lockWatcher.cancel();
            m_log.info("cancelLockRequest for " + m_currlockName);
            unlockState();
        }

        protected void cancelLockRequest() {
            lockLocalState();
            cancelLockUnderLocalLock();
            unlockLocalState();
        }

        protected abstract void lockRequestCompleted();



        // This is only called by the subclass to update the community state
        protected void proposeStateChange(ByteBuffer proposedState) {
            lockLocalState();
            // Only the lock owner can initiate a barrier request
            assert(m_currlockName != null);
            assert(m_requestedInitialState == null);
            assert(m_currentParticipants == 0);
            m_proposedState = proposedState;
            m_stateChangeInitiator = true;
            try {
                ByteBuffer states = buildResultPair(m_synchronizedState, m_proposedState);
                List<String> results = m_zk.getChildren(m_barrierResultsPath, false);
                for (String resultNode : results) {
                    m_zk.delete(ZKUtil.joinZKPath(m_barrierResultsPath, resultNode), -1);
                }
                m_zk.setData(m_barrierResultsPath, states.array(), -1);
                m_zk.create(m_myPartiticpantsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            assignStateChangeAgreement(true);
        }

        private void assignStateChangeAgreement(boolean acceptable)
        {
            byte result[] = new byte[1];
            result[0] = (byte)(acceptable?1:0);
            try {
                m_zk.create(m_myResultPath, result, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                if (acceptable) {
                    if (m_proposedState != null) {
                        // Since we are only interested in the results when we agree with the proposal
                        checkForBarrierResultsChanges();
                    }
                    else {
                        unlockLocalState();
                    }
                }
                else {
                    // Don't remove myself from the participant list
                    m_zk.delete(m_myPartiticpantsPath, -1);
                    unlockLocalState();
                }
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
                unlockLocalState();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
                unlockLocalState();
            } catch (InterruptedException e) {
                e.printStackTrace();
                unlockLocalState();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
        }

        protected abstract void stateChangeProposed(ByteBuffer proposedState);

        protected void requestedStateChangeAcceptable(boolean acceptable)
        {
            lockLocalState();
            assert(!m_stateChangeInitiator);
            assignStateChangeAgreement(acceptable);
        }

        protected abstract void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success);

        private void unlockState() {
            assert(isLocalStateLocked());
            assert(m_currlockName != null);
            try {
                m_zk.delete(m_currlockName, -1);
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in LeaderElector.", true, e);
            }
            m_currlockName = null;
        }

        protected ByteBuffer getCurrentState() {
            lockLocalState();
            ByteBuffer currentState = m_synchronizedState.asReadOnlyBuffer();
            unlockLocalState();
            return currentState;
        }
    };

    public SynchronizedStatesManager(ZooKeeper zk, String rootNode, String memberId, int registeredInstances) {
        m_zk = zk;
        // We will not add ourselves as members in zookeeper until all StateMachineInstances have registered
        m_registeredStateMachines = new StateMachineInstance[registeredInstances];
        m_stateMachineRoot = ZKUtil.joinZKPath(VoltZK.syncStateMachine, rootNode);
        addIfMissing(m_stateMachineRoot, CreateMode.PERSISTENT, null);
        m_stateMachineMemberPath = ZKUtil.joinZKPath(m_stateMachineRoot, "members");
        m_memberId = memberId;
    }

    private final Runnable childrenEventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                checkForTopologyChanges();
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in LeaderElector.", true, e);
            }
        }
    };

    private class TopologyWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            try {
                if (!m_done.get()) {
                    m_shared_es.submit(childrenEventHandler);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    }

    public synchronized void registerStateMachine(StateMachineInstance machine) {
        assert(m_registeredStateMachineInstances < m_registeredStateMachines.length);
        m_registeredStateMachines[m_registeredStateMachineInstances] = machine;
        m_registeredStateMachineInstances++;
        if (m_registeredStateMachineInstances == m_registeredStateMachines.length) {
            if (machine.m_log.isDebugEnabled()) {
                // lets make sure all the state machines are using unique paths
                Set<String> instanceNames = new HashSet<String>();
                for (StateMachineInstance instance : m_registeredStateMachines) {
                    if (!instanceNames.add(instance.m_statePath)) {
                        machine.m_log.error("Multiple state machine instances with the same path registered: " + instance.m_statePath);
                    }
                }
            }
            // First become a member of the community
            addIfMissing(m_stateMachineMemberPath, CreateMode.PERSISTENT, null);
            // This could fail because of an old ephemeral that has not aged out yet but assume this does not happen
            addIfMissing(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId), CreateMode.EPHEMERAL, null);

            try {
                m_members = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, false));
                // Then initialize each instance
                for (StateMachineInstance instance : m_registeredStateMachines) {
                    instance.initializeStateMachine(m_members);
                }
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in LeaderElector.", true, e);
            }
            m_shared_es.submit(childrenEventHandler);
        }
    }

    private final TopologyWatcher m_topologyWatcher = new TopologyWatcher();

    /*
     * Track state machine membership. If it changes, notify all state machine instances
     */
    private void checkForTopologyChanges() throws KeeperException, InterruptedException {
        Set<String> children = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, m_topologyWatcher));

        Set<String> removedHosts;
        Set<String> addedHosts;
        if (m_registeredStateMachineInstances == m_registeredStateMachines.length && !m_members.equals(children)) {
            removedHosts = Sets.difference(m_members, children);
            addedHosts = Sets.difference(children, m_members);
            m_members = children;
            for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                stateMachine.topologyChanged(m_members, addedHosts, removedHosts);
            }
        }
    }

}
