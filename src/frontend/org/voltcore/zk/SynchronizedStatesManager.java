package org.voltcore.zk;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltZK;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/*
 * This state machine coexists with other SynchronizedStateManagers by using different branches of the
 * ZooKeeper tree. This class provides a service to the StateMachineInstance class that reports changes
 * to the membership. The ZooKeeper tree hierarchy is (quoted are reserved node names):
 *     |- rootNode
 *     |\
 *     | |- 'MEMBERS'
 *     |  \
 *     |   |- memberId
 *     \
 *      |- instanceName
 *      |\
 *      | |- 'LOCK_CONTENDERS'
 *      |\
 *      | |- 'BARRIER_PARTICIPANTS'
 *       \
 *        |- 'BARRIER_RESULTS'
 *
 * This hierarchy supports multiple topologies so one SynchronizedStatesManager could be used to track
 * cluster membership while another could be used to track partition membership. Each SynchronizedStatesManager
 * instance has a memberId that must be unique to the member community it is participating in. As the
 * SynchronizedStatesManager is a shared service that can provide membership changes of other
 * StateMachineInstances joining or leaving the community, all instances are considered tightly bound to the
 * SynchronizedStatesManager. This means that the SynchronizedStatesManager will not join a community until
 * all instances using that manager have registered with the SynchronizedStatesManager. Thus when a
 * SynchronizedStatesManager informs its instances that a new member has joined the community (the set of
 * nodes under the 'MEMBERS' node), the instance can be assured that it's peer instance is also there. This
 * implies that StateMachineInstances can only coexist in the same SynchronizedStatesManager if all instances
 * can be initialized (registered) before any individual instance needs to access the shared state.
 */
public class SynchronizedStatesManager {
    private final AtomicBoolean m_done = new AtomicBoolean(false);
    private final static String m_memberNode = "MEMBERS";
    private Set<String> m_groupMembers = new HashSet<String>();
    private final StateMachineInstance m_registeredStateMachines [];
    private int m_registeredStateMachineInstances = 0;
    private static final ListeningExecutorService m_shared_es = CoreUtils.getListeningExecutorService("SMI Daemon", 1);
    // We assume that we are far enough along that the HostMessenger is up and running. Otherwise add to constructor.
    private final ZooKeeper m_zk;
    private final String m_ssmRootNode;
    private final String m_stateMachineRoot;
    private final String m_stateMachineMemberPath;
    private final String m_memberId;

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

    protected static boolean addIfMissing(ZooKeeper zk, String absolutePath, CreateMode createMode, byte[] data)
            throws KeeperException, InterruptedException {
        try {
            zk.create(absolutePath, data, Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }
        return true;
    }

    protected boolean addIfMissing(String absolutePath, CreateMode createMode, byte[] data)
            throws KeeperException, InterruptedException {
        return addIfMissing(m_zk, absolutePath, createMode, data);
    }

    protected String getMemberId() {
        return m_memberId;
    }

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
     * 5. When a member detects a transition in the BARRIER_PARTICIPANTS node
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

    public abstract class StateMachineInstance {
        protected final String m_stateMachineId;
        private Set<String> m_knownMembers;
        private boolean m_membershipChangePending = false;
        private final String m_statePath;
        private final String m_lockPath;
        private final String m_barrierResultsPath;
        private final String m_myResultPath;
        private final String m_barrierParticipantsPath;
        private final String m_myPartiticpantsPath;
        private boolean m_stateChangeInitiator = false;
        private String m_ourDistributedLockName = null;
        private String m_lockWaitingOn = null;
        private final VoltLogger m_log;
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
            protected Boolean initialValue()
            {
                return new Boolean(false);
            }
        };
        private int m_mutexLine0 = 0;
        private int m_mutexLine1 = 0;

        private void lockLocalState() {
            m_mutex.lock();
            assert(m_mutexLockedCnt == 0);
            m_mutexLockedCnt++;
            m_mutexLocked.set(new Boolean(true));
            m_mutexLine1 = m_mutexLine0;
            StackTraceElement l = new Exception().getStackTrace()[1];
            m_mutexLine0 = l.getLineNumber();
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
            assert(m_mutexLockedCnt == 1);
            m_mutexLockedCnt--;
            m_mutexLocked.set(new Boolean(false));
            m_mutex.unlock();
        }

        protected boolean isLocalStateLocked() {
            return m_mutexLocked.get();
//            return true;
        }

        protected boolean isLocalStateUnlocked() {
          return !m_mutexLocked.get();
//            return true;
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
            public REQUEST_TYPE m_requestType;
            public ByteBuffer m_previousState;
            public ByteBuffer m_proposal;

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

        public StateMachineInstance(String instanceName, VoltLogger logger) {
            assert(!instanceName.equals(m_memberNode));
            m_statePath = ZKUtil.joinZKPath(m_stateMachineRoot, instanceName);
            m_lockPath = ZKUtil.joinZKPath(m_statePath, "LOCK_CONTENDERS");
            m_barrierResultsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_RESULTS");
            m_myResultPath = ZKUtil.joinZKPath(m_barrierResultsPath, m_memberId);
            m_barrierParticipantsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_PARTICIPANTS");
            m_myPartiticpantsPath = ZKUtil.joinZKPath(m_barrierParticipantsPath, m_memberId);
            m_log = logger;
            m_stateMachineId = "SMI " + m_ssmRootNode + "/" + instanceName  + "/" + m_memberId;
            m_log.debug(m_stateMachineId + " created.");
        }


        public void registerStateMachineWithManager(ByteBuffer requestedInitialState) throws InterruptedException {
            assert(requestedInitialState != null);
            assert(requestedInitialState.remaining() < Short.MAX_VALUE);
            m_requestedInitialState = requestedInitialState;

            registerStateMachine(this);
        }


        private void initializeStateMachine(Set<String> knownMembers) throws KeeperException, InterruptedException {
            addIfMissing(m_statePath, CreateMode.PERSISTENT, null);
            addIfMissing(m_lockPath, CreateMode.PERSISTENT, null);
            addIfMissing(m_barrierParticipantsPath, CreateMode.PERSISTENT, null);
            lockLocalState();
            boolean ownDistributedLock = requestDistributedLock();
            ByteBuffer startStates = buildProposal(REQUEST_TYPE.INITIALIZING,
                    m_requestedInitialState, m_requestedInitialState);
            boolean stateMachineNodeCreated = addIfMissing(m_barrierResultsPath, CreateMode.PERSISTENT, startStates.array());
            if (m_membershipChangePending) {
                getLatestMembership();
            }
            else {
                m_knownMembers = knownMembers;
            }
            // We need to always monitor participants so that if we are initialized we can add ourselves and insert
            // our results and if we are not initialized, we can always auto-insert agreement.
            if (stateMachineNodeCreated) {
                assert(ownDistributedLock);
                m_synchronizedState = m_requestedInitialState;
                m_requestedInitialState = null;
                m_lastProposalVersion = getProposalVersion();
                ByteBuffer readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
                // Add an acceptable result so the next initializing member recognizes an immediate quorum.
                byte result[] = new byte[1];
                result[0] = (byte)(1);
                addResultEntry(result);
                m_lockWaitingOn = "bogus"; // Avoids call to notifyDistributedLockWaiter
                cancelDistributedLock();
                checkForBarrierParticipantsChange();
                // Notify the derived object that we have a stable state
                m_log.debug(m_stateMachineId + " initialized (first member).");
                setInitialState(readOnlyResult);
            }
            else {
                // To get a stable result set, we need to get the lock for this state machine. If someone else has the
                // lock they can clear the stale results out from under us.
                if (ownDistributedLock) {
                    initializeFromActiveCommunity();
                }
                else {
                    // This means we will ignore the current update if there is one in progress.
                    // Note that if we are not the next waiter for the lock, we will blindly
                    // accept the next proposal and use the outcome to set our initial state.
                    addResultEntry(null);
                    Stat nodeStat = new Stat();
                    m_zk.getData(m_barrierResultsPath, false, nodeStat);
                    m_lastProposalVersion = nodeStat.getVersion();
                    checkForBarrierParticipantsChange();
                }
                assert(isLocalStateUnlocked());
            }
        }

        protected void disableMembership() throws InterruptedException {
            lockLocalState();
            try {
                m_zk.delete(m_myPartiticpantsPath, -1);
                if (m_ourDistributedLockName != null) {
                    m_zk.delete(m_ourDistributedLockName, -1);
                }
                System.out.println(m_stateMachineId + ": Removing " + m_memberId + " from participants (disableMembership)");
            }
            catch (KeeperException e) {
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
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            return proposalVersion;
        }

        private ByteBuffer buildProposal(REQUEST_TYPE requestType,  ByteBuffer existingState, ByteBuffer proposedState) {
            ByteBuffer states = ByteBuffer.allocate(proposedState.remaining() + existingState.remaining() + 4);
            states.putShort((short)requestType.ordinal());
            states.putShort((short)existingState.remaining());
            states.put(existingState);
            existingState.rewind();
            states.put(proposedState);
            proposedState.rewind();
            states.flip();
            return states;
        }

        private StateChangeRequest getExistingAndProposedBuffersFromResultsNode(byte oldAndNewState[]) {
            // Either the barrier or the state node should have the current state
            assert(oldAndNewState != null);
            ByteBuffer states = ByteBuffer.wrap(oldAndNewState);
            // Maximum state size is 64K
            REQUEST_TYPE requestType = REQUEST_TYPE.values()[states.getShort()];
            int oldStateSize = states.getShort();
            states.position(oldStateSize+4);
            ByteBuffer proposedState = states.slice();
            states.flip();      // just the old state
            states.position(4);
            return new StateChangeRequest(requestType, states, proposedState);
        }

        private void checkForBarrierParticipantsChange() {
            assert(isLocalStateLocked());
//            if (m_pendingProposal != null) {
//                System.out.println(m_stateMachineId + ": ParticipantMonitor canceled");
//                // Since there is a proposal in progress, disable the participant monitor until a result is available
//                assert(m_currentParticipants != 0);
//                unlockLocalState();
//                return;
//            }
            try {
                Set<String> children = ImmutableSet.copyOf(m_zk.getChildren(m_barrierParticipantsPath, m_barrierParticipantsWatcher));
                Stat nodeStat = new Stat();
                // inspect the m_barrierResultsPath and get the new and old states and version
                byte statePair[] = m_zk.getData(m_barrierResultsPath, false, nodeStat);
                int proposalVersion = nodeStat.getVersion();
                System.out.println(m_stateMachineId + ": Waking up partipantMonitor (lastVer "+ m_lastProposalVersion + ") from " + proposalVersion + "/" + m_currentParticipants + " with: " + children.toString());
                if (proposalVersion != m_lastProposalVersion) {
                    m_lastProposalVersion = proposalVersion;
                    m_currentParticipants = children.size();
                    if (!m_stateChangeInitiator) {
                        assert(m_pendingProposal == null);

                        // This is an indication that a new state change is being requested
                        StateChangeRequest existingAndProposedStates = getExistingAndProposedBuffersFromResultsNode(statePair);
                        m_currentRequestType = existingAndProposedStates.m_requestType;
                        if (m_requestedInitialState != null) {
                            // We track the number of people waiting on the results so we know when the result is stale and
                            // the next lock holder can initiate a new state proposal.
                            m_zk.create(m_myPartiticpantsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            System.out.println(m_stateMachineId + ": Adding " + m_memberId + " to participants (checkForBarrierParticipantsChange)");

                            // This member just joined and has not yet been initialized so piggyback the initialization onto
                            // another member's proposal
                            assert(m_requestedInitialState == m_pendingProposal);

                            // Since we have not initialized yet, we acknowledge this proposal with an empty result.
                            addResultEntry(null);

                            unlockLocalState();
                        }
                        else {
                            // Don't add ourselves as a participant because we don't care about the results
                            REQUEST_TYPE type = m_currentRequestType;
                            if (type == REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST) {
                                // The request is from a new member who found the results in an ambiguous state.
                                byte result[] = new byte[1];
                                if (existingAndProposedStates.m_proposal.equals(m_synchronizedState)) {
                                    result[0] = (byte)1;
                                    addResultEntry(result);
                                }
                                else {
                                    assert(existingAndProposedStates.m_previousState.equals(m_synchronizedState));
                                    result[0] = (byte)0;
                                    addResultEntry(result);
                                }
                                unlockLocalState();
                            }
                            else {
                                // We track the number of people waiting on the results so we know when the result is stale and
                                // the next lock holder can initiate a new state proposal.
                                m_zk.create(m_myPartiticpantsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                                System.out.println(m_stateMachineId + ": Adding " + m_memberId + " to participants (checkForBarrierParticipantsChange)");

                                m_pendingProposal = existingAndProposedStates.m_proposal;
                                ByteBuffer proposedState = m_pendingProposal.asReadOnlyBuffer();
                                assert(existingAndProposedStates.m_previousState.equals(m_synchronizedState));
                                unlockLocalState();
                                if (type == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                                    m_log.debug(m_stateMachineId + ": new state proposed");
                                    stateChangeProposed(proposedState);
                                }
                                else {
                                    m_log.info(m_stateMachineId + ": task Requested");
                                    taskRequested(proposedState);
                                }
                            }
                        }
                    }
                    else {
                        // We initiated a proposal and if it is a TASK we need to call our derived object to kick it off
                        assert(m_pendingProposal != null);
                        if (m_currentRequestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK ||
                                m_currentRequestType == REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK) {
                            // This is a Task request made by us so notify the derived state machine to perform the task
                            // provide a result.
                            ByteBuffer taskRequest = m_pendingProposal.asReadOnlyBuffer();
                            unlockLocalState();
                            m_log.info(m_stateMachineId + ": task Requested (by this member)");
                            taskRequested(taskRequest);
                        }
                        else {
                            unlockLocalState();
                        }
                    }
                }
                else {
                    m_currentParticipants = children.size();
                    if (m_ourDistributedLockName != null && m_ourDistributedLockName == m_lockWaitingOn && children.size() == 0) {
                        // We can finally notify the lock waiter because everyone is finished evaluating the previous state proposal
                        notifyDistributedLockWaiter();
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
            assert(isLocalStateUnlocked());
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
                result = m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, memberId), false, null);
                if (result != null) {
                    if (result[0] == 0) {
                        return RESULT_CONCENSUS.DISAGREE;
                    }
                    agree = true;
                }
            }
            if (agree) {
                return RESULT_CONCENSUS.AGREE;
            }
            return RESULT_CONCENSUS.NO_QUORUM;
        }

        private Set<ByteBuffer> getUncorrelatedResults(Set<String> memberList) {
            // Treat ZooKeeper failures as empty result
            Set<ByteBuffer> results = new HashSet<ByteBuffer>();
            try {
                for (String memberId : memberList) {
                    byte result[];
                    result = m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, memberId), false, null);
                    if (result != null) {
                        results.add(ByteBuffer.wrap(result));
                    }
                }
                // Remove ourselves from the participants list to unblock the next distributed lock waiter
                m_zk.delete(m_myPartiticpantsPath, -1);
                System.out.println(m_stateMachineId + ": Removing " + m_memberId + " from participants (getUncorrelatedResults)");
            } catch (KeeperException.SessionExpiredException e) {
                results = new HashSet<ByteBuffer>();
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                results = new HashSet<ByteBuffer>();
                e.printStackTrace();
            } catch (InterruptedException e) {
                results = new HashSet<ByteBuffer>();
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            return results;
        }

        private Map<String, ByteBuffer> getCorrelatedResults(Set<String> memberList) {
            // Treat ZooKeeper failures as empty result
            Map<String, ByteBuffer> results = new HashMap<String, ByteBuffer>();
            try {
                for (String memberId : memberList) {
                    byte result[];
                    result = m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, memberId), false, null);
                    if (result != null) {
                        results.put(memberId, ByteBuffer.wrap(result));
                    }
                }
                // Remove ourselves from the participants list to unblock the next distributed lock waiter
                m_zk.delete(m_myPartiticpantsPath, -1);
                System.out.println(m_stateMachineId + ": Removing " + m_memberId + " from participants (getCorrelatedResults)");
            } catch (KeeperException.SessionExpiredException e) {
                results = new HashMap<String, ByteBuffer>();
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                results = new HashMap<String, ByteBuffer>();
                e.printStackTrace();
            } catch (InterruptedException e) {
                results = new HashMap<String, ByteBuffer>();
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            return results;
        }

        // The number of results is a superset of the membership so analyze the results
        private void processResultQuorum(Set<String> memberList) {
            assert(m_currentRequestType != REQUEST_TYPE.INITIALIZING);
            m_memberResults = null;
            if (m_requestedInitialState != null) {
                // We can now initialize this state machine instance
                assert(m_ourDistributedLockName != null);
                try {
                    assert(m_synchronizedState == null);
                    Stat versionInfo = new Stat();
                    byte oldAndProposedState[] = m_zk.getData(m_barrierResultsPath, false, versionInfo);
                    assert(versionInfo.getVersion() == m_lastProposalVersion);
                    StateChangeRequest existingAndProposedStates =
                            getExistingAndProposedBuffersFromResultsNode(oldAndProposedState);
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
                                        m_synchronizedState, m_synchronizedState);
                                Stat newProposalStat = m_zk.setData(m_barrierResultsPath, stableState.array(), -1);
                                m_lastProposalVersion = newProposalStat.getVersion();
                            }
                        }
                        else {
                            // There was at least one member that knows the definitive state
                            if (result == RESULT_CONCENSUS.AGREE) {
                                m_synchronizedState = existingAndProposedStates.m_proposal;
                            }
                            else {
                                m_synchronizedState = existingAndProposedStates.m_previousState;
                            }
                        }
                    }
                    else
                    if (m_currentRequestType == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                        // We don't have a result yet but it is available
                        RESULT_CONCENSUS result = resultsAgreeOnSuccess(memberList);
                        if (result == RESULT_CONCENSUS.AGREE) {
                            m_synchronizedState = existingAndProposedStates.m_proposal;
                        }
                        else {
                            assert(result == RESULT_CONCENSUS.DISAGREE);
                            m_synchronizedState = existingAndProposedStates.m_previousState;
                        }
                    }
                    else {
                        // If we are processing a TASK or a updated STABLE result, we don't need a quorum
                        m_synchronizedState = existingAndProposedStates.m_previousState;
                    }

                    // Remove ourselves from the participants list to unblock the next distributed lock waiter
                    m_zk.delete(m_myPartiticpantsPath, -1);
                    System.out.println(m_stateMachineId + ": Removing " + m_memberId + " to participants (processResultQuorum1)");
                } catch (KeeperException.SessionExpiredException e) {
                    e.printStackTrace();
                } catch (KeeperException.ConnectionLossException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
                    unlockLocalState();
                    m_log.debug(m_stateMachineId + " initialized (concensus).");
                    setInitialState(readOnlyResult);

                    // If we are ready to provide an initial state to the derived state machine, add us to
                    // participants watcher so we can see the next request
                    monitorParticipantChanges();
                }
                else {
                    unlockLocalState();
                }
            }
            else {
                assert(m_currentRequestType != REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST);
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
                        System.out.println(m_stateMachineId + ": Removing " + m_memberId + " from participants (processResultQuorum2)");
                        if (result == RESULT_CONCENSUS.AGREE) {
                            success = true;
                            m_synchronizedState = m_pendingProposal;
                        }
                        else {
                            assert(result == RESULT_CONCENSUS.DISAGREE);
                        }
                        m_pendingProposal = null;
                        if (m_stateChangeInitiator) {
                            // Since we don't care if we are the last to go away, remove ourselves from the participant list
                            assert(m_ourDistributedLockName != null);
                            m_stateChangeInitiator = false;
                            cancelDistributedLock();
                        }
                    } catch (KeeperException.SessionExpiredException e) {
                        success = false;
                        e.printStackTrace();
                    } catch (KeeperException.ConnectionLossException e) {
                        success = false;
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        success = false;
                        e.printStackTrace();
                    } catch (Exception e) {
                        org.voltdb.VoltDB.crashLocalVoltDB(
                                "Unexepected failure in StateMachine.", true, e);
                        success = false;
                    }
                    unlockLocalState();
                    // Notify the derived state machine engine of the current state
                    proposedStateResolved(initiator, attemptedChange, success);
                    monitorParticipantChanges();
                }
                else {
                    // Process the results of a TASK request
                    ByteBuffer taskRequest = m_pendingProposal.asReadOnlyBuffer();
                    m_pendingProposal = null;
                    if (m_currentRequestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK) {
                        Map<String, ByteBuffer> results = getCorrelatedResults(memberList);
                        if (m_stateChangeInitiator) {
                            // Since we don't care if we are the last to go away, remove ourselves from the participant list
                            assert(m_ourDistributedLockName != null);
                            m_stateChangeInitiator = false;
                            cancelDistributedLock();
                        }
                        unlockLocalState();
                        correlatedTaskCompleted(initiator, taskRequest, results);
                    }
                    else {
                        Set<ByteBuffer> results = getUncorrelatedResults(memberList);
                        if (m_stateChangeInitiator) {
                            // Since we don't care if we are the last to go away, remove ourselves from the participant list
                            assert(m_ourDistributedLockName != null);
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
            assert(isLocalStateLocked());
            if (m_pendingProposal == null) {
                // Don't check for barrier results until we notice the participant list change.
                System.out.println(m_stateMachineId + ": Canceled Result Monitor");
                unlockLocalState();
                return;
            }
            Set<String> membersWithResults;
            try {
                membersWithResults = ImmutableSet.copyOf(m_zk.getChildren(m_barrierResultsPath,
                        m_barrierResultsWatcher));
                System.out.println(m_stateMachineId + ": Waking up resultMonitor with: " + membersWithResults.toString());
            } catch (KeeperException.SessionExpiredException e) {
                membersWithResults = new TreeSet<String>(Arrays.asList("We died so avoid Quorum path"));
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                membersWithResults = new TreeSet<String>(Arrays.asList("We died so avoid Quorum path"));
                e.printStackTrace();
            } catch (InterruptedException e) {
                membersWithResults = new TreeSet<String>(Arrays.asList("We died so avoid Quorum path"));
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
                membersWithResults = new TreeSet<String>();
            }
            if (Sets.difference(m_knownMembers, membersWithResults).isEmpty()) {
                processResultQuorum(membersWithResults);
                assert(isLocalStateUnlocked());
            }
            else {
                m_memberResults = membersWithResults;
                unlockLocalState();
            }
        }

        private void initializeFromActiveCommunity() {
            ByteBuffer readOnlyResult = null;
            ByteBuffer staleTask = null;
            byte oldAndProposedState[];
            try {
                Stat lastProposal = new Stat();
                oldAndProposedState = m_zk.getData(m_barrierResultsPath, false, lastProposal);
                StateChangeRequest existingAndProposedStates =
                        getExistingAndProposedBuffersFromResultsNode(oldAndProposedState);
                if (existingAndProposedStates.m_requestType == REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST) {
                    RESULT_CONCENSUS result = resultsAgreeOnSuccess(m_knownMembers);
                    if (result == RESULT_CONCENSUS.AGREE) {
                        existingAndProposedStates.m_previousState = existingAndProposedStates.m_proposal;
                        // Fall through to set up the initial state and provide notification of initialization
                        existingAndProposedStates.m_requestType = REQUEST_TYPE.INITIALIZING;
                    }
                    else
                    if (result == RESULT_CONCENSUS.DISAGREE) {
                        // Fall through to set up the initial state and provide notification of initialization
                        existingAndProposedStates.m_requestType = REQUEST_TYPE.INITIALIZING;
                    }
                    else {
                        // Another members outcome request was completed but a subsequent proposing member died
                        // between the time the results of this last request were removed and the new proposal
                        // was made. Fall through and propose a new LAST_CHANGE_OUTCOME_REQUEST.
                        existingAndProposedStates.m_requestType = REQUEST_TYPE.STATE_CHANGE_REQUEST;
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
                }
                else {
                    m_synchronizedState = existingAndProposedStates.m_previousState;
                    m_requestedInitialState = null;
                    readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
                    m_lastProposalVersion = lastProposal.getVersion();
                    if (existingAndProposedStates.m_requestType != REQUEST_TYPE.INITIALIZING) {
                        staleTask = existingAndProposedStates.m_proposal;
                    }
                    cancelDistributedLock();
                    // Add an acceptable result so the next initializing member recognizes an immediate quorum.
                    m_lockWaitingOn = "bogus"; // Avoids call to notifyDistributedLockWaiter below
                    checkForBarrierParticipantsChange();
                    m_log.debug(m_stateMachineId + " initialized (from stable community).");
                }
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
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
            assert(m_ourDistributedLockName != null);
            assert(m_currentParticipants == 0);
            int newProposalVersion = -1;
            try {
                List<String> results = m_zk.getChildren(m_barrierResultsPath, false);
                for (String resultNode : results) {
                    m_zk.delete(ZKUtil.joinZKPath(m_barrierResultsPath, resultNode), -1);
                }
                Stat newProposalStat = m_zk.setData(m_barrierResultsPath, proposal, -1);
                m_zk.create(m_myPartiticpantsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                newProposalVersion = newProposalStat.getVersion();
                System.out.println(m_stateMachineId + ": Adding " + m_memberId + " to participants (wakeCommunityWithProposal)");
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
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
                assert(isLocalStateUnlocked());
            }
        };

        private final Runnable HandlerForBarrierResultsEvent = new Runnable() {
            @Override
            public void run() {
                lockLocalStateForResultsRunner();
                checkForBarrierResultsChanges();
                assert(isLocalStateUnlocked());
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
            String nextlower = null;
            //Until we have previous nodes and we set a watch on previous node.
            while (iter.hasPrevious()) {
                //Process my lower nodes and put a watch on whats live
                String previous = ZKUtil.joinZKPath(m_lockPath, iter.previous());
                if (m_zk.exists(previous, m_lockWatcher) != null) {
                    m_log.info(m_stateMachineId + ": Waiting for " + previous);
                    nextlower = previous;
                    break;
                }
            }
            //If we could not watch any lower node we are lowest and must own the lock.
            if (nextlower == null) {
                return m_ourDistributedLockName;
            }
            return nextlower;
        }

        private final Runnable HandlerForDistributedLockEvent = new Runnable() {
            @Override
            public void run() {
                lockLocalStateForLockRunner();
                if (m_ourDistributedLockName != null) {
                    try {
                        m_lockWaitingOn = getNextLockNodeFromList();
                    } catch (KeeperException.SessionExpiredException e) {
                        e.printStackTrace();
                        m_lockWaitingOn = "We died so we can't ever get the distributed lock";
                    } catch (KeeperException.ConnectionLossException e) {
                        e.printStackTrace();
                        m_lockWaitingOn = "We died so we can't ever get the distributed lock";
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        m_lockWaitingOn = "We died so we can't ever get the distributed lock";
                    } catch (Exception e) {
                        org.voltdb.VoltDB.crashLocalVoltDB(
                                "Unexepected failure in StateMachine.", true, e);
                    }
                    if (m_lockWaitingOn.equals(m_ourDistributedLockName) && m_currentParticipants == 0) {
                        // There are no more members still processing the last result
                        notifyDistributedLockWaiter();
                    }
                    else {
                        unlockLocalState();
                    }
                }
                else {
                    // lock was cancelled
                    unlockLocalState();
                }
                assert(isLocalStateUnlocked());
            }
        };

        private void cancelDistributedLock() {
            assert(isLocalStateLocked());
            m_log.info(m_stateMachineId + ": cancelLockRequest for " + m_ourDistributedLockName);
            assert(m_ourDistributedLockName != null);
            try {
                m_zk.delete(m_ourDistributedLockName, -1);
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in SynchronizedStatesManager.", true, e);
            }
            m_ourDistributedLockName = null;
        }

        private void checkMembership() {
            lockLocalState();
            m_membershipChangePending = true;
            System.out.println(m_stateMachineId + ": early warning of Membership; old membership was " + m_groupMembers);
            unlockLocalState();
        }

        private void membershipChanged(Set<String> knownHosts, Set<String> addedMembers, Set<String> removedMembers) {
            lockLocalState();
            // Even though we got a direct update, membership could have changed again between the
            m_knownMembers = knownHosts;
            m_membershipChangePending = false;
            boolean notInitializing = m_requestedInitialState == null;
            if (m_pendingProposal != null && m_memberResults != null &&
                    Sets.difference(m_knownMembers, m_memberResults).isEmpty()) {
                // We can stop watching for results since we have a quorum.
                processResultQuorum(m_memberResults);
            }
            else {
                unlockLocalState();
            }
            if (notInitializing) {
                membershipChanged(addedMembers, removedMembers);
            }
            assert(isLocalStateUnlocked());
        }

        private void getLatestMembership() {
            try {
                m_knownMembers = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, null));
                System.out.println(m_stateMachineId + ": Membership adjusted after distributed lock " + m_groupMembers);
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in SynchronizedStatesManager.", true, e);
            }
        }

        private void notifyDistributedLockWaiter() {
            assert(isLocalStateLocked());
            assert(m_currentParticipants == 0);
            if (m_membershipChangePending) {
                getLatestMembership();
            }
            if (m_requestedInitialState != null) {
                // We are still waiting to initialize the state. Now that we have have the state lock we can
                // look at the last set of results. We need to set proposedState so results will be checked.
                assert(m_pendingProposal == null);
                m_pendingProposal = m_requestedInitialState;
                initializeFromActiveCommunity();
                assert(isLocalStateUnlocked());
            }
            else {
                m_log.info(m_stateMachineId + ": granted lockRequest for " + m_ourDistributedLockName);
                m_lockWaitingOn = null;
                unlockLocalState();
                // Notify the derived class that the lock is available
                lockRequestCompleted();
            }
        }

        private boolean requestDistributedLock() {
            try {
                assert(m_ourDistributedLockName == null);
                assert(isLocalStateLocked());
                m_ourDistributedLockName = m_zk.create(ZKUtil.joinZKPath(m_lockPath,"lock_"), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                m_lockWaitingOn = getNextLockNodeFromList();
                if (m_lockWaitingOn.equals(m_ourDistributedLockName) && m_currentParticipants == 0) {
                    // Prevents a second notification.
                    m_log.info(m_stateMachineId + ": requestLock returned successfully");
                    m_lockWaitingOn = null;
                    if (m_membershipChangePending) {
                        getLatestMembership();
                    }
                    return true;
                }
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
            return false;
        }

        private void addResultEntry(byte result[]) {
            try {
                m_zk.create(m_myResultPath, result, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in StateMachine.", true, e);
            }
        }

        private void assignStateChangeAgreement(boolean acceptable) {
            assert(isLocalStateLocked());
            assert(m_pendingProposal != null);
            assert(m_currentRequestType == REQUEST_TYPE.STATE_CHANGE_REQUEST);
            byte result[] = new byte[1];
            result[0] = (byte)(acceptable?1:0);
            addResultEntry(result);
            if (acceptable) {
                // Since we are only interested in the results when we agree with the proposal
                checkForBarrierResultsChanges();
            }
            else {
                try {
                    // Since we don't care about the outcome remove ourself from the participant list
                    m_zk.delete(m_myPartiticpantsPath, -1);
                    System.out.println(m_stateMachineId + ": From " + m_memberId + " from participants (assignStateChangeAgreement)");
                } catch (KeeperException.ConnectionLossException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Unexepected failure in StateMachine.", true, e);
                }
                unlockLocalState();
            }
        }

        protected boolean isInitialized() {
            boolean initialized;
            lockLocalState();
            initialized = m_requestedInitialState == null;
            unlockLocalState();
            return initialized;
        }

        /*
         * Notifies of full participation and the current agreed state
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
            assert(m_pendingProposal == null);
            cancelDistributedLock();
            unlockLocalState();
        }

        /*
         * Notifies of the successful completion of a previous lock request
         */
        protected abstract void lockRequestCompleted();

        /*
         * Propose a new state. Only call after successful acquisition of the distributed lock.
         */
        protected void proposeStateChange(ByteBuffer proposedState) {
            assert(proposedState != null);
            assert(proposedState.remaining() < Short.MAX_VALUE);
            lockLocalState();
            // Only the lock owner can initiate a barrier request
            assert(m_requestedInitialState == null);
            m_pendingProposal = ByteBuffer.allocate(proposedState.remaining());
            m_pendingProposal.put(proposedState.array(),
                    proposedState.arrayOffset()+proposedState.position(), proposedState.remaining());
            m_pendingProposal.flip();

            m_stateChangeInitiator = true;
            m_currentRequestType = REQUEST_TYPE.STATE_CHANGE_REQUEST;
            ByteBuffer stateChange = buildProposal(REQUEST_TYPE.STATE_CHANGE_REQUEST,
                    m_synchronizedState, m_pendingProposal);
            m_lastProposalVersion = wakeCommunityWithProposal(stateChange.array());
            assignStateChangeAgreement(true);
        }

        /*
         * Notification of a new proposed state change by another member.
         */
        protected abstract void stateChangeProposed(ByteBuffer proposedState);

        /*
         * Called to accept or reject a new proposed state change by another member.
         */
        protected void requestedStateChangeAcceptable(boolean acceptable) {
            lockLocalState();
            assert(!m_stateChangeInitiator);
            assignStateChangeAgreement(acceptable);
        }

        /*
         * Notification of the outcome of a previous state change proposal.
         */
        protected abstract void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success);

        /*
         * Propose a new task. Only call after successful acquisition of the distributed lock.
         */
        protected void initiateCoordinatedTask(boolean correlated, ByteBuffer proposedTask) {
            assert(proposedTask != null);
            assert(proposedTask.remaining() < Short.MAX_VALUE);
            lockLocalState();
            // Only the lock owner can initiate a barrier request
            assert(m_requestedInitialState == null);
            m_pendingProposal = ByteBuffer.allocate(proposedTask.remaining());
            m_pendingProposal.put(proposedTask.array(),
                    proposedTask.arrayOffset()+proposedTask.position(), proposedTask.remaining());
            m_pendingProposal.flip();

            m_stateChangeInitiator = true;
            m_currentRequestType = correlated ?
                    REQUEST_TYPE.CORRELATED_COORDINATED_TASK :
                    REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK;
            ByteBuffer taskProposal = buildProposal(m_currentRequestType, m_synchronizedState, proposedTask);
            m_pendingProposal = proposedTask;
            // Since we don't update m_lastProposalVersion, we will wake ourselves up
            wakeCommunityWithProposal(taskProposal.array());
            unlockLocalState();
        }

        /*
         * Notification of a new task request by this member or another member.
         */
        protected abstract void taskRequested(ByteBuffer proposedTask);

        /*
         * Notification of a task request for newly joined members.
         */
        protected abstract void staleTaskRequestNotification(ByteBuffer proposedTask);

        /*
         * Called to accept or reject a new proposed state change by another member.
         */
        protected void requestedTaskComplete(ByteBuffer result)
        {
            lockLocalState();
            assert(m_pendingProposal != null);
            addResultEntry(result.array());
            checkForBarrierResultsChanges();
        }

        /*
         * Notification of the outcome of the task by all members sorted by memberId.
         */
        protected abstract void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results);

        /*
         * Notification of the outcome of the task by all members.
         */
        protected abstract void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Set<ByteBuffer> results);

        protected ByteBuffer getCurrentState() {
            lockLocalState();
            ByteBuffer currentState = m_synchronizedState.asReadOnlyBuffer();
            unlockLocalState();
            return currentState;
        }

        protected abstract void membershipChanged(Set<String> addedMembers, Set<String> removedMembers);

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
    }

    public SynchronizedStatesManager(ZooKeeper zk, String rootNode, String memberId, int registeredInstances)
            throws KeeperException, InterruptedException {
        m_zk = zk;
        // We will not add ourselves as members in ZooKeeper until all StateMachineInstances have registered
        m_registeredStateMachines = new StateMachineInstance[registeredInstances];
        m_ssmRootNode = rootNode;
        m_stateMachineRoot = ZKUtil.joinZKPath(VoltZK.syncStateMachine, rootNode);
        ByteBuffer numberOfInstances = ByteBuffer.allocate(4);
        numberOfInstances.putInt(registeredInstances);
        addIfMissing(m_stateMachineRoot, CreateMode.PERSISTENT, numberOfInstances.array());
        m_stateMachineMemberPath = ZKUtil.joinZKPath(m_stateMachineRoot, m_memberNode);
        m_memberId = memberId;
    }

    public void ShutdownSynchronizedStatesManager() throws InterruptedException {
        ListenableFuture<?> disableComplete = m_shared_es.submit(disableInstances);
        try {
            disableComplete.get();
        }
        catch (ExecutionException e) {
            throw new InterruptedException(e.getMessage());
        }

    }

    private final Runnable disableInstances = new Runnable() {
        @Override
        public void run() {
            try {
                for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                    stateMachine.disableMembership();
                }
                m_done.set(true);
                m_zk.delete(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId), -1);
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
                m_done.set(true);
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
                m_done.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
                m_done.set(true);
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in SynchronizedStatesManager.", true, e);
                m_done.set(true);
            }
        }
    };

    private final Runnable membershipEventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                checkForMembershipChanges();
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in SynchronizedStatesManager.", true, e);
            }
        }
    };

    private class MembershipWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            try {
                if (!m_done.get()) {
                    for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                        stateMachine.checkMembership();
                    }
                    m_shared_es.submit(membershipEventHandler);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    }

    private final MembershipWatcher m_membershipWatcher = new MembershipWatcher();

    private final Runnable initializeInstances = new Runnable() {
        @Override
        public void run() {
            try {
                byte[] expectedInstances = m_zk.getData(m_stateMachineRoot, false, null);
                assert(m_registeredStateMachineInstances == ByteBuffer.wrap(expectedInstances).getInt());
                // First become a member of the community
                addIfMissing(m_stateMachineMemberPath, CreateMode.PERSISTENT, null);
                // This could fail because of an old ephemeral that has not aged out yet but assume this does not happen
                addIfMissing(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId), CreateMode.EPHEMERAL, null);

                m_groupMembers = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, m_membershipWatcher));
                // Then initialize each instance
                for (StateMachineInstance instance : m_registeredStateMachines) {
                    instance.initializeStateMachine(m_groupMembers);
                }
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
                m_done.set(true);
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
                m_done.set(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
                m_done.set(true);
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in initializeInstances.", true, e);
                m_done.set(true);
            }
        }
    };

    private synchronized void registerStateMachine(StateMachineInstance machine) throws InterruptedException {
        assert(m_registeredStateMachineInstances < m_registeredStateMachines.length);

        m_registeredStateMachines[m_registeredStateMachineInstances] = machine;
        m_registeredStateMachineInstances++;
        if (m_registeredStateMachineInstances == m_registeredStateMachines.length) {
            if (machine.m_log.isDebugEnabled()) {
                // lets make sure all the state machines are using unique paths
                Set<String> instanceNames = new HashSet<String>();
                for (StateMachineInstance instance : m_registeredStateMachines) {
                    if (!instanceNames.add(instance.m_statePath)) {
                        machine.m_log.error("Multiple state machine instances with the same path registered: " +
                                instance.m_statePath);
                    }
                }
            }
            // Do all the initialization and notifications on the executor to avoid a race with
            // the group membership changes
            ListenableFuture<?> initComplete = m_shared_es.submit(initializeInstances);
            try {
                initComplete.get();
            }
            catch (ExecutionException e) {
                throw new InterruptedException(e.getMessage());
            }
        }
    }

    /*
     * Track state machine membership. If it changes, notify all state machine instances
     */
    private void checkForMembershipChanges() throws KeeperException, InterruptedException {
        Set<String> children = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, m_membershipWatcher));

        Set<String> removedMembers;
        Set<String> addedMembers;
        if (m_registeredStateMachineInstances == m_registeredStateMachines.length && !m_groupMembers.equals(children)) {
            removedMembers = Sets.difference(m_groupMembers, children);
            addedMembers = Sets.difference(children, m_groupMembers);
            m_groupMembers = children;
            System.out.println(m_ssmRootNode + "/" + m_memberId + ": Membership changed " + m_groupMembers);
            for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                stateMachine.membershipChanged(m_groupMembers, addedMembers, removedMembers);
            }
        }
    }

}
