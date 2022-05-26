/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.zookeeper_voltpatches.AsyncCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.ConnectionLossException;
import org.apache.zookeeper_voltpatches.KeeperException.NoNodeException;
import org.apache.zookeeper_voltpatches.KeeperException.SessionExpiredException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.utils.Mutex;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

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
    private final static String s_memberNode = "MEMBERS";

    private final AtomicReference<State> m_state = new AtomicReference<>(State.RUNNING);
    private Set<String> m_groupMembers = new HashSet<String>();
    private final StateMachineInstance m_registeredStateMachines[];
    private int m_registeredStateMachineInstances = 0;
    private static final ListeningExecutorService s_sharedEs = CoreUtils.getListeningExecutorService("SSM Daemon", 1);
    // We assume that we are far enough along that the HostMessenger is up and running. Otherwise add to constructor.
    private final ZooKeeper m_zk;
    private final String m_ssmRootNode;
    private final String m_stateMachineRoot;
    private final String m_stateMachineMemberPath;
    private String m_memberId;
    private final String m_canonical_memberId;
    private int m_resetCounter;
    private int m_resetLimit;
    private final int m_resetAllowance; // number of resets allowed within a reset clear threshold duration
    private long m_lastResetTimeInMillis = -1L;
    private SettableFuture<Boolean> m_initComplete;
    // Log for global events that happen outside individual state machines
    private static final VoltLogger ssmLog = new VoltLogger("SSM");

    private static final long RESET_CLEAR_THRESHOLD = TimeUnit.DAYS.toMillis(1);

    private enum REQUEST_TYPE {
        INITIALIZING,
        LAST_CHANGE_OUTCOME_REQUEST,
        STATE_CHANGE_REQUEST,
        CORRELATED_COORDINATED_TASK,
        UNCORRELATED_COORDINATED_TASK
    }

    private enum RESULT_CONCENSUS {
        AGREE,
        DISAGREE,
        NO_QUORUM
    }

    private enum State {
        RUNNING, ERROR, SHUTDOWN
    }

    // Utility methods for handling uncaught exceptions from runnables and callables
    private static void addExceptionHandler(ListenableFuture<?> future) {
        future.addListener(() -> {
            try {
                future.get();
            } catch (ExecutionException e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e.getCause());
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, t);
            }
        }, MoreExecutors.directExecutor());
    }

    static void submitRunnable(Runnable runnable) {
        addExceptionHandler(s_sharedEs.submit(runnable));
    }

    static void submitCallable(Callable<?> callable) {
        addExceptionHandler(s_sharedEs.submit(callable));
    }

    protected boolean addIfMissing(String absolutePath, CreateMode createMode, byte[] data)
            throws KeeperException, InterruptedException {
        return ZKUtil.addIfMissing(m_zk, absolutePath, createMode, data);
    }

    protected String getMemberId() {
        return m_memberId;
    }

    protected abstract class ZKAsyncCreateHandler implements AsyncCallback.StringCallback, Runnable {
        KeeperException.Code m_resultCode;
        String m_resultString;

        ZKAsyncCreateHandler(String path, byte[] data, CreateMode createMode) {
            m_zk.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode, this, null);
        }

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            try {
                m_resultCode = KeeperException.Code.get(rc);
                if (m_resultCode == KeeperException.Code.OK) {
                    m_resultString = name;
                }
                if (isRunning()) {
                    // Will execute the specialized implementation of Run()
                    submitRunnable(this);
                }
            } catch (RejectedExecutionException e) {
                ssmLog.warn("Async initialization of ZK directory for SSM was rejected by the SSM Thread: " + path);
            }
        }

        @Override
        public final void run() {
            if (isRunning()) {
                runImpl();
            }
        }

        abstract void runImpl();
    }

    protected abstract class ZKAsyncChildrenHandler implements AsyncCallback.ChildrenCallback, Runnable {
        KeeperException.Code m_resultCode; // should be either OK or NONODE
        List<String> m_resultChildren;

        ZKAsyncChildrenHandler(String path, Watcher childrenWatcher) {
            m_zk.getChildren(path, childrenWatcher, this, null);
        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            try {
                m_resultCode = KeeperException.Code.get(rc);
                if (m_resultCode == KeeperException.Code.OK) {
                    m_resultChildren = children;
                }
                else {
                    assert(m_resultCode == KeeperException.Code.NONODE ||
                            m_resultCode == KeeperException.Code.CONNECTIONLOSS ||
                            m_resultCode == KeeperException.Code.SESSIONEXPIRED);
                }
                if (isRunning()) {
                    // Will execute the specialized implementation of Run()
                    submitRunnable(this);
                }
            } catch (RejectedExecutionException e) {
                ssmLog.warn("Async getChildren for SSM was rejected by the SSM Thread: " + path);
            }
        }

        @Override
        public final void run() {
            if (isRunning()) {
                runImpl();
            }
        }

        abstract void runImpl();
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
     *14b. One exception to 14 is when a member is initializing and current proposer dies before
     *    any other members can add a participant node. In this case the initializing member
     *    can grab the lock but has to create a new proposal which is a replay of the latest
     *    proposal. The member can then complete initialization once all hosts have posted a
     *    result for the proposal replay
     */
    public abstract class StateMachineInstance {
        protected String m_stateMachineId;
        private final String m_stateMachineName;
        private Set<String> m_knownMembers;
        private volatile boolean m_membershipChangePending = false;
        private volatile boolean m_participantsChangePending = false;
        private final String m_statePath;
        private final String m_lockPath;
        private final String m_barrierResultsPath;
        private String m_myResultPath;
        private final String m_barrierParticipantsPath;
        private String m_myParticipantPath;
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
        private Result m_lastResult = null;

        private boolean m_initializationCompleted = false;

        private boolean isInitializationCompleted() {
            try (Mutex.Releaser r = m_mutex.acquire()) {
                return m_initializationCompleted;
            }
        }

        public int getResetCounter() {
            return m_resetCounter;
        }

        private final Mutex m_mutex = new Mutex();

        boolean debugIsLocalStateLocked() {
            return m_mutex.isHeldByCurrentThread();
        }

        private class LockWatcher implements Watcher {

            @Override
            public void process(final WatchedEvent event) {
                try {
                    if (isRunning()) {
                        submitCallable(HandlerForDistributedLockEvent);
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
                    if (isRunning()) {
                        m_participantsChangePending = true;
                        submitCallable(HandlerForBarrierParticipantsEvent);
                    }
                } catch (RejectedExecutionException e) {
                    ssmLog.warn("ZK watch of Participant Barrier was rejected by the SSM Thread");
                }
            }
        }

        /**
         * Deserialize a {@link Proposal} object from {@code proposal}
         *
         * @param proposal serialized proposal
         * @return {@link Proposal} instance derived from {@code proposal}
         */
        private Proposal getProposalFromResultsNode(byte[] proposal) {
            // Either the barrier or the state node should have the current state
            assert (proposal != null);
            ByteBuffer proposalData = ByteBuffer.wrap(proposal).asReadOnlyBuffer();
            REQUEST_TYPE requestType = REQUEST_TYPE.values()[proposalData.get()];
            int originalVersion = proposalData.getInt();
            // Maximum state size is 64K
            int oldStateSize = proposalData.getShort();
            int origLimit = proposalData.limit();
            assert Proposal.s_prevStateIndex == proposalData.position();

            int endOfOldState = Proposal.s_prevStateIndex + oldStateSize;
            proposalData.limit(endOfOldState);
            ByteBuffer oldState = proposalData.slice();
            proposalData.position(endOfOldState);
            proposalData.limit(origLimit);
            ByteBuffer proposedState = proposalData.slice();
            return new Proposal(requestType, originalVersion, oldState, proposedState);
        }

        private class Proposal {
            static final int s_prevStateIndex = Byte.BYTES + Integer.BYTES + Short.BYTES;
            public final REQUEST_TYPE m_requestType;
            // original proposal version if replay otherwise -1
            public final int m_originalVersion;
            public final ByteBuffer m_previousState;
            public final ByteBuffer m_proposal;

            Proposal(REQUEST_TYPE requestType, ByteBuffer previousState, ByteBuffer proposal) {
                this(requestType, -1, previousState, proposal);
            }

            private Proposal(REQUEST_TYPE requestType, int originalVersion, ByteBuffer previousState,
                    ByteBuffer proposal) {
                m_requestType = requestType;
                m_originalVersion = originalVersion;
                m_previousState = previousState;
                m_proposal = proposal;
            }

            /**
             * @return A new proposal instance which is a replay of this proposal
             */
            Proposal asReplay(int originalVersion) {
                return isReplay() ? this : new Proposal(m_requestType, originalVersion, m_previousState, m_proposal);
            }

            boolean isReplay() {
                return m_originalVersion >= 0;
            }

            /**
             * @return this proposal serialized as a {@code byte[]}
             */
            byte[] serialize() {
                if (m_log.isTraceEnabled()) {
                    m_log.trace(String.format("%s: Building proposal %s: previous %s action %s", m_stateMachineId,
                            m_requestType, stateToString(m_previousState.slice()),
                            (m_requestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK
                                    || m_requestType == REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK)
                                            ? taskToString(m_proposal.slice())
                                            : stateToString(m_proposal.slice())));
                }
                ByteBuffer serialized = ByteBuffer
                        .allocate(s_prevStateIndex
                        + m_proposal.remaining() + m_previousState.remaining());

                serialized.put((byte) m_requestType.ordinal());
                serialized.putInt(m_originalVersion);
                serialized.putShort((short) m_previousState.remaining());
                serialized.put(m_previousState.slice());
                serialized.put(m_proposal.slice());
                assert !serialized.hasRemaining();
                return serialized.array();
            }
        }

        private Result createResult(byte[] resultBody) {
            ByteBuffer buffer = ByteBuffer.allocate(Result.s_bodyStart + (resultBody == null ? 0 : resultBody.length));
            buffer.putInt(m_lastProposalVersion);
            buffer.put((byte) (resultBody == null ? 0 : 1));
            if (resultBody != null) {
                buffer.put(resultBody);
            }
            buffer.flip();
            return new Result(buffer);
        }

        // Set of classes to retrieve and interpret a proposal result

        /**
         * Base class for all result types. Result header is parsed and can be accessed through this class
         */
        private class Result {
            private static final int s_versionIndex = 0;
            private static final int s_hasResultIndex = s_versionIndex + Integer.BYTES;
            static final int s_bodyStart = s_hasResultIndex + Byte.BYTES;
            final ByteBuffer m_resultData;

            Result(String memberId) throws KeeperException, InterruptedException {
                this(memberId, null);
            }

            Result(String memberId, Watcher watcher) throws KeeperException, InterruptedException {
                this(ByteBuffer.wrap(m_zk.getData(ZKUtil.joinZKPath(m_barrierResultsPath, memberId), watcher, null)));
            }

            Result(ByteBuffer resultData) {
                m_resultData = resultData;
            }

            /**
             * @return {@code true} if there was data associated with the result node
             */
            boolean hasResult() {
                return m_resultData.get(s_hasResultIndex) == 1;
            }

            /**
             * @return the version of the proposal for which this result was generated
             */
            int getProposalVersion() {
                return m_resultData.getInt(s_versionIndex);
            }

            /**
             * @return the same result but with the current {@link StateMachineInstance#m_lastProposalVersion}
             */
            byte[] withRefreshedVersion() {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(String.format("%s: Replaying response with version %s as version %s", m_stateMachineId,
                            getProposalVersion(), m_lastProposalVersion));
                }
                ByteBuffer refreshed = ByteBuffer.allocate(m_resultData.limit());
                ByteBuffer source = m_resultData.asReadOnlyBuffer();
                source.position(s_hasResultIndex);
                refreshed.putInt(m_lastProposalVersion);
                refreshed.put(source);
                return refreshed.array();
            }

            byte[] serialize() {
                byte[] data = new byte[m_resultData.remaining()];
                m_resultData.asReadOnlyBuffer().get(data);
                return data;
            }
        }

        /**
         * Class for retrieving results which are simple agree, disagree or abstain
         */
        private class AgreementResult extends Result {
            AgreementResult(String memberId) throws KeeperException, InterruptedException {
                super(memberId);
            }

            /**
             * @return {@code true} if the host agrees with the proposal
             * @throws IndexOutOfBoundsException If {@link #hasResult()} returns {@code false}
             */
            boolean agrees() throws IndexOutOfBoundsException {
                return m_resultData.get(s_bodyStart) == 1;
            }

            @Override
            public String toString() {
                return hasResult() ? Boolean.toString(agrees()) : null;
            }
        }

        /**
         * Class for retrieving a task result
         */
        private class TaskResult extends Result {
            TaskResult(String memberId) throws KeeperException, InterruptedException {
                super(memberId);
            }

            /**
             * @return The result of the task as a read only {@link ByteBuffer}
             */
            ByteBuffer taskResult() {
                if (!hasResult()) {
                    return null;
                }
                ByteBuffer ro = m_resultData.asReadOnlyBuffer();
                ro.position(s_bodyStart);
                return ro.slice();
            }
        }

        private final BarrierParticipantsWatcher m_barrierParticipantsWatcher = new BarrierParticipantsWatcher();

        private class BarrierResultsWatcher implements Watcher {

            @Override
            public void process(final WatchedEvent event) {
                try {
                    if (isRunning()) {
                        submitCallable(HandlerForBarrierResultsEvent);
                    }
                } catch (RejectedExecutionException e) {
                    ssmLog.warn("ZK watch of Result Barrier was rejected by the SSM Thread");
                }
            }
        }

        private final BarrierResultsWatcher m_barrierResultsWatcher = new BarrierResultsWatcher();

        // Used only for Mocking StateMachineInstance
        public StateMachineInstance()
        {
            m_statePath = "MockInstanceStatePath";
            m_barrierResultsPath = "MockBarrierResultsPath";
            m_myResultPath = "MockMyResultPath";
            m_barrierParticipantsPath = "MockParticipantsPath";
            m_myParticipantPath = "MockMyParticipantPath";
            m_lockPath = "MockLockPath";
            m_log = null;
            m_stateMachineId = "MockStateMachineId";
            m_stateMachineName = "MockStateMachineName";
        }

        public StateMachineInstance(String instanceName, VoltLogger logger) throws RuntimeException {
            if (instanceName.equals(s_memberNode)) {
                throw new RuntimeException("State machine name may not be named " + s_memberNode);
            }
            assert(!instanceName.equals(s_memberNode));
            m_stateMachineName = instanceName;
            m_statePath = ZKUtil.joinZKPath(m_stateMachineRoot, instanceName);
            m_lockPath = ZKUtil.joinZKPath(m_statePath, "LOCK_CONTENDERS");
            m_barrierResultsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_RESULTS");
            m_myResultPath = ZKUtil.joinZKPath(m_barrierResultsPath, m_memberId);
            m_barrierParticipantsPath = ZKUtil.joinZKPath(m_statePath, "BARRIER_PARTICIPANTS");
            m_myParticipantPath = ZKUtil.joinZKPath(m_barrierParticipantsPath, m_memberId);
            m_log = logger;
            m_stateMachineId = "SMI " + m_ssmRootNode + "/" + m_stateMachineName + "/" + m_memberId;
            if (m_log.isDebugEnabled()) {
                m_log.debug(m_stateMachineId + " created.");
            }
        }

        private void setRequestedInitialState(ByteBuffer requestedInitialState) {
            assert(requestedInitialState != null);
            assert(requestedInitialState.remaining() < Short.MAX_VALUE);
            m_requestedInitialState = requestedInitialState;
        }

        /**
         * This method is called to register new StateMachine instances with the SSM. The SSM will start the
         * full initialization (join the community) when the correct number of instances have registered. If
         * this registration is the last instance, the method will wait until the initialization has been
         * completed. However, if this is not the last instance it will return immediately. This is generally
         * not an issue because callback will be generated to {@link setInitialState} when each StateMachine
         * knows the current state from the Quorum. When the registering object wants to directly wait on a
         * {@link Future}, the method {@link registerStateMachineWithManagerAsync} should be used instead.
         * @param requestedInitialState
         * @throws InterruptedException
         */
        public void registerStateMachineWithManager(ByteBuffer requestedInitialState) throws InterruptedException {
            setRequestedInitialState(requestedInitialState);
            ListenableFuture<Boolean> initComplete = registerStateMachine(this, false);
            if (initComplete == null) {
                return;
            }
            try {
                Boolean initSuccessful = initComplete.get();
                assert(initSuccessful);
            }
            catch (ExecutionException e) {
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e.getCause());
            }
        }

        /**
         * This method is called to register new StateMachine instances with the SSM. The SSM will start the
         * full initialization (join the community) when the correct number of instances have registered. This
         * method will always return immediately with a {@link Future} that can be used to determine when the
         * SSM has completed registration. Note that it is not necessary to wait for initialization to complete
         * since the SSM will also execute a callback to {@link setInitialState} when each StateMachine knows
         * the current state from the Quorum.
         * @param requestedInitialState
         * @throws InterruptedException
         * @return a Future indicating when the initialization is complete and a Boolean indication that the
         *         initialization was successful
         */
        public ListenableFuture<Boolean> registerStateMachineWithManagerAsync(ByteBuffer requestedInitialState) throws InterruptedException {
            setRequestedInitialState(requestedInitialState);
            return registerStateMachine(this, true);
        }

        /**
         * Notify the derived class that the state machine instance is being reset,
         * the derived class should reset its own specific states and provide a reset state
         * @param isDirectVictim true if the reset is caused by callback exception in this instance
         * @return a ByteBuffer encapsulating the reset state provided by the derived class
         */
        protected abstract ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim);

        private class AsyncStateMachineInitializer {
            final AtomicInteger m_pendingStateMachineInits;
            final Set<String> m_startingMemberSet;

            private void initializationFailed() {
                m_state.set(State.ERROR);
                m_initComplete.set(false);
            }

            private boolean noCommonKeeperExceptions(KeeperException.Code code) {
                if (code == KeeperException.Code.SESSIONEXPIRED ||
                        code == KeeperException.Code.CONNECTIONLOSS) {
                    // lost the full connection. some test cases do this...
                    // means zk shutdown without the elector being shutdown.
                    // ignore.
                    initializationFailed();
                    return false;
                }
                else if (code != KeeperException.Code.NODEEXISTS && code != KeeperException.Code.OK) {
                    VoltDB.crashLocalVoltDB(
                            "Unexpected failure (" + code.name() + ") in ZooKeeper while initializeInstances.",
                            true, null);
                }
                return true;
            }

            public AsyncStateMachineInitializer(final AtomicInteger pendingStateMachineInits,
                    Set<String> knownMembers) {
                m_pendingStateMachineInits = pendingStateMachineInits;
                m_startingMemberSet = knownMembers;
            }

            public void startInitialization() {
                new ZKAsyncCreateHandler(m_statePath, null, CreateMode.PERSISTENT) {
                    @Override
                    public void runImpl() {
                        if (noCommonKeeperExceptions(m_resultCode)) {
                            addLockPath();
                        }
                    }
                };
            }

            private void addLockPath() {
                new ZKAsyncCreateHandler(m_lockPath, null, CreateMode.PERSISTENT) {
                    @Override
                    public void runImpl() {
                        if (noCommonKeeperExceptions(m_resultCode)) {
                            addBarrierParticipantPath();
                        }
                    }
                };
            }

            private void addBarrierParticipantPath() {
                new ZKAsyncCreateHandler(m_barrierParticipantsPath, null, CreateMode.PERSISTENT) {
                    @Override
                    public void runImpl() {
                        if (noCommonKeeperExceptions(m_resultCode)) {
                            try {
                                initializeStateMachine(m_startingMemberSet);
                                if (m_pendingStateMachineInits.decrementAndGet() == 0) {
                                    m_initComplete.set(true);
                                }
                            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                                    | InterruptedException e) {
                                // Lost the connection or interrupted for shutdown
                                if (m_log.isDebugEnabled()) {
                                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                                            + " in addBarrierParticipantPath");
                                }
                                initializationFailed();
                            } catch (KeeperException e) {
                                VoltDB.crashLocalVoltDB("Unexpected failure in StateMachine.", true, e);
                            }
                        }
                    }
                };
            }
        }

        private AsyncStateMachineInitializer createAsyncInitializer(final AtomicInteger pendingStateMachineInits,
                final Set<String> knownMembers) {
            return new AsyncStateMachineInitializer(pendingStateMachineInits, knownMembers);
        }

        private void syncStateMachineInitialize(final Set<String> knownMembers) throws KeeperException, InterruptedException {
            addIfMissing(m_statePath, CreateMode.PERSISTENT, null);
            addIfMissing(m_lockPath, CreateMode.PERSISTENT, null);
            addIfMissing(m_barrierParticipantsPath, CreateMode.PERSISTENT, null);
            initializeStateMachine(knownMembers);
        }

        private void initializeStateMachine(Set<String> knownMembers)
                throws KeeperException, InterruptedException {
            SmiCallable outOfLockWork = null;
            try (Mutex.Releaser r = m_mutex.acquire()) {
                // Make sure the child count is correct so that an init does not race with a released lock where the
                // results have not been processed yet. If it is non-zero, it means results still need to be collected
                // by some nodes even if the distributed lock list is empty.
                m_currentParticipants = m_zk.getChildren(m_barrierParticipantsPath, null).size();
                boolean ownDistributedLock = requestDistributedLock();
                Proposal proposal = new Proposal(REQUEST_TYPE.INITIALIZING,
                        m_requestedInitialState.asReadOnlyBuffer(), m_requestedInitialState.asReadOnlyBuffer());
                addIfMissing(m_barrierResultsPath, CreateMode.PERSISTENT, proposal.serialize());
                boolean stateMachineNodeCreated = false;
                if (ownDistributedLock) {
                    // Only the very first initializer of the state machine will both get the lock and successfully
                    // allocate "STATE_INITIALIZED". This guarantees that only one node will assign the initial state.
                    stateMachineNodeCreated = addIfMissing(ZKUtil.joinZKPath(m_statePath, "STATE_INITIALIZED"),
                            CreateMode.PERSISTENT, null);
                }

                if (m_membershipChangePending) {
                    getLatestMembership();
                } else if (m_knownMembers == null) {
                    // Members could be set by the callback which was handled between the async initialization calls
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
                    addResultEntry(new byte[] { 1 });
                    m_lockWaitingOn = "bogus"; // Avoids call to notifyDistributedLockWaiter
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Initialized (first member) with State "
                                + stateToString(m_synchronizedState.asReadOnlyBuffer()));
                    }
                    m_initializationCompleted = true;
                    cancelDistributedLock();
                    outOfLockWork = new ChainedCallable(checkForBarrierParticipantsChange()) {
                        @Override
                        protected void callImpl() {
                            // Notify the derived object that we have a stable state
                            try {
                                setInitialState(readOnlyResult);
                            } catch (Exception e) {
                                if (m_log.isDebugEnabled()) {
                                    m_log.debug("Error in StateMachineInstance callbacks.", e);
                                }
                                m_initializationCompleted = false;
                                submitCallable(new CallbackExceptionHandler(StateMachineInstance.this));
                            }
                        }
                    };

                }
                else {
                    // To get a stable result set, we need to get the lock for this state machine. If someone else has
                    // the
                    // lock they can clear the stale results out from under us.
                    if (ownDistributedLock) {
                        outOfLockWork = initializeFromActiveCommunity();
                    } else {
                        // This means we will ignore the current update if there is one in progress.
                        // Note that if we are not the next waiter for the lock, we will blindly
                        // accept the next proposal and use the outcome to set our initial state.
                        Stat nodeStat = new Stat();
                        Proposal currentProposal = getProposalFromResultsNode(
                                m_zk.getData(m_barrierResultsPath, false, nodeStat));
                        m_lastProposalVersion = nodeStat.getVersion();
                        addNullResultEntry(currentProposal);
                        outOfLockWork = checkForBarrierParticipantsChange();
                    }
                }
            }

            if (outOfLockWork != null) {
                outOfLockWork.call();
            }
        }

        /*
         * This state machine and all other state machines under this manager are being removed
         */
        private void disableMembership() {
            try (Mutex.Releaser r = m_mutex.acquire()) {
                if (m_log.isTraceEnabled()) {
                    m_log.trace(m_stateMachineId + ": Disabling member");
                }
                // put in two separate try-catch blocks so that both actions are attempted
                try {
                    m_zk.delete(m_myParticipantPath, -1);
                }
                catch (KeeperException | InterruptedException e) {
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                                + " in disableMembership");
                    }
                }
                try {
                    if (m_ourDistributedLockName != null) {
                        if (m_log.isDebugEnabled()) {
                            m_log.debug(m_stateMachineId + ": cancelLockRequest (Shutdown) for "
                                    + m_ourDistributedLockName);
                        }
                        m_zk.delete(m_ourDistributedLockName, -1);
                    }
                }
                catch (KeeperException | InterruptedException e) {
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                                + " in disableMembership");
                    }
                }
                m_initializationCompleted = false;
            }
        }

        private void reset(boolean isDirectVictim) {
            ByteBuffer resetState = notifyOfStateMachineReset(isDirectVictim);
            // if we are the direct victim, use the reset state as requested initial state
            if (isDirectVictim) {
                m_requestedInitialState = resetState;
            }
            // else simply use currently requested initial state or synchronized state as requested initial state
            else {
                if (m_requestedInitialState == null) {
                    assert(m_synchronizedState != null);
                    m_requestedInitialState = m_synchronizedState;
                }
            }
            m_synchronizedState = null;

            m_membershipChangePending = false;
            m_stateChangeInitiator = false;
            m_ourDistributedLockName = null;
            m_lockWaitingOn = null;
            m_knownMembers = null;
            m_holdingDistributedLock = false;
            m_pendingProposal = null;
            m_currentRequestType = REQUEST_TYPE.INITIALIZING;
            m_memberResults = null;
            m_lastProposalVersion = 0;

            m_myResultPath = ZKUtil.joinZKPath(m_barrierResultsPath, m_memberId);
            m_myParticipantPath = ZKUtil.joinZKPath(m_barrierParticipantsPath, m_memberId);
            m_stateMachineId = "SMI " + m_ssmRootNode + "/" + m_stateMachineName + "/" + m_memberId;
        }

        private int getProposalVersion() throws KeeperException {
            int proposalVersion = -1;
            try {
                proposalVersion = m_zk.exists(m_barrierResultsPath, null).getVersion();
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received "+e.getClass().getSimpleName()+" in getProposalVersion");
                }
            }
            return proposalVersion;
        }

        /**
         * Post a replay proposal if a result exists for the original proposal or a result is currently being generated
         *
         * @param proposal Current proposal being handled
         * @return {@code true} if a replay result or equivalent is or will be posted
         * @throws KeeperException
         * @throws InterruptedException
         */
        private boolean replayLastResult(Proposal proposal) throws KeeperException, InterruptedException {
            if (proposal.isReplay()) {
                if (m_pendingProposal != null) {
                    // Proposal is still being processed so that result will count as replay result
                    return true;
                }

                if (m_lastResult == null || m_lastResult.getProposalVersion() < proposal.m_originalVersion) {
                    // Do not have a previous result to replay
                    return false;
                }

                // Proposal is a replay and there is a last result which matches so upsert the replay result
                byte[] result = m_lastResult.withRefreshedVersion();
                if (m_log.isDebugEnabled()) {
                    m_log.debug(String.format("%s: Replaying response to request %d:%s with result: %s",
                            m_stateMachineId, m_lastProposalVersion, m_currentRequestType,
                            (result == null || result.length < 10 || m_log.isTraceEnabled()) ? Arrays.toString(result)
                                    : "suppressed"));
                }
                try {
                    m_zk.setData(m_myResultPath, result, -1);
                } catch (KeeperException.NoNodeException e) {
                    m_zk.create(m_myResultPath, result, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                return true;
            }
            return false;
        }

        private SmiCallable checkForBarrierParticipantsChange() throws KeeperException {
            assert(debugIsLocalStateLocked());
            try {
                m_participantsChangePending = false;
                int newParticipantCnt = m_zk.getChildren(m_barrierParticipantsPath, m_barrierParticipantsWatcher).size();
                Stat nodeStat = new Stat();
                // inspect the m_barrierResultsPath and get the new and old states and version
                // At some point this can be optimized to not examine the proposal all the time
                byte statePair[] = m_zk.getData(m_barrierResultsPath, false, nodeStat);
                int proposalVersion = nodeStat.getVersion();
                if (proposalVersion != m_lastProposalVersion) {
                    m_lastProposalVersion = proposalVersion;
                    m_currentParticipants = newParticipantCnt;
                    if (!m_stateChangeInitiator) {
                        // This is an indication that a new proposal or replay has been made
                        Proposal proposal = getProposalFromResultsNode(statePair);
                        if (replayLastResult(proposal)) {
                            return null;
                        }

                        assert(m_pendingProposal == null);

                        m_currentRequestType = proposal.m_requestType;
                        if (m_requestedInitialState != null) {
                            // Since we have not initialized yet, we acknowledge this proposal with an empty result.
                            addNullResultEntry(proposal);
                        }
                        else {
                            // Don't add ourselves as a participant because we don't care about the results
                            REQUEST_TYPE type = m_currentRequestType;
                            if (type == REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST) {
                                // The request is from a new member who found the results in an ambiguous state.
                                byte result[] = new byte[1];
                                if (proposal.m_proposal.equals(m_synchronizedState)) {
                                    result[0] = (byte)1;
                                }
                                else {
                                    assert(proposal.m_previousState.equals(m_synchronizedState));
                                    result[0] = (byte)0;
                                }
                                addResultEntry(result);
                            }
                            else {
                                // We track the number of people waiting on the results so we know when the result is stale and
                                // the next lock holder can initiate a new state proposal.
                                m_zk.create(m_myParticipantPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                                // increment the participant count by 1, so that lock notifications can be correctly guarded
                                m_currentParticipants += 1;

                                m_pendingProposal = proposal.m_proposal;
                                ByteBuffer proposedState = m_pendingProposal.asReadOnlyBuffer();
                                assert (proposal.m_previousState.equals(m_synchronizedState)) : String
                                        .format("%s: %s proposed previous: %s, actual previous: %s", m_stateMachineId,
                                                type, stateToString(proposal.m_previousState),
                                                stateToString(m_synchronizedState));
                                if (m_log.isDebugEnabled()) {
                                    if (type == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                                        m_log.debug(m_stateMachineId + ": Received new State proposal " +
                                                stateToString(proposedState.asReadOnlyBuffer()));
                                    }
                                    else {
                                        m_log.debug(m_stateMachineId + ": Received new Task request " +
                                                taskToString(proposedState.asReadOnlyBuffer()));
                                    }
                                }
                                return () -> {
                                    if (type == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                                        try {
                                            stateChangeProposed(proposedState);
                                        } catch (Exception e) {
                                            if (m_log.isDebugEnabled()) {
                                                m_log.debug("Error in StateMachineInstance callbacks.", e);
                                            }
                                            m_initializationCompleted = false;
                                            submitCallable(new CallbackExceptionHandler(this));
                                        }
                                    }
                                    else {
                                        try {
                                            taskRequested(proposedState);
                                        } catch (Exception e) {
                                            if (m_log.isDebugEnabled()) {
                                                m_log.debug("Error in StateMachineInstance callbacks.", e);
                                            }
                                            m_initializationCompleted = false;
                                            submitCallable(new CallbackExceptionHandler(this));
                                        }
                                    }
                                };
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
                            return () -> {
                                try {
                                    taskRequested(taskRequest);
                                } catch (Exception e) {
                                    if (m_log.isDebugEnabled()) {
                                        m_log.debug("Error in StateMachineInstance callbacks.", e);
                                    }
                                    m_initializationCompleted = false;
                                    submitCallable(new CallbackExceptionHandler(this));
                                }
                            };
                        }
                    }
                }
                else {
                    m_currentParticipants = newParticipantCnt;
                    if (canObtainDistributedLock()) {
                        // We can finally notify the lock waiter because everyone is finished evaluating the previous state proposal
                        return notifyDistributedLockWaiter();
                    }
                }
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                // Lost the connection or interrupted for shutdown
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in checkForBarrierParticipantsChange");
                }
            }
            return null;
        }

        /**
         * Utility method to validate that all of the conditions are met to be able to obtain the distributed lock
         *
         * @return {@code true} if this host is able to obtain the distributed lock
         */
        private boolean canObtainDistributedLock() {
            return m_ourDistributedLockName != null && !m_participantsChangePending
                    && m_ourDistributedLockName.equals(m_lockWaitingOn) && m_currentParticipants == 0;
        }

        private void monitorParticipantChanges() throws KeeperException {
            // always start checking for participation changes after the result notifications
            // or initialization notifications to ensure these notifications happen before lock
            // ownership notifications.
            SmiCallable outOfLockWork;
            try (Mutex.Releaser r = m_mutex.acquire()) {
                outOfLockWork = checkForBarrierParticipantsChange();
            }
            if (outOfLockWork != null) {
                outOfLockWork.call();
            }
        }

        private RESULT_CONCENSUS resultsAgreeOnSuccess(Set<String> memberList)
                throws KeeperException, InterruptedException {
            boolean agree = false;
            for (String memberId : memberList) {
                try {
                    AgreementResult result = new AgreementResult(memberId);
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(String.format("%s: Checking result from member %s: %s", m_stateMachineId, memberId,
                                result));
                    }
                    if (result.hasResult()) {
                        if (!result.agrees()) {
                            return RESULT_CONCENSUS.DISAGREE;
                        }
                        agree = true;
                    }
                } catch (NoNodeException ke) {
                    // This can happen when a new member joins and other members detect the new member before
                    // it's initialization code is called and a Null result is supplied to treat this as a null result.
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(String.format("%s: Could not find a result from member %s", m_stateMachineId,
                                memberId));
                    }
                }
            }
            if (agree) {
                return RESULT_CONCENSUS.AGREE;
            }
            return RESULT_CONCENSUS.NO_QUORUM;
        }

        private List<ByteBuffer> getUncorrelatedResults(ByteBuffer taskRequest, Set<String> memberList)
                throws KeeperException {
            return getCorrelatedResults(taskRequest, memberList).values().stream().filter(r -> r != null)
                    .collect(Collectors.toList());
        }

        private Map<String, ByteBuffer> getCorrelatedResults(ByteBuffer taskRequest, Set<String> memberList)
                throws KeeperException {
            // Treat ZooKeeper failures as empty result
            Map<String, ByteBuffer> results = new HashMap<String, ByteBuffer>();
            try {
                for (String memberId : memberList) {
                    TaskResult result = new TaskResult(memberId);
                    if (result.hasResult()) {
                        ByteBuffer bb = result.taskResult();
                        results.put(memberId, bb);
                        if (m_log.isDebugEnabled()) {
                            m_log.debug(m_stateMachineId + ":    " + memberId + " reports Result " +
                                    taskResultToString(taskRequest.asReadOnlyBuffer(), bb.asReadOnlyBuffer()));
                        }
                    }
                    else {
                        if (m_log.isDebugEnabled()) {
                            m_log.debug(m_stateMachineId + ":    " + memberId + " did not supply a Task Result");
                        }
                        // Signal the caller that a member didn't answer
                        results.put(memberId, null);
                    }
                }
                // Remove ourselves from the participants list to unblock the next distributed lock waiter
                m_zk.delete(m_myParticipantPath, -1);
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                results = new HashMap<String, ByteBuffer>();
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in getCorrelatedResults");
                }
            }
            return results;
        }

        // The number of results is a superset of the membership so analyze the results
        private SmiCallable processResultQuorum(Set<String> memberList) throws KeeperException {
            assert (m_currentRequestType != REQUEST_TYPE.INITIALIZING
                    || (m_requestedInitialState != null && m_stateChangeInitiator));
            m_memberResults = null;
            if (m_requestedInitialState != null) {
                // We can now initialize this state machine instance
                assert(m_holdingDistributedLock);
                try {
                    if (m_stateChangeInitiator && m_currentRequestType == REQUEST_TYPE.INITIALIZING) {
                        // Not initialized and state change initiator but request type is initializing means a quorum
                        // was not previously seen. Only check known members since only those members can post results
                        for (String member : m_knownMembers) {
                            Result result = new Result(member, m_barrierResultsWatcher);
                            if (result.getProposalVersion() != m_lastProposalVersion) {
                                if (m_log.isDebugEnabled()) {
                                    m_log.debug(
                                            String.format("%s: %s version does not match %s != %s", m_stateMachineId,
                                                    member, result.getProposalVersion(), m_lastProposalVersion));
                                }
                                m_memberResults = memberList;
                                return null;
                            }
                        }
                        m_stateChangeInitiator = false;
                        m_pendingProposal = null;
                        m_zk.delete(m_myParticipantPath, -1);
                        // Initialize from the community skipping the quorum check
                        return --m_currentParticipants == 0 ? initializeFromActiveCommunity(false) : null;
                    }

                    assert(m_synchronizedState == null);
                    Stat versionInfo = new Stat();
                    byte oldAndProposedState[] = m_zk.getData(m_barrierResultsPath, false, versionInfo);
                    assert(versionInfo.getVersion() == m_lastProposalVersion);
                    Proposal existingAndProposedStates =
                            getProposalFromResultsNode(oldAndProposedState);
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
                                Proposal stableState = new Proposal(REQUEST_TYPE.INITIALIZING,
                                        m_synchronizedState.asReadOnlyBuffer(), m_synchronizedState.asReadOnlyBuffer());
                                Stat newProposalStat = m_zk.setData(m_barrierResultsPath, stableState.serialize(), -1);
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
                    m_zk.delete(m_myParticipantPath, -1);
                } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                        | InterruptedException e) {
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                                + " in processResultQuorum");
                    }
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
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Initialized (concensus) with State " +
                                stateToString(m_synchronizedState.asReadOnlyBuffer()));
                    }
                    m_initializationCompleted = true;
                    return () -> {
                        try {
                            setInitialState(readOnlyResult);
                        } catch (Exception e) {
                            if (m_log.isDebugEnabled()) {
                                m_log.debug("Error in StateMachineInstance callbacks.", e);
                            }
                            m_initializationCompleted = false;
                            submitCallable(new CallbackExceptionHandler(this));
                        }

                        if (m_initializationCompleted) {
                            // If we are ready to provide an initial state to the derived state machine, add us to
                            // participants watcher so we can see the next request
                            monitorParticipantChanges();
                        }
                    };
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
                        m_zk.delete(m_myParticipantPath, -1);
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
                            assert(m_holdingDistributedLock);
                            m_stateChangeInitiator = false;
                            cancelDistributedLock();
                        }
                    } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                            | InterruptedException e) {
                        success = false;
                        if (m_log.isDebugEnabled()) {
                            m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                                    + " in processResultQuorum");
                        }
                    }
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Proposed state " + (success?"succeeded ":"failed ") +
                                stateToString(attemptedChange.asReadOnlyBuffer()));
                    }
                    final boolean finalSuccess = success;
                    return () -> {
                        // Notify the derived state machine engine of the current state
                        try {
                            proposedStateResolved(initiator, attemptedChange, finalSuccess);
                        } catch (Exception e) {
                            if (m_log.isDebugEnabled()) {
                                m_log.debug("Error in StateMachineInstance callbacks.", e);
                            }
                            m_initializationCompleted = false;
                            submitCallable(new CallbackExceptionHandler(this));
                        }

                        if (m_initializationCompleted) {
                            monitorParticipantChanges();
                        }
                    };
                }
                else {
                    // Process the results of a TASK request
                    ByteBuffer taskRequest = m_pendingProposal.asReadOnlyBuffer();
                    m_pendingProposal = null;
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": All members completed task " + taskToString(taskRequest.asReadOnlyBuffer()));
                    }
                    if (m_currentRequestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK) {
                        Map<String, ByteBuffer> results = getCorrelatedResults(taskRequest, memberList);
                        if (m_stateChangeInitiator) {
                            // Since we don't care if we are the last to go away, remove ourselves from the participant
                            // list
                            assert (m_holdingDistributedLock);
                            m_stateChangeInitiator = false;
                            cancelDistributedLock();
                        }
                        return () -> {
                            try {
                                correlatedTaskCompleted(initiator, taskRequest, results);
                            } catch (Exception e) {
                                if (m_log.isDebugEnabled()) {
                                    m_log.debug("Error in StateMachineInstance callbacks.", e);
                                }
                                m_initializationCompleted = false;
                                submitCallable(new CallbackExceptionHandler(this));
                            }

                            if (m_initializationCompleted) {
                                monitorParticipantChanges();
                            }
                        };
                    }
                    else {
                        List<ByteBuffer> results = getUncorrelatedResults(taskRequest, memberList);
                        if (m_stateChangeInitiator) {
                            // Since we don't care if we are the last to go away, remove ourselves from the participant list
                            assert(m_holdingDistributedLock);
                            m_stateChangeInitiator = false;
                            cancelDistributedLock();
                        }
                        return () -> {
                            try {
                                uncorrelatedTaskCompleted(initiator, taskRequest, results);
                            } catch (Exception e) {
                                if (m_log.isDebugEnabled()) {
                                    m_log.debug("Error in StateMachineInstance callbacks.", e);
                                }
                                m_initializationCompleted = false;
                                submitCallable(new CallbackExceptionHandler(this));
                            }

                            if (m_initializationCompleted) {
                                monitorParticipantChanges();
                            }
                        };
                    }
                }
            }
            return null;
        }

        private SmiCallable checkForBarrierResultsChanges() throws KeeperException {
            assert(debugIsLocalStateLocked());
            if (m_pendingProposal == null) {
                // Don't check for barrier results until we notice the participant list change.
                return null;
            }
            Set<String> membersWithResults;
            try {
                membersWithResults = ImmutableSet.copyOf(m_zk.getChildren(m_barrierResultsPath,
                        m_barrierResultsWatcher));
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                membersWithResults = new TreeSet<String>(Arrays.asList("We died so avoid Quorum path"));
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in checkForBarrierResultsChanges");
                }
            }

            if (membersWithResults.containsAll(m_knownMembers)) {
                return processResultQuorum(membersWithResults);
            }
            m_memberResults = membersWithResults;
            return null;
        }

        /*
         * Assumes this state machine owns the distributed lock and can either interrogate the existing state
         * or request the current state from the community (if the existing state is ambiguous)
         */
        private SmiCallable initializeFromActiveCommunity() throws KeeperException {
            return initializeFromActiveCommunity(true);
        }

        private SmiCallable initializeFromActiveCommunity(boolean performQuorumCheck) throws KeeperException {
            byte oldAndProposedState[];
            try {
                Stat lastProposal = new Stat();
                oldAndProposedState = m_zk.getData(m_barrierResultsPath, false, lastProposal);
                m_lastProposalVersion = lastProposal.getVersion();

                Proposal currentProposal = getProposalFromResultsNode(oldAndProposedState);

                if (performQuorumCheck && currentProposal.m_requestType != REQUEST_TYPE.INITIALIZING
                        && !isThereExistingResultsQuorum()) {
                    // No quorum exists so force all hosts to post a result just to make sure nothing is outstanding
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(String.format("%s: No quorum found in results. Replaying last proposal",
                                m_stateMachineId));
                    }
                    m_stateChangeInitiator = true;
                    Proposal replayProposal = currentProposal.asReplay(m_lastProposalVersion);
                    m_lastProposalVersion = wakeCommunityWithProposal(replayProposal);
                    m_pendingProposal = m_requestedInitialState;
                    addNullResultEntry(replayProposal);
                    return checkForBarrierResultsChanges();
                }

                if (currentProposal.m_requestType == REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST) {
                    RESULT_CONCENSUS result = resultsAgreeOnSuccess(m_knownMembers);
                    if (result == RESULT_CONCENSUS.AGREE) {
                        // Fall through to set up the initial state and provide notification of initialization
                        currentProposal = new Proposal(REQUEST_TYPE.INITIALIZING,
                                currentProposal.m_proposal.asReadOnlyBuffer(),
                                currentProposal.m_proposal.asReadOnlyBuffer());
                    }
                    else
                    if (result == RESULT_CONCENSUS.DISAGREE) {
                        // Fall through to set up the initial state and provide notification of initialization
                        currentProposal = new Proposal(REQUEST_TYPE.INITIALIZING,
                                currentProposal.m_previousState.asReadOnlyBuffer(),
                                currentProposal.m_previousState.asReadOnlyBuffer());
                    }
                    else {
                        // Another members outcome request was completed but a subsequent proposing member died
                        // between the time the results of this last request were removed and the new proposal
                        // was made. Fall through and propose a new LAST_CHANGE_OUTCOME_REQUEST.
                        currentProposal = new Proposal(REQUEST_TYPE.STATE_CHANGE_REQUEST,
                                currentProposal.m_previousState.asReadOnlyBuffer(),
                                currentProposal.m_proposal.asReadOnlyBuffer());
                    }
                }

                if (currentProposal.m_requestType == REQUEST_TYPE.STATE_CHANGE_REQUEST) {
                    // The last lock owner died before completing a state change or determining the current state
                    m_stateChangeInitiator = true;
                    m_pendingProposal = m_requestedInitialState;
                    m_currentRequestType = REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST;
                    Proposal stateChange = new Proposal(REQUEST_TYPE.LAST_CHANGE_OUTCOME_REQUEST,
                            currentProposal.m_previousState, currentProposal.m_proposal);
                    m_lastProposalVersion = wakeCommunityWithProposal(stateChange);
                    addNullResultEntry(stateChange);
                    return checkForBarrierResultsChanges();
                }
                else {
                    assert(currentProposal.m_requestType == REQUEST_TYPE.INITIALIZING ||
                            currentProposal.m_requestType == REQUEST_TYPE.CORRELATED_COORDINATED_TASK ||
                            currentProposal.m_requestType == REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK);
                    m_synchronizedState = currentProposal.m_previousState;
                    m_requestedInitialState = null;
                    ByteBuffer readOnlyResult = m_synchronizedState.asReadOnlyBuffer();
                    m_lastProposalVersion = lastProposal.getVersion();
                    m_pendingProposal = null;
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Initialized (existing) with State " +
                                stateToString(m_synchronizedState.asReadOnlyBuffer()));
                    }

                    m_initializationCompleted = true;
                    cancelDistributedLock();
                    // Add an acceptable result so the next initializing member recognizes an immediate quorum.
                    m_lockWaitingOn = "bogus"; // Avoids call to notifyDistributedLockWaiter below
                    return new ChainedCallable(checkForBarrierParticipantsChange()) {
                        @Override
                        public void callImpl() {
                            // Notify the derived object that we have a stable state
                            try {
                                setInitialState(readOnlyResult);
                            } catch (Exception e) {
                                if (m_log.isDebugEnabled()) {
                                    m_log.debug("Error in StateMachineInstance callbacks.", e);
                                }
                                m_initializationCompleted = false;
                                submitCallable(new CallbackExceptionHandler(StateMachineInstance.this));
                            }
                        }
                    };
                }
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in initializeFromActiveCommunity");
                }
            }
            return null;
        }

        /**
         * @return if all known members except this one have a posted result
         * @throws KeeperException      If there is an error retrieving the result list from zookeeper
         * @throws InterruptedException If this thread was interrupted
         */
        private boolean isThereExistingResultsQuorum() throws KeeperException, InterruptedException {
            return m_zk.getChildren(m_barrierResultsPath, null).containsAll(
                    m_knownMembers.stream().filter(m -> !m_memberId.equals(m)).collect(Collectors.toSet()));
        }

        private int wakeCommunityWithProposal(Proposal proposal) throws KeeperException {
            assert(m_holdingDistributedLock);
            assert(m_currentParticipants == 0);
            int newProposalVersion = -1;
            try {
                // No need to delete existing results for a replay since proposal version is used to detect quorum
                if (!proposal.isReplay()) {
                    List<String> results = m_zk.getChildren(m_barrierResultsPath, false);
                    for (String resultNode : results) {
                        try {
                            m_zk.delete(ZKUtil.joinZKPath(m_barrierResultsPath, resultNode), -1);
                        } catch (KeeperException.NoNodeException e) {
                            if (m_log.isDebugEnabled()) {
                                m_log.debug(m_stateMachineId + ": Skipped externally deleted "
                                        + "barrier child node in wakeCommunityWithProposal");
                            }
                        }
                    }
                }
                Stat newProposalStat = m_zk.setData(m_barrierResultsPath, proposal.serialize(), -1);
                m_zk.create(m_myParticipantPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                newProposalVersion = newProposalStat.getVersion();
                // force the participant count to be 1, so that lock notifications can be correctly guarded
                m_currentParticipants = 1;
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in wakeCommunityWithProposal");
                }
            }
            return newProposalVersion;
        }

        private final Callable<Void> HandlerForBarrierParticipantsEvent = new Callable<Void>() {
            @Override
            public Void call() throws KeeperException {
                SmiCallable outOfLockWork;
                try (Mutex.Releaser r = m_mutex.acquire()) {
                    if (!isRunning()) {
                        return null;
                    }
                    outOfLockWork = checkForBarrierParticipantsChange();
                }
                if (outOfLockWork != null) {
                    outOfLockWork.call();
                }
                return null;
            }
        };

        private final Callable<Void> HandlerForBarrierResultsEvent = new Callable<Void>() {
            @Override
            public Void call() throws KeeperException {
                SmiCallable outOfLockWork;
                try (Mutex.Releaser r = m_mutex.acquire()) {
                    if (!isRunning()) {
                        return null;
                    }
                    outOfLockWork = checkForBarrierResultsChanges();
                }

                if (outOfLockWork != null) {
                    outOfLockWork.call();
                }
                return null;
            }
        };

        private String getNextLockNodeFromList() throws KeeperException, InterruptedException {
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
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": " + m_ourDistributedLockName + " waiting on " + previous);
                    }
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

        private final Callable<Void> HandlerForDistributedLockEvent = new Callable<Void>() {
            @Override
            public Void call() throws KeeperException {
                SmiCallable outOfLockWork = null;
                try (Mutex.Releaser r = m_mutex.acquire()) {
                    if (!isRunning()) {
                        return null;
                    }
                    if (m_ourDistributedLockName != null) {
                        try {
                            m_lockWaitingOn = getNextLockNodeFromList();
                        } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                                | InterruptedException e) {
                            if (m_log.isDebugEnabled()) {
                                m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                                        + " in HandlerForDistributedLockEvent");
                            }
                            m_lockWaitingOn = "We died so we can't ever get the distributed lock";
                        }
                        if (canObtainDistributedLock()) {
                            // There are no more members still processing the last result
                            outOfLockWork = notifyDistributedLockWaiter();
                        }
                    }
                }

                if (outOfLockWork != null) {
                    outOfLockWork.call();
                }

                return null;
            }
        };

        private void cancelDistributedLock() throws KeeperException {
            assert(debugIsLocalStateLocked());
            if (m_log.isDebugEnabled()) {
                m_log.debug(m_stateMachineId + ": cancelLockRequest for " + m_ourDistributedLockName);
            }
            assert(m_holdingDistributedLock);
            try {
                m_zk.delete(m_ourDistributedLockName, -1);
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in cancelDistributedLock");
                }
            }
            m_ourDistributedLockName = null;
            m_holdingDistributedLock = false;
        }

        /*
         * Warns about membership change notification waiting in the executor thread
         */
        private void checkMembership() {
            m_membershipChangePending = true;
        }

        /*
         * Callback notification from executor thread of membership change
         */
        private void membershipChanged(Set<String> knownHosts, Set<String> addedMembers, Set<String> removedMembers)
                throws KeeperException {
            boolean notInitializing;
            SmiCallable outOfLockWork = null;
            try (Mutex.Releaser r = m_mutex.acquire()) {
                // Even though we got a direct update, membership could have changed again between the
                m_knownMembers = knownHosts;
                m_membershipChangePending = false;
                notInitializing = m_requestedInitialState == null;
                if (m_pendingProposal != null && m_memberResults != null
                        && m_memberResults.containsAll(m_knownMembers)) {
                    // We can stop watching for results since we have a quorum.
                    outOfLockWork = processResultQuorum(m_memberResults);
                }
            }
            if (outOfLockWork != null) {
                outOfLockWork.call();
            }
            if (notInitializing) {
                try {
                    membershipChanged(addedMembers, removedMembers);
                } catch (Exception e) {
                    if (m_log.isDebugEnabled()) {
                        m_log.debug("Error in StateMachineInstance callbacks.", e);
                    }
                    m_initializationCompleted = false;
                    submitCallable(new CallbackExceptionHandler(this));
                }
            }
        }

        /*
         * Retrieves member set from ZooKeeper
         */
        private void getLatestMembership() throws KeeperException {
            try {
                m_knownMembers = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, null));
                if (m_log.isDebugEnabled()) {
                    m_log.debug(String.format("%s: getLatestMembership Updating known members to: %s", m_stateMachineId,
                            m_knownMembers));
                }
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in getLatestMembership");
                }
            }
        }

        private SmiCallable notifyDistributedLockWaiter() throws KeeperException {
            assert(debugIsLocalStateLocked());
            assert(m_currentParticipants == 0);
            m_holdingDistributedLock = true;
            if (m_membershipChangePending) {
                getLatestMembership();
            }
            if (m_requestedInitialState != null) {
                // We are still waiting to initialize the state. Now that we have have the state lock we can
                // look at the last set of results. We need to set proposedState so results will be checked.
                assert(m_pendingProposal == null);
                m_pendingProposal = m_requestedInitialState;
                return initializeFromActiveCommunity();
            }
            else {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Granted lockRequest for " + m_ourDistributedLockName);
                }
                m_lockWaitingOn = null;
                // Notify the derived class that the lock is available
                return () -> {
                    try {
                        lockRequestCompleted();
                    } catch (Exception e) {
                        cancelLockRequest();
                        if (m_log.isDebugEnabled()) {
                            m_log.debug("Error in StateMachineInstance callbacks.", e);
                        }
                        m_initializationCompleted = false;
                        submitCallable(new CallbackExceptionHandler(this));
                    }
                };
            }
        }

        private boolean requestDistributedLock() throws KeeperException {
            try {
                if (m_ourDistributedLockName != null) {
                    m_log.error(m_stateMachineId + ": Requested distributed lock before prior state change or task has been completed");
                    return false;
                }
                assert(debugIsLocalStateLocked());
                m_ourDistributedLockName = m_zk.create(ZKUtil.joinZKPath(m_lockPath, "lock_"), null,
                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                m_lockWaitingOn = getNextLockNodeFromList();

                if (canObtainDistributedLock()) {
                    // Prevents a second notification.
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": requestLock successful for " + m_ourDistributedLockName);
                    }
                    m_lockWaitingOn = null;
                    m_holdingDistributedLock = true;
                    if (m_membershipChangePending) {
                        getLatestMembership();
                    }
                    return true;
                }
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                            + " in requestDistributedLock");
                }
            }
            return false;
        }

        /**
         * Add a new result entry node
         *
         * @param resultBody body for result. Must not be {@code null}
         * @throws KeeperException
         */
        private void addResultEntry(byte resultBody[]) throws KeeperException {
            assert resultBody != null;
            addResultEntry(resultBody, false);
        }

        /**
         * Add a null result entry for the {@code proposal}
         *
         * @param proposal
         * @throws KeeperException
         */
        private void addNullResultEntry(Proposal proposal) throws KeeperException {
            // Need to force a result to be posted for replays
            addResultEntry(null, proposal.isReplay());
        }

        /**
         * Add a new result entry. Should only be called by {@link #addResultEntry(byte[])} or
         * {@link #addNullResultEntry(Proposal)}
         *
         * @param resultBody     body of result
         * @param updateIfExists if the result node exists should it be updated with the new result
         * @throws KeeperException
         */
        private void addResultEntry(byte resultBody[], boolean updateIfExists) throws KeeperException {
            if (m_log.isDebugEnabled()) {
                m_log.debug(String.format("%s: Responding to request %d:%s with result: %s", m_stateMachineId,
                        m_lastProposalVersion, m_currentRequestType,
                        (resultBody == null || resultBody.length < 10 || m_log.isTraceEnabled())
                                ? Arrays.toString(resultBody)
                                : "suppressed"));
            }

            try {
                Result result = createResult(resultBody);
                byte[] serialized = result.serialize();
                do {
                    // Loop trying to post the result until either the result is posted or an unhandled exception is hit
                    try {
                        m_zk.create(m_myResultPath, serialized, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        break;
                    } catch (KeeperException.NodeExistsException e) {
                        /*
                         * There is a race during initialization where a null result is assigned once so a current
                         * proposal is not hung. However, if a new proposal is submitted by another instance between
                         * that assignment and the call to checkForBarrierParticipantsChange, a second null result is
                         * assigned.
                         */
                        if (m_requestedInitialState == null || resultBody != null) {
                            byte[] previousResult = null;
                            try {
                                previousResult = m_zk.getData(m_myResultPath, null, null);
                            } catch (Exception e1) {
                                m_log.error("Failed to retrieve existing result", e1);
                            }
                            org.voltdb.VoltDB.crashLocalVoltDB(
                                    "Unexpected failure in StateMachine; Two results created for one proposal.: "
                                            + Arrays.toString(previousResult),
                                    true, e);
                        } else if (updateIfExists) {
                            try {
                                m_zk.setData(m_myResultPath, serialized, -1);
                                break;
                            } catch (KeeperException.NoNodeException e1) {
                                if (m_log.isDebugEnabled()) {
                                    m_log.debug(m_stateMachineId + ": Failed to update result node retrying create");
                                }
                            }
                        } else {
                            // Return so last result is not updated
                            return;
                        }
                    }
                } while (true);
                m_lastResult = result;
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                if (m_log.isDebugEnabled()) {
                    m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName() + " in addResultEntry");
                }
            }
        }

        private SmiCallable assignStateChangeAgreement(boolean acceptable) throws KeeperException {
            assert(debugIsLocalStateLocked());
            assert(m_pendingProposal != null);
            assert(m_currentRequestType == REQUEST_TYPE.STATE_CHANGE_REQUEST);
            byte result[] = { (byte) (acceptable ? 1 : 0) };
            addResultEntry(result);
            if (acceptable) {
                // Since we are only interested in the results when we agree with the proposal
                return checkForBarrierResultsChanges();
            }
            else {
                m_pendingProposal = null;
                try {
                    // Since we don't care about the outcome remove ourself from the participant list
                    m_zk.delete(m_myParticipantPath, -1);
                } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                        | InterruptedException e) {
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Received " + e.getClass().getSimpleName()
                                + " in assignStateChangeAgreement");
                    }
                }
            }
            return null;
        }

        /*
         * Returns true when this state machine is a full participant of the community and has the
         * current state of the community
         */
        protected boolean isInitialized() {
            try (Mutex.Releaser r = m_mutex.acquire()) {
                return m_initializationCompleted && m_requestedInitialState == null;
            }
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
            try (Mutex.Releaser r = m_mutex.acquire()) {
                if (m_initializationCompleted) {
                    return requestDistributedLock();
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e);
            }
            return false;
        }

        /*
         * Cancels an outstanding lock request. Should only be used if a previously desirable
         * proposal is now obsolete (ie. use when lockRequestCompleted is called and a proposal
         * is no longer necessary).
         */
        protected void cancelLockRequest() {
            try (Mutex.Releaser r = m_mutex.acquire()) {
                if (m_initializationCompleted) {
                    assert (m_pendingProposal == null);
                    cancelDistributedLock();
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e);
            }
        }

        /*
         * Notifies of the successful completion of a previous lock request
         */
        protected void lockRequestCompleted() {}

        /*
         * Propose a new state. Only call after successful acquisition of the distributed lock.
         */
        protected void proposeStateChange(ByteBuffer proposedState) {
            assert m_holdingDistributedLock;
            assert(proposedState != null);
            assert(proposedState.remaining() < Short.MAX_VALUE);
            try {
                SmiCallable outOfLockWork;
                try (Mutex.Releaser r = m_mutex.acquire()) {
                    if (!m_initializationCompleted) {
                        return;
                    }
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
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId + ": Proposing new state "
                                + stateToString(m_pendingProposal.asReadOnlyBuffer()));
                    }
                    m_stateChangeInitiator = true;
                    m_currentRequestType = REQUEST_TYPE.STATE_CHANGE_REQUEST;
                    Proposal stateChange = new Proposal(REQUEST_TYPE.STATE_CHANGE_REQUEST,
                            m_synchronizedState.asReadOnlyBuffer(), m_pendingProposal.asReadOnlyBuffer());

                    m_lastProposalVersion = wakeCommunityWithProposal(stateChange);
                    outOfLockWork = assignStateChangeAgreement(true);
                }

                if (outOfLockWork != null) {
                    outOfLockWork.call();
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e);
            }
        }

        /*
         * Notification of a new proposed state change by another member.
         * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
         */
        protected void stateChangeProposed(ByteBuffer proposedState) {}

        /*
         * Called to accept or reject a new proposed state change by another member.
         */
        protected void requestedStateChangeAcceptable(boolean acceptable) {
            try {
                SmiCallable outOfLockWork;
                try (Mutex.Releaser r = m_mutex.acquire()) {
                    if (!m_initializationCompleted) {
                        return;
                    }
                    assert (!m_stateChangeInitiator);
                    if (m_log.isDebugEnabled()) {
                        m_log.debug(m_stateMachineId
                                + (acceptable ? ": Agrees with State proposal" : ": Disagrees with State proposal"));
                    }

                    outOfLockWork = assignStateChangeAgreement(acceptable);
                }
                if (outOfLockWork != null) {
                    outOfLockWork.call();
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e);
            }
        }

        /*
         * Notification of the outcome of a previous state change proposal.
         * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
         */
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {}

        /*
         * Propose a new task. Only call after successful acquisition of the distributed lock.
         */
        protected void initiateCoordinatedTask(boolean correlated, ByteBuffer proposedTask) {
            assert (proposedTask != null);
            assert (proposedTask.remaining() < Short.MAX_VALUE);
            try (Mutex.Releaser r = m_mutex.acquire()) {
                if (m_initializationCompleted) {
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
                            m_log.debug(m_stateMachineId + ": Requested new Task "
                                    + taskToString(m_pendingProposal.asReadOnlyBuffer()));
                        } else {
                            m_log.debug(m_stateMachineId + ": Requested unspecified new Task");
                        }
                    }
                    m_stateChangeInitiator = true;
                    m_currentRequestType = correlated ? REQUEST_TYPE.CORRELATED_COORDINATED_TASK
                            : REQUEST_TYPE.UNCORRELATED_COORDINATED_TASK;
                    Proposal taskProposal = new Proposal(m_currentRequestType,
                            m_synchronizedState.asReadOnlyBuffer(), proposedTask.asReadOnlyBuffer());
                    m_pendingProposal = proposedTask;
                    // Since we don't update m_lastProposalVersion, we will wake ourselves up
                    wakeCommunityWithProposal(taskProposal);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e);
            }
        }

        /*
         * Notification of a new task request by this member or another member.
         * warning: The ByteBuffer is not guaranteed to start at position 0 (avoid rewind, flip, ...)
         */
        protected void taskRequested(ByteBuffer proposedTask) {}

        /*
         * Called to accept or reject a new proposed state change by another member.
         */
        protected void requestedTaskComplete(ByteBuffer result)
        {
            SmiCallable outOfLockWork;
            try {
                try (Mutex.Releaser r = m_mutex.acquire()) {
                    if (!m_initializationCompleted) {
                        return;
                    }
                    assert (m_pendingProposal != null);
                    if (m_log.isDebugEnabled()) {
                        if (result.hasRemaining()) {
                            m_log.debug(m_stateMachineId + ": Local Task completed with result " + taskResultToString(
                                    m_pendingProposal.asReadOnlyBuffer(), result.asReadOnlyBuffer()));
                        } else {
                            m_log.debug(m_stateMachineId + ": Local Task completed with empty result");
                        }
                    }

                    addResultEntry(result.array());
                    outOfLockWork = checkForBarrierResultsChanges();
                }
                if (outOfLockWork != null) {
                    outOfLockWork.call();
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e);
            }
        }

        /*
         * Notification of the outcome of the task by all members sorted by memberId.
         * warning: The ByteBuffer taskRequest is not guaranteed to start at position 0 (avoid rewind, flip, ...)
         */
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results) {}

        /*
         * Notification of the outcome of the task by all members.
         * warning: The ByteBuffer taskRequest is not guaranteed to start at position 0 (avoid rewind, flip, ...)
         */
        protected void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, List<ByteBuffer> results) {}

        /*
         * Returns the current State. This request ignores pending proposals or outcomes. And may well be invalid as soon
         * as the result is returned if a new proposed state is pending. To avoid this get the Distributed Lock first.
         * warning: The ByteBuffer taskRequest is not guaranteed to start at position 0 (avoid rewind, flip, ...)
         */
        protected ByteBuffer getCurrentState() {
            try (Mutex.Releaser r = m_mutex.acquire()) {
                return m_initializationCompleted ? m_synchronizedState.asReadOnlyBuffer() : ByteBuffer.allocate(0);
            }
        }

        protected void membershipChanged(Set<String> addedMembers, Set<String> removedMembers) {}

        protected Set<String> getCurrentMembers() {
            try (Mutex.Releaser r = m_mutex.acquire()) {
                if (m_membershipChangePending) {
                    getLatestMembership();
                }
                return ImmutableSet.copyOf(m_knownMembers);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("SSM: Unexpected error encountered", true, e);
                return null; // never get here
            }
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
            try (Mutex.Releaser r = m_mutex.acquire()) {
                return m_holdingDistributedLock;
            }
        }

        protected abstract String stateToString(ByteBuffer state);

        protected String taskToString(ByteBuffer task) { return ""; }

        protected String taskResultToString(ByteBuffer task, ByteBuffer taskResult) { return ""; }

        /** Simple class which chains one callable to another */
        private abstract class ChainedCallable implements SmiCallable {
            private final SmiCallable m_previous;

            ChainedCallable(SmiCallable callable) {
                assert (debugIsLocalStateLocked());
                m_previous = callable;
            }

            @Override
            public final void call() throws KeeperException {
                if (m_previous != null) {
                    m_previous.call();
                }
                callImpl();
            }

            protected abstract void callImpl() throws KeeperException;
        }
    }

    public SynchronizedStatesManager(ZooKeeper zk, String rootPath, String ssmNodeName, String memberId)
            throws KeeperException, InterruptedException {
        this(zk, rootPath, ssmNodeName, memberId, 1);
    }

    // Used only for Mocking StateMachineInstance
    public SynchronizedStatesManager() {
        m_zk = null;
        m_registeredStateMachines = null;
        m_ssmRootNode = "MockRootForZooKeeper";
        m_stateMachineRoot = "MockRootForSSM";
        m_stateMachineMemberPath = "MockRootMembershipNode";
        m_memberId = "MockMemberId";
        m_canonical_memberId = "MockCanonicalMemberId";
        m_resetAllowance = 5;
        m_resetLimit = m_resetAllowance;
    }

    public SynchronizedStatesManager(ZooKeeper zk, String rootPath, String ssmNodeName, String memberId, int registeredInstances)
            throws KeeperException, InterruptedException {
        this(zk, rootPath, ssmNodeName, memberId, registeredInstances, 5); // default resetAllowance is 5
    }

    public SynchronizedStatesManager(ZooKeeper zk, String rootPath, String ssmNodeName, String memberId, int registeredInstances, int resetAllowance)
            throws KeeperException, InterruptedException {
        m_zk = zk;
        // We will not add ourselves as members in ZooKeeper until all StateMachineInstances have registered
        m_registeredStateMachines = new StateMachineInstance[registeredInstances];
        m_ssmRootNode = ssmNodeName;
        m_stateMachineRoot = ZKUtil.joinZKPath(rootPath, ssmNodeName);
        ByteBuffer numberOfInstances = ByteBuffer.allocate(4);
        numberOfInstances.putInt(registeredInstances);
        addIfMissing(m_stateMachineRoot, CreateMode.PERSISTENT, numberOfInstances.array());
        m_stateMachineMemberPath = ZKUtil.joinZKPath(m_stateMachineRoot, s_memberNode);
        m_canonical_memberId = memberId;
        m_resetCounter = 0;
        m_resetAllowance = resetAllowance;
        m_resetLimit = m_resetAllowance;
        m_memberId = m_canonical_memberId + "_v" + m_resetCounter;
    }

    public void shutdownSynchronizedStatesManager() throws InterruptedException {
        ListenableFuture<?> disableComplete = shutdownSynchronizedStatesManagerAsync();
        try {
            disableComplete.get();
        }
        catch (ExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }

    }

    public ListenableFuture<?> shutdownSynchronizedStatesManagerAsync() {
        return s_sharedEs.submit(disableInstances);
    }

    boolean isRunning() {
        return m_state.get() == State.RUNNING;
    }

    private final Callable<Void> disableInstances = new Callable<Void>() {
        @Override
        public Void call() throws KeeperException {
            if (ssmLog.isDebugEnabled()) {
                ssmLog.debug(m_stateMachineRoot + ": Shutting down");
            }
            try {
                m_state.set(State.SHUTDOWN);
                for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                    stateMachine.disableMembership();
                }
                m_zk.delete(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId), -1);
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                    | InterruptedException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
            } catch (KeeperException.NoNodeException e) {
                // FIXME: need to investigate why this happens on multinode shutdown
            }
            return null;
        }
    };

    private final Callable<Void> membershipEventHandler = new Callable<Void>() {
        @Override
        public Void call() throws KeeperException {
            if (isRunning()) {
                try {
                    checkForMembershipChanges();
                } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                        | InterruptedException e) {
                    // lost the full connection. some test cases do this...
                    // means shutdown without the elector being
                    // shutdown; ignore.
                }
            }
            return null;
        }
    };

    private class MembershipWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            try {
                if (isRunning()) {
                    for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                        stateMachine.checkMembership();
                    }
                    submitCallable(membershipEventHandler);
                }
            } catch (RejectedExecutionException e) {
                ssmLog.warn("ZK watch of Membership change was rejected by the SSM Thread");
            }
        }
    }

    private final MembershipWatcher m_membershipWatcher = new MembershipWatcher();

    private class AsyncSSMInitializer implements Callable<Void> {
        final AtomicInteger m_pendingStateMachineInits;

        public AsyncSSMInitializer() {
            assert(m_initComplete != null && !m_initComplete.isDone());
            m_pendingStateMachineInits = new AtomicInteger(m_registeredStateMachineInstances);
        }

        private void initializationFailed() {
            m_state.set(State.ERROR);
            m_initComplete.set(false);
        }

        private boolean noCommonKeeperExceptions(KeeperException.Code code) {
            if (code == KeeperException.Code.SESSIONEXPIRED ||
                    code == KeeperException.Code.CONNECTIONLOSS) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                initializationFailed();
                return false;
            }
            else if (code != KeeperException.Code.NODEEXISTS && code != KeeperException.Code.OK) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexpected failure (" + code.name() + ") in ZooKeeper while initializeInstances.",
                        true, null);
            }
            return true;
        }

        @Override
        public Void call() throws KeeperException {
            try {
                assert (m_registeredStateMachineInstances == ByteBuffer
                        .wrap(m_zk.getData(m_stateMachineRoot, false, null)).getInt());
            } catch (InterruptedException | SessionExpiredException | ConnectionLossException e) {
                if (ssmLog.isDebugEnabled()) {
                    ssmLog.debug(m_stateMachineRoot + ": Failed to double check registered state machine instances", e);
                }
                initializationFailed();
                return null;
            }

            // First become a member of the community
            new ZKAsyncCreateHandler(m_stateMachineMemberPath, null, CreateMode.PERSISTENT) {
                @Override
                public void runImpl() {
                    if (noCommonKeeperExceptions(m_resultCode)) {
                        addMemberIdIfMissing();
                    }
                }
            };
            return null;
        }

        private void addMemberIdIfMissing() {
            // This could fail because of an old ephemeral that has not aged out yet but assume this does not happen
            new ZKAsyncCreateHandler(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId),
                    null, CreateMode.EPHEMERAL) {
                @Override
                public void runImpl() {
                    if (noCommonKeeperExceptions(m_resultCode)) {
                        setInitialGroupMembers();
                    }
                }
            };
        }

        private void setInitialGroupMembers() {
            new ZKAsyncChildrenHandler(m_stateMachineMemberPath, m_membershipWatcher) {
                @Override
                public void runImpl() {
                    if (noCommonKeeperExceptions(m_resultCode)) {
                        m_groupMembers = ImmutableSet.copyOf(m_resultChildren);

                        // Then initialize each instance
                        for (StateMachineInstance instance : m_registeredStateMachines) {
                            StateMachineInstance.AsyncStateMachineInitializer init = instance.createAsyncInitializer(
                                    m_pendingStateMachineInits, m_groupMembers);
                            init.startInitialization();
                        }
                    }
                }
            };
        }
    }

    // During normal initialization the ZK tree can be set up for each statemachine asynchronously.
    // However, after a unexpected failure such as a fault inside a callback, the whole the SSM needs to
    // be locked down and reinitialized in a single blocking action so we will do all the ZK work
    // synchronously on the SSM thread and block all other SSM state machines.
    // TODO: See if it is possible to do this work asynchronously so other SSMs like DR are not blocked
    //       by a reset of a given Export SSM.
    void syncSSMInitialize() throws KeeperException {
        try {
            // First become a member of the community
            addIfMissing(m_stateMachineMemberPath, CreateMode.PERSISTENT, null);
            // This could fail because of an old ephemeral that has not aged out yet but assume this does not happen
            addIfMissing(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId), CreateMode.EPHEMERAL, null);

            m_groupMembers = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, m_membershipWatcher));
            // Then initialize each instance
            for (StateMachineInstance instance : m_registeredStateMachines) {
                instance.syncStateMachineInitialize(m_groupMembers);
            }
        } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException
                | InterruptedException e) {
            // lost the full connection. some test cases do this...
            // means zk shutdown without the elector being shutdown.
            // ignore.
            m_state.set(State.SHUTDOWN);
        }
    }

    private synchronized ListenableFuture<Boolean> registerStateMachine(StateMachineInstance machine, boolean asyncRequested) throws InterruptedException {
        assert(m_registeredStateMachineInstances < m_registeredStateMachines.length);

        m_registeredStateMachines[m_registeredStateMachineInstances] = (machine);
        if (m_registeredStateMachineInstances == 0) {
            m_initComplete = SettableFuture.create();
        }
        ++m_registeredStateMachineInstances;
        if (m_registeredStateMachineInstances == m_registeredStateMachines.length) {
            if (machine.m_log.isDebugEnabled()) {
                // lets make sure all the state machines are using unique paths
                Set<String> instanceNames = new HashSet<String>();
                for (StateMachineInstance instance : m_registeredStateMachines) {
                    if (!instanceNames.add(instance.m_statePath)) {
                        machine.m_log.error(": Multiple state machine instances with the same instanceName [" +
                                instance.m_statePath + "]");
                    }
                }
            }
            // Do all the initialization and notifications on the executor to avoid a race with
            // the group membership changes
            submitCallable(new AsyncSSMInitializer());
            return m_initComplete;
        }
        return asyncRequested ? m_initComplete : null;
    }

    private class CallbackExceptionHandler implements Callable<Void> {
        final StateMachineInstance m_directVictim;

        CallbackExceptionHandler(StateMachineInstance directVictim) {
            m_directVictim = directVictim;
        }

        @Override
        public Void call() throws Exception {
            // if the direct victim has already been reset, ignore the stale callback exception handling task
            if (!m_directVictim.isInitializationCompleted()) {
                assert (m_registeredStateMachineInstances > 0 && m_registeredStateMachineInstances == m_registeredStateMachines.length);

                disableInstances.call();

                if (m_lastResetTimeInMillis == -1L) {
                    m_lastResetTimeInMillis = System.currentTimeMillis();
                } else {
                    long currentTimeInMillis = System.currentTimeMillis();
                    // if a full reset clear threshold duration has passed after last reset, bump the reset limit
                    if (currentTimeInMillis - m_lastResetTimeInMillis >= RESET_CLEAR_THRESHOLD) {
                        m_resetLimit = m_resetCounter + m_resetAllowance;
                    }
                    m_lastResetTimeInMillis = currentTimeInMillis;
                }

                ++m_resetCounter;
                if (m_resetCounter > m_resetLimit) {
                    return null;
                }

                m_memberId = m_canonical_memberId + "_v" + m_resetCounter;
                try {
                    for (StateMachineInstance instance : m_registeredStateMachines) {
                        instance.reset(instance == m_directVictim);
                    }
                } catch (Exception e) {
                    return null; // if something wrong happened in reset(), give up as if the reset limit is hit
                }
                m_state.set(State.RUNNING);

                syncSSMInitialize();
            }
            return null;
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
            for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                stateMachine.membershipChanged(m_groupMembers, addedMembers, removedMembers);
            }
        }
    }

    /** Interface used by {@link StateMachineInstance} for work that needs to be performed outside of the member lock */
    private interface SmiCallable {
        void call() throws KeeperException;
    }
}
