/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltcore.zk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class StateMachineSnipits extends ZKTestBase {
    VoltLogger log = new VoltLogger("HOST");
    private final int NUM_AGREEMENT_SITES = 4;
    private final String stateMachineManagerRoot = "/test/db/statemachine";

    @Before
    public void setUp() throws Exception {
        setUpZK(NUM_AGREEMENT_SITES);
        ZooKeeper zk = m_messengers.get(0).getZK();
        ZKUtil.addIfMissing(zk, "/test", CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(zk, "/test/db", CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(zk, stateMachineManagerRoot, CreateMode.PERSISTENT, null);
    }

    @After
    public void tearDown() throws Exception {
        tearDownZK();
    }


    public void failSite(int site) throws Exception {
        m_messengers.get(site).shutdown();
        m_messengers.set(site, null);
    }

    public SynchronizedStatesManager addStateMachineManagerFor(int siteId, String communityName,
            String managerId, int numberOfStateMachines) {
        SynchronizedStatesManager ssm = null;
        try {
            assert(siteId < NUM_AGREEMENT_SITES);
            ssm = new SynchronizedStatesManager(m_messengers.get(siteId).getZK(),
                    stateMachineManagerRoot, communityName,
                    managerId, numberOfStateMachines);
        }
        catch (KeeperException | InterruptedException e) {
        }
        catch (Exception e) {
        }
        return ssm;
    }

    class DistributedLock extends SynchronizedStatesManager.StateMachineInstance {
        public boolean m_locked = false;
        private Lock m_mutex;

        public DistributedLock(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
        }

        @Override
        protected String stateToString(ByteBuffer state)
        {
            return "";
        }

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            return ByteBuffer.allocate(0);
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState)
        {
        }

        @Override
        protected void lockRequestCompleted() {
            // This won't always work because of a race between a cancel and a complete
            // but you get the idea.
            if (m_mutex == null)
                return;
            synchronized(m_mutex) {
                m_locked = true;
                m_mutex.notifyAll();
            }
        }

        public void acquireDistriutedLock() {
            assert(!m_locked);
            assert(m_mutex == null);
            m_mutex = new ReentrantLock();
            synchronized(m_mutex) {
                if (!requestLock()) {
                    try {
                        while(!m_locked) {
                            m_mutex.wait();
                        }
                    }
                    catch (InterruptedException e) {
                    }
                }
                else {
                    m_locked = true;
                }
            }
        }

        public void start() throws InterruptedException {
            registerStateMachineWithManager(ByteBuffer.allocate(0));
        }

        // With this you can monitor the lock without blocking
        public boolean acquireDistributedLockAsync(Lock passedMutex)
        {
            assert(m_mutex == null);
            assert(!m_locked);
            m_mutex = passedMutex;
            synchronized(m_mutex) {
                m_locked = requestLock();
            }
            return m_locked;
        }

        public void releaseDistributedLock() {
            assert(m_mutex != null);
            assert(m_locked);
            cancelLockRequest();
            synchronized(m_mutex) {
                m_locked = false;
            }
            m_mutex = null;
        }
    }

    private interface MemberChangeCallback {
        void membersAdded(Set<String> addedMembers);
        void membersRemoved(Set<String> removedMembers);
        void membersAddedAndRemoved(Set<String> addedMembers, Set<String> removedMembers);
    }

    public class MembershipChangeMonitor extends SynchronizedStatesManager.StateMachineInstance {
        protected MemberChangeCallback m_cb = null;

        public MembershipChangeMonitor(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
        }

        @Override
        protected void membershipChanged(Set<String> addedMembers, Set<String> removedMembers) {
            if (m_cb != null) {
                if (addedMembers.isEmpty()) {
                    if (!removedMembers.isEmpty()) {
                        m_cb.membersRemoved(removedMembers);
                    }
                }
                else
                if (removedMembers.isEmpty()) {
                    m_cb.membersAdded(addedMembers);

                }
                else {
                    m_cb.membersAddedAndRemoved(addedMembers, removedMembers);;
                }
            }
        }

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            return ByteBuffer.allocate(0);
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState)
        {
            m_cb.membersAdded(getCurrentMembers());
        }

        @Override
        protected String stateToString(ByteBuffer state)
        {
            return "";
        }

        /*
         * Returns the most accurate set of members. Note that the returned set may include or
         * members for which a notification has not yet been provided through the
         * MemberChangeCallback.
         */
        public Set<String> getKnownMembers() {
            return getCurrentMembers();
        }

        /*
         * Returns the memberId of the StateMachineManager this instance is registered with.
         */
        public String GetMyMemberId() {
            return getMemberId();
        }
    }


    private interface StateMachineCallback {
        void stateMachineInitialized();
        void lockRequestGranted();
        void stateChangeProposed(ByteBuffer newState);
        void stateChanged();
        void ourProposalAccepted();
        void ourProposalRejected();
    }

    public abstract class StateMachine extends SynchronizedStatesManager.StateMachineInstance {
        private ByteBuffer m_startingState;
        protected volatile ByteBuffer m_currentStateBuffer = null;
        protected StateMachineCallback m_cb;
        private final AtomicBoolean m_proposalPending = new AtomicBoolean(false);
        private boolean m_haveLock = false;

        protected StateMachine(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
        }

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            return m_startingState;
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState)
        {
            m_currentStateBuffer = currentAgreedState;
            m_cb.stateMachineInitialized();
        }

        @Override
        protected void lockRequestCompleted()
        {
            m_haveLock = true;
            m_cb.lockRequestGranted();
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState)
        {
            assert(!m_haveLock);
            boolean wasProposing = m_proposalPending.getAndSet(true);
            assert(!wasProposing);
            m_cb.stateChangeProposed(proposedState);
        }

        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
            if (ourProposal) {
                if (success) {
                    m_currentStateBuffer = proposedState;
                    m_cb.ourProposalAccepted();
                }
                else {
                    m_cb.ourProposalRejected();
                }
                m_haveLock = false;
                ourProposal = false;
            }
            else {
                if (success) {
                    m_currentStateBuffer = proposedState;
                    m_cb.stateChanged();
                }
            }
            m_proposalPending.set(false);
        }

        protected void setDefaultState(ByteBuffer defaultState) {
            m_startingState = defaultState;
        }

        public void start() throws InterruptedException {
            registerStateMachineWithManager(m_startingState);
        }

        public ListenableFuture<Boolean> startAsync() throws InterruptedException {
            return registerStateMachineWithManagerAsync(m_startingState);
        }

        public ByteBuffer getCurrentStateBuff() {
            return m_currentStateBuffer;
        }

        public void attemptStateChange() {
            assert(isInitialized());
            assert(!m_haveLock);
            if (requestLock()) {
                m_haveLock = true;
                m_cb.lockRequestGranted();
            }
        }

        public void cancelAttempt() {
            assert(isInitialized());
            assert(m_haveLock);
            m_haveLock = false;
            cancelLockRequest();
        }

        public void proposeChange(ByteBuffer proposal) {
            assert(isInitialized());
            assert(m_haveLock);
            boolean wasProposing = m_proposalPending.getAndSet(true);
            assert(!wasProposing);
            proposeStateChange(proposal);
        }
    }

    private interface TaskCallback {
        public void lockRequestGranted();
        public void processTask(ByteBuffer newTask);
        public void ourCorrelatedTaskComplete(Map<String, ByteBuffer> results);
        public void ourUncorrelatedTaskComplete(List<ByteBuffer> results);
        public void externalCorrelatedTaskComplete(Map<String, ByteBuffer> results);
        public void externalUncorrelatedTaskComplete(List<ByteBuffer> results);
    }

    public abstract class Task extends SynchronizedStatesManager.StateMachineInstance {
        protected TaskCallback m_cb;
        private final AtomicBoolean m_taskPending = new AtomicBoolean(false);
        private boolean m_haveLock = false;

        public Task(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
        }

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            return ByteBuffer.allocate(0);
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState) {
        }

        @Override
        protected void lockRequestCompleted() {
            m_haveLock = true;
            m_cb.lockRequestGranted();
        }

        @Override
        protected void taskRequested(ByteBuffer proposedTask) {
            boolean hadTask = m_taskPending.getAndSet(true);
            assert(!hadTask);
            m_cb.processTask(proposedTask);
        }

        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results) {
            boolean processingTask = m_taskPending.getAndSet(false);
            assert(processingTask);
            if (ourTask) {
                assert(m_haveLock);
                m_cb.ourCorrelatedTaskComplete(results);
                m_haveLock = false;
            }
            else {
                m_cb.externalCorrelatedTaskComplete(results);
            }
        }

        @Override
        protected void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, List<ByteBuffer> results) {
            boolean processingTask = m_taskPending.getAndSet(false);
            assert(processingTask);
            if (ourTask) {
                assert(m_haveLock);
                m_cb.ourUncorrelatedTaskComplete(results);
                m_haveLock = false;
            }
            else {
                m_cb.externalUncorrelatedTaskComplete(results);
            }
        }

        public void attemptTask() {
            assert(isInitialized());
            assert(!m_haveLock);
            if (requestLock()) {
                m_haveLock = true;
                m_cb.lockRequestGranted();
            }
        }

        public void cancelAttempt() {
            assert(isInitialized());
            assert(m_haveLock);
            m_haveLock = false;
            cancelLockRequest();
        }

        public void startTask(boolean correlated, ByteBuffer proposedTask) {
            assert(isInitialized());
            assert(m_haveLock);
            initiateCoordinatedTask(correlated, proposedTask);
        }

        public void supplyResponse(ByteBuffer taskResponse) {
            requestedTaskComplete(taskResponse);
        }
    }


    @Test
    public void testLockingExample() {
        // Create a state machine manager operating in the LOCK_TESTER domain that will run on ZooKeeper0.
        // This manager is named manager1 and it expects a single state machine instance.
        SynchronizedStatesManager ssm1 = addStateMachineManagerFor(0, "LOCK_TESTER", "manager1", 1);
        // Create a state machine manager operating in the LOCK_TESTER domain that will run on ZooKeeper1.
        // This manager is named manager2 and it expects a single state machine instance.
        SynchronizedStatesManager ssm2 = addStateMachineManagerFor(1, "LOCK_TESTER", "manager2", 1);

        // This is a little convoluted, but because StateMachineInstances are a nested class of
        // SynchronizedStatesManager we must specify the correct SynchronizedStatesManager in the
        // constructor. The second parameter is the state machine name shared by all instances wishing
        // to participate in the same shared machine across Manager instances using the same
        // domain (LOCK_TESTER).
        DistributedLock distributedLock1 = new DistributedLock(ssm1, "lockingStateMachine");
        try {
            distributedLock1.start();
        }
        catch (InterruptedException e1) {
            fail();
        }

        DistributedLock distributedLock2 = new DistributedLock(ssm2, "lockingStateMachine");
        try {
            distributedLock2.start();
        }
        catch (InterruptedException e1) {
            fail();
        }

        while (!distributedLock1.isInitialized()) {
            Thread.yield();
        }
        while (!distributedLock2.isInitialized()) {
            Thread.yield();
        }
        distributedLock1.acquireDistriutedLock();
        Lock waiterForLock2 = new ReentrantLock();
        distributedLock2.acquireDistributedLockAsync(waiterForLock2);
        try {
            ssm2 = null;
            failSite(1);
            distributedLock1.releaseDistributedLock();
        }
        catch (Exception e) {
            fail();
        }

        // Create a state machine manager operating in the LOCK_TESTER domain that will run on ZooKeeper0.
        // This manager is named manager3 and it expects a single state machine instance.
        SynchronizedStatesManager ssm3 = addStateMachineManagerFor(0, "LOCK_TESTER", "manager3", 1);
        DistributedLock distributedLock3 = new DistributedLock(ssm3, "lockingStateMachine");
        try {
            distributedLock3.start();
        }
        catch (InterruptedException e1) {
            fail();
        }

        try {
            // Note that distributedLock3 won't be initialized until distributedLock1 and distributedLock2 both
            // release their locks.
            while (!distributedLock3.isInitialized()) {
                Thread.yield();
            }

            Lock waiterForLock3 = new ReentrantLock();
            distributedLock3.acquireDistributedLockAsync(waiterForLock3);
            synchronized(waiterForLock3) {
                while (!distributedLock3.m_locked)
                    waiterForLock3.wait();
            }
            waiterForLock3 = null;
            distributedLock3.releaseDistributedLock();
            tearDownZK();
        }
        catch (Exception e) {
            fail();
        }
    }

    public class MembershipMonitor extends MembershipChangeMonitor implements MemberChangeCallback {
        public Set<String> m_members = new HashSet<String>();

        public MembershipMonitor(SynchronizedStatesManager ssm, String instanceName) {
            super(ssm, instanceName);
        }

        public void start() throws InterruptedException {
            m_cb = this;
            registerStateMachineWithManager(ByteBuffer.allocate(0));
        }

        public void membersAdded(Set<String> addedMembers) {
            m_members.addAll(addedMembers);
        }

        public void membersRemoved(Set<String> removedMembers) {
            m_members.removeAll(removedMembers);
        }

        public void membersAddedAndRemoved(Set<String> addedMembers, Set<String> removedMembers) {
            m_members.removeAll(removedMembers);
            m_members.addAll(addedMembers);
        }

        public boolean hasIdenticalMembership(MembershipMonitor membership) {
            if ( m_members.size() != membership.m_members.size() ) {
                return false;
            }
            HashSet<String> clone = new HashSet<String>(membership.m_members); // just use h2 if you don't need to save the original h2
            Iterator<String> it = m_members.iterator();
            while (it.hasNext() ){
                String member = it.next();
                if (clone.contains(member)){ // replace clone with h2 if not concerned with saving data from h2
                    clone.remove(member);
                } else {
                    return false;
                }
            }
            return true; // will only return true if sets are equal
        }
    }

    @Test
    public void testMembershipExample() {
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper0.
        // This manager is named monitor1 and it expects a single state machine instance.
        SynchronizedStatesManager ssm1 = addStateMachineManagerFor(0, "COMMUNITY", "monitor1", 1);
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper1.
        // This manager is named monitor2 and it expects a single state machine instance.
        SynchronizedStatesManager ssm2 = addStateMachineManagerFor(1, "COMMUNITY", "monitor2", 1);
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper0.
        // This manager is named monitor3 and it expects a single state machine instance.
        SynchronizedStatesManager ssm3 = addStateMachineManagerFor(0, "COMMUNITY", "monitor3", 1);
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper2.
        // This manager is named monitor4 and it expects a single state machine instance.
        SynchronizedStatesManager ssm4 = addStateMachineManagerFor(2, "COMMUNITY", "monitor4", 1);

        try {
            // This is a little convoluted, but because StateMachineInstances are a nested class of
            // SynchronizedStatesManager we must specify the correct SynchronizedStatesManager in the
            // constructor. The second parameter is the state machine name shared by all instances wishing
            // to participate in the same shared state machine across Manager instances using the same
            // domain (COMMUNITY).
            MembershipMonitor monitorForManager1 = new MembershipMonitor(ssm1, "memberMonitor");
            monitorForManager1.start();
            while (!monitorForManager1.isInitialized()) {
                Thread.yield();
            }

            MembershipMonitor monitorForManager2 = new MembershipMonitor(ssm2, "memberMonitor");
            monitorForManager2.start();
            while (!monitorForManager2.isInitialized()) {
                Thread.yield();
            }

            while (!monitorForManager1.hasIdenticalMembership(monitorForManager2)) {
                Thread.yield();
            }
            assertTrue(monitorForManager1.m_members.size() == 2);

            // This is sharing the same zookeeper client as monitorForManager1
            MembershipMonitor monitorForManager3 = new MembershipMonitor(ssm3, "memberMonitor");
            monitorForManager3.start();
            while (!monitorForManager3.isInitialized()) {
                Thread.yield();
            }

            while (!monitorForManager1.hasIdenticalMembership(monitorForManager3) &&
                    !monitorForManager1.hasIdenticalMembership(monitorForManager3)) {
                Thread.yield();
            }
            assertTrue(monitorForManager1.m_members.size() == 3);

            // Remove MembershipMonitor1 from ZooKeeper0 while keeping MembershipMonitor3 alive
            ssm1.shutdownSynchronizedStatesManager();

            while (monitorForManager3.m_members.size() != 2 &&
                    !monitorForManager2.hasIdenticalMembership(monitorForManager3)) {
                Thread.yield();
            }

            MembershipMonitor monitorForManager4 = new MembershipMonitor(ssm4, "memberMonitor");
            monitorForManager4.start();
            while (!monitorForManager4.isInitialized()) {
                Thread.yield();
            }

            // kill ZooKeeper0, removing monitor3.
            try {
                failSite(0);
            }
            catch (Exception e) {
                fail();
            }

            while (monitorForManager4.m_members.size() != 2 &&
                    !monitorForManager4.hasIdenticalMembership(monitorForManager2)) {
                Thread.yield();
            }
        }
        catch (InterruptedException e) {
            fail();
        }

    }


    public class ByteStateChanger extends StateMachine {
        private volatile boolean m_myChangePending = false;
        private byte m_currentState;
        private byte m_pendingState = 0;

        @Override
        protected String stateToString(ByteBuffer state)
        {
            return Byte.toString(state.get());
        }

        public ByteStateChanger(SynchronizedStatesManager ssm, String instanceName, byte defaultState) {
            super(ssm, instanceName);
            setDefaultState(ByteBuffer.wrap(new byte[] {defaultState}));
            m_cb = new smCallback();
        }

        private class smCallback implements StateMachineCallback {
            @Override
            public void stateMachineInitialized() {
                m_currentState = m_currentStateBuffer.get();
                m_currentStateBuffer.rewind();
            }

            @Override
            public void lockRequestGranted() {
                assert(m_myChangePending);
                byte[] arr = new byte[] {m_pendingState};
                proposeStateChange(ByteBuffer.wrap(arr));
            }

            @Override
            public void stateChangeProposed(ByteBuffer newState) {
                // This could implement an evaluation of the proposed state
                requestedStateChangeAcceptable(true);
            }

            @Override
            public synchronized void stateChanged() {
                m_currentState = m_currentStateBuffer.get();
                m_currentStateBuffer.rewind();

                // We can respond to the notification here
                if (m_myChangePending && m_currentState == m_pendingState) {
                    m_myChangePending = false;
                    cancelAttempt();
                }
            }

            @Override
            public void ourProposalAccepted() {
                m_currentState = m_pendingState;
                m_myChangePending = false;
            }

            @Override
            public void ourProposalRejected() {
                m_myChangePending = false;
            }
        }

        public void changeState(byte proposedState) {
            m_pendingState = proposedState;
            m_myChangePending = true;
            attemptStateChange();
        }

        public boolean changeStillPending() {
            return m_myChangePending;
        }

        public byte getState() {
            return m_currentState;
        }
    }

    public class ShortStateChanger extends StateMachine {
        private volatile boolean m_myChangePending = false;
        private short m_currentState;
        private short m_pendingState = 0;

        @Override
        protected String stateToString(ByteBuffer state)
        {
            return Short.toString(state.get());
        }

        public ShortStateChanger(SynchronizedStatesManager ssm, String instanceName, short defaultState) {
            // assumes big endian
            super(ssm, instanceName);
            ByteBuffer defaultStateBuff = ByteBuffer.allocate(2).putShort(defaultState);
            defaultStateBuff.flip();
            setDefaultState(defaultStateBuff);
            m_cb = new smCallback();
        }

        private class smCallback implements StateMachineCallback {
            @Override
            public void stateMachineInitialized() {
                m_currentState = m_currentStateBuffer.getShort();
                m_currentStateBuffer.rewind();
            }

            @Override
            public void lockRequestGranted() {
                assert(m_myChangePending);
                ByteBuffer pending = ByteBuffer.allocate(2);
                pending.putShort(m_pendingState);
                pending.flip();
                proposeStateChange(pending);
            }

            @Override
            public void stateChangeProposed(ByteBuffer newState) {
                // This could implement an evaluation of the proposed state
                requestedStateChangeAcceptable(true);
            }

            @Override
            public synchronized void stateChanged() {
                m_currentState = m_currentStateBuffer.getShort();
                m_currentStateBuffer.rewind();

                // We can respond to the notification here
                if (m_myChangePending && m_currentState == m_pendingState) {
                    m_myChangePending = false;
                    cancelAttempt();
                }
            }

            @Override
            public void ourProposalAccepted() {
                m_currentState = m_pendingState;
                m_myChangePending = false;
            }

            @Override
            public void ourProposalRejected() {
                m_myChangePending = false;
            }
        }

        public void changeState(short proposedState) {
            m_pendingState = proposedState;
            m_myChangePending = true;
            attemptStateChange();
        }

        public boolean changeStillPending() {
            return m_myChangePending;
        }

        public short getState() {
            return m_currentState;
        }
    }

    @Test
    public void testStateChangeExample() throws InterruptedException, ExecutionException {
        byte[] byteStates = new byte[] {5, 23, 54, 92, 118, 122};
        short[] shortStates = new short[] {5000, 6000, 7000, 8000, 9000, 10000};

        // Create a state machine manager operating in the COMMUNITY1 domain that will run on ZooKeeper0.
        // This manager is named manager1 and it expects 2 state machine instances.
        SynchronizedStatesManager ssmC1M1 = addStateMachineManagerFor(0, "COMMUNITY1", "manager1", 2);
        // Create a state machine manager operating in the COMMUNITY2 domain that will run on ZooKeeper1.
        // This manager is named manager1 and it expects 1 state machine instance.
        SynchronizedStatesManager ssmC2M1 = addStateMachineManagerFor(1, "COMMUNITY2", "manager1", 1);
        // Create a state machine manager operating in the COMMUNITY2 domain that will run on ZooKeeper0.
        // This manager is named manager2 and it expects 1 state machine instance.
        SynchronizedStatesManager ssmC2M2 = addStateMachineManagerFor(0, "COMMUNITY2", "manager2", 1);
        // Create a state machine manager operating in the COMMUNITY2 domain that will run on ZooKeeper0.
        // This manager is named manager2 and it expects 2 state machine instance.s
        SynchronizedStatesManager ssmC1M2 = addStateMachineManagerFor(2, "COMMUNITY1", "manager2", 2);

        // Initialize ssmC1M1 with a byte (with 5) and short (with 7000) state machine
        ByteStateChanger bC1M1 = new ByteStateChanger(ssmC1M1, "byteMachine", byteStates[0]);
        ShortStateChanger sC1M1 = new ShortStateChanger(ssmC1M1, "shortMachine", shortStates[2]);
        ListenableFuture<Boolean> bc1m1Ready = bC1M1.startAsync();
        ListenableFuture<Boolean> sc1m1Ready = sC1M1.startAsync();
        // This should be the same future because they share the same SSM
        assertEquals(bc1m1Ready, sc1m1Ready);
        while (!bC1M1.isInitialized() && !sC1M1.isInitialized()) {
            Thread.yield();
        }
        bc1m1Ready.get();
        assertTrue(bC1M1.m_currentState == byteStates[0]);
        assertTrue(sC1M1.m_currentState == shortStates[2]);

        // Initialize ssmC2M1 with a short state machine that will not participate in the same domain as sC1M1
        ShortStateChanger sC2M1 = new ShortStateChanger(ssmC2M1, "shortMachine", shortStates[1]);
        sC2M1.start();
        while (!sC2M1.isInitialized()) {
            Thread.yield();
        }
        assertTrue(sC2M1.m_currentState == shortStates[1]);

        // Initialize ssmC2M2 with a short state machine that will not participate in the same domain as sC1M1
        ShortStateChanger sC2M2 = new ShortStateChanger(ssmC2M2, "shortMachine", shortStates[3]);
        sC2M2.start();
        while (!sC2M2.isInitialized()) {
            Thread.yield();
        }
        assertTrue(sC2M2.m_currentState == sC2M1.m_currentState && sC2M2.m_currentState == shortStates[1]);

        // Change the state of sC2M2 and verify it is changed in sC2M1
        sC2M2.changeState(shortStates[0]);
        while (sC2M2.changeStillPending() && sC2M2.getState() != sC2M1.getState()) {
            Thread.yield();
        }
        assertFalse(sC2M2.getState() == sC1M1.getState());

        // Now we will add the byte and short state machines to synchronize with bC1M1 and sC1M1 respectively
        ByteStateChanger bC1M2 = new ByteStateChanger(ssmC1M2, "byteMachine", byteStates[4]);
        ShortStateChanger sC1M3 = new ShortStateChanger(ssmC1M2, "shortMachine", shortStates[4]);
        ListenableFuture<Boolean> bc1m2Ready = bC1M2.startAsync();
        // The first statemachine was started Asynchronously and this one is started Synchronously
        sC1M3.start();
        while (!bC1M2.isInitialized() && !sC1M3.isInitialized()) {
            Thread.yield();
        }
        assertTrue(bc1m2Ready.get() != null && bc1m2Ready.isDone());
        assertTrue(bC1M2.getState() == byteStates[0] && sC1M3.getState() == shortStates[2]);

        // Make 2 state changes in a row from 2 different members and verify the final state is the last one
        bC1M2.changeState(byteStates[0]);
        bC1M1.changeState(byteStates[4]);
        while (bC1M1.changeStillPending() && bC1M1.getState() == bC1M2.getState()) {
            Thread.yield();
        }
    }

    protected class TaskPerformanceAnalyzer extends Task implements TaskCallback {
        Date m_proposersStartTime;
        Date m_receiveTaskTime;
        Date m_taskCompleteTime;
        volatile long m_slowestResponseTime;
        long m_fastestResponseTime;
        String m_slowestResponder;
        String m_fastestResponder;

        protected String stateToString(ByteBuffer state) { return ""; }
        protected String taskToString(ByteBuffer task) { return "Start Timed Task"; }

        public TaskPerformanceAnalyzer(SynchronizedStatesManager ssm, String instanceName) {
            super(ssm, instanceName);
        }

        public void lockRequestGranted() {
            ByteBuffer taskRequest = ByteBuffer.allocate(8);
            taskRequest.putLong(new Date().getTime());
            taskRequest.flip();
            // We are only demonstrating correlated tasks
            startTask(true, taskRequest);
        }

        public void processTask(ByteBuffer newTask) {
            m_slowestResponseTime = 0;
            m_proposersStartTime = new Date(newTask.getLong());
            ByteBuffer taskResponse = ByteBuffer.allocate(8);
            m_receiveTaskTime = new Date();
            taskResponse.putLong(m_receiveTaskTime.getTime());
            taskResponse.flip();
            supplyResponse(taskResponse);
        }

        public void ourUncorrelatedTaskComplete(List<ByteBuffer> results) {
            // This would allow you to determine the slowest responder
        }
        public void externalUncorrelatedTaskComplete(List<ByteBuffer> results) {
            // This would allow you to determine the slowest responder
        }

        private void getLongestResponseTime(Map<String, ByteBuffer> results) {
            m_taskCompleteTime = new Date();
            long slowest = 0;
            long fastest = Long.MAX_VALUE;
            for (Map.Entry<String, ByteBuffer> result : results.entrySet()) {
                long responseTime = result.getValue().getLong();
                if (responseTime > slowest) {
                    m_slowestResponder = result.getKey();
                    slowest = responseTime;
                }
                if (responseTime < fastest) {
                    m_fastestResponder = result.getKey();
                    fastest = responseTime;
                }
            }
            m_fastestResponseTime = fastest - m_proposersStartTime.getTime();
            m_slowestResponseTime = slowest - m_proposersStartTime.getTime();
        }
        public void ourCorrelatedTaskComplete(Map<String, ByteBuffer> results) {
            getLongestResponseTime(results);
        }
        public void externalCorrelatedTaskComplete(Map<String, ByteBuffer> results) {
            getLongestResponseTime(results);
        }

        public void start() throws InterruptedException {
            registerStateMachineWithManager(ByteBuffer.allocate(0));
            m_cb = this;
        }

        public void startTimerCalculation() {
            m_slowestResponseTime = 0;
            attemptTask();
        }

        public boolean taskDone() {
            return m_slowestResponseTime != 0;
        }

        public long getRequestDelay() {
            return m_receiveTaskTime.getTime() - m_proposersStartTime.getTime();
        }

        public long getResponseDelay() {
            return m_taskCompleteTime.getTime() - m_receiveTaskTime.getTime();
        }

        public long getRoundTripDelay() {
            return m_taskCompleteTime.getTime() - m_proposersStartTime.getTime();
        }
    }

    @Test
    public void testTimingTask() {
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper0.
        // This manager is named timer1 and it expects 1 state machine instance.
        SynchronizedStatesManager ssm1 = addStateMachineManagerFor(0, "COMMUNITY", "timer1", 1);
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper1.
        // This manager is named timer2 and it expects a 1 state machine instance.
        SynchronizedStatesManager ssm2 = addStateMachineManagerFor(1, "COMMUNITY", "timer2", 1);
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper0.
        // This manager is named timer3 and it expects 1 state machine instance.
        SynchronizedStatesManager ssm3 = addStateMachineManagerFor(0, "COMMUNITY", "timer3", 1);
        // Create a state machine manager operating in the COMMUNITY domain that will run on ZooKeeper2.
        // This manager is named timer4 and it expects 1 state machine instance.
        SynchronizedStatesManager ssm4 = addStateMachineManagerFor(2, "COMMUNITY", "timer4", 1);

        TaskPerformanceAnalyzer tpa1 = new TaskPerformanceAnalyzer(ssm1, "performanceStateMachine");
        TaskPerformanceAnalyzer tpa2 = new TaskPerformanceAnalyzer(ssm2, "performanceStateMachine");
        TaskPerformanceAnalyzer tpa3 = new TaskPerformanceAnalyzer(ssm3, "performanceStateMachine");
        TaskPerformanceAnalyzer tpa4 = new TaskPerformanceAnalyzer(ssm4, "performanceStateMachine");

        try {
            tpa1.start();
            tpa2.start();
            tpa3.start();
            tpa4.start();

            while (!tpa1.isInitialized() || !tpa2.isInitialized() ||
                    !tpa3.isInitialized() || !tpa4.isInitialized()) {
                Thread.yield();
            }

            tpa1.startTimerCalculation();
            while (!tpa1.taskDone() || !tpa2.taskDone() || !tpa3.taskDone() || !tpa4.taskDone()) {
                Thread.yield();
            }

            System.out.println("SM Id | Request | Response | roundTrip");
            System.out.printf("  1     %5d      %5d      %5d\n", tpa1.getRequestDelay(), tpa1.getResponseDelay(), tpa1.getRoundTripDelay());
            System.out.printf("  2     %5d      %5d      %5d\n", tpa2.getRequestDelay(), tpa2.getResponseDelay(), tpa2.getRoundTripDelay());
            System.out.printf("  3     %5d      %5d      %5d\n", tpa3.getRequestDelay(), tpa3.getResponseDelay(), tpa3.getRoundTripDelay());
            System.out.printf("  4     %5d      %5d      %5d\n", tpa4.getRequestDelay(), tpa4.getResponseDelay(), tpa4.getRoundTripDelay());
        }
        catch (InterruptedException e) {
            fail();
        }

    }
}
