/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;

import com.google_voltpatches.common.base.Charsets;

public class TestStateMachine extends ZKTestBase {
    private final int NUM_AGREEMENT_SITES = 4;
    enum stateMachines {
        SMI1,
        SMI2
    };
    private final String stateMachineManagerRoot = "/test/db/States";

    VoltLogger log = new VoltLogger("HOST");
    SynchronizedStatesManager[] m_stateMachineGroup1 = new SynchronizedStatesManager[NUM_AGREEMENT_SITES];
    SynchronizedStatesManager[] m_stateMachineGroup2 = new SynchronizedStatesManager[NUM_AGREEMENT_SITES];
    BooleanStateMachine[] m_booleanStateMachinesForGroup1 = new BooleanStateMachine[NUM_AGREEMENT_SITES];
    BooleanStateMachine[] m_booleanStateMachinesForGroup2 = new BooleanStateMachine[NUM_AGREEMENT_SITES];
    ByteStateMachine[] m_byteStateMachinesForGroup2 = new ByteStateMachine[NUM_AGREEMENT_SITES];

    Boolean[] rawBooleanStates = new Boolean[] {false, true};
    ByteBuffer[] bsm_states = new ByteBuffer[] {ByteBuffer.wrap(rawBooleanStates[0].toString().getBytes(Charsets.UTF_8)),
                                                ByteBuffer.wrap(rawBooleanStates[1].toString().getBytes(Charsets.UTF_8))};
    byte[] rawByteStates = new byte[] {100, 110, 120};
    ByteBuffer[] msm_states = new ByteBuffer[] {ByteBuffer.wrap(new byte[]{rawByteStates[0]}),
                                                ByteBuffer.wrap(new byte[]{rawByteStates[1]}),
                                                ByteBuffer.wrap(new byte[]{rawByteStates[2]})};
    final String defaultTaskResult = "FINISHED THE WORK";


    byte getNextByteState(byte oldState) {

        for (int ii=0; ii<rawByteStates.length; ii++) {
            if (rawByteStates[ii] == oldState) {
                return rawByteStates[(ii+1)%rawByteStates.length];
            }
        }
        fail("The above should always resolve to a transition");
        return 0;
    }

    public void addStateMachinesFor(int Site) {
        String siteString = "zkClient" + Integer.toString(Site);
        try {
            // Create a SynchronizedStatesManager to manage a single BooleanStateMachine
            SynchronizedStatesManager ssm1 = new SynchronizedStatesManager(m_messengers.get(Site).getZK(),
                    stateMachineManagerRoot, "ssm1", siteString);
            m_stateMachineGroup1[Site] = ssm1;
            BooleanStateMachine bsm1 = new BooleanStateMachine(ssm1, "bool");
            m_booleanStateMachinesForGroup1[Site] = bsm1;

            // Create a SynchronizedStatesManager to manage both a BooleanStateMachine and ByteStateMachine
            SynchronizedStatesManager ssm2 = new SynchronizedStatesManager(m_messengers.get(Site).getZK(),
                    stateMachineManagerRoot, "ssm2", siteString, stateMachines.values().length);
            m_stateMachineGroup2[Site] = ssm2;
            BooleanStateMachine bsm2 = new BooleanStateMachine(ssm2, "bool");
            m_booleanStateMachinesForGroup2[Site] = bsm2;
            ByteStateMachine msm2 = new ByteStateMachine(ssm2, "byte");
            m_byteStateMachinesForGroup2[Site] = msm2;
        }
        catch (KeeperException | InterruptedException e) {
            //  Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void removeStateMachinesFor(int Site) {
        m_booleanStateMachinesForGroup1[Site] = null;
        m_booleanStateMachinesForGroup2[Site] = null;
        m_byteStateMachinesForGroup2[Site] = null;
        m_stateMachineGroup1[Site] = null;
        m_stateMachineGroup2[Site] = null;
    }

    public void registerGroup1BoolFor(int Site) throws InterruptedException {
        m_booleanStateMachinesForGroup1[Site].registerStateMachineWithManager(bsm_states[0]);
    }

    @Before
    public void setUp() throws Exception {
        setUpZK(NUM_AGREEMENT_SITES);
        ZooKeeper zk = m_messengers.get(0).getZK();
        ZKUtil.addIfMissing(zk, "/test", CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(zk, "/test/db", CreateMode.PERSISTENT, null);
        ZKUtil.addIfMissing(zk, stateMachineManagerRoot, CreateMode.PERSISTENT, null);
        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }
    }

    @After
    public void tearDown() throws Exception {
        // Clearing the arrays will deallocate the state machines
        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        tearDownZK();
    }

    public void failSite(int site) throws Exception {
        removeStateMachinesFor(site);
        m_messengers.get(site).shutdown();
        m_messengers.set(site, null);
    }

    public void recoverSite(int site) throws Exception {
        HostMessenger.Config config = new HostMessenger.Config();
        int recoverPort = config.internalPort + NUM_AGREEMENT_SITES - 1;
        config.internalPort += site;
        int clientPort = m_ports.next();
        config.zkInterface = "127.0.0.1:" + clientPort;
        m_siteIdToZKPort.put(site, clientPort);
        config.networkThreads = 1;
        config.coordinatorIp = new InetSocketAddress( recoverPort );
        HostMessenger hm = new HostMessenger(config, null, null);
        hm.start(null);
        m_messengers.set(site, hm);
        addStateMachinesFor(site);
    }


    class BooleanStateMachine extends SynchronizedStatesManager.StateMachineInstance {
        volatile boolean initialized = false;
        boolean makeProposal = false;
        boolean startTask = false;
        volatile int proposalsOrTasksCompleted = 0;
        volatile boolean ourProposalOrTaskFinished = false;
        boolean acceptProposalOrTask = true;
        boolean justHoldTheLock = false;
        boolean ignoreProposal = false;
        ByteBuffer proposed;
        volatile boolean state;
        boolean coorelatedTask = true;
        final String taskString = "DO SOME WORK";
        String taskResultString = defaultTaskResult;
        volatile Map<String, ByteBuffer> correlatedResults;
        volatile List<ByteBuffer> uncorrelatedResults;

        public boolean toBoolean(ByteBuffer buff) {
            byte[] b = new byte[buff.remaining()];
            buff.get(b, 0, b.length);
            String str = new String(b);
            return Boolean.valueOf(str);
        }

        public ByteBuffer toByteBuffer(boolean b) {
            String str = Boolean.toString(b);
            return ByteBuffer.wrap(str.getBytes(Charsets.UTF_8));
        }

        public BooleanStateMachine(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
            assertFalse("State machine local lock held after bool initialization", debugIsLocalStateLocked());
        }

        @Override
        protected void membershipChanged(Set<String> addedHosts, Set<String> removedHosts) {
            assertFalse("State machine local lock held after bool membership change", debugIsLocalStateLocked());
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState) {
            state = toBoolean(currentAgreedState);
            initialized = true;
            assertFalse("State machine local lock held after bool initial state notification", debugIsLocalStateLocked());
        }

        @Override
        protected void lockRequestCompleted() {
            assertFalse("State machine local lock held after bool distributed lock notification", debugIsLocalStateLocked());
            if (justHoldTheLock) {
                justHoldTheLock = false;
            }
            else {
                if (makeProposal) {
                    proposed = toByteBuffer(!state);
                    proposeStateChange(proposed);
                    assertFalse("State machine local lock held after bool delayed state change request", debugIsLocalStateLocked());
                }
                else {
                    assertTrue(startTask);
                    proposed = ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8));
                    initiateCoordinatedTask(coorelatedTask, proposed);
                    assertFalse("State machine local lock held after bool delayed task request", debugIsLocalStateLocked());
                }
            }
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState) {
            assertFalse("State machine local lock held after bool state change notification", debugIsLocalStateLocked());
            if (!ignoreProposal) {
                requestedStateChangeAcceptable(acceptProposalOrTask);
                assertFalse("State machine local lock held after bool state change acceptance", debugIsLocalStateLocked());
            }
            if (!acceptProposalOrTask) {
                acceptProposalOrTask = true;
                proposalsOrTasksCompleted++;
            }
        }

        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
            assertFalse("State machine local lock held after bool state change resolution", debugIsLocalStateLocked());
            assertTrue("Test state inconsistent with state machine", ourProposal == makeProposal);
            if (success) {
                state = toBoolean(proposedState);
            }
            if (ourProposal) {
                makeProposal = false;
                ourProposalOrTaskFinished = true;
            }
            acceptProposalOrTask = true;
            proposalsOrTasksCompleted++;
        }

        void switchState() {
            ourProposalOrTaskFinished = false;
            makeProposal = true;
            if (requestLock()) {
                proposed = toByteBuffer(!state);
                proposeStateChange(proposed);
                assertFalse("State machine local lock held after bool state change request", debugIsLocalStateLocked());
            }
        }

        @Override
        protected void taskRequested(ByteBuffer taskRequest) {
            assertFalse("State machine local lock held after bool task notification", debugIsLocalStateLocked());
            assertTrue(taskRequest.equals(ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8))));
            if (!ignoreProposal) {
                ByteBuffer completedResult = ByteBuffer.wrap(taskResultString.getBytes(Charsets.UTF_8));
                requestedTaskComplete(completedResult);
                assertFalse("State machine local lock held after bool task completion", debugIsLocalStateLocked());
            }
        }

        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results) {
            assertFalse("State machine local lock held after bool correlated task completion", debugIsLocalStateLocked());
            assertTrue(taskRequest.equals(ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8))));
            assertTrue(ourTask == startTask);
            startTask = false;
            acceptProposalOrTask = true;
            taskResultString = defaultTaskResult;
            correlatedResults = results;
            if (ourTask) {
                startTask = false;
                ourTask = false;
                ourProposalOrTaskFinished = true;
            }
            proposalsOrTasksCompleted++;
        }

        @Override
        protected void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, List<ByteBuffer> results) {
            assertFalse("State machine local lock held after bool uncorrelated task completion", debugIsLocalStateLocked());
            assertTrue(taskRequest.equals(ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8))));
            assertTrue(ourTask == startTask);
            coorelatedTask = true;
            startTask = false;
            acceptProposalOrTask = true;
            taskResultString = defaultTaskResult;
            uncorrelatedResults = results;
            if (ourTask) {
                ourProposalOrTaskFinished = true;
                startTask = false;
                ourTask = false;
            }
            proposalsOrTasksCompleted++;
        }

        void startTask() {
            ourProposalOrTaskFinished = false;
            correlatedResults = null;
            uncorrelatedResults = null;
            startTask = true;
            if (requestLock()) {
                proposed = ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8));
                initiateCoordinatedTask(coorelatedTask, proposed);
                assertFalse("State machine local lock held after bool task request", debugIsLocalStateLocked());
            }
        }

        @Override
        protected void staleTaskRequestNotification(ByteBuffer proposedTask) {
        }

        @Override
        protected String stateToString(ByteBuffer state)
        {
            byte[] b = new byte[state.remaining()];
            state.get(b, 0, b.length);
            return new String(b);
        }

        @Override
        protected String taskToString(ByteBuffer task)
        {
            byte[] b = new byte[task.remaining()];
            task.get(b, 0, b.length);
            return new String(b);
        }
    };

    class ByteStateMachine extends SynchronizedStatesManager.StateMachineInstance {
        volatile boolean initialized = false;
        boolean makeProposal = false;
        boolean startTask = false;
        volatile int proposalsOrTasksCompleted = 0;
        volatile boolean ourProposalOrTaskFinished = false;
        boolean acceptProposalOrTask = true;
        boolean justHoldTheLock = false;
        boolean ignoreProposal = false;
        ByteBuffer proposed;
        volatile byte state;
        boolean coorelatedTask = true;
        final String taskString = "DO SOME OTHER WORK";
        String taskResultString = defaultTaskResult;
        volatile Map<String, ByteBuffer> correlatedResults;
        volatile List<ByteBuffer> uncorrelatedResults;

        public byte toByte(ByteBuffer buff) {
            return buff.get();
        }

        public ByteBuffer toByteBuffer(byte b) {
            byte[] arr = new byte[] {b};
            return ByteBuffer.wrap(arr);
        }

        public ByteStateMachine(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
            assertFalse("State machine local lock held after byte initialization", debugIsLocalStateLocked());
        }

        @Override
        protected void membershipChanged(Set<String> addedHosts, Set<String> removedHosts) {
            assertFalse("State machine local lock held after byte membership change", debugIsLocalStateLocked());
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState) {
            state = toByte(currentAgreedState);
            initialized = true;
            assertFalse("State machine local lock held after byte initial state notification", debugIsLocalStateLocked());
        }

        @Override
        protected void lockRequestCompleted() {
            assertFalse("State machine local lock held after byte distributed lock notification", debugIsLocalStateLocked());
            if (justHoldTheLock) {
                justHoldTheLock = false;
            }
            else {
                if (makeProposal) {
                    proposed = toByteBuffer(getNextByteState(state));
                    proposeStateChange(proposed);
                    assertFalse("State machine local lock held after byte delayed state change request", debugIsLocalStateLocked());
                }
                else {
                    assertTrue(startTask);
                    proposed = ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8));
                    initiateCoordinatedTask(coorelatedTask, proposed);
                    assertFalse("State machine local lock held after byte delayed task request", debugIsLocalStateLocked());
                }
            }
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState) {
            assertFalse("State machine local lock held after byte state change notification", debugIsLocalStateLocked());
            if (!ignoreProposal) {
                requestedStateChangeAcceptable(acceptProposalOrTask);
                assertFalse("State machine local lock held after byte state change acceptance", debugIsLocalStateLocked());
            }
            if (!acceptProposalOrTask) {
                acceptProposalOrTask = true;
                proposalsOrTasksCompleted++;
            }
        }

        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
            assertFalse("State machine local lock held after byte state change resolution", debugIsLocalStateLocked());
            if (success) {
                state = toByte(proposedState);
            }
            if (ourProposal) {
                makeProposal = false;
                ourProposalOrTaskFinished = true;
            }
            proposalsOrTasksCompleted++;
        }

        void switchState() {
            ourProposalOrTaskFinished = false;
            makeProposal = true;
            if (requestLock()) {
                proposed = toByteBuffer(getNextByteState(state));
                proposeStateChange(proposed);
                assertFalse("State machine local lock held after byte state change request", debugIsLocalStateLocked());
            }
        }

        @Override
        protected void taskRequested(ByteBuffer taskRequest) {
            assertFalse("State machine local lock held after byte task notification", debugIsLocalStateLocked());
            assertTrue(taskRequest.equals(ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8))));
            if (!ignoreProposal) {
                ByteBuffer completedResult = ByteBuffer.wrap(taskResultString.getBytes(Charsets.UTF_8));
                requestedTaskComplete(completedResult);
                assertFalse("State machine local lock held after byte task completion", debugIsLocalStateLocked());
            }
        }

        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results) {
            assertFalse("State machine local lock held after byte correlated task completion", debugIsLocalStateLocked());
            assertTrue(taskRequest.equals(ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8))));
            assertTrue(ourTask == startTask);
            assertTrue(!ourTask || coorelatedTask);
            startTask = false;
            acceptProposalOrTask = true;
            taskResultString = defaultTaskResult;
            correlatedResults = results;
            if (ourTask) {
                ourTask = false;
                startTask = false;
                ourProposalOrTaskFinished = true;
            }
            proposalsOrTasksCompleted++;
        }

        @Override
        protected void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, List<ByteBuffer> results) {
            assertFalse("State machine local lock held after byte uncorrelated task completion", debugIsLocalStateLocked());
            assertTrue(taskRequest.equals(ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8))));
            assertTrue(ourTask == startTask);
            assertFalse(ourTask || coorelatedTask);
            coorelatedTask = true;
            startTask = false;
            acceptProposalOrTask = true;
            taskResultString = defaultTaskResult;
            uncorrelatedResults = results;
            if (ourTask) {
                ourTask = false;
                startTask = false;
                ourProposalOrTaskFinished = true;
            }
            proposalsOrTasksCompleted++;
       }

        void startTask() {
            correlatedResults = null;
            uncorrelatedResults = null;
            startTask = true;
            ourProposalOrTaskFinished = false;
            if (requestLock()) {
                proposed = ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8));
                initiateCoordinatedTask(coorelatedTask, proposed);
                assertFalse("State machine local lock held after byte task request", debugIsLocalStateLocked());
            }
        }

        @Override
        protected void staleTaskRequestNotification(ByteBuffer proposedTask) {
        }

        @Override
        protected String stateToString(ByteBuffer state)
        {
            return Byte.toString(state.get());
        }

        @Override
        protected String taskToString(ByteBuffer task)
        {
            byte[] b = new byte[task.remaining()];
            task.get(b, 0, b.length);
            return new String(b);
        }
    };

    boolean boolProposalOrTaskFinished(BooleanStateMachine[] machines, int expectedCompletions) {
        for (BooleanStateMachine sm : machines) {
            if (sm != null) {
                if (sm.proposalsOrTasksCompleted != expectedCompletions) {
                    return false;
                }
            }
        }
        return true;
    }

    boolean boolsSynchronized(BooleanStateMachine[] machines) {
        Boolean firstState = null;
        for (BooleanStateMachine sm : machines) {
            if (sm != null) {
                if (firstState == null) {
                    firstState = new Boolean(sm.state);
                }
                else
                if (sm.state != firstState) {
                    return false;
                }
            }
        }
        return true;
    }

    boolean boolsTaskCorrelatedResultsAgree(BooleanStateMachine[] machines, int expectedCompletions) {
        Map<String, ByteBuffer> firstCorrelatedResult = null;
        for (BooleanStateMachine sm : machines) {
            if (sm != null) {
                if (sm.proposalsOrTasksCompleted != expectedCompletions)
                    return false;
                if (firstCorrelatedResult == null) {
                    firstCorrelatedResult = sm.correlatedResults;
                }
                else
                if (!firstCorrelatedResult.equals(sm.correlatedResults)) {
                    return false;
                }
            }
        }
        return firstCorrelatedResult != null;
    }

    boolean boolsTaskUncorrelatedResultsAgree(BooleanStateMachine[] machines, int expectedCompletions) {
        List<ByteBuffer> firstUncorrelatedResult = null;
        for (BooleanStateMachine sm : machines) {
            if (sm != null) {
                if (sm.proposalsOrTasksCompleted != expectedCompletions)
                    return false;
                if (firstUncorrelatedResult == null) {
                    firstUncorrelatedResult = sm.uncorrelatedResults;
                }
                else
                if (!firstUncorrelatedResult.equals(sm.uncorrelatedResults)) {
                    return false;
                }
            }
        }
        return firstUncorrelatedResult != null;
    }

    boolean boolsInitialized(BooleanStateMachine[] machines) {
        for (BooleanStateMachine sm : machines) {
            if (sm != null) {
                if (!sm.initialized) {
                    return false;
                }
            }
        }
        return true;
    }


    boolean bytesSynchronized(ByteStateMachine[] machines) {
        Byte firstState = null;
        for (ByteStateMachine sm : machines) {
            if (sm != null) {
                if (firstState == null) {
                    firstState = new Byte(sm.state);
                }
                else
                if (sm.state != firstState) {
                    return false;
                }
            }
        }
        return true;
    }

    boolean byteProposalOrTaskFinished(ByteStateMachine[] machines, int expectedCompletions) {
        for (ByteStateMachine sm : machines) {
            if (sm != null) {
                if (sm.proposalsOrTasksCompleted != expectedCompletions) {
                    return false;
                }
            }
        }
        return true;
    }

    boolean bytesInitialized(ByteStateMachine[] machines) {
        for (ByteStateMachine sm : machines) {
            if (sm != null) {
                if (!sm.initialized) {
                    return false;
                }
            }
        }
        return true;
    }


    @Test
    public void testSingleNodeStateChange() {
        log.info("Starting testSuccessfulStateChange");
        try {
            m_booleanStateMachinesForGroup1[1] = null;
            m_booleanStateMachinesForGroup1[2] = null;
            m_booleanStateMachinesForGroup1[3] = null;
            registerGroup1BoolFor(0);

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            int ii = 0;
            for (; ii < 10; ii++) {
                if (i0.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(ii < 10);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertTrue(i0.state == newVal);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }


    @Test
    public void testSuccessfulStateChange() {
        log.info("Starting testSuccessfulStateChange");
        try {
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            int ii = 0;
            for (; ii < 10; ii++) {
                if (i0.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(ii < 10);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertTrue(i0.state == newVal);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testSingleRejectedProposal() {
        log.info("Starting testSingleRejectedProposal");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            i1.acceptProposalOrTask = false;
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            int ii = 0;
            for (; ii < 10; ii++) {
                if (i0.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(ii < 10);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertFalse(i0.state == newVal);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testAllRejectedProposal() {
        log.info("Starting testAllRejectedProposal");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            BooleanStateMachine i2 = m_booleanStateMachinesForGroup1[2];
            BooleanStateMachine i3 = m_booleanStateMachinesForGroup1[3];

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }
            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            i1.acceptProposalOrTask = false;
            i2.acceptProposalOrTask = false;
            i3.acceptProposalOrTask = false;
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i0.ourProposalOrTaskFinished && boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertFalse(i0.state == newVal);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testVerifyProposerCantReject() {
        log.info("Starting testVerifyProposerCantReject");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            i0.acceptProposalOrTask = false;
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i0.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(i0.state == newVal);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testLateJoiner() {
        log.info("Starting testLateJoiner");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            m_booleanStateMachinesForGroup1[1] = null;
            registerGroup1BoolFor(0);
            registerGroup1BoolFor(2);
            registerGroup1BoolFor(3);

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(i0.state == newVal);

            // Initialize last state machine
            m_booleanStateMachinesForGroup1[1] = i1;
            registerGroup1BoolFor(1);
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (boolsInitialized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            // make sure it came up
            assertTrue(boolsInitialized(m_booleanStateMachinesForGroup1));
            // make sure the updated state is consistent
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testAllLateJoiners() {
        log.info("Starting testAllLateJoiners");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            BooleanStateMachine i2 = m_booleanStateMachinesForGroup1[2];
            BooleanStateMachine i3 = m_booleanStateMachinesForGroup1[3];
            m_booleanStateMachinesForGroup1[0] = null;
            m_booleanStateMachinesForGroup1[2] = null;
            m_booleanStateMachinesForGroup1[3] = null;

            registerGroup1BoolFor(1);

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i1.state;
            i1.switchState();
            for (int ii = 0; ii < 10; ii++) {
                if (i1.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(i1.state == newVal);

            // Initialize last state machine
            m_booleanStateMachinesForGroup1[0] = i0;
            m_booleanStateMachinesForGroup1[2] = i2;
            m_booleanStateMachinesForGroup1[3] = i3;
            registerGroup1BoolFor(0);
            registerGroup1BoolFor(2);
            registerGroup1BoolFor(3);
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (boolsInitialized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            // make sure it came up
            assertTrue(boolsInitialized(m_booleanStateMachinesForGroup1));
            // make sure the updated state is consistent
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertTrue(i0.state == newVal);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testRecoverFromDeadHostHoldingLock() {
        log.info("Starting testRecoverFromDeadHostHoldingLock");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                m_booleanStateMachinesForGroup1[ii].registerStateMachineWithManager(bsm_states[0]);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i0.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(i0.state == newVal);

            i0.requestLock();
            // Don't propose anything. Just keep the lock and reset justHoldTheLock when we have the lock
            i1.justHoldTheLock = true;
            // i1 should not get the lock because i0 is holding it
            assertFalse(i1.requestLock());
            i0 = null;
            failSite(0);
            Thread.sleep(2000);
            // After i0 fails, i1 should have been notified that it has the lock
            assertFalse(i1.justHoldTheLock);
        }
        catch (Exception e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testRecoverFromContendingDeadHostRequestingLock() {
        log.info("Starting testRecoverFromContendingDeadHostRequestingLock");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            BooleanStateMachine i2 = m_booleanStateMachinesForGroup1[2];
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                m_booleanStateMachinesForGroup1[ii].registerStateMachineWithManager(bsm_states[0]);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;

            i0.justHoldTheLock = true;
            i0.requestLock();
            i1.justHoldTheLock = true;
            // t1 should not get the lock because t0 is holding it
            assertFalse(i1.requestLock());
            i2.switchState();
            i1 = null;
            failSite(1);
            Thread.sleep(1000);
            i0.cancelLockRequest();
            // After i1 fails and i0 release the lock i2's state change should be applied
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i2.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(i0.state == newVal);
        }
        catch (Exception e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testRoundRobinStates() {
        log.info("Starting testRoundRobinStates");
        try {
            ByteStateMachine i0 = m_byteStateMachinesForGroup2[0];
            ByteStateMachine i1 = m_byteStateMachinesForGroup2[1];
            ByteStateMachine i2 = m_byteStateMachinesForGroup2[2];
            ByteStateMachine i3 = m_byteStateMachinesForGroup2[3];

            // For any site all state machine instances must be registered before it participates with other sites
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                m_booleanStateMachinesForGroup2[ii].registerStateMachineWithManager(bsm_states[0]);
                m_byteStateMachinesForGroup2[ii].registerStateMachineWithManager(msm_states[0]);
            }

            while (!bytesInitialized(m_byteStateMachinesForGroup2)) {
                Thread.sleep(500);
            }
            assertTrue(bytesSynchronized(m_byteStateMachinesForGroup2));
            i0.switchState();   // will be rawByteStates[1]
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalOrTaskFinished && bytesSynchronized(m_byteStateMachinesForGroup2)) {
                    break;
                }
                Thread.sleep(500);
            }
            assert(i0.state == rawByteStates[1]);

            i1.switchState();   // will be rawByteStates[2]
            i2.switchState();   // will be rawByteStates[0]
            i3.switchState();   // will be rawByteStates[1]
            i0.switchState();   // will be rawByteStates[2]
            // We should now be back in the original state
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i0.ourProposalOrTaskFinished && bytesSynchronized(m_byteStateMachinesForGroup2)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(i2.state == rawByteStates[2]);
        }
        catch (Exception e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testMultipleStateMachines() {
        log.info("Starting testMultipleStateMachines");
        try {
            BooleanStateMachine g1i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine g2i0 = m_booleanStateMachinesForGroup2[0];
            ByteStateMachine g2j0 = m_byteStateMachinesForGroup2[0];
            ByteStateMachine g2j1 = m_byteStateMachinesForGroup2[1];

            // For any site all state machine instances must be registered before it participates with other sites
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                m_booleanStateMachinesForGroup2[ii].registerStateMachineWithManager(bsm_states[0]);
                m_byteStateMachinesForGroup2[ii].registerStateMachineWithManager(msm_states[0]);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup2)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup2));

            while (!bytesInitialized(m_byteStateMachinesForGroup2)) {
                Thread.sleep(500);
            }
            assertTrue(bytesSynchronized(m_byteStateMachinesForGroup2));

            // StateMachine Group 2 is stable. Change some states in Group 2 and start up the Group 1
            g2j0.switchState();     // set Group 2 Byte State to rawByteStates[1]
            g2i0.switchState();     // set Group 2 Bool State to true
            g2j1.switchState();     // set Group 2 Byte State to rawByteStates[2]

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                m_booleanStateMachinesForGroup1[ii].registerStateMachineWithManager(bsm_states[0]);
            }

            // Make sure group 1 is stable
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (boolsInitialized(m_booleanStateMachinesForGroup1) &&
                        boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop<10);

            // Change state of Group1
            g1i0.switchState();     // set Group 1 Bool State to true

            // Now make sure all Group1 and Group2 state changes were successful
            waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (boolProposalOrTaskFinished(m_booleanStateMachinesForGroup2, 1) &&
                        boolsSynchronized(m_booleanStateMachinesForGroup2) &&
                        byteProposalOrTaskFinished(m_byteStateMachinesForGroup2, 2) &&
                        bytesSynchronized(m_byteStateMachinesForGroup2) &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1) &&
                        boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            // Verify that the group2 state machine instances updated while group 1 was initializing
            assertTrue(g2i0.state);
            assertTrue(g2j0.state == rawByteStates[2]);
            assertTrue(g1i0.state);
       }
        catch (Exception e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testFailureDuringProposalStateChange() {
        log.info("Starting testFailureDuringProposalStateChange");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            i0.ignoreProposal = true;

            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i1.switchState();

            // Verify that state was not transitioned
            int waitLoop = 0;
            for (; waitLoop < 5; waitLoop++) {
                if (i1.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop == 5);

            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertFalse(i1.state);

            // Fail i0
            i0 = null;
            failSite(0);

            waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i1.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(i1.state == newVal);
        }
        catch (Exception e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testFailureDuringProposedTask() {
        log.info("Starting testFailureDuringProposedTask");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            i0.ignoreProposal = true;

            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            i1.startTask();

            // Verify that state was not transitioned
            int waitLoop = 0;
            for (; waitLoop < 5; waitLoop++) {
                if (i1.ourProposalOrTaskFinished || i1.correlatedResults != null) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop == 5);

            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));

            // Fail i0
            i0 = null;
            failSite(0);

            waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i1.ourProposalOrTaskFinished && boolsTaskCorrelatedResultsAgree(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
        }
        catch (Exception e) {
            fail("Exception occured during test.");
        }
    }


    @Test
    public void testSuccessfulUncorrelatedWithStateChangeTask() {
        log.info("Starting testSimpleTask");
        try {
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            i0.coorelatedTask = false;
            i0.startTask();
            // after task add a usually contending state change
            i1.switchState();

            boolean taskAndSwitchCompleted = false;
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i1.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 2) &&
                        boolsTaskUncorrelatedResultsAgree(m_booleanStateMachinesForGroup1, 2)) {
                    taskAndSwitchCompleted = true;
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(taskAndSwitchCompleted);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertTrue(i0.state);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testSuccessfulCorrelatedTask() {
        log.info("Starting testSimpleTask");
        try {
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            i0.coorelatedTask = true;
            i0.startTask();

            boolean taskCompleted = false;
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i0.ourProposalOrTaskFinished &&
                        boolsTaskCorrelatedResultsAgree(m_booleanStateMachinesForGroup1, 1)) {
                    taskCompleted = true;
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertTrue(taskCompleted);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Test
    public void testSingleTaskWithOneUniqueResult() {
        log.info("Starting testSingleRejectedProposal");
        try {
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            i1.acceptProposalOrTask = false;
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                if (i0.ourProposalOrTaskFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(waitLoop < 10);
            assertFalse(i0.state == newVal);
        }
        catch (InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

}
