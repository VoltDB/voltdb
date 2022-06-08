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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltcore.common.Constants;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.StartAction;
import org.voltdb.probe.MeshProber;
import org.voltdb.test.utils.RandomTestRule;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class TestStateMachine extends ZKTestBase {
    private static final int NUM_AGREEMENT_SITES = 4;
    enum stateMachines {
        SMI1,
        SMI2
    }

    @Rule
    public final TestName m_name = new TestName();

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

    private String [] coordinators;
    private MeshProber criteria;


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
        addStateMachinesFor(Site, false, false, false);
    }

    public void addStateMachinesFor(int Site, boolean g1BooleanBroken, boolean g2BooleanBroken, boolean g2ByteBroken) {
        String siteString = "zkClient" + Integer.toString(Site);
        try {
            // Create a SynchronizedStatesManager to manage a single BooleanStateMachine
            SynchronizedStatesManager ssm1 = new SynchronizedStatesManager(m_messengers.get(Site).getZK(),
                    stateMachineManagerRoot, "ssm1", siteString);
            m_stateMachineGroup1[Site] = ssm1;
            BooleanStateMachine bsm1 = g1BooleanBroken ?
                    new BrokenBooleanStateMachine(ssm1, "bool") : new BooleanStateMachine(ssm1, "bool");
            m_booleanStateMachinesForGroup1[Site] = bsm1;

            // Create a SynchronizedStatesManager to manage both a BooleanStateMachine and ByteStateMachine
            SynchronizedStatesManager ssm2 = new SynchronizedStatesManager(m_messengers.get(Site).getZK(),
                    stateMachineManagerRoot, "ssm2", siteString, stateMachines.values().length);
            m_stateMachineGroup2[Site] = ssm2;
            BooleanStateMachine bsm2 = g2BooleanBroken ?
                    new BrokenBooleanStateMachine(ssm2, "bool") : new BooleanStateMachine(ssm2, "bool");
            m_booleanStateMachinesForGroup2[Site] = bsm2;
            ByteStateMachine msm2 = g2ByteBroken ?
                    new BrokenByteStateMachine(ssm2, "byte") : new ByteStateMachine(ssm2, "byte");
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
        log.info("Starting " + m_name.getMethodName());
        setUpZK(NUM_AGREEMENT_SITES);
        coordinators = IntStream.range(0, NUM_AGREEMENT_SITES)
                .mapToObj(i -> ":" + (i+Constants.DEFAULT_INTERNAL_PORT))
                .toArray(s -> new String[s]);
        criteria = MeshProber.builder()
                .coordinators(coordinators)
                .startAction(StartAction.PROBE)
                .hostCount(NUM_AGREEMENT_SITES)
                .build();
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
        HostMessenger.Config config = new HostMessenger.Config(false);
        config.internalPort += site;
        config.acceptor = criteria;
        int clientPort = m_ports.next();
        config.zkInterface = "127.0.0.1";
        config.zkPort = clientPort;
        m_siteIdToZKPort.put(site, clientPort);
        config.networkThreads = 1;
        HostMessenger hm = new HostMessenger(config, null, randomHostDisplayName());
        hm.start();
        MeshProber.prober(hm).waitForDetermination();
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
        boolean correlatedTask = true;
        final String taskString = "DO SOME WORK";
        String taskResultString = defaultTaskResult;
        volatile Map<String, ByteBuffer> correlatedResults;
        volatile List<ByteBuffer> uncorrelatedResults;

        boolean notifiedOfReset = false;
        boolean isDirectVictim;

        final CompletableFuture<?> initializedFuture = new CompletableFuture<>();
        volatile CompletableFuture<?> workFuture;

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
            initializedFuture.complete(null);
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
                    initiateCoordinatedTask(correlatedTask, proposed);
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
            assertEquals("Test state inconsistent with state machine", (proposed != null), ourProposal);
            if (success) {
                state = toBoolean(proposedState);
            }
            if (ourProposal) {
                proposed = null;
                makeProposal = false;
                ourProposalOrTaskFinished = true;
                CompletableFuture<?> f = workFuture;
                workFuture = null;
                f.complete(null);
            }
            acceptProposalOrTask = true;
            proposalsOrTasksCompleted++;
        }

        Future<?> switchState() {
            assertNull(workFuture);
            workFuture = new CompletableFuture<>();
            ourProposalOrTaskFinished = false;
            makeProposal = true;
            if (requestLock()) {
                proposed = toByteBuffer(!state);
                proposeStateChange(proposed);
                assertFalse("State machine local lock held after bool state change request", debugIsLocalStateLocked());
            }

            return workFuture;
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
            assertEquals("Getting unexepected completions ourTask state", proposed != null, ourTask);
            acceptProposalOrTask = true;
            taskResultString = defaultTaskResult;
            correlatedResults = results;
            if (ourTask) {
                proposed = null;
                startTask = false;
                ourTask = false;
                ourProposalOrTaskFinished = true;
                CompletableFuture<?> f = workFuture;
                workFuture = null;
                f.complete(null);
            }
            proposalsOrTasksCompleted++;
        }

        @Override
        protected void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, List<ByteBuffer> results) {
            assertFalse("State machine local lock held after bool uncorrelated task completion", debugIsLocalStateLocked());
            assertTrue(taskRequest.equals(ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8))));
            assertTrue(ourTask == startTask);
            correlatedTask = true;
            acceptProposalOrTask = true;
            taskResultString = defaultTaskResult;
            uncorrelatedResults = results;
            if (ourTask) {
                proposed = null;
                ourProposalOrTaskFinished = true;
                startTask = false;
                ourTask = false;
                CompletableFuture<?> f = workFuture;
                workFuture = null;
                f.complete(null);
            }
            proposalsOrTasksCompleted++;
        }

        Future<?> startTask() {
            assertNull(workFuture);
            workFuture = new CompletableFuture<>();
            ourProposalOrTaskFinished = false;
            correlatedResults = null;
            uncorrelatedResults = null;
            startTask = true;
            if (requestLock()) {
                proposed = ByteBuffer.wrap(taskString.getBytes(Charsets.UTF_8));
                initiateCoordinatedTask(correlatedTask, proposed);
                assertFalse("State machine local lock held after bool task request", debugIsLocalStateLocked());
            }
            return workFuture;
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

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            this.isDirectVictim = isDirectVictim;
            makeProposal = false;
            startTask = false;
            proposalsOrTasksCompleted = 0;
            ourProposalOrTaskFinished = false;
            acceptProposalOrTask = true;
            justHoldTheLock = false;
            ignoreProposal = false;
            correlatedTask = true;

            notifiedOfReset = true;

            return bsm_states[0]; // FALSE as reset state
        }
    }

    class BrokenBooleanStateMachine extends BooleanStateMachine {
        String brokenCallbackName = "DEFAULT";

        public BrokenBooleanStateMachine(SynchronizedStatesManager ssm, String instanceName) {
            super(ssm, instanceName);
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState) {
            if (brokenCallbackName.equals("setInitialState")) {
                throw new NullPointerException();
            }
            else {
                super.setInitialState(currentAgreedState);
            }
        }

        @Override
        protected void taskRequested(ByteBuffer taskRequest) {
            if (brokenCallbackName.equals("taskRequested")) {
                throw new NullPointerException();
            }
            else {
                super.taskRequested(taskRequest);
            }
        }

        @Override
        protected void correlatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, Map<String, ByteBuffer> results) {
            if (brokenCallbackName.equals("correlatedTaskCompleted")) {
                throw new NullPointerException();
            }
            else {
                super.correlatedTaskCompleted(ourTask, taskRequest, results);
            }
        }

        @Override
        protected void uncorrelatedTaskCompleted(boolean ourTask, ByteBuffer taskRequest, List<ByteBuffer> results) {
            if (brokenCallbackName.equals("uncorrelatedTaskCompleted")) {
                throw new NullPointerException();
            }
            else {
                super.uncorrelatedTaskCompleted(ourTask, taskRequest, results);
            }
        }

        @Override
        protected void lockRequestCompleted() {
            if (brokenCallbackName.equals("lockRequestCompleted")) {
                throw new NullPointerException();
            }
            else {
                super.lockRequestCompleted();
            }
        }

        @Override
        protected void membershipChanged(Set<String> addedHosts, Set<String> removedHosts) {
            if (brokenCallbackName.equals("membershipChanged")) {
                throw new NullPointerException();
            }
            else {
                super.membershipChanged(addedHosts, removedHosts);
            }
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState) {
            if (brokenCallbackName.equals("stateChangeProposed")) {
                throw new NullPointerException();
            }
            else {
                super.stateChangeProposed(proposedState);
            }
        }

        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
            if (brokenCallbackName.equals("proposedStateResolved")) {
                throw new NullPointerException();
            }
            else {
                super.proposedStateResolved(ourProposal, proposedState, success);
            }
        }
    }

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
        boolean correlatedTask = true;
        final String taskString = "DO SOME OTHER WORK";
        String taskResultString = defaultTaskResult;
        volatile Map<String, ByteBuffer> correlatedResults;
        volatile List<ByteBuffer> uncorrelatedResults;

        boolean notifiedOfReset = false;
        boolean isDirectVictim;

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
                    initiateCoordinatedTask(correlatedTask, proposed);
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
            assertTrue(!ourTask || correlatedTask);
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
            assertFalse(ourTask || correlatedTask);
            correlatedTask = true;
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
                initiateCoordinatedTask(correlatedTask, proposed);
                assertFalse("State machine local lock held after byte task request", debugIsLocalStateLocked());
            }
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

        @Override
        protected ByteBuffer notifyOfStateMachineReset(boolean isDirectVictim) {
            this.isDirectVictim = isDirectVictim;
            makeProposal = false;
            startTask = false;
            proposalsOrTasksCompleted = 0;
            ourProposalOrTaskFinished = false;
            acceptProposalOrTask = true;
            justHoldTheLock = false;
            ignoreProposal = false;
            correlatedTask = true;

            notifiedOfReset = true;

            return msm_states[0]; // 100 as reset state
        }
    }

    class BrokenByteStateMachine extends ByteStateMachine {
        String brokenCallbackName = "DEFAULT";

        public BrokenByteStateMachine(SynchronizedStatesManager ssm, String instanceName) {
            super(ssm, instanceName);
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState) {
            if (brokenCallbackName.equals("stateChangeProposed")) {
                throw new NullPointerException();
            }
            else {
                super.stateChangeProposed(proposedState);
            }
        }
    }

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
                if (sm.proposalsOrTasksCompleted != expectedCompletions) {
                    return false;
                }
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
                if (sm.proposalsOrTasksCompleted != expectedCompletions) {
                    return false;
                }
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
            fail("Exception occurred during test.");
        }
    }


    @Test
    public void testSuccessfulStateChange() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testSingleRejectedProposal() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testAllRejectedProposal() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testVerifyProposerCantReject() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testLateJoiner() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testAllLateJoiners() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testRecoverFromDeadHostHoldingLock() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testRecoverFromContendingDeadHostRequestingLock() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testRoundRobinStates() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testMultipleStateMachines() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testCallbackExceptionCorrectlyResetOtherStateMachines() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, false, true, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine g2i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup2[0];
            g2i0.brokenCallbackName = "stateChangeProposed";
            BooleanStateMachine g2i1 = m_booleanStateMachinesForGroup2[1];
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
            g2j1.switchState();     // set Group 2 Byte State to rawByteStates[2]
            while (!byteProposalOrTaskFinished(m_byteStateMachinesForGroup2, 2) ||
                    !bytesSynchronized(m_byteStateMachinesForGroup2)) {
                Thread.sleep(500);
            }
            g2i1.switchState();     // set Group 2 Bool State to true, will trigger the reset

            int waitLoop = 0;
            for (; waitLoop < 5; waitLoop++) {
                if (boolProposalOrTaskFinished(m_booleanStateMachinesForGroup2, 1) &&
                        boolsSynchronized(m_booleanStateMachinesForGroup2)) {
                    break;
                }
                Thread.sleep(500);
            }
            // i0 should have never stepped into proposedStateResolved because of the reset, so the actual
            // completion count is 0, hence the timeout, but the state will be correctly switched after the reset
            assertEquals(5, waitLoop);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup2));
            assertTrue(g2i0.state);
            assertTrue(g2i0.notifiedOfReset);
            assertEquals(1, g2i0.getResetCounter());
            assertTrue(g2i0.isDirectVictim);

            // Verify that the group2 byte state machine at site 0 was also reset, and reinitialized with correct state
            assertTrue(bytesSynchronized(m_byteStateMachinesForGroup2));
            assertEquals(rawByteStates[2], g2j0.state);
            assertTrue(g2j0.notifiedOfReset);
            assertEquals(1, g2j0.getResetCounter());
            assertFalse(g2j0.isDirectVictim);
        }
        catch (Exception e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testHandleMultipleCallbackExceptionHandlerFromDifferentSMI() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, false, true, true);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine g2i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup2[0];
            g2i0.brokenCallbackName = "stateChangeProposed";
            BooleanStateMachine g2i1 = m_booleanStateMachinesForGroup2[1];
            BrokenByteStateMachine g2j0 = (BrokenByteStateMachine) m_byteStateMachinesForGroup2[0];
            g2j0.brokenCallbackName = "stateChangeProposed";
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

            g2i1.switchState();     // set Group 2 Bool State to true, will trigger the reset
            g2j1.switchState();     // set Group 2 Byte State to 110, will also trigger the reset

            int waitLoop = 0;
            for (; waitLoop < 5; waitLoop++) {
                if (boolProposalOrTaskFinished(m_booleanStateMachinesForGroup2, 1) &&
                        boolsSynchronized(m_booleanStateMachinesForGroup2)) {
                    break;
                }
                Thread.sleep(500);
            }
            // i0 should have never stepped into proposedStateResolved because of the reset, so the actual
            // completion count is 0, hence the timeout, but the state will be correctly switched after the reset
            assertEquals(5, waitLoop);
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup2));
            assertTrue(g2i0.state);
            assertTrue(g2i0.notifiedOfReset);
            // when the handler for the second exception is executed, reinitialization caused by the first
            // exception may or may not have completed; if it is completed, the handler will be ignored, hence
            // only 1 reset, otherwise there will be 2 resets
            assertTrue(g2i0.getResetCounter() == 1 || g2i0.getResetCounter() == 2);

            assertTrue(bytesSynchronized(m_byteStateMachinesForGroup2));
            assertEquals(rawByteStates[1], g2j0.state);
            assertTrue(g2j0.notifiedOfReset);
            assertEquals(g2i0.getResetCounter(), g2j0.getResetCounter());

            // only one of the two state machines will be direct victim at a time, depending on the resets executed
            assertTrue(g2i0.isDirectVictim ^ g2j0.isDirectVictim);
        }
        catch (Exception e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testFailureDuringProposalStateChange() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testFailureDuringProposedTask() {
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
            fail("Exception occurred during test.");
        }
    }


    @Test
    public void testSuccessfulUncorrelatedWithStateChangeTask() {
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
            i0.correlatedTask = false;
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testSuccessfulCorrelatedTask() {
        try {
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            BooleanStateMachine i0 = m_booleanStateMachinesForGroup1[0];
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            i0.correlatedTask = true;
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testSingleTaskWithOneUniqueResult() {
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
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInSetInitialState() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            i0.brokenCallbackName = "setInitialState";
            assertEquals(0, i0.getResetCounter());

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            int ii = 0;
            for (; ii < 5; ii++) {
                if (boolsInitialized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }

            // i0 should have never been initialized successfully
            assertEquals(5, ii);

            // i0 should have reached the reset limit
            assertTrue(i0.notifiedOfReset);
            assertEquals(6, i0.getResetCounter());
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInTaskRequested() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            i0.brokenCallbackName = "taskRequested";
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));

            assertEquals(0, i0.getResetCounter());

            // let i1 start a task so that i0 will be notified via taskRequested()
            i1.startTask();
            i0.switchState();
            int ii = 0;
            for (; ii < 5; ii++) {
                if (i0.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 2)) {
                    break;
                }
                Thread.sleep(500);
            }

            // i0 should have never incremented the completion count because it will be reset upon i1's task request
            assertEquals(5, ii);

            // i0's switch should fail because it won't have got the lock before being reset
            // i0's state should have been reinitialized to false with consensus
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertFalse(i0.state);
            assertTrue(i0.notifiedOfReset);
            assertEquals(1, i0.getResetCounter());
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInCorrelatedTaskCompleted() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            i0.brokenCallbackName = "correlatedTaskCompleted";
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));

            assertEquals(0, i0.getResetCounter());

            // let i1 start a correlated task so that i0 will be notified via correlatedTaskCompleted()
            i1.startTask();
            int ii = 0;
            for (; ii < 5; ii++) {
                if (boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }

            // i0 should have never incremented the completion count because it will be reset upon i1's task completion notification
            assertEquals(5, ii);

            // i0's switch should fail and state should have been reinitialized to false with consensus
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertFalse(i0.state);
            assertTrue(i0.notifiedOfReset);
            assertEquals(1, i0.getResetCounter());
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInUncorrelatedTaskCompleted() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            i0.brokenCallbackName = "uncorrelatedTaskCompleted";
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));

            assertEquals(0, i0.getResetCounter());

            // let i1 start a correlated task so that i0 will be notified via uncorrelatedTaskCompleted()
            i1.correlatedTask = false;
            i1.startTask();
            int ii = 0;
            for (; ii < 5; ii++) {
                if (boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }

            // i0 should have never incremented the completion count because it will be reset upon i1's task completion notification
            assertEquals(5, ii);

            // i0's switch should fail and state should have been reinitialized to false with consensus
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertFalse(i0.state);
            assertTrue(i0.notifiedOfReset);
            assertEquals(1, i0.getResetCounter());
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInLockRequestCompleted() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            i0.brokenCallbackName = "lockRequestCompleted";
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));

            assertEquals(0, i0.getResetCounter());

            // let i1 grab the lock first, so that i0 will be notified with lock via lockRequestCompleted()
            i1.startTask();
            i0.switchState();
            int ii = 0;
            for (; ii < 5; ii++) {
                if (i0.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 2)) {
                    break;
                }
                Thread.sleep(500);
            }

            // i0 should have never incremented the completion count because it will never get the lock (broken callback)
            assertEquals(5, ii);

            // i0's switch should fail and state should have been reinitialized to false with consensus
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertFalse(i0.state);
            assertTrue(i0.notifiedOfReset);
            assertEquals(1, i0.getResetCounter());
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInMembershipChanged() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            assertEquals(0, i0.getResetCounter());

            i0.brokenCallbackName = "membershipChanged";
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                // introduce some interval between member nodes being created in ZK
                // so there will be several membership change notifications, hence several resets for i0
                Thread.sleep(500);
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }

            // i0 will be reset whenever it is notified of membership changes (broken callback).
            // After all other members have been initialized, i0 can be initialized to false with consensus
            // properly (as there is no further membership change)
            assertFalse(i0.state);
            assertTrue(i0.notifiedOfReset);
            assertTrue(i0.getResetCounter() > 0);
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInStateChangeProposed() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            i0.brokenCallbackName = "stateChangeProposed";
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));

            assertEquals(0, i0.getResetCounter());

            i1.switchState();
            int ii = 0;
            for (; ii < 5; ii++) {
                if (i1.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }

            // i0 should have never stepped into proposedStateResolved because of the reset, so the actual
            // completion count is 0, hence the timeout
            assertEquals(5, ii);

            // i1's switch should succeed because the old i0 is removed from members and the new i0 responded with NULL result,
            // state should have been reset and reinitialized to true with consensus
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertTrue(i0.state);
            assertTrue(i0.notifiedOfReset);
            assertEquals(1, i0.getResetCounter());
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    @Test
    public void testResetIfExceptionInProposedStateResolved() {

        for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
            removeStateMachinesFor(ii);
        }
        addStateMachinesFor(0, true, false, false);
        for (int ii = 1; ii < NUM_AGREEMENT_SITES; ii++) {
            addStateMachinesFor(ii);
        }

        try {
            BrokenBooleanStateMachine i0 = (BrokenBooleanStateMachine) m_booleanStateMachinesForGroup1[0];
            BooleanStateMachine i1 = m_booleanStateMachinesForGroup1[1];
            i0.brokenCallbackName = "proposedStateResolved";
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                registerGroup1BoolFor(ii);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));

            assertEquals(0, i0.getResetCounter());

            // any instance can propose the change, here we use i1
            i1.switchState();
            int ii = 0;
            for (; ii < 5; ii++) {
                if (i1.ourProposalOrTaskFinished &&
                        boolProposalOrTaskFinished(m_booleanStateMachinesForGroup1, 1)) {
                    break;
                }
                Thread.sleep(500);
            }

            // i0 should have never incremented the completion count because proposedStateResolved()
            // is broken, hence the timeout
            assertEquals(5, ii);

            // switch should succeed and state should have been reinitialized to true with consensus
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            assertTrue(i0.state);
            assertTrue(i0.notifiedOfReset);
            assertEquals(1, i0.getResetCounter());
        }
        catch (InterruptedException e) {
            fail("Exception occurred during test.");
        }
    }

    /*
     * Test that if a host dies immediately after making a proposal another host grabbing the distributed lock will wait
     * until that proposal has been resolved
     */
    @Test(timeout = 10_000)
    public void testHostFailureAfterProposalGrabLock() throws Exception {
        m_booleanStateMachinesForGroup1[0].registerStateMachineWithManager(ByteBuffer.allocate(1));
        m_booleanStateMachinesForGroup1[1].registerStateMachineWithManager(ByteBuffer.allocate(1));
        CyclicBarrier barrier = new CyclicBarrier(2);
        ListeningExecutorService ssmEs = getField(SynchronizedStatesManager.class, "s_sharedEs");

        assertTrue(m_booleanStateMachinesForGroup1[0].requestLock());

        // Submit a runnable which will block execution of callbacks in the ssm
        ssmEs.submit(() -> {
            barrier.await();
            return null;
        });

        m_booleanStateMachinesForGroup1[0].makeProposal = true;
        m_booleanStateMachinesForGroup1[0].proposeStateChange(
                m_booleanStateMachinesForGroup1[0].toByteBuffer(!m_booleanStateMachinesForGroup1[0].state));

        m_messengers.get(0).getZK().close();

        // Give the system time to react to close
        Thread.sleep(200);

        m_booleanStateMachinesForGroup1[1].makeProposal = true;
        assertFalse(m_booleanStateMachinesForGroup1[1].requestLock());

        // Should not have made proposal yet
        assertNull(m_booleanStateMachinesForGroup1[1].proposed);

        // Let the state machines process callbacks
        barrier.await();

        // Loop until proposal is made or timeout
        do {
            // Submit a runnable to wake this thread up once the queue is drained
            ssmEs.submit(() -> {
                barrier.await();
                return null;
            });

            // Wait for all queued callbacks to be processed
            barrier.await();
        } while (m_booleanStateMachinesForGroup1[1].makeProposal);
    }

    /*
     * Test that if a host dies immediately after making a proposal another host waiting on the distributed lock will
     * wait until that proposal has been resolved
     */
    @Test(timeout = 10_000)
    public void testHostFailureAfterProposalWaitingOnLock() throws Exception {
        m_booleanStateMachinesForGroup1[0].registerStateMachineWithManager(ByteBuffer.allocate(1));
        m_booleanStateMachinesForGroup1[1].registerStateMachineWithManager(ByteBuffer.allocate(1));
        CyclicBarrier barrier = new CyclicBarrier(2);
        ListeningExecutorService ssmEs = getField(SynchronizedStatesManager.class, "s_sharedEs");

        assertTrue(m_booleanStateMachinesForGroup1[0].requestLock());

        m_booleanStateMachinesForGroup1[1].makeProposal = true;
        assertFalse(m_booleanStateMachinesForGroup1[1].requestLock());

        // Submit a runnable which will block execution of callbacks in the ssm
        ssmEs.submit(() -> {
            barrier.await();
            return null;
        });

        m_booleanStateMachinesForGroup1[0].makeProposal = true;
        m_booleanStateMachinesForGroup1[0].proposeStateChange(
                m_booleanStateMachinesForGroup1[0].toByteBuffer(!m_booleanStateMachinesForGroup1[0].state));

        m_messengers.get(0).getZK().close();

        // Give the system time to react to close
        Thread.sleep(200);

        // Force the lock callback to run first
        ((Callable<?>) getField(m_booleanStateMachinesForGroup1[1], "HandlerForDistributedLockEvent")).call();

        // Should not have made proposal yet
        assertNull(m_booleanStateMachinesForGroup1[1].proposed);

        // Let the state machines process callbacks
        barrier.await();

        // Loop until proposal is made or timeout
        do {
            // Submit a runnable to wake this thread up once the queue is drained
            ssmEs.submit(() -> {
                barrier.await();
                return null;
            });

            // Wait for all queued callbacks to be processed
            barrier.await();
        } while (m_booleanStateMachinesForGroup1[1].makeProposal);
    }

    /*
     * Test starting a statemachine and shutting it down in a loop with other state machines
     */
    @Test(timeout = 60_000)
    public void thrashRestartingStateMachines() throws Exception {
        final int loopCount = 10;
        // Use a cyclic barrier so all state machines shutdown at the same point
        CyclicBarrier barrier = new CyclicBarrier(4);
        CompletableFuture<?> future = new CompletableFuture<>();

        // Thread to wrap each state machine manager and instance
        class SSMThrasher extends Thread {
            final ZooKeeper m_zooKeeper;
            final int m_instanceId;
            SynchronizedStatesManager m_ssm;
            BooleanStateMachine m_bsm;
            RandomTestRule m_random = new RandomTestRule();

            public SSMThrasher(ZooKeeper zooKeeper, int instanceId) {
                super(m_name.getMethodName() + '_' + instanceId);
                setDaemon(true);
                m_zooKeeper = zooKeeper;
                m_instanceId = instanceId;
                m_random.apply(null, org.junit.runner.Description.createTestDescription(
                        TestStateMachine.class.getName(), m_name.getMethodName() + '_' + m_instanceId));
            }

            @Override
            public void run() {
                try {
                    for (int i = 0; i < loopCount; ++i) {
                        if (m_instanceId == 0) {
                            System.out.println("LOOP " + i);
                        }
                        m_ssm = new SynchronizedStatesManager(m_zooKeeper, stateMachineManagerRoot,
                                m_name.getMethodName(), Integer.toString(m_instanceId));
                        m_bsm = new BooleanStateMachine(m_ssm, "test");

                        Thread.sleep(m_random.nextInt(5));
                        m_bsm.registerStateMachineWithManagerAsync(m_bsm.toByteBuffer(true));
                        m_bsm.initializedFuture.get();

                        Thread.sleep(m_random.nextInt(5));
                        m_bsm.startTask().get();

                        Thread.sleep(m_random.nextInt(5));
                        if (i % barrier.getParties() == m_instanceId) {
                            m_bsm.switchState().get();
                        }

                        Thread.sleep(m_random.nextInt(5));
                        barrier.await();

                        // Sometimes start a task or proposal but don't wait for it to finish before shutdown
                        if ((i % barrier.getParties() == m_instanceId && i % 3 == 0)
                                || ((i + 1 % barrier.getParties() == m_instanceId && i % 2 == 0))) {
                            Thread.sleep(m_random.nextInt(5));
                            m_bsm.startTask();
                        } else if (i + 1 % barrier.getParties() == m_instanceId) {
                            Thread.sleep(m_random.nextInt(5));
                            m_bsm.switchState();
                        }

                        Thread.sleep(m_random.nextInt(5));
                        m_ssm.shutdownSynchronizedStatesManager();
                    }
                    future.complete(null);
                } catch (BrokenBarrierException e) {
                    System.out.println("Encountered broke barrier: " + m_instanceId);
                } catch (Throwable t) {
                    barrier.reset();
                    future.completeExceptionally(t);
                }
            }
        }

        SSMThrasher[] thrashers = new SSMThrasher[barrier.getParties()];

        for (int i = 0; i < thrashers.length; ++i) {
            thrashers[i] = new SSMThrasher(m_messengers.get(i).getZK(), i);
            thrashers[i].start();
        }

        future.get();
    }

    // Utility methods for extracting a field from a class or instance
    static <F> F getField(Class<?> clazz, String fieldName)
            throws NoSuchFieldException, SecurityException, IllegalAccessException {
        return getField(clazz, null, fieldName);
    }

    static <F> F getField(Object instance, String fieldName)
            throws NoSuchFieldException, SecurityException, IllegalAccessException {
        Class<?> clazz = instance.getClass();
        NoSuchFieldException exception = null;
        do {
            try {
                return getField(clazz, instance, fieldName);
            } catch (NoSuchFieldException e) {
                if (exception == null) {
                    exception = e;
                }
            }
        } while ((clazz = clazz.getSuperclass()) != null);
        throw exception;
    }

    @SuppressWarnings("unchecked")
    static <F> F getField(Class<?> clazz, Object instance, String fieldName)
            throws NoSuchFieldException, SecurityException, IllegalAccessException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (F) field.get(instance);
    }
}
