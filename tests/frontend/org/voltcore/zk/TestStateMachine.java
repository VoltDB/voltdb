package org.voltcore.zk;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.VoltZK;

public class TestStateMachine extends ZKTestBase {
    private final int NUM_AGREEMENT_SITES = 4;
    enum stateMachines {
        SMI1,
        SMI2
    };

    VoltLogger log = new VoltLogger("HOST");
    SynchronizedStatesManager[] m_stateMachineGroup1 = new SynchronizedStatesManager[NUM_AGREEMENT_SITES];
    SynchronizedStatesManager[] m_stateMachineGroup2 = new SynchronizedStatesManager[NUM_AGREEMENT_SITES];
    BooleanStateMachine[] m_booleanStateMachinesForGroup1 = new BooleanStateMachine[NUM_AGREEMENT_SITES];
    BooleanStateMachine[] m_booleanStateMachinesForGroup2 = new BooleanStateMachine[NUM_AGREEMENT_SITES];
    ByteStateMachine[] m_byteStateMachinesForGroup2 = new ByteStateMachine[NUM_AGREEMENT_SITES];

    Boolean[] rawBooleanStates = new Boolean[] {false, true};
    ByteBuffer[] bsm_states = new ByteBuffer[] {ByteBuffer.wrap(rawBooleanStates[0].toString().getBytes()),
                                                ByteBuffer.wrap(rawBooleanStates[1].toString().getBytes())};
    byte[] rawByteStates = new byte[] {100, 110, 120};
    ByteBuffer[] msm_states = new ByteBuffer[] {ByteBuffer.wrap(new byte[]{rawByteStates[0]}),
                                                ByteBuffer.wrap(new byte[]{rawByteStates[1]}),
                                                ByteBuffer.wrap(new byte[]{rawByteStates[2]})};

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
            SynchronizedStatesManager ssm1 = new SynchronizedStatesManager(m_messengers.get(Site).getZK(),
                    "stateManager1", siteString, 1);
            m_stateMachineGroup1[Site] = ssm1;
            BooleanStateMachine bsm1 = new BooleanStateMachine(ssm1, "machine1");
            m_booleanStateMachinesForGroup1[Site] = bsm1;

            SynchronizedStatesManager ssm2 = new SynchronizedStatesManager(m_messengers.get(Site).getZK(),
                    "stateManager2", siteString, stateMachines.values().length);
            m_stateMachineGroup2[Site] = ssm2;
            BooleanStateMachine bsm2 = new BooleanStateMachine(ssm2, "machine1");
            m_booleanStateMachinesForGroup2[Site] = bsm2;
            ByteStateMachine msm2 = new ByteStateMachine(ssm2, "machine2");
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

    public void registerGroup1BoolFor(int Site) {
        try {
            m_stateMachineGroup1[Site].registerStateMachine(m_booleanStateMachinesForGroup1[Site], bsm_states[0]);
        }
        catch (KeeperException | InterruptedException e) {
            fail("Exception occured during test.");
        }
    }

    @Before
    public void setUp() throws Exception {
        setUpZK(NUM_AGREEMENT_SITES);
        ZooKeeper zk = m_messengers.get(0).getZK();
        SynchronizedStatesManager.addIfMissing(zk, "/db", CreateMode.PERSISTENT, null);
        SynchronizedStatesManager.addIfMissing(zk, VoltZK.syncStateMachine, CreateMode.PERSISTENT, null);
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
        HostMessenger hm = new HostMessenger(config);
        hm.start();
        m_messengers.set(site, hm);
        addStateMachinesFor(site);
    }


    class BooleanStateMachine extends SynchronizedStatesManager.StateMachineInstance {
        volatile boolean initialized = false;
        boolean makeProposal = false;
        volatile boolean ourProposalFinished = false;
        boolean acceptProposal = true;
        boolean waitOnLockAck = false;
        ByteBuffer proposed;
        public volatile boolean state;

        public boolean toBoolean(ByteBuffer buff) {
            byte[] b = new byte[buff.remaining()];
            buff.get(b, 0, b.length);
            String str = new String(b);
            return Boolean.valueOf(str);
        }

        public ByteBuffer toByteBuffer(boolean b) {
            String str = Boolean.toString(b);
            return ByteBuffer.wrap(str.getBytes());
        }

        public BooleanStateMachine(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
            assertFalse(isLocalStateLocked());
        }

        @Override
        protected void membershipChanged(Set<String> addedHosts, Set<String> removedHosts) {
            assertFalse(isLocalStateLocked());
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState) {
            state = toBoolean(currentAgreedState);
            initialized = true;
            assertFalse(isLocalStateLocked());
        }

        @Override
        protected void lockRequestCompleted() {
            assertFalse(isLocalStateLocked());
            if (waitOnLockAck) {
                waitOnLockAck = false;
            }
            else {
                proposed = toByteBuffer(!state);
                proposeStateChange(proposed);
            }
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState) {
            assertFalse(isLocalStateLocked());
            requestedStateChangeAcceptable(acceptProposal);
        }

        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
            assertFalse(isLocalStateLocked());
            if (success) {
                state = toBoolean(proposedState);
            }
            if (ourProposal) {
                ourProposalFinished = true;
                makeProposal = false;
            }
        }

        void switchState() {
            makeProposal = true;
            ourProposalFinished = false;
            if (requestLock()) {
                proposed = toByteBuffer(!state);
                proposeStateChange(proposed);
            }
            else {
                assertTrue(true);
            }
        }
    };

    class ByteStateMachine extends SynchronizedStatesManager.StateMachineInstance {
        public volatile boolean initialized = false;
        boolean makeProposal = false;
        boolean ourProposalFinished = false;
        boolean acceptProposal = true;
        boolean waitOnLockAck = false;
        ByteBuffer proposed;
        public volatile byte state;

        public byte toByte(ByteBuffer buff) {
            return buff.get();
        }

        public ByteBuffer toByteBuffer(byte b) {
            byte[] arr = new byte[] {b};
            return ByteBuffer.wrap(arr);
        }

        public ByteStateMachine(SynchronizedStatesManager ssm, String instanceName) {
            ssm.super(instanceName, log);
            assertFalse(isLocalStateLocked());
        }

        @Override
        protected void membershipChanged(Set<String> addedHosts, Set<String> removedHosts) {
            assertFalse(isLocalStateLocked());
        }

        @Override
        protected void setInitialState(ByteBuffer currentAgreedState) {
            state = toByte(currentAgreedState);
            initialized = true;
            assertFalse(isLocalStateLocked());
        }

        @Override
        protected void lockRequestCompleted() {
            assertFalse(isLocalStateLocked());
            if (waitOnLockAck) {
                waitOnLockAck = false;
            }
            else {
                proposed = toByteBuffer(getNextByteState(state));
                proposeStateChange(proposed);
            }
        }

        @Override
        protected void stateChangeProposed(ByteBuffer proposedState) {
            assertFalse(isLocalStateLocked());
            requestedStateChangeAcceptable(acceptProposal);
        }

        @Override
        protected void proposedStateResolved(boolean ourProposal, ByteBuffer proposedState, boolean success) {
            if (success) {
                state = toByte(proposedState);
            }
            if (ourProposal) {
                makeProposal = false;
                ourProposalFinished = true;
            }
        }

        void switchState() {
            makeProposal = true;
            ourProposalFinished = false;
            if (requestLock()) {
                proposed = toByteBuffer(getNextByteState(state));
                proposeStateChange(proposed);
            }
        }
    };

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
    public void testSimpleSuccess() {
        log.info("Starting testSimpleSuccess");
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
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
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
            i1.acceptProposal = false;
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
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
            i1.acceptProposal = false;
            i2.acceptProposal = false;
            i3.acceptProposal = false;
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
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
            i0.acceptProposal = false;
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
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
                if (i0.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(i0.state == newVal);

            // Initialize last state machine
            m_booleanStateMachinesForGroup1[1] = i1;
            registerGroup1BoolFor(1);
            for (int ii = 0; ii < 10; ii++) {
                if (boolsInitialized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
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
                if (i1.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
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
            for (int ii = 0; ii < 10; ii++) {
                if (boolsInitialized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
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
                m_stateMachineGroup1[ii].registerStateMachine(m_booleanStateMachinesForGroup1[ii], bsm_states[0]);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup1)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup1));
            boolean newVal = !i0.state;
            i0.switchState();
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertTrue(i0.state == newVal);

            i0.requestLock();
            i1.waitOnLockAck = true;
            // t1 should not get the lock because t0 is holding it
            assertFalse(i1.requestLock());
            i0 = null;
            failSite(0);
            Thread.sleep(1000);
            // After t0 fails, t1 should be able to get the lock
            assertFalse(i1.waitOnLockAck);
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
                m_stateMachineGroup2[ii].registerStateMachine(m_booleanStateMachinesForGroup2[ii], bsm_states[0]);
                m_stateMachineGroup2[ii].registerStateMachine(m_byteStateMachinesForGroup2[ii], msm_states[0]);
            }

            while (!bytesInitialized(m_byteStateMachinesForGroup2)) {
                Thread.sleep(500);
            }
            assertTrue(bytesSynchronized(m_byteStateMachinesForGroup2));
            i0.switchState();
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalFinished && bytesSynchronized(m_byteStateMachinesForGroup2)) {
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
            for (int ii = 0; ii < 10; ii++) {
                if (i0.ourProposalFinished && boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                    break;
                }
                Thread.sleep(500);
            }
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

            // For any site all state machine instances must be registered before it participates with other sites
            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                m_stateMachineGroup2[ii].registerStateMachine(m_booleanStateMachinesForGroup2[ii], bsm_states[0]);
                m_stateMachineGroup2[ii].registerStateMachine(m_byteStateMachinesForGroup2[ii], msm_states[0]);
            }

            while (!boolsInitialized(m_booleanStateMachinesForGroup2)) {
                Thread.sleep(500);
            }
            assertTrue(boolsSynchronized(m_booleanStateMachinesForGroup2));

            while (!bytesInitialized(m_byteStateMachinesForGroup2)) {
                Thread.sleep(500);
            }
            assertTrue(bytesSynchronized(m_byteStateMachinesForGroup2));

            // StateMachine Group 2 is stable. Change some states and start up the second group
            boolean oldBoolVal = g2i0.state;
            byte oldByteVal = g2j0.state;
            g2i0.switchState();
            g2j0.switchState();

            for (int ii = 0; ii < NUM_AGREEMENT_SITES; ii++) {
                m_stateMachineGroup1[ii].registerStateMachine(m_booleanStateMachinesForGroup1[ii], bsm_states[0]);
            }
            while(!g1i0.initialized) {
                Thread.yield();
            }
            g1i0.switchState();
            while(!g1i0.ourProposalFinished) {
                Thread.yield();
            }
            // Verify that the state has been switched even though the request was made during initialization
            boolean unboxedExpectedState = rawBooleanStates[1];
            assertTrue(g1i0.state == unboxedExpectedState);

            int waitLoop = 0;
            for (; waitLoop < 10; waitLoop++) {
                 if (boolsInitialized(m_booleanStateMachinesForGroup1) &&
                         boolsSynchronized(m_booleanStateMachinesForGroup1)) {
                     break;
                 }
                 Thread.sleep(500);
            }
            assertTrue(waitLoop<10);

            for (int ii = 0; ii < 10; ii++) {
                if (g2i0.ourProposalFinished && g2j0.ourProposalFinished &&
                        boolsSynchronized(m_booleanStateMachinesForGroup2) &&
                        bytesSynchronized(m_byteStateMachinesForGroup2)) {
                    break;
                }
                Thread.sleep(500);
            }
            // Verify that the group2 state machine instances updated while group 1 was initializing
            assertFalse(g2i0.state == oldBoolVal);
            assertFalse(g2j0.state == oldByteVal);
        }
        catch (Exception e) {
            fail("Exception occured during test.");
        }
    }
}
