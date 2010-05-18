/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState;
import org.voltdb.dtxn.*;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.messaging.*;

public class TestExecutionSite extends TestCase {

    // ExecutionSite's snapshot processor requires the shared library
    static { EELibraryLoader.loadExecutionEngineLibrary(true); }

    // Topology parameters
    private static final int K_FACTOR = 1;
    private static final int PARTITION_COUNT = 3;
    private static final int SITE_COUNT = PARTITION_COUNT * (K_FACTOR + 1);

    MockVoltDB m_voltdb;
    RestrictedPriorityQueue m_rpqs[] = new RestrictedPriorityQueue[SITE_COUNT];
    ExecutionSite m_sites[] = new ExecutionSite[SITE_COUNT];
    MockMailbox m_mboxes[] = new MockMailbox[SITE_COUNT];

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        m_voltdb = new MockVoltDB();
        m_voltdb.setFaultDistributor(new FaultDistributor());

        // one host and one initiator per site
        for (int ss=0; ss < SITE_COUNT; ss++) {
            m_voltdb.addHost(getHostIdForSiteId(ss));
            m_voltdb.addSite(getInitiatorIdForSiteId(ss),
                             getHostIdForSiteId(ss),
                             getPartitionIdForSiteId(ss),
                             false);
        }

        // create k+1 sites per partition
        int siteid = 0;
        for (int pp=0; pp < PARTITION_COUNT; pp++) {
            m_voltdb.addPartition(pp);
            for (int kk=0; kk < (K_FACTOR + 1); kk++) {
                m_voltdb.addSite(siteid, getHostIdForSiteId(siteid), pp, true);
                ++siteid;
            }
        }

        if (siteid != SITE_COUNT) {
            throw new RuntimeException("Invalid setup logic.");
        }

        Procedure proc = null;
        proc = m_voltdb.addProcedureForTest(MockSPVoltProcedure.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(true);

        proc = m_voltdb.addProcedureForTest(MockROSPVoltProcedure.class.getName());
        proc.setReadonly(true);
        proc.setSinglepartition(true);

        proc = m_voltdb.addProcedureForTest(MockMPVoltProcedure.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(false);

        // Done with the logical topology.
        VoltDB.replaceVoltDBInstanceForTest(m_voltdb);

        // Create the real objects
        for (int ss=0; ss < SITE_COUNT; ++ss) {
            m_mboxes[ss] = new MockMailbox(new LinkedBlockingQueue<VoltMessage>());
            m_rpqs[ss] = new RestrictedPriorityARRR(getInitiatorIds(), ss, m_mboxes[ss]);
            m_sites[ss] = new ExecutionSite(m_voltdb, m_mboxes[ss], ss, null, m_rpqs[ss]);
            MockMailbox.registerMailbox(ss, m_mboxes[ss]);
        }
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        for (int i=0; i < m_sites.length; ++i) {
            m_sites[i] = null;
            m_mboxes[i] = null;
        }
        m_voltdb = null;
    }

    /* Partitions are assigned to sites in sequence: a,a,b,b,c,c.. */
    int getPartitionIdForSiteId(int siteId) {
        return (int) Math.floor(siteId / (K_FACTOR + 1));
    }

    /* Initiator ids are site ids + 1,000 */
    int getInitiatorIdForSiteId(int siteId) {
        return  siteId + 1000;
    }

    /* return a new array of initiator ids */
    int[] getInitiatorIds() {
        int[] ids = new int[SITE_COUNT];
        for (int ss=0; ss < SITE_COUNT; ss++) {
            ids[ss] = getInitiatorIdForSiteId(ss);
        }
        return ids;
    }

    /* Host ids are site ids + 10,000 */
    int getHostIdForSiteId(int siteId) {
        return siteId + 10000;
    }


    /* Fake RestrictedPriorityQueue implementation */
    public static class RestrictedPriorityARRR
    extends RestrictedPriorityQueue
    {
        private static final long serialVersionUID = 1L;

        /**
         * Initialize the RPQ with the set of initiators in the system and
         * the corresponding execution site's mailbox. Ugh.
         */
        public
        RestrictedPriorityARRR(int[] initiatorSiteIds, int siteId, Mailbox mbox)
        {
            super(initiatorSiteIds, siteId, mbox);
        }
    }


    /* Single partition write */
    public static class MockSPVoltProcedure extends VoltProcedure
    {
        public static int m_called = 0;

        boolean testReadOnly() {
            return false;
        }

        @Override
        ClientResponseImpl call(TransactionState txnState, Object... paramList)
        {
            m_site.simulateExecutePlanFragments(txnState.txnId, testReadOnly());

            final ClientResponseImpl response = new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                    new VoltTable[] {}, "MockSPVoltProcedure Response");
            ++m_called;
            return response;
        }
    }

     /* Single partition read */
    public static class MockROSPVoltProcedure extends MockSPVoltProcedure
    {
        @Override
        boolean testReadOnly() {
            return true;
        }
    }

    /* Multi-partition - mock VoltProcedure.slowPath() */
    public static class MockMPVoltProcedure extends VoltProcedure
    {
        // Enable these simulated faults before running the procedure by setting
        // one of these booleans to true. Allows testcases to simulate various
        // coordinator node failures. Faults are turned back off once simulated
        // by the procedure (since they're static...)
        public static boolean simulate_coordinator_dies_during_commit = false;

        public static int m_called = 0;

        boolean finalTask() { return false; }
        boolean nonTransactional() { return false; }

        /** Helper to turn object list into parameter set buffer */
        private ByteBuffer createParametersBuffer(Object... paramList) {
            ParameterSet paramSet = new ParameterSet(true);
            paramSet.setParameters(paramList);
            FastSerializer fs = new FastSerializer();
            try {
                fs.writeObject(paramSet);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            ByteBuffer paramBuf = fs.getBuffer();
            return paramBuf;
        }

        @SuppressWarnings("deprecation")
        @Override
        ClientResponseImpl call(TransactionState txnState, Object... paramList)
        {
            ByteBuffer paramBuf = createParametersBuffer(paramList);

            // Build the aggregator and the distributed tasks.
            int localTask_startDep = txnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
            int localTask_outputDep = txnState.getNextDependencyId();

            FragmentTaskMessage localTask =
                new FragmentTaskMessage(txnState.initiatorSiteId,
                                 txnState.coordinatorSiteId,
                                 txnState.txnId,
                                 txnState.isReadOnly,
                                 new long[] {0},
                                 new int[] {localTask_outputDep},
                                 new ByteBuffer[] {paramBuf},
                                 finalTask());

            localTask.addInputDepId(0, localTask_startDep);

            FragmentTaskMessage distributedTask =
                new FragmentTaskMessage(txnState.initiatorSiteId,
                                 txnState.coordinatorSiteId,
                                 txnState.txnId,
                                 txnState.isReadOnly,
                                 new long[] {0},
                                 new int[] {localTask_startDep},
                                 new ByteBuffer[] {paramBuf},
                                 finalTask());

            txnState.createLocalFragmentWork(localTask, nonTransactional() && finalTask());
            txnState.createAllParticipatingFragmentWork(distributedTask);
            txnState.setupProcedureResume(finalTask(), new int[] {localTask_outputDep});

            final Map<Integer, List<VoltTable>> resultDeps =
                m_site.recursableRun(txnState);
            assertTrue(resultDeps != null);

            ++m_called;

            // simulate node failure: no commit sent to participant
            if (simulate_coordinator_dies_during_commit) {
                // turn off the fault for the next time through
                simulate_coordinator_dies_during_commit = false;
                Thread.currentThread().stop();
            }

            // Return a made up table (no EE interaction anyway.. )
            VoltTable[] vta = new VoltTable[1];
            vta[0] = new VoltTable(new VoltTable.ColumnInfo("", VoltType.INTEGER));
            vta[0].addRow(new Integer(1));

            return new ClientResponseImpl(ClientResponse.SUCCESS, vta, null);
        }
    }

    /*
     * SinglePartition basecase. Show that recursableRun completes a
     * single partition transaction.
     */
    public void testSinglePartitionTxn()
    {
        final boolean readOnly = false;
        final boolean singlePartition = true;

        // That the full procedure name is necessary is a bug in the
        // mock objects - or perhaps an issue with a nested class?
        // Or maybe a difference in what ClientInterface does?
        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockSPVoltProcedure");
        tx1_spi.setParams(new Integer(0));

        final InitiateTaskMessage tx1_mn =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0),
                                    0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);

        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes[0], m_sites[0], tx1_mn);

        int callcheck = MockSPVoltProcedure.m_called;

        assertFalse(tx1.isDone());
        assertEquals(0, m_sites[0].lastCommittedTxnId);
        assertEquals(0, m_sites[0].lastCommittedMultiPartTxnId);
        m_sites[0].m_transactionsById.put(tx1.txnId, tx1);
        m_sites[0].recursableRun(tx1);

        assertTrue(tx1.isDone());
        assertEquals(null, m_sites[0].m_transactionsById.get(tx1.txnId));
        assertEquals((++callcheck), MockSPVoltProcedure.m_called);
        assertEquals(1000, m_sites[0].lastCommittedTxnId);
        assertEquals(0, m_sites[0].lastCommittedMultiPartTxnId);
    }

    /*
     * Single partition read-only
     */
    public void testROSinglePartitionTxn()
    {
        final boolean readOnly = true;
        final boolean singlePartition = true;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockROSPVoltProcedure");
        tx1_spi.setParams(new Integer(0));

        final InitiateTaskMessage tx1_mn =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);

        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes[0], m_sites[0], tx1_mn);

        int callcheck = MockSPVoltProcedure.m_called;

        assertFalse(tx1.isDone());
        m_sites[0].m_transactionsById.put(tx1.txnId, tx1);
        m_sites[0].recursableRun(tx1);
        assertTrue(tx1.isDone());
        assertEquals(null, m_sites[0].m_transactionsById.get(tx1.txnId));
        assertEquals((++callcheck), MockSPVoltProcedure.m_called);
    }

    /*
     * Multipartition basecase. Show that recursableRun completes a
     * multi partition transaction.
     */
    public void testMultiPartitionTxn() throws InterruptedException {
        final boolean readOnly = false, singlePartition = false;
        Thread es1, es2;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
        tx1_spi.setParams(new Integer(0));

        // site 1 is the coordinator
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);
        tx1_mn_1.setNonCoordinatorSites(new int[] {1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], tx1_mn_1);

        // site 2 is a participant
        final MultiPartitionParticipantMessage tx1_mn_2 =
            new MultiPartitionParticipantMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly);

        final MultiPartitionParticipantTxnState tx1_2 =
            new MultiPartitionParticipantTxnState(m_mboxes[1], m_sites[1], tx1_mn_2);

        // pre-conditions
        int callcheck = MockMPVoltProcedure.m_called;
        assertFalse(tx1_1.isDone());
        assertFalse(tx1_2.isDone());

        assertEquals(0, m_sites[0].lastCommittedTxnId);
        assertEquals(0, m_sites[0].lastCommittedMultiPartTxnId);
        assertEquals(0, m_sites[1].lastCommittedTxnId);
        assertEquals(0, m_sites[1].lastCommittedMultiPartTxnId);

        m_sites[0].m_transactionsById.put(tx1_1.txnId, tx1_1);
        m_sites[1].m_transactionsById.put(tx1_2.txnId, tx1_2);

        // execute transaction
        es1 = new Thread(new Runnable() {
            public void run() {m_sites[0].recursableRun(tx1_1);}});
        es1.start();

        es2 = new Thread(new Runnable() {
            public void run() {m_sites[1].recursableRun(tx1_2);}});
        es2.start();

        es1.join();
        es2.join();

        // post-conditions
        assertTrue(tx1_1.isDone());
        assertTrue(tx1_2.isDone());

        assertEquals(null, m_sites[0].m_transactionsById.get(tx1_1.txnId));
        assertEquals(null, m_sites[1].m_transactionsById.get(tx1_2.txnId));
        assertEquals(1000, m_sites[0].lastCommittedTxnId);
        assertEquals(1000, m_sites[0].lastCommittedMultiPartTxnId);
        assertEquals(1000, m_sites[1].lastCommittedTxnId);
        assertEquals(1000, m_sites[1].lastCommittedMultiPartTxnId);

        assertEquals((++callcheck), MockMPVoltProcedure.m_called);
    }


    public
    void testMultipartitionParticipantCommitsOnFailure()
    throws InterruptedException
    {
        // cause the coordinator to die before committing.
        TestExecutionSite.MockMPVoltProcedure.
        simulate_coordinator_dies_during_commit = true;

        // The initiator's global commit point will be -1 because
        // the restricted priority queue is never fed by this testcase.
        // TxnIds in this testcase are chosen to make -1 a valid
        // global commit point. (Where -1 is DUMMY_LAST_SEEN...)

        // Want to commit this participant. Global commit pt must
        // be GT than the running txnid.
        m_sites[0].lastCommittedMultiPartTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID + 1;
        m_sites[1].lastCommittedMultiPartTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID + 1;

        boolean test_rollback = false;
        multipartitionNodeFailure(test_rollback, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);
    }

    public
    void testMultiPartitionParticipantRollsbackOnFailure()
    throws InterruptedException
    {
        // cause the coordinator to die before committing.
        TestExecutionSite.MockMPVoltProcedure.
        simulate_coordinator_dies_during_commit = true;

        // The initiator's global commit point will be -1 because
        // the restricted priority queue is never fed by this testcase.
        // TxnIds in this testcase are chosen to make -1 a valid
        // global commit point. (Where -1 is DUMMY_LAST_SEEN...)

        // Want to NOT commit this participant. Global commit pt must
        // be LT than the running txnid.
        m_sites[0].lastCommittedMultiPartTxnId =  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID - 1;
        m_sites[1].lastCommittedMultiPartTxnId =  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID - 1;

        boolean test_rollback = true;
        multipartitionNodeFailure(test_rollback, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);
    }

    /*
     * Simulate a multipartition participant blocked because the coordinating
     * node failed; at least one other node in the cluster has completed
     * this transaction -- and therefore it must commit at this participant.
     */
    private
    void multipartitionNodeFailure(boolean should_rollback, long txnid)
    throws InterruptedException
    {
        final boolean readOnly = false, singlePartition = false;
        Thread es1, es2;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
        tx1_spi.setParams(new Integer(0));

        // site 1 is the coordinator
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, txnid, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);
        tx1_mn_1.setNonCoordinatorSites(new int[] {1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], tx1_mn_1);

        // site 2 is a participant
        final MultiPartitionParticipantMessage tx1_mn_2 =
            new MultiPartitionParticipantMessage(getInitiatorIdForSiteId(0), 0, txnid, readOnly);

        final MultiPartitionParticipantTxnState tx1_2 =
            new MultiPartitionParticipantTxnState(m_mboxes[1], m_sites[1], tx1_mn_2);

        // pre-conditions
        int callcheck = MockMPVoltProcedure.m_called;
        assertFalse(tx1_1.isDone());
        assertFalse(tx1_2.isDone());
        m_sites[0].m_transactionsById.put(tx1_1.txnId, tx1_1);
        m_sites[1].m_transactionsById.put(tx1_2.txnId, tx1_2);

        // execute transaction
        es1 = new Thread(new Runnable() {
            public void run() {m_sites[0].recursableRun(tx1_1);}});
        es1.start();

        es2 = new Thread(new Runnable() {
            public void run() {m_sites[1].recursableRun(tx1_2);}});
        es2.start();

        es1.join();

        // coordinator is now dead. Update the survivor's catalog and
        // push a fault notice to the participant. Must supply the host id
        // corresponding to the coordinator site id.
        m_voltdb.killSite(0);

        // the fault data message from the "other surviving" sites
        // (all but the site actually running and the site that failed).
        for (int ss=2; ss < SITE_COUNT; ++ss) {
            m_mboxes[1].deliver(new FailureSiteUpdateMessage(ss, getHostIdForSiteId(0),
                                                             getInitiatorIdForSiteId(0),
                                                             txnid, (txnid -1 )));
        }
        // the fault distributer message to the execution site.
        m_mboxes[1].deliver(new ExecutionSite.ExecutionSiteNodeFailureMessage(getHostIdForSiteId(0)));
        es2.join();

        // post-conditions
        assertFalse(tx1_1.isDone());      // did not run to completion because of simulated fault
        assertTrue(tx1_2.isDone());       // did run to completion because of globalCommitPt.
        assertEquals(should_rollback, tx1_2.didRollback()); // did not rollback because of globalCommitPt.
        assertEquals(null, m_sites[1].m_transactionsById.get(tx1_2.txnId));
        assertEquals((++callcheck), MockMPVoltProcedure.m_called);
    }

    /*
     * Create a multipartition work unit to test the removal of non-coordinator
     * site ids on failure. A little out of place in this file but the configured
     * ExecutionSite and Mailbox are necessary to construct a MP txn state.
     */
    public void testMultiPartitionParticipantTxnState_handleSiteFaults() {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams(57, "gooniestoo");
        InitiateTaskMessage mn = new InitiateTaskMessage(-1, -1, -1, false, false, spi, Long.MIN_VALUE);
        mn.setNonCoordinatorSites(new int[] {1,2,3,4,5});

        MultiPartitionParticipantTxnState ts =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], mn);

        // fail middle and last site
        ArrayList<Integer> failedSites = new ArrayList<Integer>();
        failedSites.add(1);
        failedSites.add(2);
        failedSites.add(3);
        failedSites.add(5);
        ts.handleSiteFaults(failedSites);

        // steal dump accessors to peek at some internals
        ExecutorTxnState dumpContents = ts.getDumpContents();
        assertEquals(1, dumpContents.nonCoordinatingSites.length);
        assertEquals(4, dumpContents.nonCoordinatingSites[0]);

        // fail first site
        mn.setNonCoordinatorSites(new int[] {1,2,3,4,5});
        ts = new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], mn);
        failedSites.clear();
        failedSites.add(1);
        ts.handleSiteFaults(failedSites);

        dumpContents = ts.getDumpContents();
        assertEquals(4, dumpContents.nonCoordinatingSites.length);
        assertEquals(2, dumpContents.nonCoordinatingSites[0]);
        assertEquals(3, dumpContents.nonCoordinatingSites[1]);
        assertEquals(4, dumpContents.nonCoordinatingSites[2]);
        assertEquals(5, dumpContents.nonCoordinatingSites[3]);

        // fail site that isn't a non-coordinator site
        mn.setNonCoordinatorSites(new int[] {1,2,3,4,5});
        ts = new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], mn);
        failedSites.clear();
        failedSites.add(6);
        failedSites.add(7);
        ts.handleSiteFaults(failedSites);

        dumpContents = ts.getDumpContents();
        assertEquals(5, dumpContents.nonCoordinatingSites.length);
        for (int i=0; i < 5; i++) {
            assertEquals(i+1, dumpContents.nonCoordinatingSites[i]);
        }

    }

    /*
     * Show that a multi-partition transaction proceeds if one of the participants
     * fails
     */
    public void testFailedMultiPartitionParticipant() throws InterruptedException {
        final boolean readOnly = false, singlePartition = false;
        Thread es1;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
        tx1_spi.setParams(new Integer(0));

        // site 1 is the coordinator. Use the txn id (DUMMY...) that the R.P.Q.
        // thinks is a valid safe-to-run txnid.
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0),
                                    0,
                                    DtxnConstants.DUMMY_LAST_SEEN_TXN_ID,
                                    readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);
        tx1_mn_1.setNonCoordinatorSites(new int[] {1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], tx1_mn_1);

        // Site 2 won't exist; we'll claim it fails.

        // pre-conditions
        int callcheck = MockMPVoltProcedure.m_called;
        assertFalse(tx1_1.isDone());
        m_sites[0].m_transactionsById.put(tx1_1.txnId, tx1_1);

        // execute transaction
        es1 = new Thread(new Runnable() {
            public void run() {m_sites[0].recursableRun(tx1_1);}});
        es1.start();

        m_voltdb.killSite(1);

        // the fault data message from the "other surviving" sites
        // (all but the site actually running and the site that failed).
        for (int ss=0; ss < SITE_COUNT; ++ss) {
            if ((ss != 1) || (ss != 0)) {
                m_mboxes[0].deliver(new FailureSiteUpdateMessage
                                    (ss, getHostIdForSiteId(1),
                                     getInitiatorIdForSiteId(1),
                                     tx1_1.txnId, tx1_1.txnId));
            }
        }

        // the fault message from the fault distributer to the execution site
        m_mboxes[0].deliver(new ExecutionSite.ExecutionSiteNodeFailureMessage(getHostIdForSiteId(1)));

        es1.join();

        // post-conditions
        assertTrue(tx1_1.isDone());
        assertFalse(tx1_1.didRollback());
        assertEquals(null, m_sites[0].m_transactionsById.get(tx1_1.txnId));
        assertEquals((++callcheck), MockMPVoltProcedure.m_called);
    }
}
