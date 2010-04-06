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
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.MultiPartitionParticipantTxnState;
import org.voltdb.dtxn.SinglePartitionTxnState;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.messages.FragmentTask;
import org.voltdb.messages.InitiateTask;
import org.voltdb.messages.MultiPartitionParticipantNotice;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.MockMailbox;
import org.voltdb.messaging.VoltMessage;

public class TestExecutionSite extends TestCase {

    // ExecutionSite's snapshot processor requires the shared library
    static { EELibraryLoader.loadExecutionEngineLibrary(true); }

    MockVoltDB m_voltdb;
    ExecutionSite m_sites[] = new ExecutionSite[2];
    MockMailbox m_mboxes[] = new MockMailbox[2];

    // catalog identifiers (siteX also used as array offsets)
    int site1 = 0;
    int site2 = 1;
    int initiator1 = 100;
    int initiator2 = 200;

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

            final ClientResponseImpl response = new ClientResponseImpl();
            response.setResults(ClientResponseImpl.SUCCESS,
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

        @Override
        ClientResponseImpl call(TransactionState txnState, Object... paramList)
        {
            ByteBuffer paramBuf = createParametersBuffer(paramList);

            // Build the aggregator and the distributed tasks.
            int localTask_startDep = txnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
            int localTask_outputDep = txnState.getNextDependencyId();

            FragmentTask localTask =
                new FragmentTask(txnState.initiatorSiteId,
                                 txnState.coordinatorSiteId,
                                 txnState.txnId,
                                 txnState.isReadOnly,
                                 new long[] {0},
                                 new int[] {localTask_outputDep},
                                 new ByteBuffer[] {paramBuf},
                                 finalTask());

            localTask.addInputDepId(0, localTask_startDep);

            FragmentTask distributedTask =
                new FragmentTask(txnState.initiatorSiteId,
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

            // Return a made up table (no EE interaction anyway.. )
            VoltTable[] vta = new VoltTable[1];
            vta[0] = new VoltTable(new VoltTable.ColumnInfo("", VoltType.INTEGER));
            vta[0].addRow(new Integer(1));

            ++m_called;
            return new ClientResponseImpl(ClientResponse.SUCCESS, vta, null);
        }
    }



    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        Procedure proc = null;

        m_voltdb = new MockVoltDB();
        m_voltdb.setFaultDistributor(new FaultDistributor());
        m_voltdb.addHost(0);
        m_voltdb.addPartition(0);
        m_voltdb.addSite(site1, 0, 0, true);
        m_voltdb.addPartition(1);
        m_voltdb.addSite(site2, 0, 1, true);

        proc = m_voltdb.addProcedureForTest(MockSPVoltProcedure.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(true);

        proc = m_voltdb.addProcedureForTest(MockROSPVoltProcedure.class.getName());
        proc.setReadonly(true);
        proc.setSinglepartition(true);

        proc = m_voltdb.addProcedureForTest(MockMPVoltProcedure.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(false);

        VoltDB.replaceVoltDBInstanceForTest(m_voltdb);

        m_mboxes[site1] = new MockMailbox(new LinkedBlockingQueue<VoltMessage>());
        m_mboxes[site2] = new MockMailbox(new LinkedBlockingQueue<VoltMessage>());
        m_sites[site1] = new ExecutionSite(m_voltdb, m_mboxes[site1], site1);
        m_sites[site2] = new ExecutionSite(m_voltdb, m_mboxes[site2], site2);

        MockMailbox.registerMailbox(site1, m_mboxes[site1]);
        MockMailbox.registerMailbox(site2, m_mboxes[site2]);
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

        final InitiateTask tx1_mn =
            new InitiateTask(initiator1, site1, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);

        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes[site1], m_sites[site1], tx1_mn);

        int callcheck = MockSPVoltProcedure.m_called;

        assertFalse(tx1.isDone());
        m_sites[site1].m_transactionsById.put(tx1.txnId, tx1);
        m_sites[site1].recursableRun(tx1);
        assertTrue(tx1.isDone());
        assertEquals(null, m_sites[site1].m_transactionsById.get(tx1.txnId));
        assertEquals((++callcheck), MockSPVoltProcedure.m_called);
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

        final InitiateTask tx1_mn =
            new InitiateTask(initiator1, site1, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);

        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes[site1], m_sites[site1], tx1_mn);

        int callcheck = MockSPVoltProcedure.m_called;

        assertFalse(tx1.isDone());
        m_sites[site1].m_transactionsById.put(tx1.txnId, tx1);
        m_sites[site1].recursableRun(tx1);
        assertTrue(tx1.isDone());
        assertEquals(null, m_sites[site1].m_transactionsById.get(tx1.txnId));
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
        final InitiateTask tx1_mn_1 =
            new InitiateTask(initiator1, site1, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);
        tx1_mn_1.setNonCoordinatorSites(new int[] {site2});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes[site1], m_sites[site1], tx1_mn_1);

        // site 2 is a participant
        final MultiPartitionParticipantNotice tx1_mn_2 =
            new MultiPartitionParticipantNotice(initiator1, site1, 1000, readOnly);

        final MultiPartitionParticipantTxnState tx1_2 =
            new MultiPartitionParticipantTxnState(m_mboxes[site2], m_sites[site2], tx1_mn_2);

        // pre-conditions
        int callcheck = MockMPVoltProcedure.m_called;
        assertFalse(tx1_1.isDone());
        assertFalse(tx1_2.isDone());
        m_sites[site1].m_transactionsById.put(tx1_1.txnId, tx1_1);
        m_sites[site2].m_transactionsById.put(tx1_2.txnId, tx1_2);

        // execute transaction
        es1 = new Thread(new Runnable() {
            public void run() {m_sites[site1].recursableRun(tx1_1);}});
        es1.start();

        es2 = new Thread(new Runnable() {
            public void run() {m_sites[site2].recursableRun(tx1_2);}});
        es2.start();

        es1.join();
        es2.join();

        // post-conditions
        assertTrue(tx1_1.isDone());
        assertTrue(tx1_2.isDone());
        assertEquals(null, m_sites[site1].m_transactionsById.get(tx1_1.txnId));
        assertEquals(null, m_sites[site2].m_transactionsById.get(tx1_2.txnId));
        assertEquals((++callcheck), MockMPVoltProcedure.m_called);
    }

    /*
     * Create a multipartition work unit just test the removal of non-coordinator
     * site ids on failure. A little out of place in this file but the configured
     * ExecutionSite and Mailbox are necessary to construct a MP txn state.
     */
    public void testMultiPartitionParticipantTxnState_handleSiteFaults() {
        InitiateTask mn = new InitiateTask();
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


}
