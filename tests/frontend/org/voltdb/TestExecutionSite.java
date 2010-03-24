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

import java.util.*;

import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.SinglePartitionTxnState;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messages.InitiateTask;
import org.voltdb.messaging.MockMailbox;
import org.voltdb.messaging.VoltMessage;

import junit.framework.TestCase;

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

    /*
     * Mock single VoltProcedure that does nothing.
     */
    public static class MockSPVoltProcedure extends VoltProcedure
    {
        @Override
        ClientResponseImpl call(TransactionState txnState, Object... paramList)
        {
            // not read-only.
            m_site.getNextUndoToken();
            ClientResponseImpl response = new ClientResponseImpl();
            response.setResults(ClientResponseImpl.SUCCESS,
                                new VoltTable[] {}, "MockSPVoltProcedure Response");
            return response;
        }
    }

    /*
     * Mock multi-partition VoltProcedure's through slowPath().
     */
    public static class MockMPVoltProcedure extends VoltProcedure
    {
        @Override
        ClientResponseImpl call(TransactionState txnState, Object... paramList) {
            // Basically the txnState state transitions done by VoltProcedure.slowPath()

            // txnState.getNextDependencyId()
            // txnState.setupProcedureResume(finalTask, depsToResume)
            // txnState.createLocalFragmentWork(localTask, fragsNonTransactional && finalTask)
            // txnState.createAllParticipatingFragmentWork(distributedTasks)

            Map<Integer, List<VoltTable>> resultDeps = m_site.recursableRun(txnState);
            // recursableRun will then call txnState.doWork() {
            //     the coordinator will possibly hit the sneak-in optimization
            //     non-coordinator sites will not.
            // }
            return null;
        }
    }

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        Procedure proc = null;

        m_voltdb = new MockVoltDB();
        m_voltdb.addHost(0);
        m_voltdb.addPartition(0);
        m_voltdb.addSite(site1, 0, 0, true);
        m_voltdb.addPartition(1);
        m_voltdb.addSite(site2, 0, 1, true);

        proc = m_voltdb.addProcedureForTest(MockSPVoltProcedure.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(true);

        VoltDB.replaceVoltDBInstanceForTest(m_voltdb);

        m_mboxes[site1] = new MockMailbox(new LinkedList<VoltMessage>());
        m_mboxes[site2] = new MockMailbox(new LinkedList<VoltMessage>());
        m_sites[site1] = new ExecutionSite(m_voltdb, m_mboxes[site1], site1);
        m_sites[site2] = new ExecutionSite(m_voltdb, m_mboxes[site2], site2);

        m_sites[site1].getNextUndoToken();
        m_sites[site2].getNextUndoToken();

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
    public void testMockSinglePartitionTxn()
    {
        // That the full procedure name is necessary is a bug in the
        // mock objects - or perhaps an issue with a nested class?
        StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockSPVoltProcedure");
        tx1_spi.setParams(new Integer(0));

        InitiateTask tx1_mn =
            new InitiateTask(initiator1, site1, 1000, false, true, tx1_spi);

        SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes[site1], m_sites[site1], tx1_mn);

        assertFalse(tx1.isDone());
        m_sites[site1].m_transactionsById.put(tx1.txnId, tx1);
        m_sites[site1].recursableRun(tx1);
        assertTrue(tx1.isDone());
        assertEquals(null, m_sites[site1].m_transactionsById.get(tx1.txnId));
    }


}
