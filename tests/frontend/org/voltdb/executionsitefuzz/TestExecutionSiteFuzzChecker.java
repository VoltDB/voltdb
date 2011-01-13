/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.executionsitefuzz;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.TestCase;

public class TestExecutionSiteFuzzChecker extends TestCase
{
    final static int SITECOUNT = 2;
    final static int PARTCOUNT = 3;

    // cases to cover
    //
    // basics:
    // all sites report rollback succeeds
    // all sites report completion succeeds
    //
    // one site rollback and one site commit where neither one failed fails
    // fewer than the number of 'live' sites respond and they aren't rollback
    //
    // extra degree of difficulty:
    // - ENG-646 coordinator completes TXN and fails later but other sites
    //   see that failure in the middle of TXN

    HashMap<Integer, HashMap<Integer, StringWriter>> m_siteLogs;

    protected void setUp()
    {
        int site_id = 0;
        m_siteLogs = new HashMap<Integer, HashMap<Integer, StringWriter>>();
        for (int part = 0; part < PARTCOUNT; part++)
        {
            m_siteLogs.put(part, new HashMap<Integer, StringWriter>());
            for (int site = 0; site < SITECOUNT; site++)
            {
                m_siteLogs.get(part).put(site_id, new StringWriter());
                site_id++;
            }
        }
    }

    protected void tearDown()
    {
        m_siteLogs = null;
    }

    // Convenience methods for generating log strings
    void beginNewTxn(int partitionId, int siteId, int txnId, boolean isMulti,
                     boolean isReadOnly, boolean isCoord)
    {
        StringWriter sw = m_siteLogs.get(partitionId).get(siteId);
        String msg = "FUZZTEST beginNewTxn " + txnId +
                     (isMulti ? " multi" : " single") + " " +
                     (isReadOnly ? "readonly" : "readwrite") + " " +
                     (isCoord ? "coord" : "part");

        sw.getBuffer().append(msg + "\n");
    }

    void rollbackTransaction(int partitionId, int siteId, int txnId)
    {
        StringWriter sw = m_siteLogs.get(partitionId).get(siteId);
        String msg = "FUZZTEST rollbackTransaction " + txnId;
        sw.getBuffer().append(msg + "\n");
    }

    void completeTransaction(int partitionId, int siteId, int txnId)
    {
        StringWriter sw = m_siteLogs.get(partitionId).get(siteId);
        String msg = "FUZZTEST completeTransaction " + txnId;
        sw.getBuffer().append(msg + "\n");
    }

    void selfNodeFailure(int partitionId, int siteId, int nodeId)
    {
        StringWriter sw = m_siteLogs.get(partitionId).get(siteId);
        String msg = "FUZZTEST selfNodeFailure " + nodeId;
        sw.getBuffer().append(msg + "\n");
    }

    void handleNodeFault(int partitionId, int siteId, ArrayList<Integer> nodeIds)
    {
        StringWriter sw = m_siteLogs.get(partitionId).get(siteId);
        StringBuilder nodestring = new StringBuilder();
        for (Integer node : nodeIds)
        {
            nodestring.append(node).append(" ");
        }
        String msg = "FUZZTEST handleNodeFault " + nodestring.toString() +
                     " with DONT CARE";
        sw.getBuffer().append(msg + "\n");
    }

    void handleNodeFault(int partitionId, int siteId, int nodeId)
    {
        ArrayList<Integer> nodes = new ArrayList<Integer>();
        nodes.add(nodeId);
        handleNodeFault(partitionId, siteId, nodes);
    }

    // convenience method for generating a clean completed TXN
    void addCommitTxn(int partitionId, int siteId, int txnId, boolean isMulti,
                      boolean isReadOnly, boolean isCoord)
    {
        beginNewTxn(partitionId, siteId, txnId, isMulti, isReadOnly, isCoord);
        completeTransaction(partitionId, siteId, txnId);
    }

    // convenience method for generating a clean rolled-back TXN
    void addRollbackTxn(int partitionId, int siteId, int txnId, boolean isMulti,
                        boolean isReadOnly, boolean isCoord)
    {
        beginNewTxn(partitionId, siteId, txnId, isMulti, isReadOnly, isCoord);
        rollbackTransaction(partitionId, siteId, txnId);
        completeTransaction(partitionId, siteId, txnId);
    }

    public void testNormalOperation()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();

        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                boolean isCoord = false;
                if (site == 0) { isCoord = true; }
                addRollbackTxn(partition, site, 10000, true, false, isCoord);
                addRollbackTxn(partition, site, 10001, true, false, isCoord);
                addCommitTxn(partition, site, 10002, true, true, isCoord);
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // The fuzztester should complain if the sites don't all make the same
    // commit/rollback decision
    public void testMismatchedTxn()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the inconsistent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                if (site == 0)
                {
                    addCommitTxn(partition, site, 10000, true, false, true);
                }
                else
                {
                    addRollbackTxn(partition, site, 10000, true, false, false);
                }
            }
        }
        dut.dumpLogs();
        assertFalse(dut.validateLogs());
    }

    public void testBasicFailureWhenSeen()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the failed one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                if (site == 0)
                {
                    addRollbackTxn(partition, site, 10000, true, false, true);
                    beginNewTxn(partition, site, 10001, true, false, true);
                    selfNodeFailure(partition, site, 66000);
                }
                else
                {
                    addRollbackTxn(partition, site, 10000, true, false, false);
                    beginNewTxn(partition, site, 10001, true, false, false);
                    handleNodeFault(partition, site, 66000);
                    completeTransaction(partition, site, 10001);
                }
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // Things work even if the other sites didn't see this failure during this
    // transaction
    public void testBasicFailureWhenUnseen()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the failed one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                if (site == 0)
                {
                    addRollbackTxn(partition, site, 10000, true, false, true);
                    beginNewTxn(partition, site, 10001, true, false, true);
                    selfNodeFailure(partition, site, 66000);
                }
                else
                {
                    addRollbackTxn(partition, site, 10000, true, false, false);
                    beginNewTxn(partition, site, 10001, true, false, false);
                    completeTransaction(partition, site, 10001);
                }
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // There are cases where one or more sites will see a coordinator failure
    // before they even start the transaction, but one or more other sites
    // will have started it and roll it back, so we see fewer responses than
    // the number of sites still live.  validate that it's okay with
    // rollback.  next test will validate that we fail if it's not rolled back.
    public void testFewerResponsesRollback()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the absent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                // Need one clean transaction for all sites so site 0 doesn't
                // appear to have failed
                if (site == 0)
                {
                    addCommitTxn(partition, site, 10000, true, false, true);
                    // ADD NO TRANSACTION 10001
                    // Need one clean transaction after so we don't
                    // think site 0 is done.
                    // This is technically a bug, we should error if one site
                    // doesn't report anything for the last transaction
                    addCommitTxn(partition, site, 10002, true, false, true);
                }
                else
                {
                    addCommitTxn(partition, site, 10000, true, false, false);
                    addRollbackTxn(partition, site, 10001, true, false, false);
                    addCommitTxn(partition, site, 10002, true, false, false);
                }
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // close to the above case, but commit the transactions instead
    public void testFewerResponsesCommit()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the absent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                // Need one clean transaction for all sites so site 0 doesn't
                // appear to have failed
                if (site == 0)
                {
                    addCommitTxn(partition, site, 10000, true, false, true);
                    // ADD NO TRANSACTION 10001
                    addCommitTxn(partition, site, 10002, true, false, true);
                }
                else
                {
                    addCommitTxn(partition, site, 10000, true, false, false);
                    addCommitTxn(partition, site, 10001, true, false, false);
                    addCommitTxn(partition, site, 10002, true, false, false);
                }
                // Need one clean transaction after so we don't
                // think site 0 is done.
                // This is technically a bug, we should error if one site
                // doesn't report anything for hte last transaction
            }
        }
        dut.dumpLogs();
        assertFalse(dut.validateLogs());
    }

    // There's a case where, if a site fails immediately and has no
    // transactions, we would see an ArrayIndexOutOfBounds error.
    // Part of ENG-646.
    public void testEmptySiteTransactionLog()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the absent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                if (site == 0)
                {
                    // ADD NO TRANSACTION
                }
                else
                {
                    addCommitTxn(partition, site, 10001, true, false, true);
                }
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // If a transaction is multipartition read-only, the participants can
    // have different rollback results, but the coordinator must rollback if
    // any participant rolls back
    public void testReadOnlyMultiPartCoordRollbackDifference()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the absent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                // Site 0 is the coordinator and rolls back
                if (site == 0)
                {
                    addRollbackTxn(partition, site, 10000, true, true, true);
                }
                // every even site ID also rolls back
                else if (site % 2 == 0)
                {
                    addRollbackTxn(partition, site, 10000, true, true, false);
                }
                // every odd site commits
                else
                {
                    addCommitTxn(partition, site, 10000, true, true, false);
                }
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // Same as the above, but verify that if the coordinator commits that we
    // interpret it as an error
    public void testReadOnlyMultiPartCoordCommitDifference()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the absent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                // Site 0 is the coordinator and commits
                if (site == 0)
                {
                    addCommitTxn(partition, site, 10000, true, true, true);
                }
                // every even site ID also rolls back
                else if (site % 2 == 0)
                {
                    addRollbackTxn(partition, site, 10000, true, true, false);
                }
                // every odd site commits
                else
                {
                    addCommitTxn(partition, site, 10000, true, true, false);
                }
            }
        }
        dut.dumpLogs();
        assertFalse(dut.validateLogs());
    }

    // If a transaction is single partition read-only, the participants
    // still must have identical rollback or commit results
    public void testReadOnlySinglePartRollbackDifference()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the absent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                if (site % 2 == 0)
                {
                    addRollbackTxn(partition, site, 10000, false, true, true);
                }
                // every odd site commits
                else
                {
                    addCommitTxn(partition, site, 10000, false, true, true);
                }
            }
        }
        dut.dumpLogs();
        assertFalse(dut.validateLogs());
    }

    // The fuzz checker should notice if a transaction ID reappears
    // (the transaction ID should increase between every transaction)
    public void testBadTxnIdOrder()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the absent one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                // Let's say that site 0 tries to do something speculatively
                // and does it out of order
                if (site == 0)
                {
                    addCommitTxn(partition, site, 10001, false, false, true);
                    addCommitTxn(partition, site, 10000, false, false, true);
                }
                else
                {
                    addCommitTxn(partition, site, 10000, false, false, true);
                    addCommitTxn(partition, site, 10001, false, false, true);
                }
            }
        }
        dut.dumpLogs();
        assertFalse(dut.validateLogs());
    }

    // There are some failure cases where both the coordinator and a participant
    // will fail concurrently at the end of a transaction where the failed
    // participant is the only site that has received an ack and completed
    // the transaction; since the coordinator then fails, the rest of the
    // participants roll back.  The fuzz checker should notice and discard
    // this 'false' completion of the failed participant.
    public void testMultiPartDiscardCommmit()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the failed one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                if (site == 0) // site 0 will be the failing coord
                {
                    addCommitTxn(partition, site, 10000, true, false, true);
                    beginNewTxn(partition, site, 10001, true, false, true);
                    selfNodeFailure(partition, site, 0);
                }
                else if (site == 1) // site 1 will be the failing participant
                {
                    addCommitTxn(partition, site, 10000, true, false, false);
                    addCommitTxn(partition, site, 10001, true, false, false);
                    addCommitTxn(partition, site, 10002, false, false, true);
                    handleNodeFault(partition, site, 0);
                    selfNodeFailure(partition, site, 1);
                }
                else // all other sites will see both failures (1 then 0)
                {
                    addCommitTxn(partition, site, 10000, true, false, false);
                    beginNewTxn(partition, site, 10001, true, false, false);
                    ArrayList<Integer> nodefaults = new ArrayList<Integer>();
                    nodefaults.add(0);
                    nodefaults.add(1);
                    handleNodeFault(partition, site, nodefaults);
                    rollbackTransaction(partition, site, 10001);
                    completeTransaction(partition, site, 10001);
                }
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // This is similar to the above case, but there's a read-only case
    // where the transaction can complete, the coordinator can move on, but
    // it will appear to have failed during that transaction according to
    // other participants that haven't finished yet.  This should NEVER
    // happen during a read-write transaction, so verify that we notice
    // failure in that case.
    public void testMultiPartCoordCantFinishReadWriteEarly()
    {
        ExecutionSiteFuzzChecker dut = new ExecutionSiteFuzzChecker();
        // pick site 0 to be the failed one.
        for (Integer partition : m_siteLogs.keySet())
        {
            for (Integer site : m_siteLogs.get(partition).keySet())
            {
                dut.addSite(site, partition, m_siteLogs.get(partition).get(site));
                if (site == 0) // site 0 will be the failing coord
                {
                    addCommitTxn(partition, site, 10000, true, false, true);
                    addRollbackTxn(partition, site, 10001, true, false, true);
                    selfNodeFailure(partition, site, 0);
                }
                else // all other sites will see both failures (1 then 0)
                {
                    addCommitTxn(partition, site, 10000, true, false, false);
                    beginNewTxn(partition, site, 10001, true, false, false);
                    handleNodeFault(partition, site, 0);
                    rollbackTransaction(partition, site, 10001);
                    completeTransaction(partition, site, 10001);
                }
            }
        }
        dut.dumpLogs();
        assertFalse(dut.validateLogs());
    }
}

