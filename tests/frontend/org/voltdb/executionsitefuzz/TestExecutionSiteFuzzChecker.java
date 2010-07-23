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

package org.voltdb.executionsitefuzz;

import java.io.StringWriter;
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

    // Convenience methods for generating log strings
    void beginNewTxn(int partitionId, int siteId, int txnId, boolean isMulti)
    {
        StringWriter sw = m_siteLogs.get(partitionId).get(siteId);
        String msg = "FUZZTEST beginNewTxn " + txnId + (isMulti ? " multi" : " single");
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

    void handleNodeFault(int partitionId, int siteId, int nodeId)
    {
        StringWriter sw = m_siteLogs.get(partitionId).get(siteId);
        String msg = "FUZZTEST handleNodeFault " + nodeId;
        sw.getBuffer().append(msg + "\n");
    }

    // convenience method for generating a clean completed TXN
    void addCommitTxn(int partitionId, int siteId, int txnId, boolean isMulti)
    {
        beginNewTxn(partitionId, siteId, txnId, isMulti);
        completeTransaction(partitionId, siteId, txnId);
    }

    // convenience method for generating a clean rolled-back TXN
    void addRollbackTxn(int partitionId, int siteId, int txnId, boolean isMulti)
    {
        beginNewTxn(partitionId, siteId, txnId, isMulti);
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
                addRollbackTxn(partition, site, 10000, true);
                addRollbackTxn(partition, site, 10001, true);
                addCommitTxn(partition, site, 10002, true);
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
                    addCommitTxn(partition, site, 10000, true);
                }
                else
                {
                    addRollbackTxn(partition, site, 10000, true);
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
                addRollbackTxn(partition, site, 10000, true);
                if (site == 0)
                {
                    beginNewTxn(partition, site, 10001, true);
                    selfNodeFailure(partition, site, 66000);
                }
                else
                {
                    beginNewTxn(partition, site, 10001, true);
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
                addRollbackTxn(partition, site, 10000, true);
                if (site == 0)
                {
                    beginNewTxn(partition, site, 10001, true);
                    selfNodeFailure(partition, site, 66000);
                }
                else
                {
                    beginNewTxn(partition, site, 10001, true);
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
                addCommitTxn(partition, site, 10000, true);
                if (site == 0)
                {
                    // ADD NO TRANSACTION
                }
                else
                {
                    addRollbackTxn(partition, site, 10001, true);
                }
                // Need one clean transaction after so we don't
                // think site 0 is done.
                // This is technically a bug, we should error if one site
                // doesn't report anything for hte last transaction
                addCommitTxn(partition, site, 10002, true);
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }

    // close to the above case, but commit the transactions instead.  This
    // should fail s
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
                addCommitTxn(partition, site, 10000, true);
                if (site == 0)
                {
                    // ADD NO TRANSACTION
                }
                else
                {
                    addCommitTxn(partition, site, 10001, true);
                }
                // Need one clean transaction after so we don't
                // think site 0 is done.
                // This is technically a bug, we should error if one site
                // doesn't report anything for hte last transaction
                addCommitTxn(partition, site, 10002, true);
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
                    addCommitTxn(partition, site, 10001, true);
                }
            }
        }
        dut.dumpLogs();
        assertTrue(dut.validateLogs());
    }
}

