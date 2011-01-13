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

public class SiteLog
{
    private Integer m_siteId;
    private Integer m_partitionId;
    private StringWriter m_logBuffer;
    private int m_logIndex;
    private HashMap<Long, TransactionRecord> m_txns;
    private ArrayList<TransactionRecord> m_txnInitOrder;
    private ArrayList<TransactionRecord> m_txnCloseOrder;

    SiteLog(Integer siteId, Integer partitionId, StringWriter logBuffer)
    {
        m_siteId = siteId;
        m_partitionId = partitionId;
        m_logBuffer = logBuffer;
        m_logIndex = 0;
        m_txns = new HashMap<Long, TransactionRecord>();
        m_txnInitOrder = new ArrayList<TransactionRecord>();
        m_txnCloseOrder = new ArrayList<TransactionRecord>();
    }

    void reset()
    {
        m_logIndex = 0;
    }

    Integer getSiteId()
    {
        return m_siteId;
    }

    Integer getPartitionId()
    {
        return m_partitionId;
    }

    void logComplete()
    {
        // inefficient use of space
        String[] logStrings = m_logBuffer.toString().split("\n");
        // Process the log output into transaction records
        for (int i = 0; i < logStrings.length; i++)
        {
            LogString next = new LogString(logStrings[i]);
            if (!next.isFuzz())
            {
                continue;
            }
            TransactionRecord txn = null;
            if (next.isTxnStart())
            {
                // create a new TransactionRecord
                assert(!m_txns.containsKey(next.getTxnId()));
                txn = new TransactionRecord(next);
                m_txns.put(next.getTxnId(), txn);
                m_txnInitOrder.add(txn);
            }
            else if (next.isRollback() || next.isTxnEnd())
            {
                txn = m_txns.get(next.getTxnId());
                txn.updateRecord(next);
                if (next.isTxnEnd())
                {
                    m_txnCloseOrder.add(txn);
                }
            }
            else if (next.isSelfFault() || next.isOtherFault())
            {
                // mark this fault in all open transactions
                // This is horribly inefficient, any number of smarter faster options
                // if it's too slow
                for (TransactionRecord txn1 : m_txns.values())
                {
                    if (!txn1.isClosed())
                    {
                        txn1.updateRecord(next);
                    }
                }
            }
            else
            {
                // Unknown log type
                assert(false);
            }
        }
    }

    TransactionRecord currentTxn()
    {
        // if this site failed immediately and
        // need to return a bogus TransactionRecord that indicates failure
        // and doesn't interfere with the rest of the system.
        if (m_txnInitOrder.size() == 0)
        {
            TransactionRecord null_txn = new TransactionRecord();
            return null_txn;
        }
        else if (isDone())
        {
            return m_txnInitOrder.get(m_txnInitOrder.size() - 1);
        }
        return m_txnInitOrder.get(m_logIndex);
    }

    boolean isDone()
    {
        return (m_logIndex >= m_txnInitOrder.size());
    }

    // Contrive to stay at the last log message if we've reached the end.
    void advanceLog()
    {
        if (!isDone())
        {
            m_logIndex++;
        }
    }
}
