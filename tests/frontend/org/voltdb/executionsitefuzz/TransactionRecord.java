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

import java.util.HashSet;

public class TransactionRecord
{
    long m_txnId;
    boolean m_closed;
    boolean m_rollback;
    boolean m_multipart;
    boolean m_readonly;
    boolean m_coord;
    boolean m_selfFail;
    boolean m_otherFail;

    HashSet<Integer> m_failedSites;

    // Creation of a TransactionRecord with no initial log string will
    // create a null record.  Yes, ugly.
    TransactionRecord()
    {
        // These two settings will keep the null record from affecting
        // the state that the ExecutionFuzzChecker sees.
        m_txnId = Long.MAX_VALUE;
        m_selfFail = true;

        m_closed = false;
        m_rollback = false;
        m_multipart = false;
        m_otherFail = false;
        m_readonly = false;
        m_coord = false;
    }

    TransactionRecord(LogString logString)
    {
        assert(logString.isTxnStart());
        m_txnId = logString.getTxnId();
        m_closed = false;
        m_rollback = false;
        m_multipart = logString.isMultiPart();
        m_readonly = logString.isReadOnly();
        m_coord = logString.isCoordinator();
        m_selfFail = false;
        m_otherFail = false;
        m_failedSites = new HashSet<Integer>();
    }

    void updateRecord(LogString logString)
    {
        assert(!logString.isTxnStart());
        assert(!isClosed());
        if (logString.isRollback())
        {
            assert(logString.getTxnId() == m_txnId);
            m_rollback = true;
        }
        else if (logString.isSelfFault())
        {
            m_selfFail = true;
        }
        else if (logString.isOtherFault())
        {
            // XXX future add record of other failure site ID
            m_otherFail = true;
            m_failedSites.addAll(logString.getFaultNodes());
        }
        else if (logString.isTxnEnd())
        {
            assert(logString.getTxnId() == m_txnId);
            m_closed = true;
        }
    }

    Long getTxnId()
    {
        return m_txnId;
    }

    HashSet<Integer> getFailedSites()
    {
        return m_failedSites;
    }

    boolean isMultiPart()
    {
        return m_multipart;
    }

    boolean isReadOnly()
    {
        return m_readonly;
    }

    boolean isCoordinator()
    {
        return m_coord;
    }

    boolean isClosed()
    {
        return m_closed;
    }

    boolean rolledBack()
    {
        return m_rollback;
    }

    boolean failed()
    {
        return m_selfFail;
    }

    boolean sawFailure()
    {
        return m_otherFail;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("TXN: ").append(m_txnId);
        sb.append("  Type: ").append(m_multipart ? "multi" : "single");
        sb.append("  Read-Only: ").append(m_readonly);
        sb.append("  Role: ").append(m_coord ? "coordinator" : "participant");
        sb.append("  Rollback: ").append(m_rollback).append(", Closed: ").append(m_closed);
        sb.append("  Self-fail: ").append(m_selfFail);
        sb.append("  Saw failures: ").append(m_otherFail);
        if (m_otherFail)
        {
            sb.append("  Failed Nodes: ").append(m_failedSites.toString());
        }
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (o == this)
        {
            return true;
        }
        if (!(o instanceof TransactionRecord))
        {
            return false;
        }
        boolean retval = true;
        TransactionRecord other = (TransactionRecord) o;
        retval &= (other.m_txnId == m_txnId);
        retval &= (other.m_closed == m_closed);
        retval &= (other.m_multipart == m_multipart);
        retval &= (other.m_readonly == m_readonly);
        retval &= (other.m_rollback == m_rollback);
        return retval;
    }

    // Similar to equals() but rather than enforcing exact equality
    // checks that various combinations of multi/single, readonly/readwrite,
    // coord/participant are consistent even if they differ
    public boolean isConsistent(TransactionRecord other)
    {
        if (other == this)
        {
            return true;
        }
        boolean retval = true;
        retval &= (other.m_txnId == m_txnId);
        retval &= (other.m_closed == m_closed);
        retval &= (other.m_multipart == m_multipart);
        retval &= (other.m_readonly == m_readonly);
        // If the transaction is multipartition
        //   If both records are from participants
        //     If the transaction is readonly
        //       Rollback doesn't have to match
        //     If the transaction is not readonly
        //       Rollback does have to match (barring failure, ignore for now)
        //   If one record is from the coordinator
        //     If the participant rolls back
        //       The coordinator must roll back
        // If the transaction is singlepartition
        //   Everything must match (barring failure)
        if (m_multipart)
        {
            if (!m_coord && !other.m_coord)
            {
                if (!m_readonly)
                {
                    // participants in a non-readonly multi-part transaction
                    // have to agree about the outcome (since this should
                    // be dictated by the coordinator)
                    retval &= (other.m_rollback == m_rollback);
                }
            }
            else
            {
                // if the participant rolled back
                if ((!m_coord && m_rollback) || (!other.m_coord && other.m_rollback))
                {
                    // then the coordinator should also roll back
                    retval &= (other.m_rollback == m_rollback);
                }
            }
        }
        else
        {
            retval &= (other.m_rollback == m_rollback);
            // All single-part are coordinators, this is a sanity check
            retval &= (other.m_coord == m_coord);
        }
        return retval;
    }
}
