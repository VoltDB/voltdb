/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.List;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.utils.CoreUtils;

import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.utils.LogKeys;

import org.voltdb.VoltDB;

/**
 * Implements the Single-partition everywhere procedure ProcedureTask.
 * Creates one SP transaction per partition. These are produced on
 * the MPI so that all involved SPs have the same happens-before
 * relationship with concurrent MPs.
 */
public class EveryPartitionTask extends TransactionTask
{
    final long[] m_initiatorHSIds;
    final Iv2InitiateTaskMessage m_msg;
    final Mailbox m_mailbox;

    EveryPartitionTask(Mailbox mailbox, long txnId, TransactionTaskQueue queue,
                  Iv2InitiateTaskMessage msg, List<Long> pInitiators)
    {
        super(new SpTransactionState(txnId, msg), queue);
        m_msg = msg;
        m_initiatorHSIds = com.google.common.primitives.Longs.toArray(pInitiators);
        m_mailbox = mailbox;
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        try {
            m_mailbox.send(m_initiatorHSIds, m_msg);
        } catch (MessagingException e) {
            VoltDB.crashLocalVoltDB("Failed to serialize initiation for " +
                    m_msg.getStoredProcedureName(), true, e);
        }
        m_queue.flush();
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        hostLog.debug("COMPLETE: " + this);
    }

    @Override
    public long getMpTxnId()
    {
        return m_txn.txnId;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("EveryPartitionTask:");
        sb.append("  MP TXN ID: ").append(getMpTxnId());
        sb.append("  LOCAL TXN ID: ").append(getLocalTxnId());
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_mailbox.getHSId()));
        return sb.toString();
    }
}
