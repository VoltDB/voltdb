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

import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.utils.LogKeys;

/**
 * Implements the Multi-partition procedure ProcedureTask.
 * Runs multi-partition transaction, causing work to be distributed
 * across the cluster as necessary.
 */
public class MpProcedureTask extends ProcedureTask
{
    final long[] m_initiatorHSIds;
    final Iv2InitiateTaskMessage m_msg;

    MpProcedureTask(Mailbox mailbox, ProcedureRunner runner,
                  long txnId, TransactionTaskQueue queue,
                  Iv2InitiateTaskMessage msg, List<Long> pInitiators,
                  long buddyHSId)
    {
        super(mailbox, runner,
              new MpTransactionState(mailbox, txnId, msg, pInitiators,
                                     buddyHSId),
              queue);
        m_msg = msg;
        m_initiatorHSIds = com.google.common.primitives.Longs.toArray(pInitiators);
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        // Cast up. Could avoid ugliness with Iv2TransactionClass baseclass
        MpTransactionState txn = (MpTransactionState)m_txn;
        m_txn.setBeginUndoToken(siteConnection.getLatestUndoToken());
        // Exception path out of here for rollback is going to need to
        // call m_txn.setDone() somehow
        final InitiateResponseMessage response = processInitiateTask(txn.m_task);
        if (!response.shouldCommit()) {
            txn.setNeedsRollback();
        }
        completeInitiateTask(siteConnection);
        m_initiator.deliver(response);
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        hostLog.debug("COMPLETE: " + this);
    }

    @Override
    void completeInitiateTask(SiteProcedureConnection siteConnection)
    {
        CompleteTransactionMessage complete = new CompleteTransactionMessage(
                m_initiator.getHSId(), // who is the "initiator" now??
                m_initiator.getHSId(),
                m_txn.txnId,
                m_txn.isReadOnly(),
                m_txn.needsRollback(),
                false);  // really don't want to have ack the ack.

        try {
            m_initiator.send(m_initiatorHSIds, complete);
        } catch (MessagingException fatal) {
            org.voltdb.VoltDB.crashLocalVoltDB("Messaging exception", true, fatal);
        }
        m_txn.setDone();
        m_queue.flush();
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
        sb.append("MpProcedureTask:");
        sb.append("  MP TXN ID: ").append(getMpTxnId());
        sb.append("  LOCAL TXN ID: ").append(getLocalTxnId());
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_initiator.getHSId()));
        return sb.toString();
    }
}
