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
                  long txnId, Iv2InitiateTaskMessage msg, List<Long> pInitiators)
    {
        super(mailbox, runner,
              new MpTransactionState(mailbox, txnId, msg, pInitiators,
                                     mailbox.getHSId()));
        m_msg = msg;
        m_initiatorHSIds = com.google.common.primitives.Longs.toArray(pInitiators);
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        // Cast up. Could avoid ugliness with Iv2TransactionClass baseclass
        MpTransactionState txn = (MpTransactionState)m_txn;
        m_txn.setBeginUndoToken(siteConnection.getLatestUndoToken());
        final InitiateResponseMessage response = processInitiateTask(txn.m_task);
        if (!response.shouldCommit()) {
            txn.setNeedsRollback();
        }
        completeInitiateTask(siteConnection);
        m_initiator.deliver(response);
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
    }

    @Override
    void completeInitiateTask(SiteProcedureConnection siteConnection)
    {
        // we need to handle the undo log for the local site
        siteConnection.truncateUndoLog(m_txn.needsRollback(),
                                       m_txn.getBeginUndoToken(),
                                       m_txn.txnId);
        CompleteTransactionMessage complete = new CompleteTransactionMessage(
                m_initiator.getHSId(), // who is the "initiator" now??
                m_initiator.getHSId(),
                m_txn.txnId,
                m_txn.isReadOnly(),
                m_txn.needsRollback(),
                false);  // really don't want to have ack the ack.

        try {
            // XXX IZZY hack don't send the complete transaction msg to ourselves
            // in the current MPI hack
            for (long hsid : m_initiatorHSIds) {
                if (hsid != m_initiator.getHSId()) {
                    m_initiator.send(hsid, complete);
                }
            }
        } catch (MessagingException fatal) {
            org.voltdb.VoltDB.crashLocalVoltDB("Messaging exception", true, fatal);
        }
        m_txn.setDone();
    }

    @Override
    public long getMpTxnId()
    {
        return m_msg.getTxnId();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("MpProcedureTask:");
        sb.append("\n\tMP TXN ID: ").append(getMpTxnId());
        sb.append("\n\tLOCAL TXN ID: ").append(getLocalTxnId());
        return sb.toString();
    }
}
