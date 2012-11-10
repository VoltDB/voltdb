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

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;

import org.voltdb.rejoin.TaskLog;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.utils.LogKeys;

/**
 * Implements the Multi-partition procedure ProcedureTask.
 * Runs multi-partition transaction, causing work to be distributed
 * across the cluster as necessary.
 */
public class MpProcedureTask extends ProcedureTask
{
    final List<Long> m_initiatorHSIds = new ArrayList<Long>();
    final Iv2InitiateTaskMessage m_msg;

    MpProcedureTask(Mailbox mailbox, String procName, TransactionTaskQueue queue,
                  Iv2InitiateTaskMessage msg, List<Long> pInitiators,
                  long buddyHSId)
    {
        super(mailbox, procName,
              new MpTransactionState(mailbox, msg, pInitiators,
                                     buddyHSId),
              queue);
        m_msg = msg;
        m_initiatorHSIds.addAll(pInitiators);
    }

    /**
     * Update the list of partition masters in the event of a failure/promotion.
     * Currently only thread-"safe" by virtue of only calling this on
     * MpProcedureTasks which are not at the head of the MPI's TransactionTaskQueue.
     */
    public void updateMasters(List<Long> masters)
    {
        m_initiatorHSIds.clear();
        m_initiatorHSIds.addAll(masters);
        ((MpTransactionState)getTransactionState()).updateMasters(masters);
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
        final InitiateResponseMessage response = processInitiateTask(txn.m_task, siteConnection);
        if (!response.shouldCommit()) {
            txn.setNeedsRollback();
        }
        completeInitiateTask(siteConnection);
        // Set the source HSId (ugh) to ourselves so we track the message path correctly
        response.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(response);
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        hostLog.debug("COMPLETE: " + this);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        throw new RuntimeException("MP procedure task asked to run on rejoining site.");
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException("MP procedure task asked to run from tasklog on rejoining site.");
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
                false,  // really don't want to have ack the ack.
                false,
                m_msg.isForReplay());

        complete.setTruncationHandle(m_msg.getTruncationHandle());
        complete.setOriginalTxnId(m_msg.getOriginalTxnId());
        m_initiator.send(com.google.common.primitives.Longs.toArray(m_initiatorHSIds), complete);
        m_txn.setDone();
        m_queue.flush();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("MpProcedureTask:");
        sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
        sb.append("  SP HANDLE ID: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_initiator.getHSId()));
        return sb.toString();
    }
}
