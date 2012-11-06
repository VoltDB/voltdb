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

import org.voltdb.PartitionDRGateway;

import org.voltdb.rejoin.TaskLog;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class CompleteTransactionTask extends TransactionTask
{
    final private CompleteTransactionMessage m_msg;
    final private PartitionDRGateway m_drGateway;

    public CompleteTransactionTask(TransactionState txn,
                                   TransactionTaskQueue queue,
                                   CompleteTransactionMessage msg,
                                   PartitionDRGateway drGateway)
    {
        super(txn, queue);
        m_msg = msg;
        m_drGateway = drGateway;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        if (!m_txn.isReadOnly()) {
            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_msg.isRollback(), m_txn.getBeginUndoToken(), m_txn.txnId, m_txn.spHandle);
        }
        m_txn.setDone();
        m_queue.flush();
        hostLog.debug("COMPLETE: " + this);

        // Log invocation to DR
        if (m_drGateway != null && !m_txn.isForReplay() && !m_txn.isReadOnly() && !m_msg.isRollback()) {
            FragmentTaskMessage fragment = (FragmentTaskMessage) m_txn.getNotice();
            Iv2InitiateTaskMessage initiateTask = fragment.getInitiateTask();
            assert(initiateTask != null);
            StoredProcedureInvocation invocation = initiateTask.getStoredProcedureInvocation().getShallowCopy();
            invocation.setOriginalTxnId(m_txn.txnId);
            invocation.setOriginalTimestamp(m_txn.timestamp);
            m_drGateway.onSuccessfulProcedureCall(m_txn.spHandle, m_txn.timestamp, true,
                                                  invocation, m_txn.getResults());
        }
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        taskLog.logTask(m_msg);
        m_txn.setDone();
        m_queue.flush();
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        if (!m_txn.isReadOnly()) {
            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_msg.isRollback(), m_txn.getBeginUndoToken(), m_txn.txnId, m_txn.spHandle);
        }
        m_txn.setDone();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CompleteTransactionTask:");
        sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
        sb.append("  SP HANDLE: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append("  UNDO TOKEN: ").append(m_txn.getBeginUndoToken());
        sb.append("  MSG: ").append(m_msg.toString());
        return sb.toString();
    }
}
