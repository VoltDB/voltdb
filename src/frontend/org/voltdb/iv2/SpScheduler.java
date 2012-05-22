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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltcore.messaging.MessagingException;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.ProcedureRunner;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class SpScheduler extends Scheduler
{
    private Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();

    SpScheduler(PartitionClerk clerk)
    {
        super(clerk);
    }

    public void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        final String procedureName = message.getStoredProcedureName();
        final ProcedureRunner runner = m_loadedProcs.getProcByName(procedureName);
        if (message.isSinglePartition()) {
            final SpProcedureTask task =
                new SpProcedureTask(m_mailbox, runner,
                        m_txnId.incrementAndGet(), m_pendingTasks, message);
            m_pendingTasks.offer(task);
            return;
        }
        else {
            throw new RuntimeException("SpScheduler.handleIv2InitiateTaskMessage " +
                    "should never receive multi-partition initiations.");
        }
    }

    public void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        try {
            // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
            m_mailbox.send(message.getInitiatorHSId(), message);
        }
        catch (MessagingException e) {
            hostLog.error("Failed to deliver response from execution site.", e);
        }
    }

    public void handleFragmentTaskMessage(FragmentTaskMessage message) {
        handleFragmentTaskMessage(message, null);
    }

    public void handleFragmentTaskMessage(FragmentTaskMessage message,
                                          Map<Integer, List<VoltTable>> inputDeps)
    {
        TransactionState txn = m_outstandingTxns.get(message.getTxnId());
        // bit of a hack...we will probably not want to create and
        // offer FragmentTasks for txn ids that don't match if we have
        // something in progress already
        if (txn == null) {
            long localTxnId = m_txnId.incrementAndGet();
            txn = new ParticipantTransactionState(localTxnId, message);
            m_outstandingTxns.put(message.getTxnId(), txn);
        }
        if (message.isSysProcTask()) {
            final SysprocFragmentTask task =
                new SysprocFragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                        m_pendingTasks, message, inputDeps);
            m_pendingTasks.offer(task);
        }
        else {
            final FragmentTask task =
                new FragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                 m_pendingTasks, message, inputDeps);
            m_pendingTasks.offer(task);
        }
    }

    public void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        throw new RuntimeException("Partition masters don't yet handle fragment responses");
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        TransactionState txn = m_outstandingTxns.remove(message.getTxnId());
        // We can currently receive CompleteTransactionMessages for multipart procedures
        // which only use the buddy site (replicated table read).  Ignore them for
        // now, fix that later.
        if (txn != null)
        {
            final CompleteTransactionTask task =
                new CompleteTransactionTask(txn, m_pendingTasks, message);
            m_pendingTasks.offer(task);
        }
    }
}
