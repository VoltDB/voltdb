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
import org.voltcore.messaging.VoltMessage;

import org.voltdb.messaging.BorrowTaskMessage;
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
    long[] m_replicaHSIds = new long[] {};
    private Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();

    SpScheduler()
    {
        super();
    }

    @Override
    public void updateReplicas(long[] hsids)
    {
        m_replicaHSIds = hsids;
    }

    // SpInitiators will see every message type.  The Responses currently come
    // from local work, but will come from replicas when replication is
    // implemented
    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            handleIv2InitiateTaskMessage((Iv2InitiateTaskMessage)message);
        }
        else if (message instanceof InitiateResponseMessage) {
            handleInitiateResponseMessage((InitiateResponseMessage)message);
        }
        else if (message instanceof FragmentTaskMessage) {
            handleFragmentTaskMessage((FragmentTaskMessage)message);
        }
        else if (message instanceof FragmentResponseMessage) {
            handleFragmentResponseMessage((FragmentResponseMessage)message);
        }
        else if (message instanceof CompleteTransactionMessage) {
            handleCompleteTransactionMessage((CompleteTransactionMessage)message);
        }
        else if (message instanceof BorrowTaskMessage) {
            handleBorrowTaskMessage((BorrowTaskMessage)message);
        }
        else {
            throw new RuntimeException("UNKNOWN MESSAGE TYPE, BOOM!");
        }
    }

    // SpScheduler expects to see InitiateTaskMessages corresponding to single-partition
    // procedures only.
    public void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        final String procedureName = message.getStoredProcedureName();
        final ProcedureRunner runner = m_loadedProcs.getProcByName(procedureName);
        if (message.isSinglePartition()) {
            long new_txnId = m_txnId.incrementAndGet();
            if (m_replicaHSIds.length > 0) {
                try {
                    Iv2InitiateTaskMessage replmsg =
                        new Iv2InitiateTaskMessage(m_mailbox.getHSId(),
                                m_mailbox.getHSId(),
                                message.getTxnId(),
                                message.isReadOnly(),
                                message.isSinglePartition(),
                                message.getStoredProcedureInvocation(),
                                message.getClientInterfaceHandle()) ;
                    m_mailbox.send(m_replicaHSIds, replmsg);
                } catch (MessagingException e) {
                    hostLog.error("Failed to deliver response from execution site.", e);
                }
                DuplicateCounter counter = new DuplicateCounter(
                        message.getInitiatorHSId(), m_replicaHSIds.length + 1,
                        new_txnId);
            }
            final SpProcedureTask task =
                new SpProcedureTask(m_mailbox, runner,
                        new_txnId, m_pendingTasks, message);
            m_pendingTasks.offer(task);
            return;
        }
        else {
            throw new RuntimeException("SpScheduler.handleIv2InitiateTaskMessage " +
                    "should never receive multi-partition initiations.");
        }
    }

    // InitiateResponses for single-partition initiators currently get completely handled
    // by SpInitiatorMessageHandler.  This may change when replication is added.
    public void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        // Send the message to the duplicate counter, if any
        // ADD CODE HERE

        if (message.getInitiatorHSId() != m_mailbox.getHSId()) {
            try {
                // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
                m_mailbox.send(message.getInitiatorHSId(), message);
            }
            catch (MessagingException e) {
                // hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
    }

    // BorrowTaskMessages just encapsulate a FragmentTaskMessage along with
    // its input dependency tables.
    private void handleBorrowTaskMessage(BorrowTaskMessage message) {
        handleFragmentTaskMessage(message.getFragmentTaskMessage(),
                message.getInputDepMap());
    }

    // SpSchedulers will see FragmentTaskMessage for:
    // - The scatter fragment(s) of a multi-part transaction (normal or sysproc)
    // - Borrow tasks to do the local fragment work if this partition is the
    //   buddy of the MPI.  Borrow tasks may include input dependency tables for
    //   aggregation fragments, or not, if it's a replicated table read.
    // For multi-batch MP transactions, we'll need to look up the transaction state
    // that gets created when the first batch arrives.
    public void handleFragmentTaskMessage(FragmentTaskMessage message,
                                          Map<Integer, List<VoltTable>> inputDeps)
    {
        // If we have input dependencies, it's borrow work, there's no way we
        // can actually distribute it
        if (m_replicaHSIds.length > 0 && inputDeps == null) {
            try {
                FragmentTaskMessage replmsg =
                    new FragmentTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(), message);
                m_mailbox.send(m_replicaHSIds, replmsg);
            } catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
        }

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

    // Eventually, the master for a partition set will need to be able to dedupe
    // FragmentResponses from its replicas.
    public void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        // Add duplicate counter code here
        // ADD CODE HERE

        if (message.getDestinationSiteId() != m_mailbox.getHSId()) {
            try {
                m_mailbox.send(message.getDestinationSiteId(), message);
            }
            catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        if (m_replicaHSIds.length > 0) {
            try {
                CompleteTransactionMessage replmsg = message;
                m_mailbox.send(m_replicaHSIds, replmsg);
            } catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
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
