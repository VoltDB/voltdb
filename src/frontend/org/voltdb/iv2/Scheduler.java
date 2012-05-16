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
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.ProcedureRunner;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class Scheduler
{
    VoltLogger hostLog = new VoltLogger("HOST");
    final private SiteTaskerQueue m_tasks;
    private LoadedProcedureSet m_loadedProcs;
    private PartitionClerk m_clerk;
    private Mailbox m_mailbox;
    private Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private Map<Long, DuplicateCounter> m_duplicateCounters =
        new HashMap<Long, DuplicateCounter>();
    final private TransactionTaskQueue m_pendingTasks;

    // hacky temp txnid
    AtomicLong m_txnId = new AtomicLong(0);

    Scheduler(PartitionClerk clerk)
    {
        m_tasks = new SiteTaskerQueue();
        m_pendingTasks = new TransactionTaskQueue(m_tasks);
        m_clerk = clerk;
    }

    void setMailbox(Mailbox mailbox)
    {
        m_mailbox = mailbox;
    }

    void setProcedureSet(LoadedProcedureSet loadedProcs)
    {
        m_loadedProcs = loadedProcs;
    }

    public SiteTaskerQueue getQueue()
    {
        return m_tasks;
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

        // HACK: grab the current sitetracker until we write leader notices.
        m_clerk = VoltDB.instance().getSiteTracker();

        // Handle every-site system procedures (at the MPI)
        // Let runner be null for tests that mock setup and don't load sysprocs.
        if (runner != null && runner.isSystemProcedure()) {
            SystemProcedureCatalog.Config cfg =
                SystemProcedureCatalog.listing.get(procedureName);
            if (cfg.everySite) {
                // Send an SP initiate task to all remote sites
                // Process the local message immediately.
                final Long localId = m_mailbox.getHSId();
                final long mpTxnId = m_txnId.incrementAndGet();
                Iv2InitiateTaskMessage sp = new Iv2InitiateTaskMessage(
                        localId, // make the MPI the initiator.
                        message.getCoordinatorHSId(),
                        mpTxnId,
                        message.isReadOnly(),
                        true, // isSinglePartition
                        message.getStoredProcedureInvocation(),
                        message.getClientInterfaceHandle());
                DuplicateCounter counter = new DuplicateCounter(
                        message.getInitiatorHSId(),
                        m_clerk.getHSIdsForPartitionInitiators().size(),
                        mpTxnId);
                m_duplicateCounters.put(mpTxnId, counter);
                for (Long hsid : m_clerk.getHSIdsForPartitionInitiators()) {
                    if (!hsid.equals(localId)) {
                        try {
                            System.out.println("Sending ESP to: " + hsid);
                            m_mailbox.send(hsid, sp);
                        } catch (MessagingException e) {
                            VoltDB.crashLocalVoltDB("Failed to serialize initiation for " +
                                    procedureName, true, e);
                        }
                    }
                    else {
                        System.out.println("Sending ESP to myself: " + localId);
                        handleIv2InitiateTaskMessage(sp);
                    }
                }
                return;
            }
        }

        // Multi-partition initiation (at the MPI)
        final List<Long> partitionInitiators = m_clerk.getHSIdsForPartitionInitiators();
        final MpProcedureTask task =
            new MpProcedureTask(m_mailbox, m_loadedProcs.getProcByName(procedureName),
                    m_txnId.incrementAndGet(), m_pendingTasks, message, partitionInitiators);
        m_outstandingTxns.put(task.m_txn.txnId, task.m_txn);
        m_pendingTasks.offer(task);
    }

    public void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        if (message.getTxnId() != Iv2InitiateTaskMessage.UNUSED_MP_TXNID) {
            DuplicateCounter counter = m_duplicateCounters.get(message.getTxnId());
            if (counter != null) {
                int result = counter.offer(message);
                if (result == DuplicateCounter.DONE) {
                    m_duplicateCounters.remove(message.getTxnId());
                    try {
                        m_mailbox.send(counter.m_destinationId, message);
                    } catch (MessagingException e) {
                        VoltDB.crashLocalVoltDB("Failed to send every-site response.", true, e);
                    }
                }
                else if (result == DuplicateCounter.MISMATCH) {
                    VoltDB.crashLocalVoltDB("HASH MISMATCH running every-site system procedure.", true, null);
                }
                // doing duplicate suppresion: all done.
                return;
            }
        }

        try {
            // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
            m_mailbox.send(message.getInitiatorHSId(), message);
        }
        catch (MessagingException e) {
            hostLog.error("Failed to deliver response from execution site.", e);
        }
    }


    public void handleFragmentTaskMessage(FragmentTaskMessage message)
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
                                        m_pendingTasks, message);
            m_pendingTasks.offer(task);
        }
        else {
            final FragmentTask task =
                new FragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                 m_pendingTasks, message);
            m_pendingTasks.offer(task);
        }
    }

    public void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        TransactionState txn = m_outstandingTxns.get(message.getTxnId());
        // XXX IZZY This feels like papering over something...come back and
        // be smarter later
        // We could already have received the CompleteTransactionMessage from
        // the local site and the transaction is dead, despite FragmentResponses
        // in flight from remote sites.  Drop those on the floor.
        if (txn != null)
        {
            ((MpTransactionState)txn).offerReceivedFragmentResponse(message);
        }
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        TransactionState txn = m_outstandingTxns.remove(message.getTxnId());
        // XXX IZZY This feels like papering over something...come back and
        // be smarter later
        if (txn != null)
        {
            final CompleteTransactionTask task =
                new CompleteTransactionTask(txn, m_pendingTasks, message);
            m_pendingTasks.offer(task);
        }
    }
}
