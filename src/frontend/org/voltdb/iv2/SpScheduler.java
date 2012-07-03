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

import java.util.ArrayList;
import java.util.Collections;

import java.util.concurrent.atomic.AtomicLong;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;

import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.ProcedureRunner;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class SpScheduler extends Scheduler
{
    List<Long> m_replicaHSIds = new ArrayList<Long>();
    private Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private Map<Long, DuplicateCounter> m_duplicateCounters =
        new HashMap<Long, DuplicateCounter>();
    private AtomicLong m_txnId = new AtomicLong(0);

    // the current not-needed-any-more point of the repair log.
    long m_repairLogTruncationHandle = Long.MIN_VALUE;

    SpScheduler(SiteTaskerQueue taskQueue)
    {
        super(taskQueue);
    }

    // This is going to run in the BabySitter's thread.  This and deliver are synchronized by
    // virtue of both being called on InitiatorMailbox and not directly called.
    // (That is, InitiatorMailbox's API, used by BabySitter, is synchronized on the same
    // lock deliver() is synchronized on.)
    @Override
    public void updateReplicas(List<Long> replicas)
    {
        // First - correct the official replica set.
        m_replicaHSIds = replicas;

        // Cleanup duplicate counters and collect DONE counters
        // in this list for further processing.
        List<Long> doneCounters = new LinkedList<Long>();
        for (DuplicateCounter counter : m_duplicateCounters.values()) {
            int result = counter.updateReplicas(m_replicaHSIds);
            if (result == DuplicateCounter.DONE) {
                doneCounters.add(counter.getTxnId());
            }
        }

        // Maintain the CI invariant that responses arrive in txnid order.
        Collections.sort(doneCounters);
        for (Long txnId : doneCounters) {
            DuplicateCounter counter = m_duplicateCounters.remove(txnId);
            VoltMessage resp = counter.getLastResponse();
            if (resp != null) {
                m_mailbox.send(counter.m_destinationId, resp);
            }
            else {
                hostLog.warn("TXN " + counter.getTxnId() + " lost all replicas and " +
                        "had no responses.  This should be impossible?");
            }
        }
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
            handleFragmentTaskMessage((FragmentTaskMessage)message, null);
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
            long newSpHandle;
            Iv2InitiateTaskMessage msg = message;
            if (m_isLeader) {
                // Need to set the SP handle on the received message
                // Need to copy this or the other local sites handling
                // the same initiate task message will overwrite each
                // other's memory -- the message isn't copied on delivery
                // to other local mailboxes.
                msg = new Iv2InitiateTaskMessage(message.getInitiatorHSId(),
                        message.getCoordinatorHSId(),
                        m_repairLogTruncationHandle,
                        message.getTxnId(),
                        message.isReadOnly(),
                        message.isSinglePartition(),
                        message.getStoredProcedureInvocation(),
                        message.getClientInterfaceHandle());
                newSpHandle = m_txnId.incrementAndGet();
                msg.setSpHandle(newSpHandle);
                // Also, if this is a vanilla single-part procedure, make the TXNID
                // be the SpHandle (for now)
                if (!runner.isEverySite()) {
                    msg.setTxnId(newSpHandle);
                }
                if (m_replicaHSIds.size() > 0) {
                    Iv2InitiateTaskMessage replmsg =
                        new Iv2InitiateTaskMessage(m_mailbox.getHSId(),
                                m_mailbox.getHSId(),
                                m_repairLogTruncationHandle,
                                msg.getTxnId(),
                                msg.isReadOnly(),
                                msg.isSinglePartition(),
                                msg.getStoredProcedureInvocation(),
                                msg.getClientInterfaceHandle());
                    // Update the handle in the copy
                    replmsg.setSpHandle(newSpHandle);
                    m_mailbox.send(com.google.common.primitives.Longs.toArray(m_replicaHSIds),
                            replmsg);
                    List<Long> expectedHSIds = new ArrayList<Long>(m_replicaHSIds);
                    expectedHSIds.add(m_mailbox.getHSId());
                    DuplicateCounter counter = new DuplicateCounter(
                            msg.getInitiatorHSId(),
                            msg.getTxnId(), expectedHSIds);
                    m_duplicateCounters.put(newSpHandle, counter);
                }
            }
            else {
                newSpHandle = msg.getSpHandle();
                // FUTURE: update SP handle state on replicas based on value from primary
            }
            final SpProcedureTask task =
                new SpProcedureTask(m_mailbox, runner,
                        newSpHandle, m_pendingTasks, msg);
            m_pendingTasks.offer(task);
            return;
        }
        else {
            throw new RuntimeException("SpScheduler.handleIv2InitiateTaskMessage " +
                    "should never receive multi-partition initiations.");
        }
    }

    @Override
    public void handleIv2InitiateTaskMessageRepair(List<Long> needsRepair, Iv2InitiateTaskMessage message) {
        final String procedureName = message.getStoredProcedureName();
        final ProcedureRunner runner = m_loadedProcs.getProcByName(procedureName);
        if (!message.isSinglePartition()) {
            throw new RuntimeException("SpScheduler.handleIv2InitiateTaskMessageRepair " +
                    "should never receive multi-partition initiations.");
        }

        // set up duplicate counter. expect exactly the responses corresponding
        // to needsRepair. These may, or may not, include the local site.

        // We currently send the final response into the ether, since we don't
        // have the original ClientInterface HSID stored.  It would be more
        // useful to have the original ClienInterface HSId somewhere handy.

        List<Long> expectedHSIds = new ArrayList<Long>(needsRepair);
        DuplicateCounter counter = new DuplicateCounter(
                HostMessenger.VALHALLA,
                message.getTxnId(), expectedHSIds);
        m_duplicateCounters.put(message.getSpHandle(), counter);

        // is local repair necessary?
        if (needsRepair.contains(m_mailbox.getHSId())) {
            needsRepair.remove(m_mailbox.getHSId());
            // make a copy because handleIv2 non-repair case does?
            Iv2InitiateTaskMessage localWork =
                new Iv2InitiateTaskMessage(message.getInitiatorHSId(),
                    message.getCoordinatorHSId(), message);

            final SpProcedureTask task = new SpProcedureTask(m_mailbox, runner,
                    localWork.getSpHandle(), m_pendingTasks, localWork);
            m_pendingTasks.offer(task);
        }

        // is remote repair necessary?
        if (!needsRepair.isEmpty()) {
            Iv2InitiateTaskMessage replmsg =
                new Iv2InitiateTaskMessage(m_mailbox.getHSId(), m_mailbox.getHSId(), message);
            m_mailbox.send(com.google.common.primitives.Longs.toArray(needsRepair), replmsg);
        }
    }

    // InitiateResponses for single-partition initiators currently get completely handled
    // by SpInitiatorMessageHandler.  This may change when replication is added.
    public void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        // Send the message to the duplicate counter, if any
        DuplicateCounter counter = m_duplicateCounters.get(message.getSpHandle());
        if (counter != null) {
            int result = counter.offer(message);
            if (result == DuplicateCounter.DONE) {
                m_duplicateCounters.remove(message.getSpHandle());
                m_repairLogTruncationHandle = message.getSpHandle();
                m_mailbox.send(counter.m_destinationId, message);
            }
            else if (result == DuplicateCounter.MISMATCH) {
                VoltDB.crashLocalVoltDB("HASH MISMATCH running every-site system procedure.", true, null);
            }
            // doing duplicate suppresion: all done.
            return;
        }

        // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
        m_repairLogTruncationHandle = message.getSpHandle();
        m_mailbox.send(message.getInitiatorHSId(), message);
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
        // See SUCKS comment below
        FragmentTaskMessage msg = message;
        long newSpHandle;
        if (m_isLeader) {
            // Quick hack to make progress...we need to copy the FragmentTaskMessage
            // before we start mucking with its state (SPHANDLE).  We need to revisit
            // all the messaging mess at some point.
            msg = new FragmentTaskMessage(message.getInitiatorHSId(),
                    message.getCoordinatorHSId(), message);
            newSpHandle = m_txnId.incrementAndGet();
            msg.setSpHandle(newSpHandle);
            // If we have input dependencies, it's borrow work, there's no way we
            // can actually distribute it
            if (m_replicaHSIds.size() > 0 && inputDeps == null) {
                FragmentTaskMessage replmsg =
                    new FragmentTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(), msg);
                m_mailbox.send(com.google.common.primitives.Longs.toArray(m_replicaHSIds),
                        replmsg);
                List<Long> expectedHSIds = new ArrayList<Long>(m_replicaHSIds);
                expectedHSIds.add(m_mailbox.getHSId());
                DuplicateCounter counter;
                if (message.getFragmentTaskType() != FragmentTaskMessage.SYS_PROC_PER_SITE) {
                    counter = new DuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(), expectedHSIds);
                }
                else {
                    counter = new SysProcDuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(), expectedHSIds);
                }
                m_duplicateCounters.put(newSpHandle, counter);
            }
        }
        else {
            newSpHandle = msg.getSpHandle();
        }

        TransactionState txn = m_outstandingTxns.get(msg.getTxnId());
        // bit of a hack...we will probably not want to create and
        // offer FragmentTasks for txn ids that don't match if we have
        // something in progress already
        if (txn == null) {
            txn = new ParticipantTransactionState(newSpHandle, msg);
            m_outstandingTxns.put(msg.getTxnId(), txn);
        }

        if (msg.isSysProcTask()) {
            final SysprocFragmentTask task =
                new SysprocFragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                        m_pendingTasks, msg, inputDeps);
            m_pendingTasks.offer(task);
        }
        else {
            final FragmentTask task =
                new FragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                 m_pendingTasks, msg, inputDeps);
            m_pendingTasks.offer(task);
        }
    }

    // Eventually, the master for a partition set will need to be able to dedupe
    // FragmentResponses from its replicas.
    public void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        // Send the message to the duplicate counter, if any
        DuplicateCounter counter = m_duplicateCounters.get(message.getSpHandle());
        if (counter != null) {
            int result = counter.offer(message);
            if (result == DuplicateCounter.DONE) {
                m_duplicateCounters.remove(message.getSpHandle());
                m_repairLogTruncationHandle = message.getSpHandle();
                FragmentResponseMessage resp = (FragmentResponseMessage)counter.getLastResponse();
                // MPI is tracking deps per partition HSID.  We need to make
                // sure we write ours into the message getting sent to the MPI
                resp.setExecutorSiteId(m_mailbox.getHSId());
                m_mailbox.send(counter.m_destinationId, resp);
            }
            else if (result == DuplicateCounter.MISMATCH) {
                VoltDB.crashLocalVoltDB("HASH MISMATCH running every-site system procedure.", true, null);
            }
            // doing duplicate suppresion: all done.
            return;
        }

        m_mailbox.send(message.getDestinationSiteId(), message);
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        if (m_replicaHSIds.size() > 0) {
            CompleteTransactionMessage replmsg = message;
            m_mailbox.send(com.google.common.primitives.Longs.toArray(m_replicaHSIds),
                    replmsg);
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
