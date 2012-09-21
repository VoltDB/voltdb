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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.CommandLog;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

public class SpScheduler extends Scheduler
{
    List<Long> m_replicaHSIds = new ArrayList<Long>();
    List<Long> m_sendToHSIds = new ArrayList<Long>();
    private final Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private final Map<Long, DuplicateCounter> m_duplicateCounters =
        new HashMap<Long, DuplicateCounter>();
    private CommandLog m_cl;

    // the current not-needed-any-more point of the repair log.
    long m_repairLogTruncationHandle = Long.MIN_VALUE;

    SpScheduler(int partitionId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, taskQueue);
    }

    @Override
    public void shutdown()
    {
        // nothing to do for SP shutdown.
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
        // Update the list of remote replicas that we'll need to send to
        m_sendToHSIds = new ArrayList<Long>(m_replicaHSIds);
        m_sendToHSIds.remove(m_mailbox.getHSId());

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
        } else if (message instanceof MultiPartitionParticipantMessage) {
            handleMultipartSentinel((MultiPartitionParticipantMessage)message);
        }
        else {
            throw new RuntimeException("UNKNOWN MESSAGE TYPE, BOOM!");
        }
    }

    /*
     * Use the sentinel to block the pending tasks queue until the multipart arrives
     * and forward it to replicas so that their queues are blocked as well.
     */
    private void handleMultipartSentinel(
            MultiPartitionParticipantMessage message) {
        m_pendingTasks.offerMPSentinel(message.getTxnId());
        if (m_sendToHSIds.size() > 0) {
            m_mailbox.send(com.google.common.primitives.Longs.toArray(m_sendToHSIds),
                    message);
        }
    }

    // SpScheduler expects to see InitiateTaskMessages corresponding to single-partition
    // procedures only.
    public void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        final String procedureName = message.getStoredProcedureName();
        if (message.isSinglePartition()) {
            long newSpHandle;
            long timestamp;
            Iv2InitiateTaskMessage msg = message;
            if (m_isLeader) {

                /*
                 * If this is CL replay use the txnid from the CL and also
                 * update the txnid to match the one from the CL
                 */
                if (message.isForReplay()) {
                    newSpHandle = message.getTxnId();
                    timestamp = message.getTimestamp();
                    setMaxSeenTxnId(newSpHandle);
                } else {
                    TxnEgo ego = advanceTxnEgo();
                    newSpHandle = ego.getTxnId();
                    timestamp = ego.getWallClock();
                }

                // Need to set the SP handle on the received message
                // Need to copy this or the other local sites handling
                // the same initiate task message will overwrite each
                // other's memory -- the message isn't copied on delivery
                // to other local mailboxes.
                msg = new Iv2InitiateTaskMessage(
                        message.getInitiatorHSId(),
                        message.getCoordinatorHSId(),
                        m_repairLogTruncationHandle,
                        message.getTxnId(),
                        message.getTimestamp(),
                        message.isReadOnly(),
                        message.isSinglePartition(),
                        message.getStoredProcedureInvocation(),
                        message.getClientInterfaceHandle(),
                        message.getConnectionId(),
                        message.isForReplay());

                // advanceTxnEgo();
                // newSpHandle = currentTxnEgoSequence();

                msg.setSpHandle(newSpHandle);
                // Also, if this is a vanilla single-part procedure, make the TXNID
                // be the SpHandle (for now)
                // Only system procedures are every-site, so we'll check through the SystemProcedureCatalog
                if (SystemProcedureCatalog.listing.get(procedureName) == null ||
                    !SystemProcedureCatalog.listing.get(procedureName).getEverysite()) {
                    msg.setTxnId(newSpHandle);
                    msg.setTimestamp(timestamp);
                }
                if (m_sendToHSIds.size() > 0) {
                    Iv2InitiateTaskMessage replmsg =
                        new Iv2InitiateTaskMessage(m_mailbox.getHSId(),
                                m_mailbox.getHSId(),
                                m_repairLogTruncationHandle,
                                msg.getTxnId(),
                                msg.getTimestamp(),
                                msg.isReadOnly(),
                                msg.isSinglePartition(),
                                msg.getStoredProcedureInvocation(),
                                msg.getClientInterfaceHandle(),
                                msg.getConnectionId(),
                                msg.isForReplay());
                    // Update the handle in the copy since the constructor doesn't set it
                    replmsg.setSpHandle(newSpHandle);
                    m_mailbox.send(com.google.common.primitives.Longs.toArray(m_sendToHSIds),
                            replmsg);
                    DuplicateCounter counter = new DuplicateCounter(
                            msg.getInitiatorHSId(),
                            msg.getTxnId(), m_replicaHSIds);
                    m_duplicateCounters.put(newSpHandle, counter);
                }
            }
            else {
                setMaxSeenTxnId(msg.getSpHandle());
                newSpHandle = msg.getSpHandle();
                timestamp = msg.getTimestamp();
            }
            if (!msg.isReadOnly()) {
                m_cl.log(msg, newSpHandle);
            }
            Iv2Trace.logIv2InitiateTaskMessage(message, m_mailbox.getHSId(), msg.getTxnId(), newSpHandle);
            final SpProcedureTask task =
                new SpProcedureTask(m_mailbox, procedureName, m_pendingTasks, msg);
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

            final SpProcedureTask task = new SpProcedureTask(m_mailbox, procedureName, m_pendingTasks, localWork);
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
        final long spHandle = message.getSpHandle();
        DuplicateCounter counter = m_duplicateCounters.get(spHandle);
        if (counter != null) {
            int result = counter.offer(message);
            if (result == DuplicateCounter.DONE) {
                m_duplicateCounters.remove(spHandle);
                m_repairLogTruncationHandle = spHandle;
                m_mailbox.send(counter.m_destinationId, message);
            }
            else if (result == DuplicateCounter.MISMATCH) {
                VoltDB.crashLocalVoltDB("HASH MISMATCH: replicas produced different results.", true, null);
            }
            // doing duplicate suppresion: all done.
            return;
        }

        // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
        m_repairLogTruncationHandle = spHandle;
        m_mailbox.send(message.getInitiatorHSId(), message);
    }

    // BorrowTaskMessages encapsulate a FragmentTaskMessage along with
    // input dependency tables. The MPI issues borrows to a local site
    // to perform replicated reads or aggregation fragment work.
    private void handleBorrowTaskMessage(BorrowTaskMessage message) {
        // borrows do not advance the sp handle. The handle would
        // move backwards anyway once the next message is received
        // from the SP leader.
        long newSpHandle = getCurrentTxnId();
        Iv2Trace.logFragmentTaskMessage(message.getFragmentTaskMessage(),
                m_mailbox.getHSId(), newSpHandle, true);
        TransactionState txn = m_outstandingTxns.get(message.getTxnId());

        if (txn == null) {
            // If the borrow is the first fragment for a transaction, run it as
            // a single partition fragment; Must not  engage/pause this
            // site on a MP transaction before the SP instructs to do so.
            // Do not track the borrow task as outstanding - it completes
            // immediately and is not a valid transaction state for
            // full MP participation (it claims everything can run as SP).
            txn = new BorrowTransactionState(newSpHandle, message);
        }


        if (message.getFragmentTaskMessage().isSysProcTask()) {
            final SysprocFragmentTask task =
                new SysprocFragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                        m_pendingTasks, message.getFragmentTaskMessage(),
                                        message.getInputDepMap());
            m_pendingTasks.offer(task);
        }
        else {
            final FragmentTask task =
                new FragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                        m_pendingTasks, message.getFragmentTaskMessage(),
                        message.getInputDepMap());
            m_pendingTasks.offer(task);
        }
    }

    // SpSchedulers will see FragmentTaskMessage for:
    // - The scatter fragment(s) of a multi-part transaction (normal or sysproc)
    // - Borrow tasks to do the local fragment work if this partition is the
    //   buddy of the MPI.  Borrow tasks may include input dependency tables for
    //   aggregation fragments, or not, if it's a replicated table read.
    // For multi-batch MP transactions, we'll need to look up the transaction state
    // that gets created when the first batch arrives.
    // During command log replay a new SP handle is going to be generated, but it really
    // doesn't matter, it isn't going to be used for anything.
    void handleFragmentTaskMessage(FragmentTaskMessage message)
    {
        FragmentTaskMessage msg = message;
        long newSpHandle;
        if (m_isLeader) {
            // Quick hack to make progress...we need to copy the FragmentTaskMessage
            // before we start mucking with its state (SPHANDLE).  We need to revisit
            // all the messaging mess at some point.
            msg = new FragmentTaskMessage(message.getInitiatorHSId(),
                    message.getCoordinatorHSId(), message);
            //Not going to use the timestamp from the new Ego because the multi-part timestamp is what should be used
            TxnEgo ego = advanceTxnEgo();
            newSpHandle = ego.getTxnId();
            msg.setSpHandle(newSpHandle);
            if (msg.getInitiateTask() != null) {
                msg.getInitiateTask().setSpHandle(newSpHandle);//set the handle
                msg.setInitiateTask(msg.getInitiateTask());//Trigger reserialization so the new handle is used
            }

            /*
             * If there a replicas to send it to, forward it!
             * Unless... it's read only AND not a sysproc. Read only sysprocs may expect to be sent
             * everywhere.
             * In that case don't propagate it to avoid a determinism check and extra messaging overhead
             */
            if (m_sendToHSIds.size() > 0 && (!msg.isReadOnly() || msg.isSysProcTask())) {
                FragmentTaskMessage replmsg =
                    new FragmentTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(), msg);
                m_mailbox.send(com.google.common.primitives.Longs.toArray(m_sendToHSIds),
                        replmsg);
                DuplicateCounter counter;
                if (message.getFragmentTaskType() != FragmentTaskMessage.SYS_PROC_PER_SITE) {
                    counter = new DuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(), m_replicaHSIds);
                }
                else {
                    counter = new SysProcDuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(), m_replicaHSIds);
                }
                m_duplicateCounters.put(newSpHandle, counter);
            }
        }
        else {
            newSpHandle = msg.getSpHandle();
            setMaxSeenTxnId(newSpHandle);
        }
        if (msg.getInitiateTask() != null && !msg.getInitiateTask().isReadOnly()) {
            m_cl.log(msg.getInitiateTask(), newSpHandle);
        }
        TransactionState txn = m_outstandingTxns.get(msg.getTxnId());
        Iv2Trace.logFragmentTaskMessage(message, m_mailbox.getHSId(), newSpHandle, false);
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
                                        m_pendingTasks, msg, null);
            m_pendingTasks.offer(task);
        }
        else {
            final FragmentTask task =
                new FragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                 m_pendingTasks, msg, null);
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
        if (m_sendToHSIds.size() > 0) {
            CompleteTransactionMessage replmsg = message;
            m_mailbox.send(com.google.common.primitives.Longs.toArray(m_sendToHSIds),
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

    @Override
    public void setCommandLog(CommandLog cl) {
        m_cl = cl;
    }
}
