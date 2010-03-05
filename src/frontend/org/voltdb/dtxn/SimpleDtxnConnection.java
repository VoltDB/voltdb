/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.dtxn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.ExecutionSite;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.debugstate.ExecutorContext;
import org.voltdb.messages.DebugMessage;
import org.voltdb.messages.FragmentResponse;
import org.voltdb.messages.FragmentTask;
import org.voltdb.messages.InitiateTask;
import org.voltdb.messages.MembershipNotice;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.messaging.impl.SiteMailbox;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;

public class SimpleDtxnConnection extends SiteConnection {

    /**
     *  This is thrown by the TransactionState instance when something
     *  goes wrong mid-fragment, and execution needs to back all the way
     *  out to the stored procedure call.
     */
    public static class FragmentFailureException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    // Messaging stuff
    private Mailbox m_mailbox;

    public ExecutionSite m_site;

    // Two data structures storing all active and known transactions
    HashMap<Long, TransactionState> m_transactionsById = new HashMap<Long, TransactionState>();
    RestrictedPriorityQueue m_transactionQueue = null;
    private static final Logger hostLog = Logger.getLogger(SimpleDtxnConnection.class.getName(), VoltLoggerFactory.instance());

    // Transaction currently being processed.
    private TransactionState m_current = null;
    private long m_lastCompletedTxnId = Long.MIN_VALUE;

    /**
     * @param site The ExecutionSite attached to this DTXN Connection
     * @param mbox Mailbox used to deliver messages to this site.
     */
    public SimpleDtxnConnection(ExecutionSite site, Mailbox mbox, int[] initiatorSiteIds) {
        super(site.siteId);
        m_site = site;
        m_mailbox = mbox;
        m_transactionQueue = new RestrictedPriorityQueue(initiatorSiteIds, m_siteId);
    }

    @Override
    public void createAllParticipatingWork(FragmentTask task) {
        assert(task != null);
        assert(m_current != null);

        m_current.createAllParticipatingFragmentWork(task);
    }

    @Override
    public void createLocalWork(FragmentTask task, boolean nonTransactional) {
        assert(task != null);
        assert(m_current != null);

        m_current.createLocalFragmentWork(task, nonTransactional);
    }

    @Override
    public boolean createWork(int[] partitions, FragmentTask task) {
        assert(task != null);
        assert(partitions != null);
        assert(m_current != null);

        m_current.createFragmentWork(partitions, task);

        return false;
    }

    @Override
    public void setupProcedureResume(boolean isFinal, int... dependencies) {
        assert(m_current != null);
        assert(dependencies != null);

        assert(m_current instanceof MultiPartitionParticipantTxnState);

        m_current.setupProcedureResume(isFinal, dependencies);
    }

    private void messageArrived(InitiateTask request) {
        assert (request.getInitiatorSiteId() != getSiteId());
        processTransactionMembership(request);

        assert(request.isSinglePartition() || (request.getNonCoordinatorSites() != null));
    }

    private void messageArrived(FragmentTask request) {
        assert (request.getInitiatorSiteId() != getSiteId());
        TransactionState state = processTransactionMembership(request);
        if (state == null)
            return;
        assert(state instanceof MultiPartitionParticipantTxnState);

        state.createLocalFragmentWork(request, false);
    }

    /**
     * A decision arrived for the current transaction
     */
    private void messageArrived(FragmentResponse response) {
        // the following two outs are for rollback
        // if the response comes in after the transaction is done
        //  then the reponse should be ignored
        // this makes me (john) nervous, but it seems better than
        //  trying to keep track of outstanding responses in an
        //  rollback situation.
        if ((m_current == null) || (response.getTxnId() != m_current.txnId)) {
            System.out.printf("Site %d got an old response\n", m_siteId);
            return;
        }

        assert (m_current instanceof MultiPartitionParticipantTxnState);

        m_current.processRemoteWorkResponse(response);
    }

    /**
     * When a message arrives that tells this site that it is part of a
     * multi-partition transaction, create a TxnState object for it. If
     * a transaction state message exists, return it.
     *
     * @param notice The message that contains transaction information.
     * @return The SimpleTxnState object created or found.
     */
    private TransactionState processTransactionMembership(MembershipNotice notice) {
        // handle out of order messages
        if (notice.getTxnId() <= m_lastCompletedTxnId) {
            // because of our rollback implementation, fragment
            // tasks can come in late and it's not a problem
            if (notice instanceof FragmentTask) {
                //System.out.printf("Site %d got an old notice\n", m_siteId);
                return null;
            }

            // vanilla membership notices and initiate tasks are
            // not allowed to come in out of order
            StringBuilder msg = new StringBuilder();
            msg.append("Txn ordering deadlock (DTXN) at site ").append(m_siteId).append(":\n");
            msg.append("   txn ").append(m_lastCompletedTxnId).append(" (");
            msg.append(TransactionIdManager.toString(m_lastCompletedTxnId)).append(" HB: ?");
            msg.append(") before\n");
            msg.append("   txn ").append(notice.getTxnId()).append(" (");
            msg.append(TransactionIdManager.toString(notice.getTxnId())).append(" HB:");
            msg.append(notice.isHeartBeat()).append(").\n");
            throw new RuntimeException(msg.toString());
        }

        m_transactionQueue.gotTransaction(notice.getInitiatorSiteId(), notice.getTxnId(), notice.isHeartBeat());

        // ignore heartbeats
        if (notice.isHeartBeat())
            return null;

        if ((m_current != null) && (m_current.txnId == notice.getTxnId()))
            return m_current;

        TransactionState state = m_transactionsById.get(notice.getTxnId());
        if (state == null) {
            if (notice instanceof InitiateTask) {
                InitiateTask it = (InitiateTask) notice;
                if (it.isSinglePartition())
                    state = new SinglePartitionTxnState(m_mailbox, this, m_site, it);
                else
                    state = new MultiPartitionParticipantTxnState(m_mailbox, this, m_site, it);

            }
            else {
                state = new MultiPartitionParticipantTxnState(m_mailbox, this, m_site, notice);
            }

            m_transactionQueue.add(state);
            m_transactionsById.put(state.txnId, state);
        }
        return state;
    }

    @Override
    public void shutdown() throws InterruptedException {
        requestDebugMessage(false);
        m_transactionQueue.shutdown();
    }

    /**
     * Put a message in the queue so that when the message is pulled out, it
     * will trigger an atomic boolean that will instruct the getNextWorkUnit()
     * to return a special workunit which will cause the ExecutionSite to dump
     * its state in a threadsafe way. I'm sorry for this logic...
     */
    public void requestDebugMessage(boolean shouldDump) {
        DebugMessage dmsg = new DebugMessage();
        dmsg.shouldDump = shouldDump;
        try {
            m_mailbox.send(m_siteId, 0, dmsg);
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }

    public void getDumpContents(ExecutorContext context) {
        // get the messaging log
        if (m_mailbox instanceof SiteMailbox)
            context.mailboxHistory = ((SiteMailbox) m_mailbox).getHistory();

        // get the current transaction's state (if any)
        if (m_current != null)
            context.activeTransaction = m_current.getDumpContents();

        // get the stuff inside the restricted priority queue
        m_transactionQueue.getDumpContents(context);
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(boolean shutdownAllowed) {

        // loop until the system decided to stop this thread
        // but don't return with null if not the base stack frame for this method
        while ((!shutdownAllowed) || (m_site.m_shouldContinue)) {

            //////////////////////////////////////////////////////
            // DO ALL READY-TO-RUN WORK WITHOUT MORE MESSAGES
            //////////////////////////////////////////////////////

            // repeat until:
            // 1. there is no current transaction AND there is no ready transaction
            // or:
            // 2. the current transaction is blocked (see end of loop)

            boolean moreWork = (m_current != null) ||
                               ((m_current = m_transactionQueue.poll()) != null);

            while (moreWork)
            {
                // assume there's a current txn inside this loop
                assert(m_current != null);

                // do all runnable work for the current txn
                if (m_current.doWork())
                // if doWork returns true, the transaction is over
                {
                    m_site.completeTransaction(m_current.isReadOnly);
                    TransactionState ts = m_transactionsById.remove(m_current.txnId);
                    assert(ts != null);
                    m_lastCompletedTxnId = m_current.txnId;

                    // try to get the next current in line
                    // continue if there's a ready txn
                    moreWork = (m_current = m_transactionQueue.poll()) != null;
                }
                else {
                    // the current transaction is blocked
                    moreWork = false;

                    // check if we should drop down a stack frame
                    //  note, calling this resets the internal state so
                    //  subsequent calls won't return true
                    if (m_current.shouldResumeProcedure()) {
                        Map<Integer, List<VoltTable>> retval = m_current.getPreviousStackFrameDropDependendencies();
                        assert(retval != null);
                        assert(shutdownAllowed == false);
                        return retval;
                    }
                }
            }

            //////////////////////////////////////////////////////
            // PULL ONE MESSAGE OFF OF QUEUE FROM MESSAGING LAYER
            //////////////////////////////////////////////////////
            VoltMessage message = m_mailbox.recvBlocking(5);
            m_site.tick();
            if (message == null)
                continue;

            //////////////////////////////////////////////////////
            // UPDATE THE STATE OF THE SET OF KNOWN TRANSACTIONS
            //////////////////////////////////////////////////////

            if (message instanceof InitiateTask) {
                messageArrived((InitiateTask) message);
            }
            else if (message instanceof FragmentTask) {
                messageArrived((FragmentTask) message);
            }
            else if (message instanceof FragmentResponse) {
                messageArrived((FragmentResponse) message);
            }
            else if (message instanceof MembershipNotice) {
                processTransactionMembership((MembershipNotice) message);
            }
            else if (message instanceof DebugMessage) {
                DebugMessage dmsg = (DebugMessage) message;
                if (dmsg.shouldDump)
                    DumpManager.putDump(m_site.m_dumpId, m_site.m_currentDumpTimestamp, true, m_site.getDumpContents());
            }
            else {
                hostLog.l7dlog(Level.FATAL, LogKeys.org_voltdb_dtxn_SimpleDtxnConnection_UnkownMessageClass.name(), new Object[] { message.getClass().getName() }, null);
                VoltDB.crashVoltDB();
            }
        }

        assert(shutdownAllowed);
        return null;
    }

    public void tryFetchNewWork() {

        //////////////////////////////////////////////////////
        // PULL ONE MESSAGE OFF OF QUEUE FROM MESSAGING LAYER
        //////////////////////////////////////////////////////
        VoltMessage message = m_mailbox.recv();
        m_site.tick();
        if (message == null)
            return;

        //////////////////////////////////////////////////////
        // UPDATE THE STATE OF THE SET OF KNOWN TRANSACTIONS
        //////////////////////////////////////////////////////

        if (message instanceof InitiateTask) {
            messageArrived((InitiateTask) message);
        }
        else if (message instanceof FragmentTask) {
            messageArrived((FragmentTask) message);
        }
        else if (message instanceof FragmentResponse) {
            messageArrived((FragmentResponse) message);
        }
        else if (message instanceof MembershipNotice) {
            processTransactionMembership((MembershipNotice) message);
        }
        else if (message instanceof DebugMessage) {
            DebugMessage dmsg = (DebugMessage) message;
            if (dmsg.shouldDump)
                DumpManager.putDump(m_site.m_dumpId, m_site.m_currentDumpTimestamp, true, m_site.getDumpContents());
        }
        else {
            hostLog.l7dlog(Level.FATAL, LogKeys.org_voltdb_dtxn_SimpleDtxnConnection_UnkownMessageClass.name(), new Object[] { message.getClass().getName() }, null);
            VoltDB.crashVoltDB();
        }
    }

    /**
     * Try to execute a single partition procedure if one is available in the
     * priority queue.
     *
     * @return true if a procedure was executed, false if none available
     */
    boolean tryToSneakInASinglePartitionProcedure() {
        // collect up to one message from the network
        tryFetchNewWork();

        TransactionState nextTxn = m_transactionQueue.peek();

        // nothing is ready to go
        if (nextTxn == null)
            return false;

        // only sneak in single partition work
        if (nextTxn instanceof SinglePartitionTxnState) {
            nextTxn = m_transactionQueue.peek();
            boolean success = nextTxn.doWork();
            assert(success);
            return true;
        }

        // multipartition is next
        return false;
    }

    @Override
    public int getNextDependencyId() {
        return m_current.getNextDependencyId();
    }
}
