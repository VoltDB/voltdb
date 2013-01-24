/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.TreeMap;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

/**
 * Orders work for command log replay - where fragment tasks can show up before
 * or after the partition-wise sentinels that record the correct location of a
 * multi-partition work in the partition's transaction sequence.
 *
 * Offer a message to the replay sequencer. If the sequencer rejects this
 * message, it is already correctly sequenced. Callers must check the return
 * code of <code>offer</code>. If offering makes other messages available, they
 * must be retrieved by calling poll() until it returns null.
 *
 * End of log handling: If the local partition reaches end of log first, all MPs
 * blocked waiting for sentinels will be made safe, future MP fragments will
 * also be safe automatically. If the MPI reaches end of log first, and there is
 * an outstanding sentinel in the sequencer, then all SPs blocked after this
 * sentinel will be made safe (can be polled). There cannot be any fragments in
 * the replay sequencer when the MPI EOL arrives, because the MPI will only send
 * EOLs when it has finished all previous MP work. NOTE: Once MPI end of log
 * message is received, NONE of the SPs polled from the sequencer can be
 * executed, the poller must make sure that a failure response is returned
 * appropriately instead. However, SPs rejected by offer() can always be
 * executed.
 *
 * NOTE: messages are sequenced according to the transactionId passed in to the
 * offer() method. This transaction id may differ from the value stored in the
 * ReplayEntry.m_firstFragment in the case of DR fragment tasks. The
 * ReplaySequencer MUST do all txnId comparisons on the value passed to offer
 * (which becomes a key in m_replayEntries tree map).
 *
 * Drainable: When poll() should make no more progress, we need to switch to drain().
 * These conditions are only applicable to command log replay and not DR.
 * - If we're blocked on a sentinel with no matching fragment and we've seen the MP EOL condition,
 *   then we know that we're never going to be able to order anything later than that position in
 *   the log, and we need to drain any outstanding invocations so we can respond IGNORING for them
 *   to complete command log replay.
 * - If we're blocked on a fragment with no matching sentinel and we've seen the SP EOL condition,
 *   then we know that we're never going to be able to order anything later than that position in
 *   the log.  This is currently defect ENG-4218.  We will need to do drain, plus we'll
 *   probably want to respond to any outstanding FragmentTasks with an error response of some kind to abort
 *   those transactions.
 */
public class ReplaySequencer
{
    // place holder that associates sentinel, first fragment and
    // work that follows in the transaction sequence.
    private class ReplayEntry {
        Long m_sentinalTxnId = null;
        FragmentTaskMessage m_firstFragment = null;

        private Deque<VoltMessage> m_blockedMessages = new ArrayDeque<VoltMessage>();
        private boolean m_servedFragment = false;

        boolean isReady()
        {
            // ENG-4218 fix makes this condition go away
            if (m_eolReached) {
                return m_firstFragment != null;
            }
            return m_sentinalTxnId != null && m_firstFragment != null;
        }

        boolean hasSentinel()
        {
            return m_sentinalTxnId != null;
        }

        void addBlockedMessage(VoltMessage m)
        {
            m_blockedMessages.addLast(m);
        }

        VoltMessage poll()
        {
            if (isReady()) {
               if(!m_servedFragment && m_firstFragment != null) {
                   m_servedFragment = true;
                   return m_firstFragment;
               }
               else {
                   return m_blockedMessages.poll();
               }
            }
            else {
                return null;
            }
        }

        VoltMessage drain()
        {
            if(!m_servedFragment && m_firstFragment != null) {
                m_servedFragment = true;
                return m_firstFragment;
            }
            else {
                return m_blockedMessages.poll();
            }
        }

        boolean isEmpty() {
            return isReady() && m_servedFragment && m_blockedMessages.isEmpty();
        }
    }

    // queued entries hashed by transaction id.
    TreeMap<Long, ReplayEntry> m_replayEntries = new TreeMap<Long, ReplayEntry>();

    // lastPolledFragmentTxnId tracks released MP transactions; new fragments
    // for released transactions do not need further sequencing.
    long m_lastPolledFragmentTxnId = Long.MIN_VALUE;

    // lastSeenTxnId tracks the last seen txnId for this partition
    long m_lastSeenTxnId = Long.MIN_VALUE;

    // has reached end of log for this partition, release any MP Txns for
    // replay if this is true.
    boolean m_eolReached = false;
    // has reached end of log for the MPI, no more MP fragments will come,
    // release all txns.
    boolean m_mpiEOLReached = false;
    // some combination of conditions has occurred which will result in no
    // further sequence-able transactions.  All remaining invocations in the
    // sequencer must be removed using drain()
    boolean m_mustDrain = false;

    /**
     * Dedupe initiate task messages. Check if the initiate task message is seen before.
     *
     * @param inTxnId The txnId of the message
     * @param in The initiate task message
     * @return A client response to return if it's a duplicate, otherwise null.
     */
    public InitiateResponseMessage dedupe(long inTxnId, TransactionInfoBaseMessage in)
    {
        if (in instanceof Iv2InitiateTaskMessage) {
            final Iv2InitiateTaskMessage init = (Iv2InitiateTaskMessage) in;
            final StoredProcedureInvocation invocation = init.getStoredProcedureInvocation();
            final String procName = invocation.getProcName();

            /*
             * Ning - @LoadSinglepartTable and @LoadMultipartTable always have the same txnId
             * which is the txnId of the snapshot.
             */
            if (!(procName.equalsIgnoreCase("@LoadSinglepartitionTable") ||
                    procName.equalsIgnoreCase("@LoadMultipartitionTable")) &&
                    inTxnId <= m_lastSeenTxnId) {
                // already sequenced
                final InitiateResponseMessage resp = new InitiateResponseMessage(init);
                resp.setResults(new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        ClientResponseImpl.DUPE_TRANSACTION));
                return resp;
            }
        }
        return null;
    }

    /**
     * Update the last seen txnId for this partition if it's an initiate task message.
     *
     * @param inTxnId
     * @param in
     */
    public void updateLastSeenTxnId(long inTxnId, TransactionInfoBaseMessage in)
    {
        if (in instanceof Iv2InitiateTaskMessage && inTxnId > m_lastSeenTxnId) {
            m_lastSeenTxnId = inTxnId;
        }
    }

    /**
     * Update the last polled txnId for this partition if it's a fragment task message.
     * @param inTxnId
     * @param in
     */
    public void updateLastPolledTxnId(long inTxnId, TransactionInfoBaseMessage in)
    {
        if (in instanceof FragmentTaskMessage) {
            m_lastPolledFragmentTxnId = inTxnId;
        }
    }

    // Return the next correctly sequenced message or null if none exists.
    public VoltMessage poll()
    {
        if (m_mustDrain || m_replayEntries.isEmpty()) {
            return null;
        }
        if (m_replayEntries.firstEntry().getValue().isEmpty()) {
            m_replayEntries.pollFirstEntry();
        }
        // All the drain conditions depend on being blocked, which
        // we will only really know for sure when we try to poll().
        checkDrainCondition();
        if (m_mustDrain || m_replayEntries.isEmpty()) {
            return null;
        }

        VoltMessage m = m_replayEntries.firstEntry().getValue().poll();
        updateLastPolledTxnId(m_replayEntries.firstEntry().getKey(), (TransactionInfoBaseMessage) m);
        return m;
    }

    // Pull the next message that needs an IGNORING response.  Once this
    // starts returning messages, poll() will always return null
    public VoltMessage drain()
    {
        if (!m_mustDrain || m_replayEntries.isEmpty()) {
            return null;
        }
        VoltMessage head = m_replayEntries.firstEntry().getValue().drain();
        while (head == null) {
            m_replayEntries.pollFirstEntry();
            if (!m_replayEntries.isEmpty()) {
                // This will end up null if the next ReplayEntry was just a sentinel.
                // We'll keep going.
                head = m_replayEntries.firstEntry().getValue().drain();
            }
            else {
                break;
            }
        }
        return head;
    }

    private void checkDrainCondition()
    {
        // Don't ever go backwards once the drain decision is made.
        if (m_mustDrain) {
            return;
        }
        // if we've got things to sequence, check to if we're blocked
        if (!m_replayEntries.isEmpty()) {
            ReplayEntry head = m_replayEntries.firstEntry().getValue();
            if (!head.isReady()) {
                // if we're blocked, see if we have a sentinel or a fragment.
                // we know we have one or the other but not both.  Neither
                // means we wouldn't exist, and both would make us ready.
                // if it's the sentinel, see if the MPI's command log is done
                if (head.hasSentinel() && m_mpiEOLReached) {
                    m_mustDrain = true;
                }
                else if (!head.hasSentinel() && m_eolReached) {
                    // We have a fragment and will never get the sentinel
                    // ENG-4218 will fill this in at some point
                }
            }
        }
    }

    // Offer a new message. Return false if the offered message can be run immediately.
    public boolean offer(long inTxnId, TransactionInfoBaseMessage in)
    {
        ReplayEntry found = m_replayEntries.get(inTxnId);

        if (in instanceof Iv2EndOfLogMessage) {
            if (((Iv2EndOfLogMessage) in).isMP()) {
                m_mpiEOLReached = true;
            } else {
                m_eolReached = true;
            }
            return true;
        }

        /*
         * End-of-log reached. Only FragmentTaskMessage and
         * CompleteTransactionMessage can arrive at this partition once EOL is
         * reached.
         *
         * If the txn is found, meaning that found is not null, then this might
         * be the first fragment, it needs to get through in order to free any
         * txns queued behind it.
         *
         * If the txn is not found, then there will be no matching sentinel to
         * come later, and there will be no SP txns after this MP, so release
         * the first fragment immediately.
         *
         * ENG-4218 will want to change this to queue an MP fragment which we
         * can't sequence properly so that we can drain() it appropriately later
         */
        if (m_eolReached && found == null) {
            return false;
        }

        if (in instanceof MultiPartitionParticipantMessage) {
            /*
             * DR sends multiple @LoadMultipartitionTable proc calls with the
             * same txnId, which is the snapshot txnId. For each partition,
             * there is a sentinel paired with the @LoadMultipartitionTable
             * call. Dedupe the sentinels the same way as we dedupe fragments,
             * so that there won't be sentinels end up in the sequencer where
             * matching fragments are deduped.
             */
            if (inTxnId <= m_lastPolledFragmentTxnId) {
                return true;
            }

            if (found == null) {
                ReplayEntry newEntry = new ReplayEntry();
                newEntry.m_sentinalTxnId = inTxnId;
                m_replayEntries.put(inTxnId, newEntry);
            }
            else {
                found.m_sentinalTxnId = inTxnId;
                assert(found.isReady());
            }
        }
        else if (in instanceof FragmentTaskMessage) {
            // already sequenced
            if (inTxnId <= m_lastPolledFragmentTxnId) {
                return false;
            }

            FragmentTaskMessage ftm  = (FragmentTaskMessage)in;
            if (found == null) {
                ReplayEntry newEntry = new ReplayEntry();
                newEntry.m_firstFragment = ftm;
                m_replayEntries.put(inTxnId, newEntry);
            }
            else if (found.m_firstFragment == null) {
                found.m_firstFragment = ftm;
                assert(found.isReady());
            }
            else {
                found.addBlockedMessage(ftm);
            }
        }
        else if (in instanceof CompleteTransactionMessage) {
            // already sequenced
            if (inTxnId <= m_lastPolledFragmentTxnId) {
                return false;
            }
            if (found != null && found.m_firstFragment != null) {
                found.addBlockedMessage(in);
            }
            else {
                // Always expect to see the fragment first, but there are places in the protocol
                // where CompleteTransactionMessages may arrive for transactions that this site hasn't
                // done/won't do, e.g. txn restart, so just tell the caller that we can't do
                // anything with it and hope the right thing happens.
                return false;
            }

        }
        else {
            if (dedupe(inTxnId, in) != null) {
                // Ignore an already seen txn
                return true;
            }
            updateLastSeenTxnId(inTxnId, in);

            if (m_replayEntries.isEmpty() || !m_replayEntries.lastEntry().getValue().hasSentinel()) {
                // not-blocked work; rejected and not queued.
                return false;
            }
            else {
                // blocked work queues with the newest replayEntry
                m_replayEntries.lastEntry().getValue().addBlockedMessage(in);
            }
        }
        return true;
    }
}
