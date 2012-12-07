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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.TreeMap;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

/**
 * Orders work for command log replay - where fragment tasks can
 * show up before or after the partition-wise sentinels that record
 * the correct location of a multi-partition work in the partition's
 * transaction sequence.
 *
 * Offer a message to the replay sequencer. If the sequencer rejects
 * this message, it is already correctly sequenced. Callers must
 * check the return code of <code>offer</code>. If offering makes
 * other messages available, they must be retrieved by calling poll()
 * until it returns null.
 *
 * NOTE: messages are sequenced according to the transactionId passed
 * in to the offer() method. This transaction id may differ from the
 * value stored in the ReplayEntry.m_firstFragment in the case of
 * DR fragment tasks. The ReplaySequencer MUST do all txnId comparisons
 * on the value passed to offer (which becomes a key in m_replayEntries
 * tree map).
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
            if (m_eolReached) {
                // End of log, no more sentinels, release first fragment
                return m_firstFragment != null;
            } else {
                return m_sentinalTxnId != null && m_firstFragment != null;
            }
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
               if(!m_servedFragment) {
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

        boolean isEmpty() {
            return isReady() && m_servedFragment && m_blockedMessages.isEmpty();
        }
    }

    // queued entries hashed by transaction id.
    TreeMap<Long, ReplayEntry> m_replayEntries = new TreeMap<Long, ReplayEntry>();

    // lastPolledFragmentTxnId tracks released MP transactions; new fragments
    // for released transactions do not need further sequencing.
    long m_lastPolledFragmentTxnId = Long.MIN_VALUE;

    // has reached end of log for this partition, release any MP Txns for
    // replay if this is true.
    boolean m_eolReached = false;

    public void setEOLReached()
    {
        m_eolReached = true;
    }

    // Return the next correctly sequenced message or null if none exists.
    public VoltMessage poll()
    {
        if (m_replayEntries.isEmpty()) {
            return null;
        }
        if (m_replayEntries.firstEntry().getValue().isEmpty()) {
            m_replayEntries.pollFirstEntry();
        }
        if (m_replayEntries.isEmpty()) {
            return null;
        }
        VoltMessage m = m_replayEntries.firstEntry().getValue().poll();
        if (m instanceof FragmentTaskMessage) {
            m_lastPolledFragmentTxnId = m_replayEntries.firstEntry().getKey();
        }
        return m;
    }

    // Offer a new message. Return false if the offered message can be run immediately.
    public boolean offer(long inTxnId, TransactionInfoBaseMessage in)
    {
        ReplayEntry found = m_replayEntries.get(inTxnId);

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
         */
        if (m_eolReached && found == null) {
            return false;
        }

        if (in instanceof MultiPartitionParticipantMessage) {
            // Incoming sentinel.
            // MultiPartitionParticipantMessage mppm = (MultiPartitionParticipantMessage)in;
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
            if (found != null) {
                found.addBlockedMessage(in);
            }
            else {
                // Always expect to see the fragment first, but there are places in the protocol
                // where CompleteTransactionMessages may arrive for transactions that this site hasn't
                // done/won't do, so just tell the caller that we can't do anything with it and hope
                // the right thing happens.
                return false;
            }

        }
        else {
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
