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
 */
public class ReplaySequencer
{
    // place holder that associates sentinel, first fragment and
    // work that follows in the transaction sequence.
    private static class ReplayEntry {
        Long m_sentinalTxnId = null;
        FragmentTaskMessage m_firstFragment = null;

        private Deque<VoltMessage> m_blockedMessages = new ArrayDeque<VoltMessage>();
        private boolean m_servedFragment = false;

        boolean isReady()
        {
            assert (m_sentinalTxnId.equals(m_firstFragment.getTxnId()));
            return m_sentinalTxnId != null && m_firstFragment != null;
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
        return m_replayEntries.firstEntry().getValue().poll();
    }

    // Offer a new message. Return false if the offered message can be run immediately.
    public boolean offer(TransactionInfoBaseMessage in)
    {
        long inTxnId = in.getTxnId();
        ReplayEntry found = m_replayEntries.get(inTxnId);

        if (in instanceof MultiPartitionParticipantMessage) {
            // Incoming sentinel.
            MultiPartitionParticipantMessage mppm = (MultiPartitionParticipantMessage)in;
            if (found == null) {
                ReplayEntry newEntry = new ReplayEntry();
                newEntry.m_sentinalTxnId = inTxnId;
                m_replayEntries.put(inTxnId, newEntry);
            }
            else {
                found.m_sentinalTxnId = inTxnId;
            }
        }
        else if (in instanceof FragmentTaskMessage) {
            // Incoming fragment task
            FragmentTaskMessage ftm  = (FragmentTaskMessage)in;
            if (found == null) {
                ReplayEntry newEntry = new ReplayEntry();
                newEntry.m_firstFragment = ftm;
                m_replayEntries.put(inTxnId, newEntry);
            }
            else if (found.m_firstFragment == null) {
                found.m_firstFragment = ftm;
            }
            else {
                found.addBlockedMessage(ftm);
            }
        }
        else {
            if (m_replayEntries.isEmpty()) {
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
