/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
package org.voltdb.importclient.kafka;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importclient.kafka.BaseKafkaTopicPartitionImporter.PendingWorkTracker;
import org.voltdb.importer.CommitTracker;

//Per topic per partition that we are responsible for.
//Callback for each invocation we have submitted.
public class TopicPartitionInvocationCallback implements ProcedureCallback
{
    private final long m_nextoffset;
    private final long m_offset;
    private final PendingWorkTracker m_callbackTracker;
    private final CommitTracker m_tracker;
    private final AtomicBoolean m_dontCommit;
    private final AtomicLong m_pauseOffset;

    public TopicPartitionInvocationCallback(
            final long curoffset,
            final long nextoffset,
            final PendingWorkTracker callbackTracker,
            final CommitTracker tracker,
            final AtomicBoolean dontCommit,
            final AtomicLong pauseOffset) {
        m_offset = curoffset;
        m_nextoffset = nextoffset;
        m_callbackTracker = callbackTracker;
        m_tracker = tracker;
        m_dontCommit = dontCommit;
        m_pauseOffset = pauseOffset;
    }

    private static class PausedOffsetCalculator implements LongBinaryOperator {
        @Override
        public long applyAsLong(long currentValue, long givenUpdate) {
            if (currentValue == -1){
                return givenUpdate;
            } else {
                return Math.min(currentValue, givenUpdate);
            }
        }
    }

    @Override
    public void clientCallback(ClientResponse response) throws Exception {

        m_callbackTracker.consumeWork();
        if (!m_dontCommit.get() && response.getStatus() != ClientResponse.SERVER_UNAVAILABLE) {
            m_tracker.commit(m_nextoffset);
        }
        if (response.getStatus() == ClientResponse.SERVER_UNAVAILABLE) {
            m_pauseOffset.accumulateAndGet(m_offset, new PausedOffsetCalculator());
        }
    }

    public long getOffset() {
        return m_offset;
    }
}
