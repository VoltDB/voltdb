/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.importclient.kafka.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.CommitTracker;

public class ProcedureInvocationCallback implements ProcedureCallback {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    private final long m_nextoffset;
    private final long m_offset;
    private final PendingWorkTracker m_callbackTracker;
    private final CommitTracker m_tracker;
    private final AtomicBoolean m_dontCommit;
    private final AtomicLong m_pauseOffset;
    private final String m_topicIdentifier;
    public ProcedureInvocationCallback(
            final long curoffset,
            final long nextoffset,
            final PendingWorkTracker callbackTracker,
            final CommitTracker tracker,
            final AtomicBoolean dontCommit,
            final AtomicLong pauseOffset,
            final String topicIdentifier) {
        m_offset = curoffset;
        m_nextoffset = nextoffset;
        m_callbackTracker = callbackTracker;
        m_tracker = tracker;
        m_dontCommit = dontCommit;
        m_pauseOffset = pauseOffset;
        m_topicIdentifier = topicIdentifier;
    }

    @Override
    public void clientCallback(ClientResponse response) throws Exception {
        m_callbackTracker.consumeWork();
        if (!m_dontCommit.get() && response.getStatus() != ClientResponse.SERVER_UNAVAILABLE) {
            m_tracker.commit(m_nextoffset);
        }
        if (response.getStatus() == ClientResponse.SERVER_UNAVAILABLE) {
            m_pauseOffset.accumulateAndGet(m_offset, new LongBinaryOperator() {
                @Override
                public long applyAsLong(long currentValue, long givenUpdate) {
                    return currentValue == -1 ? givenUpdate : Math.min(currentValue, givenUpdate);
                }
            });
        }
        if (LOGGER.isDebugEnabled() && response.getStatus() != ClientResponse.SUCCESS) {
            StringBuilder builder = new StringBuilder();
            builder.append("procedure call failure:" + m_topicIdentifier );
            builder.append(" status:" + response.getStatus());
            builder.append(" offset:" + m_offset + " next offset:" + m_nextoffset );
            builder.append(" pause offset:" + m_pauseOffset);
            VoltTable[] vt = response.getResults();
            if (vt != null && vt.length > 0) {
                builder.append(vt[0].toFormattedString());
            }
            LOGGER.debug(builder.toString());
        }
    }

    public long getOffset() {
        return m_offset;
    }
}
