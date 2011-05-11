/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.messaging;

import org.voltdb.utils.DBBPool;

public class MultiPartitionParticipantMessage extends TransactionInfoBaseMessage {

    MultiPartitionParticipantMessage() {
        super();
    }

    public MultiPartitionParticipantMessage(int initiatorSiteId,
                                           int coordinatorSiteId,
                                           long txnId,
                                           boolean isReadOnly) {
        super(initiatorSiteId, coordinatorSiteId, txnId, isReadOnly);
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) {
        int msgsize = super.getMessageByteCount();

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(PARTICIPANT_NOTICE_ID);

        super.writeToBuffer();

        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("MULTI_PARTITION_PARTICIPANT (FROM ");
        sb.append(m_coordinatorSiteId);
        sb.append(" TO ");
        sb.append(receivedFromSiteId);
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        return sb.toString();
    }
}
