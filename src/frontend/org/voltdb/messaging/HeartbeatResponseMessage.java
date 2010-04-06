/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

public class HeartbeatResponseMessage extends VoltMessage {

    long m_lastReceivedTxnId; // this is the largest txn acked by all partitions running the java for it

    HeartbeatResponseMessage() {
        super();
        m_lastReceivedTxnId = -1;
    }

    public HeartbeatResponseMessage(int lastReceivedTxnId) {
        m_lastReceivedTxnId = lastReceivedTxnId;
    }

    public long getLastReceivedTxnId() {
        return m_lastReceivedTxnId;
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) {
        int msgsize = 8;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(HEARTBEAT_RESPONSE_ID);

        m_buffer.putLong(m_lastReceivedTxnId);

        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id

        m_lastReceivedTxnId = m_buffer.getLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("HEARTBEAT_RESPONSE");
        sb.append(" TO ");
        sb.append(receivedFromSiteId);
        sb.append(")");

        return sb.toString();
    }

}
