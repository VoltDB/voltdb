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

public class HeartbeatMessage extends TransactionInfoBaseMessage {

    long m_lastSafeTxnId; // this is the largest txn acked by all partitions running the java for it

    HeartbeatMessage() {
        super();
    }

    public HeartbeatMessage(int initiatorSiteId, long txnId, long lastSafeTxnId) {
        super(initiatorSiteId, -1, txnId, true);
        m_lastSafeTxnId = lastSafeTxnId;
    }

    public long getLastSafeTxnId() {
        return m_lastSafeTxnId;
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) {
        int msgsize = super.getMessageByteCount();
        msgsize += 8;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(HEARTBEAT_ID);

        super.writeToBuffer();

        m_buffer.putLong(m_lastSafeTxnId);

        m_buffer.limit(m_buffer.position());

    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();

        m_lastSafeTxnId = m_buffer.getLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("HEARTBEAT (FROM ");
        sb.append(m_initiatorSiteId);
        sb.append(" TO ");
        sb.append(receivedFromSiteId);
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append(" and LAST SAFE ");
        sb.append(m_lastSafeTxnId);

        return sb.toString();
    }
}
