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

package org.voltdb.messages;

import org.voltdb.utils.DBBPool;

public class Heartbeat extends TransactionInfoBaseMessage {

    long m_lastRecievedTxnID; // this is the largest txn acked by all partitions running the java for it

    public Heartbeat() {
        super();
    }

    public Heartbeat(int initiatorSiteId, long txnId) {
        super(initiatorSiteId, -1, txnId, true);
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

        m_buffer.putLong(m_lastRecievedTxnID);

        m_buffer.limit(m_buffer.position());

    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();

        m_lastRecievedTxnID = m_buffer.getLong();
    }

}
