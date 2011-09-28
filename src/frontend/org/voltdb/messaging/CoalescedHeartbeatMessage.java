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

import java.io.IOException;

import org.voltdb.utils.DBBPool;

public class CoalescedHeartbeatMessage extends TransactionInfoBaseMessage {

    private final int m_destinationSiteIds[];
    private final long m_lastSafeTxnIds[];

    public CoalescedHeartbeatMessage() {
        m_destinationSiteIds = null;
        m_lastSafeTxnIds = null;
    }

    /*
     * Used when deserializing to construct the messages to deliver locally
     */
    private HeartbeatMessage m_messages[];
    private int m_messageDestinations[];

    public CoalescedHeartbeatMessage(int initiatorSiteId, long txnId, int destinationSiteIds[], long lastSafeTxnIds[]) {
        super(initiatorSiteId, -1, txnId, true);
        m_destinationSiteIds = destinationSiteIds;
        m_lastSafeTxnIds = lastSafeTxnIds;
    }

    public HeartbeatMessage[] getHeartbeatsToDeliver() {
        return m_messages;
    }

    public int[] getHeartbeatDestinations() {
        return m_messageDestinations;
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();

        final int numHeartbeats = m_buffer.get();
        m_messages = new HeartbeatMessage[numHeartbeats];
        m_messageDestinations = new int[numHeartbeats];
        for (int ii = 0; ii < numHeartbeats; ii++) {
            m_messageDestinations[ii] = m_buffer.getInt();
            m_messages[ii] = new HeartbeatMessage(super.m_initiatorSiteId, super.m_txnId, m_buffer.getLong());
        }
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) throws IOException {
        int msgsize = super.getMessageByteCount();
        msgsize += (12 * m_destinationSiteIds.length) + 1;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(COALESCED_HEARTBEAT_ID);

        super.writeToBuffer();

        m_buffer.put((byte)m_destinationSiteIds.length);

        for (int ii = 0; ii < m_destinationSiteIds.length; ii++) {
            m_buffer.putInt(m_destinationSiteIds[ii]);
            m_buffer.putLong(m_lastSafeTxnIds[ii]);
        }

        m_buffer.limit(m_buffer.position());
    }

}
