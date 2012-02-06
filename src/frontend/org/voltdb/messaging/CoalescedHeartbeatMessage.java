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
package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltDbMessageFactory;

public class CoalescedHeartbeatMessage extends TransactionInfoBaseMessage {

    private final long m_destinationSiteIds[];
    private final long m_lastSafeTxnIds[];

    public CoalescedHeartbeatMessage() {
        m_destinationSiteIds = null;
        m_lastSafeTxnIds = null;
    }

    /*
     * Used when deserializing to construct the messages to deliver locally
     */
    private HeartbeatMessage m_messages[];
    private long m_messageDestinations[];

    public CoalescedHeartbeatMessage(long initiatorSiteId, long txnId,
                                     long destinationSiteIds[],
                                     long lastSafeTxnIds[])
    {
        super(initiatorSiteId, -1, txnId, true);
        m_destinationSiteIds = destinationSiteIds;
        m_lastSafeTxnIds = lastSafeTxnIds;
    }

    public HeartbeatMessage[] getHeartbeatsToDeliver() {
        return m_messages;
    }

    public long[] getHeartbeatDestinations() {
        return m_messageDestinations;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 1 + // storage of count of heartbeat messages
            m_destinationSiteIds.length * (8 + 8);  // Two longs per coalesced message
        return msgsize;
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        super.initFromBuffer(buf);
        final int numHeartbeats = buf.get();
        m_messages = new HeartbeatMessage[numHeartbeats];
        m_messageDestinations = new long[numHeartbeats];
        for (int ii = 0; ii < numHeartbeats; ii++) {
            m_messageDestinations[ii] = buf.getLong();
            m_messages[ii] = new HeartbeatMessage(super.m_initiatorHSId,
                                                  super.m_txnId,
                                                  buf.getLong());
        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.COALESCED_HEARTBEAT_ID);
        super.flattenToBuffer(buf);
        buf.put((byte)m_destinationSiteIds.length);
        for (int ii = 0; ii < m_destinationSiteIds.length; ii++) {
            buf.putLong(m_destinationSiteIds[ii]);
            buf.putLong(m_lastSafeTxnIds[ii]);
        }
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

}
