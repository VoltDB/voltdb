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

package org.voltcore.messaging;

import java.nio.ByteBuffer;

import org.voltcore.agreement.DtxnConstants;

public class HeartbeatResponseMessage extends VoltMessage {

    long m_execHSId;
    long m_lastReceivedTxnId; // this is the largest txn seen by the exec site from the destination init site
    boolean m_siteIsBlocked;

    HeartbeatResponseMessage() {
        super();
        m_lastReceivedTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID; // -1
        m_execHSId = -1;
        m_siteIsBlocked = false;
    }

    public HeartbeatResponseMessage(long execHSId, long lastSeenTxnFromInitiator, boolean siteIsBlocked) {
        m_execHSId = execHSId;
        m_lastReceivedTxnId = lastSeenTxnFromInitiator;
        m_siteIsBlocked = siteIsBlocked;
    }

    public long getExecHSId() {
        return m_execHSId;
    }

    public long getLastReceivedTxnId() {
        return m_lastReceivedTxnId;
    }

    public boolean isBlocked() {
        return m_siteIsBlocked;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = 8 + 8 + 1 + super.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(VoltMessageFactory.HEARTBEAT_RESPONSE_ID);

        buf.putLong(m_execHSId);
        buf.putLong(m_lastReceivedTxnId);
        buf.put((byte) (m_siteIsBlocked ? 1 : 0));

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        m_execHSId = buf.getLong();
        m_lastReceivedTxnId = buf.getLong();
        m_siteIsBlocked = (buf.get() == 1);
        assert(buf.capacity() == buf.position());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("HEARTBEAT_RESPONSE");
        sb.append(" (EXEC ").append((int)m_execHSId).append(':').append((int)(m_execHSId >> 32));
        sb.append(", LAST_REC ").append(m_lastReceivedTxnId);
        sb.append(", BLOCKED ").append(m_siteIsBlocked ? "YES" : "NO");
        sb.append(")");

        return sb.toString();
    }
}
