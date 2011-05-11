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

import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.DBBPool;

public class HeartbeatResponseMessage extends VoltMessage {

    int m_execSiteId;
    long m_lastReceivedTxnId; // this is the largest txn seen by the exec site from the destination init site
    boolean m_siteIsBlocked;

    HeartbeatResponseMessage() {
        super();
        m_lastReceivedTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID; // -1
        m_execSiteId = -1;
        m_siteIsBlocked = false;
    }

    public HeartbeatResponseMessage(int execSiteId, long lastSeenTxnFromInitiator, boolean siteIsBlocked) {
        m_execSiteId = execSiteId;
        m_lastReceivedTxnId = lastSeenTxnFromInitiator;
        m_siteIsBlocked = siteIsBlocked;
    }

    public int getExecSiteId() {
        return m_execSiteId;
    }

    public long getLastReceivedTxnId() {
        return m_lastReceivedTxnId;
    }

    public boolean isBlocked() {
        return m_siteIsBlocked;
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) {
        int msgsize = 4 + 8 + 1;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(HEARTBEAT_RESPONSE_ID);

        m_buffer.putInt(m_execSiteId);
        m_buffer.putLong(m_lastReceivedTxnId);
        m_buffer.put((byte) (m_siteIsBlocked ? 1 : 0));

        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id

        m_execSiteId = m_buffer.getInt();
        m_lastReceivedTxnId = m_buffer.getLong();
        m_siteIsBlocked = (m_buffer.get() == 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("HEARTBEAT_RESPONSE");
        sb.append(" (EXEC ").append(m_execSiteId);
        sb.append(", LAST_REC ").append(m_lastReceivedTxnId);
        sb.append(", BLOCKED ").append(m_siteIsBlocked ? "YES" : "NO");
        sb.append(")");

        return sb.toString();
    }
}
