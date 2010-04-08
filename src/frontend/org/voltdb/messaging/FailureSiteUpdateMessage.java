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

public class FailureSiteUpdateMessage extends VoltMessage {

    /** Site id of the messaging-issuing site. */
    public int m_sourceSiteId;

    /** Host id of the failed host */
    public int m_failedHostId;

    /** Initiator id corresponding to the m_safeTxnId. */
    public int m_failedInitiatorId;

    /** Greatest 2PC transaction id at source m_sourceSiteId seen from failed initiator */
    public long m_safeTxnId;

    /** Greatest committed transaction at m_sourceSiteId */
    public long m_committedTxnId;

    public FailureSiteUpdateMessage(
            int sourceSiteId,
            int failedHostId,
            int failedInitiatorId,
            long safeTxnId,
            long committedTxnId)
    {
        m_sourceSiteId = sourceSiteId;
        m_failedHostId = failedHostId;
        m_failedInitiatorId = failedInitiatorId;
        m_safeTxnId = safeTxnId;
        m_committedTxnId = committedTxnId;
    }


    /**
     * For VoltMessage factory.
     */
    FailureSiteUpdateMessage() {
    }


    @Override
    protected void flattenToBuffer(DBBPool pool) {
        int msgsize = 3 * 4 + 2 * 8 + 1; // 3 ints, 2 longs, 1 byte
        if (m_buffer == null) {
            m_container = pool.acquire(HEADER_SIZE + msgsize);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize, pool);
        m_buffer.position(HEADER_SIZE);
        m_buffer.put(FAILURE_SITE_UPDATE_ID);
        m_buffer.putInt(m_sourceSiteId);
        m_buffer.putInt(m_failedHostId);
        m_buffer.putInt(m_failedInitiatorId);
        m_buffer.putLong(m_safeTxnId);
        m_buffer.putLong(m_committedTxnId);
    }

    @Override
    protected void initFromBuffer() {
        // ignore header and message id.
        m_buffer.position(HEADER_SIZE + 1);
        m_sourceSiteId = m_buffer.getInt();
        m_failedHostId = m_buffer.getInt();
        m_failedInitiatorId = m_buffer.getInt();
        m_safeTxnId = m_buffer.getLong();
        m_committedTxnId = m_buffer.getLong();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FAILURE_SITE_UPDATE ");
        sb.append(" from site: ");
        sb.append(m_sourceSiteId);
        sb.append(" for initiator: ");
        sb.append(m_failedInitiatorId);
        sb.append(" for failed host: ");
        sb.append(m_failedHostId);
        sb.append(" safe txn: ");
        sb.append(m_safeTxnId);
        sb.append(" committed txn: ");
        sb.append(m_committedTxnId);
        return sb.toString();
    }

    @Override
    public byte getSubject() {
        return Subject.FAILURE_SITE_UPDATE.getId();
    }
}
