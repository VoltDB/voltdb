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

import org.voltdb.debugstate.MailboxHistory.MessageState;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;

/**
 * Message from an initiator to an execution site, informing the
 * site that it may be requested to do work for a multi-partition
 * transaction, and to reserve a slot in its ordered work queue
 * for this transaction.
 *
 */
public class MembershipNotice extends VoltMessage {

    int m_initiatorSiteId;
    int m_coordinatorSiteId;
    long m_txnId;
    boolean m_isReadOnly;
    boolean m_isHeartBeat;

    /** Empty constructor for de-serialization */
    public MembershipNotice() {
        m_subject = Subject.DEFAULT.getId();
    }

    public MembershipNotice(int initiatorSiteId,
                            int coordinatorSiteId,
                            long txnId,
                            boolean isReadOnly) {
        m_initiatorSiteId = initiatorSiteId;
        m_coordinatorSiteId = coordinatorSiteId;
        m_txnId = txnId;
        m_isReadOnly = isReadOnly;
        m_subject = Subject.DEFAULT.getId();
    }

    public void setIsHeartBeat(boolean isHeartBeat) {
        m_isHeartBeat = isHeartBeat;
    }

    public int getInitiatorSiteId() {
        return m_initiatorSiteId;
    }

    public int getCoordinatorSiteId() {
        return m_coordinatorSiteId;
    }

    public boolean isHeartBeat() {
        return m_isHeartBeat;
    }

    public long getTxnId() {
        return m_txnId;
    }

    public boolean isReadOnly() {
        return m_isReadOnly;
    }

    @Override
    protected void flattenToBuffer(final DBBPool pool) {
        int msgsize = 4 + 4 + 8 + 1 + 1;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(MEMBERSHIP_NOTICE_ID);

        m_buffer.putInt(m_initiatorSiteId);
        m_buffer.putInt(m_coordinatorSiteId);
        m_buffer.putLong(m_txnId);
        m_buffer.put(m_isReadOnly ? (byte) 1 : (byte) 0);
        m_buffer.put(m_isHeartBeat ? (byte) 1 : (byte) 0);

        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        m_initiatorSiteId = m_buffer.getInt();
        m_coordinatorSiteId = m_buffer.getInt();
        m_txnId = m_buffer.getLong();
        m_isReadOnly = m_buffer.get() == 1;
        m_isHeartBeat = m_buffer.get() == 1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (m_isHeartBeat)
            sb.append("HEARTBEAT_NOTICE (FROM ");
        else
            sb.append("MEMBERSHIP_NOTICE (FROM ");
        sb.append(m_initiatorSiteId);
        sb.append(" TO ");
        sb.append(receivedFromSiteId);
        sb.append(") FOR TXN ");
        sb.append(m_txnId);


        if (!m_isHeartBeat) {
            sb.append("\n");
            if (m_isReadOnly)
                sb.append("  READ, COORD ");
            else
                sb.append("  WRITE, COORD ");
            sb.append(m_coordinatorSiteId);
        }

        return sb.toString();
    }

    @Override
    public MessageState getDumpContents() {
        MessageState ms = super.getDumpContents();
        ms.fromSiteId = m_initiatorSiteId;
        ms.txnId = m_txnId;
        return ms;
    }
}
