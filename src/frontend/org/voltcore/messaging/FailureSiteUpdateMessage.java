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


package org.voltcore.messaging;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class FailureSiteUpdateMessage extends VoltMessage {

    /** Site id of the messaging-issuing site. */
    public long m_sourceHSId;

    /** Site id of the failed sites */
    public HashSet<Long> m_failedHSIds = new HashSet<Long>();

    /** Safe txn id is for this specific initiator **/
    public long m_initiatorForSafeTxnId;

    /** Greatest 2PC transaction id at source m_sourceSiteId seen from failed initiator */
    public long m_safeTxnId;

    /** Greatest committed transaction at m_sourceSiteId */
    public long m_committedTxnId;

    public FailureSiteUpdateMessage(
            long sourceHSId,
            HashSet<Long> failedHSIds,
            long initiatorForSafeTxnId,
            long safeTxnId,
            long committedTxnId)
    {
        m_sourceHSId = sourceHSId;
        m_failedHSIds = new HashSet<Long>(failedHSIds);
        m_initiatorForSafeTxnId = initiatorForSafeTxnId;
        m_safeTxnId = safeTxnId;
        m_committedTxnId = committedTxnId;
    }


    /**
     * For VoltMessage factory.
     */
    FailureSiteUpdateMessage() {
    }

    @Override
    public int getSerializedSize() {
        int msgsize = 4 + 4 * 8 + (8 * m_failedHSIds.size()); // 3 ints, 2 longs, 1 byte, 4 byte failed host count + 4 bytes per failed host
        msgsize += super.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(FAILURE_SITE_UPDATE_ID);
        buf.putLong(m_sourceHSId);
        buf.putInt(m_failedHSIds.size());
        for (Long hostId : m_failedHSIds) {
            buf.putLong(hostId);
        }
        buf.putLong(m_initiatorForSafeTxnId);
        buf.putLong(m_safeTxnId);
        buf.putLong(m_committedTxnId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        m_sourceHSId = buf.getLong();
        int numIds = buf.getInt();
        for (int ii = 0; ii < numIds; ii++) {
            m_failedHSIds.add(buf.getLong());
        }
        m_initiatorForSafeTxnId = buf.getLong();
        m_safeTxnId = buf.getLong();
        m_committedTxnId = buf.getLong();

        assert(buf.capacity() == buf.position());
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FAILURE_SITE_UPDATE ");
        sb.append(" from site: HOST ");
        sb.append((int)m_sourceHSId).append(" SITE ").append((int)(m_sourceHSId >> 32));
        sb.append(" for failed hosts: ");
        for (Long hsId : m_failedHSIds) {
            sb.append(hsId.intValue()).append(':').append((hsId.intValue() >> 32)).append(' ');
        }
        sb.append(" initiator for safe txn:").
        append((int)m_initiatorForSafeTxnId).append(':').append((int)(m_initiatorForSafeTxnId >> 32));
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
