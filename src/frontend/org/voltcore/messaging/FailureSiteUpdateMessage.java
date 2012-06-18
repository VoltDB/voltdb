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
import java.util.Set;

import org.voltcore.utils.CoreUtils;

public class FailureSiteUpdateMessage extends VoltMessage {

    /** Site id of the failed sites */
    public HashSet<Long> m_failedHSIds = new HashSet<Long>();

    /** Safe txn id is for this specific initiator **/
    public long m_initiatorForSafeTxnId;

    /** Greatest 2PC transaction id at source m_sourceSiteId seen from failed initiator */
    public long m_safeTxnId;

    /** Greatest committed transaction at m_sourceSiteId */
    public long m_committedTxnId;

    public FailureSiteUpdateMessage(
            Set<Long> failedHSIds,
            long initiatorForSafeTxnId,
            long safeTxnId,
            long committedTxnId)
    {
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
        int msgsize =
            3 * 8 + // 3 longs (initiatorForSafeTxnId, safeTxnId, committedTxnId)
            4 + // failed host count int
            (8 * m_failedHSIds.size()); // one long per failed host
        msgsize += super.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(VoltMessageFactory.FAILURE_SITE_UPDATE_ID);
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
            sb.append(CoreUtils.hsIdToString(hsId)).append(' ');
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
