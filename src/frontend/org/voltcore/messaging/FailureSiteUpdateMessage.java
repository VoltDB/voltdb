/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.util.Map;

import org.voltcore.utils.CoreUtils;

import com.google.common.collect.ImmutableMap;

public class FailureSiteUpdateMessage extends VoltMessage {

    /** Site id of the reported failed sites */
    public Map<Long,Boolean> m_failedHSIds = ImmutableMap.of();

    /** Site id of the reported failed site **/
    public long m_failedHSId;

    /** Greatest 2PC transaction id at source m_sourceSiteId seen from failed initiator */
    public long m_safeTxnId;

    /** Greatest committed transaction at m_sourceSiteId */
    public long m_committedTxnId;

    public FailureSiteUpdateMessage(
            Map<Long,Boolean> failedHSIds,
            long failedHSId,
            long safeTxnId,
            long committedTxnId)
    {
        m_failedHSIds = ImmutableMap.copyOf(failedHSIds);
        m_failedHSId = failedHSId;
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
            ((8 + 1) * m_failedHSIds.size()); // one long + 1 byte per failed host
        msgsize += super.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        flattenToBuffer(buf, VoltMessageFactory.FAILURE_SITE_UPDATE_ID);
        buf.limit(buf.position());
        assert(buf.capacity() == buf.position());
    }

    protected void flattenToBuffer(ByteBuffer buf, byte msgId) {
        buf.put(msgId);
        buf.putInt(m_failedHSIds.size());
        for (Map.Entry<Long, Boolean> entry : m_failedHSIds.entrySet()) {
            buf.putLong(entry.getKey());
            buf.put(entry.getValue() ? (byte)1 : (byte)0);
        }
        buf.putLong(m_failedHSId);
        buf.putLong(m_safeTxnId);
        buf.putLong(m_committedTxnId);
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        int numIds = buf.getInt();
        ImmutableMap.Builder<Long, Boolean> builder = ImmutableMap.builder();
        for (int ii = 0; ii < numIds; ii++) {
            builder.put(buf.getLong(), buf.get() == (byte)0 ? false : true);
        }
        m_failedHSIds = builder.build();
        m_failedHSId = buf.getLong();
        m_safeTxnId = buf.getLong();
        m_committedTxnId = buf.getLong();

        assert(m_subject != Subject.FAILURE_SITE_UPDATE.getId()
            || buf.capacity() == buf.position());
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Subject.values()[getSubject()]).append(" ");
        sb.append(" from site: HOST ");
        sb.append((int)m_sourceHSId).append(" SITE ").append((int)(m_sourceHSId >> 32));
        sb.append(" for failed hosts: ");
        for (Long hsId : m_failedHSIds.keySet()) {
            sb.append(CoreUtils.hsIdToString(hsId)).append(' ');
        }
        sb.append(" failed site id:").
        append((int)m_failedHSId).append(':').append((int)(m_failedHSId >> 32));
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
