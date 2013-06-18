package org.voltcore.messaging;

import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableMap;

public class FailureSiteForwardMessage extends FailureSiteUpdateMessage {
    public long m_reportingHSId;

    public FailureSiteForwardMessage(FailureSiteUpdateMessage fsum) {
        m_reportingHSId = fsum.m_sourceHSId;
        m_committedTxnId = fsum.m_committedTxnId;
        m_failedHSId = fsum.m_failedHSId;
        m_failedHSIds = ImmutableMap.copyOf(fsum.m_failedHSIds);
        m_safeTxnId = fsum.m_safeTxnId;
    }

    FailureSiteForwardMessage() {
    }

    @Override
    public int getSerializedSize() {
        return super.getSerializedSize() + 8; // + 1 long for reportingHSId;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        super.flattenToBuffer(buf, VoltMessageFactory.FAILURE_SITE_FORWARD_ID);
        buf.putLong(m_reportingHSId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        super.initFromBuffer(buf);
        m_reportingHSId = buf.getLong();
        assert(m_subject != Subject.FAILURE_SITE_FORWARD.getId()
                || buf.capacity() == buf.position());

    }


    @Override
    public String toString() {
        return super.toString()
                   + " reporting site: HOST "
                   + (int)m_reportingHSId + " SITE "
                   + (int)(m_reportingHSId >> 32);
    }

    @Override
    public byte getSubject() {
        return Subject.FAILURE_SITE_FORWARD.getId();
    }
}
