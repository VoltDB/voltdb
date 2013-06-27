package org.voltcore.messaging;

import java.nio.ByteBuffer;

import org.voltcore.utils.CoreUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SiteFailureForwardMessage extends SiteFailureMessage {
    public long m_reportingHSId;

    public SiteFailureForwardMessage(SiteFailureMessage sfm) {
        m_reportingHSId = sfm.m_sourceHSId;
        m_survivors = ImmutableSet.copyOf(sfm.m_survivors);
        m_safeTxnIds = ImmutableMap.copyOf(sfm.m_safeTxnIds);
    }

    SiteFailureForwardMessage() {
    }

    @Override
    public int getSerializedSize() {
        return super.getSerializedSize() + 8; // + 1 long for reportingHSId;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        super.flattenToBuffer(buf, VoltMessageFactory.SITE_FAILURE_FORWARD_ID);
        buf.putLong(m_reportingHSId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        super.initFromBuffer(buf);
        m_reportingHSId = buf.getLong();
        assert(m_subject != Subject.SITE_FAILURE_FORWARD.getId()
                || buf.capacity() == buf.position());

    }

    @Override
    public String toString() {
        return super.toString()
                   + " reporting site: "
                   + CoreUtils.hsIdToString(m_reportingHSId);
    }

    @Override
    public byte getSubject() {
        return Subject.SITE_FAILURE_FORWARD.getId();
    }
}
