/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltcore.utils.CoreUtils;

import java.nio.ByteBuffer;

public class FaultDecisionMessage extends VoltMessage {

    private long m_failedSite;
    private boolean m_failed;

    // For serialization
    FaultDecisionMessage() {}

    public FaultDecisionMessage(long failedSite, boolean failed) {
        m_failedSite = failedSite;
        m_failed = failed;
    }

    public long getFailedSite() {
        return m_failedSite;
    }

    public boolean isFailed() {
        return m_failed;
    }

    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize() +
                   8 + // hsId of the host that is kicked out
                   1;  // is the given hsId failed
        return size;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(VoltMessageFactory.SITE_FAULT_DECISION_ID);
        buf.putLong(m_failedSite);
        buf.put((byte) (m_failed ? 1 : 0));
        buf.limit(buf.position());
        assert(buf.capacity() == buf.position());
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) {
        m_failedSite = buf.getLong();
        m_failed = buf.get() == 1;
    }

    @Override
    public byte getSubject() {
        return Subject.FAULT_DECISION.getId();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FaultDecisionMessage {failed: ");
        sb.append(CoreUtils.hsIdToString(m_failedSite));
        sb.append(", reporting: ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(", failed: ");
        sb.append(m_failed);
        return sb.toString();
    }
}
