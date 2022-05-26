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

import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;

public class SiteFailureForwardMessage extends SiteFailureMessage {
    public long m_reportingHSId;

    public SiteFailureForwardMessage(SiteFailureMessage sfm) {
        m_reportingHSId = sfm.m_sourceHSId;
        m_survivors = ImmutableSet.copyOf(sfm.m_survivors);
        m_failed = ImmutableSet.copyOf(sfm.m_failed);
        m_safeTxnIds = ImmutableMap.copyOf(sfm.m_safeTxnIds);
    }

    public SiteFailureForwardMessage() {
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
