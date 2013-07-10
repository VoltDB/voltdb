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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.voltcore.utils.CoreUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SiteFailureMessage extends VoltMessage {

    /** Site survivor set */
    public Set<Long> m_survivors = ImmutableSet.of();

    /** Greatest 2PC transaction id at source m_sourceSiteId seen from failed initiator */
    public Map<Long,Long> m_safeTxnIds = ImmutableMap.of();

    /**
     * For VoltMessage factory.
     */
    SiteFailureMessage() {
    }

    protected SiteFailureMessage(
            final Set<Long> survivors,
            final Map<Long,Long> safeTxnIds) {

        m_survivors = survivors;
        m_safeTxnIds = safeTxnIds;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) {
        int srvrcnt = buf.getInt();
        int safecnt = buf.getInt();

        Builder bldr = new Builder();

        for (int i = 0; i < srvrcnt; ++i) {
            bldr.addSurvivor(buf.getLong());
        }
        for (int i = 0; i < safecnt; ++i) {
            bldr.addSafeTxnId(buf.getLong(), buf.getLong());
        }
        bldr.initialize(this);

        assert(m_subject != Subject.SITE_FAILURE_UPDATE.getId()
                || buf.capacity() == buf.position());
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        flattenToBuffer(buf, VoltMessageFactory.SITE_FAILURE_UPDATE_ID);
        buf.limit(buf.position());
        assert(buf.capacity() == buf.position());
    }

    protected void flattenToBuffer(ByteBuffer buf, byte msgId) {
        buf.put(msgId);

        buf.putInt(m_survivors.size());
        buf.putInt(m_safeTxnIds.size());

        for (long h: m_survivors) {
            buf.putLong(h);
        }
        for (Map.Entry<Long, Long> e: m_safeTxnIds.entrySet()) {
            buf.putLong(e.getKey());
            buf.putLong(e.getValue());
        }
    }

    @Override
    public int getSerializedSize() {
        int msgsize =
            4 + // survivor host count int
            4 + // safe transactions ids count
            8 * m_survivors.size() +
            (8 + 8) * m_safeTxnIds.size();
        msgsize += super.getSerializedSize();
        return msgsize;
    }

    @Override
    public byte getSubject() {
        return Subject.SITE_FAILURE_UPDATE.getId();
    }

    public boolean hasDirectlyWitnessed(long hSid) {
        return !m_survivors.contains(hSid);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Subject.values()[getSubject()]);
        sb.append(" from site: ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" survivors: [");
        sb.append(CoreUtils.hsIdCollectionToString(m_survivors));
        sb.append("] safe transactions: [");
        int cnt = 0;
        for (Map.Entry<Long, Long> e: m_safeTxnIds.entrySet()) {
            if (cnt++ > 0) sb.append(", ");
            sb.append(CoreUtils.hsIdToString(e.getKey()));
            sb.append(": ");
            sb.append(e.getValue());
        }
        sb.append("]");
        return sb.toString();
    }

    public static class Builder {
        ImmutableSet.Builder<Long> srvrb = ImmutableSet.builder();
        ImmutableMap.Builder<Long, Long> safeb = ImmutableMap.builder();

        public void addSurvivor(long survivor) {
            srvrb.add(survivor);
        }

        public void addSurvivors(Set<Long> survivors) {
            for (long survivor: survivors) {
                addSurvivor(survivor);
            }
        }

        public void addSafeTxnId(long failedHsid, long safeTxnId) {
            safeb.put(failedHsid,safeTxnId);
        }

        public SiteFailureMessage build() {
            return new SiteFailureMessage(srvrb.build(), safeb.build());
        }

        protected void initialize(SiteFailureMessage m) {
            m.m_survivors = srvrb.build();
            m.m_safeTxnIds = safeb.build();
        }
    }

    static final public Builder builder() {
        return new Builder();
    }
}
