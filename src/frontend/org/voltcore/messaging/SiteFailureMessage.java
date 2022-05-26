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

import static com.google_voltpatches.common.base.Predicates.in;
import static com.google_voltpatches.common.base.Predicates.not;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;

public class SiteFailureMessage extends VoltMessage {

    /** Site survivor set */
    public Set<Long> m_survivors = ImmutableSet.of();

    /** Greatest 2PC transaction id at source m_sourceSiteId seen from failed initiator */
    public Map<Long,Long> m_safeTxnIds = ImmutableMap.of();

    /** indicates that this is a site decision */
    public Set<Long> m_decision = ImmutableSet.of();

    public Set<Long> m_failed = ImmutableSet.of();

    /**
     * For VoltMessage factory.
     */
    SiteFailureMessage() {
    }

    protected SiteFailureMessage(
            final Set<Long> survivors,
            final Set<Long> decision,
            final Set<Long> failed,
            final Map<Long,Long> safeTxnIds) {

        m_failed = failed;
        m_survivors = survivors;
        m_safeTxnIds = safeTxnIds;
        m_decision = decision;
        m_failed = failed;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) {
        int srvrcnt = buf.getInt();
        int safecnt = buf.getInt();
        int dcncnt  = buf.getInt();
        int fldcnt  = buf.getInt();

        Builder bldr = new Builder();

        for (int i = 0; i < srvrcnt; ++i) {
            bldr.survivor(buf.getLong());
        }
        for (int i = 0; i < safecnt; ++i) {
            bldr.safeTxnId(buf.getLong(), buf.getLong());
        }
        for (int i = 0; i < dcncnt; ++i) {
            bldr.decision(buf.getLong());
        }
        for (int i = 0; i < fldcnt; ++i) {
            bldr.failed(buf.getLong());
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
        buf.putInt(m_decision.size());
        buf.putInt(m_failed.size());

        for (long h: m_survivors) {
            buf.putLong(h);
        }
        for (Map.Entry<Long, Long> e: m_safeTxnIds.entrySet()) {
            buf.putLong(e.getKey());
            buf.putLong(e.getValue());
        }
        for (long d: m_decision) {
            buf.putLong(d);
        }
        for (long d: m_failed) {
            buf.putLong(d);
        }
    }

    public Set<Long> getFailedSites() {
        return m_failed;
    }

    @Override
    public int getSerializedSize() {
        int msgsize =
            4 + // survivor host count int
            4 + // safe transactions ids count
            4 + // decision hosts count
            4 + // failed hosts count
            8 * m_survivors.size() +
            8 * m_decision.size() +
            8 * m_failed.size() +
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

    public List<FaultMessage> asFaultMessages() {
        ImmutableList.Builder<FaultMessage> lb = ImmutableList.builder();
        if (!m_decision.isEmpty()) {
            for (long decided: m_decision) {
                lb.add(new FaultMessage(m_sourceHSId,decided,m_survivors,true));
            }
        } else {
            for (long failed: m_failed) {
                if (hasDirectlyWitnessed(failed)) {
                    lb.add(new FaultMessage(m_sourceHSId, failed, m_survivors));
                }
            }
        }
        return lb.build();
    }

    public Set<Long> getObservedFailedSites() {
        return Sets.filter(m_failed, not(in(m_survivors)));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Subject.values()[getSubject()]);
        if (!m_decision.isEmpty()) {
            sb.append(" final decision: [");
            sb.append(CoreUtils.hsIdCollectionToString(m_decision));
            sb.append("]");
        }
        sb.append(" from site: ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" survivors: [");
        sb.append(CoreUtils.hsIdCollectionToString(m_survivors));
        sb.append("] failed: [");
        sb.append(CoreUtils.hsIdCollectionToString(m_failed));
        sb.append("]");
        return sb.toString();
    }

    public static class Builder {
        ImmutableSet.Builder<Long> srvrb = ImmutableSet.builder();
        ImmutableSet.Builder<Long> dcsnb = ImmutableSet.builder();
        ImmutableSet.Builder<Long> failb = ImmutableSet.builder();
        ImmutableMap.Builder<Long, Long> safeb = ImmutableMap.builder();

        public Builder failures(Set<Long> failedSites) {
            failb.addAll(failedSites);
            return this;
        }

        public Builder failed(long failedSite) {
            failb.add(failedSite);
            return this;
        }

        public Builder decisions(Set<Long> decision) {
            dcsnb.addAll(decision);
            return this;
        }

        public Builder decision(long decisionSite) {
            dcsnb.add(decisionSite);
            return this;
        }

        public Builder survivor(long survivor) {
            srvrb.add(survivor);
            return this;
        }

        public Builder survivors(Set<Long> survivors) {
            for (long survivor: survivors) {
                survivor(survivor);
            }
            return this;
        }

        public Builder safeTxnId(long failedHsid, long safeTxnId) {
            safeb.put(failedHsid,safeTxnId);
            return this;
        }

        public Builder safeTxnIds(Map<Long,Long> safe) {
            safeb.putAll(safe);
            return this;
        }

        public SiteFailureMessage build() {
            return new SiteFailureMessage(srvrb.build(), dcsnb.build(), failb.build(), safeb.build());
        }

        protected void initialize(SiteFailureMessage m) {
            m.m_decision = dcsnb.build();
            m.m_survivors = srvrb.build();
            m.m_safeTxnIds = safeb.build();
            m.m_failed = failb.build();
        }
    }

    static final public Builder builder() {
        return new Builder();
    }
}
