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

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;

import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.messaging.Mailbox;

public class ExecutorTxnIdSafetyState {

    private class PartitionState {
        public int partitionId;
        public long newestConfirmedTxnId;
        public ArrayList<SiteState> sites = new ArrayList<SiteState>();
    }

    private class SiteState {
        public int siteId;
        public long newestConfirmedTxnId;
        public long lastSentTxnId;
        public PartitionState partition;
    }

    LinkedHashMap<Integer, SiteState> m_stateBySite = new LinkedHashMap<Integer, SiteState>();
    LinkedHashMap<Integer, PartitionState> m_stateByPartition = new LinkedHashMap<Integer, PartitionState>();

    Mailbox m_mailbox = null;
    final int m_siteId;

    public ExecutorTxnIdSafetyState(int siteId, SiteTracker tracker) {
        m_siteId = siteId;

        Set<Integer> execSites = tracker.getExecutionSiteIds();
        for (int id : execSites) {
            Site s = tracker.getSiteForId(id);
            // ignore down sites
            if (!s.getIsup()) continue;

            Partition p = s.getPartition();
            assert(p != null);
            int partitionId = Integer.parseInt(p.getTypeName());

            SiteState ss = new SiteState();
            ss.siteId = id;
            ss.newestConfirmedTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
            ss.lastSentTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
            assert(m_stateBySite.get(id) == null);
            m_stateBySite.put(id, ss);

            // look for partition state by id
            PartitionState ps = m_stateByPartition.get(partitionId);

            // create, populate and insert it
            if (ps == null) {
                ps = new PartitionState();
                ps.partitionId = partitionId;
                ps.newestConfirmedTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
                m_stateByPartition.put(partitionId, ps);
            }

            // sanity checks
            assert(ps.partitionId == partitionId);
            for (SiteState state : ps.sites) {
                assert(state != null);
                assert(state.siteId != ss.siteId);
            }

            // link the partition state and site state
            ps.sites.add(ss);
            ss.partition = ps;
        }
    }

    void setMailbox(Mailbox mbox) {
        m_mailbox = mbox;
    }

    public synchronized long getNewestSafeTxnIdForExecutorBySiteId(int executorSiteId) {
        SiteState ss = m_stateBySite.get(executorSiteId);
        assert(ss != null);
        assert(ss.siteId == executorSiteId);
        PartitionState ps = ss.partition;
        ss.lastSentTxnId = ps.newestConfirmedTxnId;
        return ps.newestConfirmedTxnId;
    }

    public synchronized void updateLastSeenTxnIdFromExecutorBySiteId(int executorSiteId, long lastSeenTxnId, boolean shouldRespond) {
        // ignore these by convention
        if (lastSeenTxnId == DtxnConstants.DUMMY_LAST_SEEN_TXN_ID)
            return;

        SiteState ss = m_stateBySite.get(executorSiteId);
        assert(ss != null);
        assert(ss.siteId == executorSiteId);

        // check if state needs changing
        if (ss.newestConfirmedTxnId < lastSeenTxnId) {

            // state needs changing at least for this site
            ss.newestConfirmedTxnId = lastSeenTxnId;

            PartitionState ps = ss.partition;
            assert(ps.sites.size() > 0);
            long min = Long.MAX_VALUE;
            for (SiteState s : ps.sites) {
                assert(s != null);
                if (s.newestConfirmedTxnId < min)
                    min = s.newestConfirmedTxnId;
            }
            assert(min != Long.MAX_VALUE);

            ps.newestConfirmedTxnId = min;
        }

        // see if the sent message is out of date
        /*if (shouldRespond) {
            HeartbeatMessage hb = new HeartbeatMessage(m_siteId, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID, ss.partition.newestConfirmedTxnId);
            try {
                m_mailbox.send(executorSiteId, VoltDB.DTXN_MAILBOX_ID, hb);
            } catch (MessagingException e) {
                throw new RuntimeException(e);
            }
        }*/
    }

    /**
     * Remove all of the state pertaining to a siteid.
     * Called from the DtxnInitiatorQueue's fault handler
     * @param executorSiteId The id of the site to remove
     */
    public synchronized void removeState(int executorSiteId) {
        SiteState ss = m_stateBySite.get(executorSiteId);
        if (ss == null) return;
        PartitionState ps = ss.partition;
        for (SiteState s : ps.sites) {
            if (s.siteId == ss.siteId) {
                ps.sites.remove(s);
                break;
            }
        }
        m_stateBySite.remove(executorSiteId);
    }
}
