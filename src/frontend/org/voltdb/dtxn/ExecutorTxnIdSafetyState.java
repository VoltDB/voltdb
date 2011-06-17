/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

public class ExecutorTxnIdSafetyState {

    private class PartitionState {
        public int partitionId;
        public long newestConfirmedTxnId;
        public ArrayList<SiteState> sites = new ArrayList<SiteState>();
    }

    private class SiteState {
        public int siteId;
        public long newestConfirmedTxnId;
        @SuppressWarnings("unused")
        public long lastSentTxnId;
        public PartitionState partition;
    }

    Map<Integer, SiteState> m_stateBySite = new LinkedHashMap<Integer, SiteState>();
    Map<Integer, PartitionState> m_stateByPartition = new LinkedHashMap<Integer, PartitionState>();

    // kept across failures and rejoins to understand the unchanging maps of sites to partitions
    Map<Integer, Integer> m_stateToPartitionMap = new HashMap<Integer, Integer>();

    final int m_siteId;

    public ExecutorTxnIdSafetyState(int mySiteId, int siteIds[]) {
        m_siteId = mySiteId;
        final int partitionId = 0;
        for (int siteId : siteIds) {
            m_stateToPartitionMap.put( siteId, partitionId);
            SiteState ss = new SiteState();
            ss.siteId = siteId;
            ss.newestConfirmedTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
            ss.lastSentTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
            assert(m_stateBySite.get(siteId) == null);
            m_stateBySite.put( siteId, ss);

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

    public ExecutorTxnIdSafetyState(int siteId, SiteTracker tracker) {
        m_siteId = siteId;

        Set<Integer> execSites = tracker.getExecutionSiteIds();
        for (int id : execSites) {
            Site s = tracker.getSiteForId(id);

            Partition p = s.getPartition();
            assert(p != null);
            int partitionId = Integer.parseInt(p.getTypeName());

            // note the site to partition mapping, even if down
            m_stateToPartitionMap.put(id, partitionId);

            // ignore down sites
            if (!s.getIsup()) continue;

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

    public long getNewestSafeTxnIdForExecutorBySiteId(int executorSiteId) {
        SiteState ss = m_stateBySite.get(executorSiteId);
        // ss will be null if the node with this failed before we got here.
        // Just return DUMMY_LAST_SEEN_TXN_ID; any message generated for the
        // failed node will get dropped gracefully and in the unlikely
        // event that we actually get DUMMY_LAST_SEEN_TXN_ID to a correctly
        // functioning execution site it will simply log an error message
        // but keep running correctly.
        if (ss == null)
        {
            return DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;
        }
        assert(ss.siteId == executorSiteId);
        PartitionState ps = ss.partition;
        ss.lastSentTxnId = ps.newestConfirmedTxnId;
        return ps.newestConfirmedTxnId;
    }

    public void updateLastSeenTxnIdFromExecutorBySiteId(int executorSiteId, long lastSeenTxnId, boolean shouldRespond) {
        // ignore these by convention
        if (lastSeenTxnId == DtxnConstants.DUMMY_LAST_SEEN_TXN_ID)
            return;

        SiteState ss = m_stateBySite.get(executorSiteId);
        // when a dead host is detected, the sites reside on that host are
        // removed. So site state returned will be null.
        if (ss == null)
            return;
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
    public void removeState(int executorSiteId) {
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

    /**
     * Once a failed node is rejoined, put it's sites back into
     * all of the data structures here.
     * @param executorSiteId
     * @param partitionId
     */
    public void addRejoinedState(int executorSiteId) {
        int partitionId = m_stateToPartitionMap.get(executorSiteId);

        SiteState ss = m_stateBySite.get(executorSiteId);
        if (ss != null) return;
        ss = new SiteState();
        ss.siteId = executorSiteId;

        PartitionState ps = m_stateByPartition.get(partitionId);
        assert(ps != null);
        ss.partition = ps;
        ps.sites.add(ss);

        ss.newestConfirmedTxnId = ps.newestConfirmedTxnId;
        ss.lastSentTxnId = ps.newestConfirmedTxnId;

        m_stateBySite.put(executorSiteId, ss);
    }
}
