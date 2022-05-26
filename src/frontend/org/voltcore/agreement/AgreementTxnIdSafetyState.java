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

package org.voltcore.agreement;

import java.util.LinkedHashMap;
import java.util.Map;

public class AgreementTxnIdSafetyState {

    private class SiteState {
        public long hsId;
        public long newestConfirmedTxnId;
    }

    Map<Long, SiteState> m_stateBySite = new LinkedHashMap<Long, SiteState>();

    final Long m_siteId;

    private long m_newestConfirmedTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID;;

    public AgreementTxnIdSafetyState(long mySiteId) {
        m_siteId = mySiteId;
    }

    public long getNewestSafeTxnIdForExecutorBySiteId(long agreementHSId) {
        SiteState ss = m_stateBySite.get(agreementHSId);
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
        assert(ss.hsId == agreementHSId);
        return ss.newestConfirmedTxnId;
    }

    public long getNewestGloballySafeTxnId()
    {
        return m_newestConfirmedTxnId;
    }

    public void updateLastSeenTxnIdFromExecutorBySiteId(long agreementHSId, long lastSeenTxnId)
    {
        // ignore these by convention
        if (lastSeenTxnId == DtxnConstants.DUMMY_LAST_SEEN_TXN_ID)
            return;

        SiteState ss = m_stateBySite.get(agreementHSId);
        // when a dead host is detected, the sites reside on that host are
        // removed. So site state returned will be null.
        if (ss == null)
            return;
        assert(ss.hsId == agreementHSId);

        // check if state needs changing
        if (ss.newestConfirmedTxnId < lastSeenTxnId) {

            // state needs changing at least for this site
            ss.newestConfirmedTxnId = lastSeenTxnId;

            long min = Long.MAX_VALUE;
            for (SiteState s : m_stateBySite.values()) {
                assert(s != null);
                if (s.newestConfirmedTxnId < min)
                    min = s.newestConfirmedTxnId;
            }
            assert(min != Long.MAX_VALUE);

            m_newestConfirmedTxnId = min;
        }
    }

    /**
     * Remove all of the state pertaining to a siteid.
     * Called from the DtxnInitiatorQueue's fault handler
     * @param executorSiteId The id of the site to remove
     */
    public void removeState(long agreementHSId) {
        m_stateBySite.remove(agreementHSId);
    }

    /**
     * Once a failed node is rejoined, put it's sites back into
     * all of the data structures here.
     * @param executorSiteId
     * @param partitionId
     */
    public void addState(long agreementHSId) {
        SiteState ss = m_stateBySite.get(agreementHSId);
        if (ss != null) return;
        ss = new SiteState();
        ss.hsId = agreementHSId;
        ss.newestConfirmedTxnId = m_newestConfirmedTxnId;
        m_stateBySite.put(agreementHSId, ss);
    }
}
