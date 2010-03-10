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
import java.util.HashMap;

/**
 * PendingTxnList records an initiator's expected transaction responses that are
 * outstanding.  Expected responsesare hashed by their transaction ID and then
 * the site ID of the coordinator(s) from which they should arrive.
 */
class PendingTxnList
{
    PendingTxnList()
    {
        m_txnIdMap =
            new HashMap<Long,
                        HashMap<Integer, InFlightTxnState>>();
    }

    /**
     * Add the InFlightTxnState for an outstanding transaction
     * @param txnId the transaction ID for the transaction
     * @param coordinatorSiteId the site ID for the coordinator this transaction was sent to
     * @param txn the InFlightTxnState object containing the data for this transaction
     */
    void addTxn(long txnId, int coordinatorSiteId, InFlightTxnState txn)
    {
        HashMap<Integer, InFlightTxnState> site_map = m_txnIdMap.get(txnId);
        if (site_map == null)
        {
            site_map = new HashMap<Integer, InFlightTxnState>();
            m_txnIdMap.put(txnId, site_map);
        }
        site_map.put(coordinatorSiteId, txn);
    }

    /**
     * Get the InFlightTxnState object corresponding to a
     * transaction ID/coordinator site ID pair.  Modifies the storage by removing
     * the InFlightTxnState returned from the data structure.
     *
     * @param txnId
     * @param coordinatorSiteId
     * @return the relevant InFlightTxnState object, null if one does not exist
     *         for the provided args.
     */
    InFlightTxnState getTxn(long txnId, int coordinatorSiteId)
    {
        HashMap<Integer, InFlightTxnState> site_map = m_txnIdMap.get(txnId);
        if (site_map == null)
        {
            return null;
        }
        InFlightTxnState state = site_map.remove(coordinatorSiteId);
        return state;
    }

    /**
     * Remove all InFlightTxnState object for transactions have been or are
     * going to be sent to the coordinator at the specified site ID.
     * @param siteId
     */
    void removeSite(int siteId)
    {
        for (long key : m_txnIdMap.keySet())
        {
            HashMap<Integer, InFlightTxnState> sitemap = m_txnIdMap.get(key);
            if (sitemap.containsKey(siteId))
            {
                sitemap.remove(siteId);
            }
        }
    }

    /**
     * Remove the empty hashmap associated with the specified transaction ID.
     * Requires that txnId exist in the map and that it be empty (no more
     * outstanding responses expected for this transaction ID).  This method is
     * a bit of a hacky work-around for an unfortunate logic path in
     * SimpleDtxnInitiator's handling of responses.
     * @param txnId
     */
    void removeTxnId(long txnId)
    {
        if (m_txnIdMap.containsKey(txnId))
        {
            if (m_txnIdMap.get(txnId).size() == 0)
            {
                m_txnIdMap.remove(txnId);
            }
            else
            {
                System.out.println("Don't remove non-empty txnId map for: " + txnId);
                assert(false);
            }
        }
        else
        {
            System.out.println("Attempt to remove txnId that doesn't exist: " + txnId);
            assert(false);
        }
    }

    /**
     * @return The number of outstanding unique transactions
     */
    int size()
    {
        return m_txnIdMap.size();
    }

    /**
     * @param txnId
     * @return The number of outstanding responses still expected for the
     *         provided transaction ID
     */
    int getTxnIdSize(long txnId)
    {
        if (m_txnIdMap.containsKey(txnId))
        {
            return m_txnIdMap.get(txnId).size();
        }
        // txnId better exist
        assert(false);
        return -1;
    }

    /**
     * Debugging method used to provide information for a dump request
     */
    ArrayList<InFlightTxnState> getInFlightTxns()
    {
        ArrayList<InFlightTxnState> retval = new ArrayList<InFlightTxnState>();
        for (long txnId : m_txnIdMap.keySet())
        {
            // horrible hack to just get one InFlightTxnState out of the inner map
            for (InFlightTxnState txn_state : m_txnIdMap.get(txnId).values())
            {
                retval.add(txn_state);
                break;
            }
        }
        return retval;
    }

    private HashMap<Long, HashMap<Integer, InFlightTxnState>> m_txnIdMap;
}

