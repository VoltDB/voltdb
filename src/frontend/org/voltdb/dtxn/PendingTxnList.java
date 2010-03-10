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

class PendingTxnList
{
    PendingTxnList()
    {
        m_txnIdMap =
            new HashMap<Long,
                        HashMap<Integer, InFlightTxnState>>();
    }

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

    int size()
    {
        return m_txnIdMap.size();
    }

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

