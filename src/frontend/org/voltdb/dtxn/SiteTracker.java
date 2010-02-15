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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.voltdb.VoltDB;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Site;

/**
 * Object which allows for mapping of sites to partitions and vice
 * versa. Is responsible for choosing sites given partitions from
 * any replica options. Is responsible for determining which sites
 * have not received a message from this site in a specified window
 * of time.
 *
 * Future updates to this class will allow it to change when the
 * catalog changes, but for now it is static.
 */
public class SiteTracker {

    int m_siteCount;

    // a map of site ids (index) to partition ids (value)
    Map<Integer, Integer> m_sitesToPartitions = new HashMap<Integer, Integer>();

    HashMap<Integer, ArrayList<Integer>> m_partitionsToSites =
        new HashMap<Integer, ArrayList<Integer>>();

    // records the timestamp of the last message sent to each sites
    HashMap<Integer, Long> m_lastHeartbeatTime = new HashMap<Integer, Long>();

    // scratch value used to compute the out of date sites
    // note: this makes
    int[] m_tempOldSitesScratch = null;

    /**
     * Given topology info, initialize all of this class's data structures.
     *
     * @param clusterSites a CatalogMap containing all the sites in the cluster
     */
    public SiteTracker(CatalogMap<Site> clusterSites)
    {
        for (Site site : clusterSites)
        {
            // don't put non-exec (has ee) sites in the list.
            if (site.getIsexec() == false)
                continue;

            int siteId = Integer.parseInt(site.getTypeName());

            int partitionId = Integer.parseInt(site.getPartition().getTypeName());

            m_sitesToPartitions.put(siteId, partitionId);
            if (!m_partitionsToSites.containsKey(partitionId))
            {
                m_partitionsToSites.put(partitionId,
                                        new ArrayList<Integer>());
            }
            m_partitionsToSites.get(partitionId).add(siteId);
        }

        m_siteCount = m_sitesToPartitions.size();

        // make sure it's an even multiple
        assert((m_siteCount % m_partitionsToSites.size()) == 0);

        m_tempOldSitesScratch = new int[m_siteCount];

        for (int siteId : m_sitesToPartitions.keySet())
        {
            m_lastHeartbeatTime.put(siteId, -1L);
        }
    }

    /**
     * Get a site that contains a copy of the given partition.
     *
     * @param partition The id of a VoltDB partition.
     * @return The id of a VoltDB site containing a copy of
     * the requested partition.
     */
    public int getOneSiteForPartition(int partition) {
        ArrayList<Integer> sites = m_partitionsToSites.get(partition);
        assert(sites != null);
        return sites.get(0);
    }

    /**
     * Get the ids of all sites that contain a copy of the
     * partition specified.
     *
     * @param partition A VoltDB partition id.
     * @return An array of VoltDB site ids.
     */
    public ArrayList<Integer> getAllSitesForPartition(int partition) {
        assert (m_partitionsToSites.containsKey(partition));
        return m_partitionsToSites.get(partition);
    }

    /**
     * Get one site id for each of the partition ids given.
     *
     * @param partitions A set of unique, non-null VoltDB
     * @return An array of VoltDB site ids.
     */
    public int[] getOneSiteForEachPartition(int[] partitions) {
        int[] retval = new int[partitions.length];
        int index = 0;
        for (int p : partitions)
            retval[index++] = getOneSiteForPartition(p);
        return retval;
    }

    /**
     * Get the ids of all sites that contain a copy of ANY of
     * the given partitions.
     *
     * @param partitions A set of unique, non-null VoltDB
     * partition ids.
     * @return An array of VoltDB site ids.
     */
    public int[] getAllSitesForEachPartition(int[] partitions) {
        ArrayList<Integer> all_sites = new ArrayList<Integer>();
        for (int p : partitions) {
            ArrayList<Integer> sites = getAllSitesForPartition(p);
            for (int site : sites)
            {
                all_sites.add(site);
            }
        }
        int[] retval = new int[all_sites.size()];
        for (int i = 0; i < all_sites.size(); i++)
        {
            retval[i] = (int) all_sites.get(i);
        }
        return retval;
    }


    /**
     * Return the id of the partition stored by a given site.
     *
     * @param siteId The id of a VoltDB site.
     * @return The id of the partition stored at the given site.
     */
    public int getPartitionForSite(int siteId)
    {
        assert(m_sitesToPartitions.containsKey(siteId));
        return m_sitesToPartitions.get(siteId);
    }

    /**
     * Inform the SiteTracker that a message was sent to a site or
     * to a set of sites. This will reset the heart-beat timer on
     * those sites
     *
     * @param time The current system time in ms from epoch
     * @param siteIds A set of VoltDB site ids.
     */
    void noteSentMessage(long time, int... siteIds) {
        for (int id : siteIds)
            m_lastHeartbeatTime.put(id, time);
    }

    /**
     * Get a list of sites which haven't been sent a message in
     * X ms, where X is the acceptable time between messages.
     *
     * @param time
     * @param timeout
     * @return An array of site ids.
     */
    int[] getSitesWhichNeedAHeartbeat(long time, long timeout) {
        int index = 0;
        for (Entry<Integer, Long> e : m_lastHeartbeatTime.entrySet()) {
            long siteTime = e.getValue();
            if ((time - siteTime) >= timeout)
                m_tempOldSitesScratch[index++] = e.getKey();
        }

        int[] retval = new int[index];
        for (int i = 0; i < index; i++)
            retval[i] = m_tempOldSitesScratch[i];

        return retval;
    }
}
