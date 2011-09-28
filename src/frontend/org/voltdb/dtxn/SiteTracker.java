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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

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

    int m_liveSiteCount = 0;
    int m_liveInitiatorCount = 0;

    // Cache a reference to the Catalog sites list
    CatalogMap<Site> m_sites;

    // a map of site ids (index) to partition ids (value)
    Map<Integer, Integer> m_sitesToPartitions = new HashMap<Integer, Integer>();

    Map<Integer, ArrayList<Integer>> m_partitionsToSites =
        new HashMap<Integer, ArrayList<Integer>>();

    Map<Integer, ArrayList<Integer>> m_partitionsToLiveSites =
        new HashMap<Integer, ArrayList<Integer>>();

    Map<Integer, ArrayList<Integer>> m_hostsToSites =
        new HashMap<Integer, ArrayList<Integer>>();

    Map<Integer, Integer> m_sitesToHost =
        new HashMap<Integer, Integer>();

    Map<Integer, HashSet<Integer>> m_nonExecSitesForHost = new HashMap<Integer, HashSet<Integer>>();

    // records the timestamp of the last message sent to each sites
    HashMap<Integer, Long> m_lastHeartbeatTime = new HashMap<Integer, Long>();

    Set<Integer> m_liveSiteIds = new TreeSet<Integer>();

    Set<Integer> m_liveHostIds = new TreeSet<Integer>();

    Set<Integer> m_downHostIds = new TreeSet<Integer>();

    private final Site m_allSites[];

    private final int[] m_firstNonExecSiteForHost;

    private final int[] m_localHeartbeatTargets;

    private final int[][] m_remoteHeartbeatTargets;

    // scratch value used to compute the out of date sites
    // note: this makes
    int[] m_tempOldSitesScratch = null;

    private final HashMap<Integer, int[]> m_upExecutionSitesExcludingSite =
        new HashMap<Integer, int[]>();

    /**
     * Given topology info, initialize all of this class's data structures.
     *
     * @param clusterSites a CatalogMap containing all the sites in the cluster
     */
    public SiteTracker(CatalogMap<Site> clusterSites)
    {
        ArrayList<Site> allSites = new ArrayList<Site>();
        m_sites = clusterSites;
        for (Site site : clusterSites)
        {
            int siteId = Integer.parseInt(site.getTypeName());
            allSites.add(site);
            int hostId = Integer.parseInt(site.getHost().getTypeName());
            m_sitesToHost.put(siteId, hostId);
            if (!m_hostsToSites.containsKey(hostId))
            {
                m_hostsToSites.put(hostId, new ArrayList<Integer>());
            }
            m_hostsToSites.get(hostId).add(siteId);
            if (!m_nonExecSitesForHost.containsKey(hostId)) {
                m_nonExecSitesForHost.put(hostId, new HashSet<Integer>());
            }

            // don't put non-exec (has ee) sites in the list.
            if (site.getIsexec() == false)
            {
                m_nonExecSitesForHost.get(hostId).add(siteId);
                if (site.getIsup())
                {
                    m_liveSiteIds.add(siteId);
                    m_liveHostIds.add(hostId);
                    m_liveInitiatorCount++;
                } else {
                    m_downHostIds.add(hostId);
                }
            }
            else
            {
                int partitionId = Integer.parseInt(site.getPartition().getTypeName());

                m_sitesToPartitions.put(siteId, partitionId);
                if (!m_partitionsToSites.containsKey(partitionId))
                {
                    m_partitionsToSites.put(partitionId,
                                            new ArrayList<Integer>());
                }
                m_partitionsToSites.get(partitionId).add(siteId);

                if (!m_partitionsToLiveSites.containsKey(partitionId))
                {
                    m_partitionsToLiveSites.put(partitionId,
                                                new ArrayList<Integer>());
                }
                if (site.getIsup() == true)
                {
                    m_liveSiteCount++;
                    m_liveSiteIds.add(siteId);
                    m_partitionsToLiveSites.get(partitionId).add(siteId);
                } else {
                    m_downHostIds.add(hostId);
                }
            }
        }
        m_allSites = new Site[allSites.size()];
        for (int ii = 0; ii < m_allSites.length; ii++) {
            m_allSites[ii] = allSites.get(ii);
        }
        m_tempOldSitesScratch = new int[m_sites.size()];

        for (int siteId : m_sitesToPartitions.keySet()) {
            if (getSiteForId(siteId).getIsup()) {
                m_lastHeartbeatTime.put(siteId, -1L);
            }
        }

        /*
         *  Calculate exec sites for each up host, these will be the heartbeat targets
         */
        HashMap<Integer, ArrayList<Integer>> upHostsToExecSites = new HashMap<Integer, ArrayList<Integer>>();
        for (Integer host : m_liveHostIds) {
            ArrayList<Integer> sites = new ArrayList<Integer>(m_hostsToSites.get(host));
            sites.removeAll(m_nonExecSitesForHost.get(host));
            upHostsToExecSites.put( host, sites);
        }

        /*
         * Local heartbeat targets go in a separate array, get individual messages delivery locally
         */
        int myHostId = 0;
        if (VoltDB.instance() != null) {
            if (VoltDB.instance().getMessenger() != null) {
                myHostId = VoltDB.instance().getHostMessenger().getHostId();
            }
        }

        int ii = 0;
        int numHosts = upHostsToExecSites.size();
        if (numHosts < 2) {
            m_remoteHeartbeatTargets = new int[0][];
        } else {
            m_remoteHeartbeatTargets = new int[upHostsToExecSites.size() - 1][];
        }
        int tempLocalHeartbeatTargets[] = new int[0];
        for (Map.Entry<Integer, ArrayList<Integer>> entry : upHostsToExecSites.entrySet()) {
            if (entry.getKey() == myHostId) {
                tempLocalHeartbeatTargets = intArrayListToArray(entry.getValue());
            } else {
                m_remoteHeartbeatTargets[ii++] = intArrayListToArray(entry.getValue());
            }
        }
        m_localHeartbeatTargets = tempLocalHeartbeatTargets;
        m_firstNonExecSiteForHost = new int[m_hostsToSites.size()];
        java.util.Arrays.fill(m_firstNonExecSiteForHost, -1);
        for (ii = 0; ii < m_firstNonExecSiteForHost.length; ii++) {
            HashSet<Integer> set = getNonExecSitesForHost(ii);
            if (set != null) {
                if (set.iterator().hasNext()) {
                    m_firstNonExecSiteForHost[ii] = set.iterator().next();
                }
            }
        }
    }

    private int[] intArrayListToArray(ArrayList<Integer> ints) {
        int retval[] = new int[ints.size()];
        for (int ii = 0; ii < retval.length; ii++) {
            retval[ii] = ints.get(ii);
        }
        return retval;
    }

    public Set<Integer> getAllLiveSites() {
        return m_liveSiteIds;
    }

    public Set<Integer> getAllLiveHosts() {
        return m_liveHostIds;
    }

    public Set<Integer> getAllDownHosts() {
        return m_downHostIds;
    }

    /**
     * @return Site object for the corresponding siteId
     */
    public Site getSiteForId(int siteId) {
        return m_sites.get(Integer.toString(siteId));
    }

    /**
     * @return The number of live executions sites currently in the cluster.
     */
    public int getLiveSiteCount()
    {
        return m_liveSiteCount;
    }

    public Site[] getAllSites() {
        return m_allSites;
    }

    /**
     * @return the lowest site ID across the live non-execution sites in the
     *         cluster
     */
    public int getLowestLiveNonExecSiteId()
    {
        int lowestNonExecSiteId = Integer.MAX_VALUE;
        for (Site site : getUpSites()) {
            if (!site.getIsexec()) {
                lowestNonExecSiteId = Math.min(lowestNonExecSiteId, Integer.parseInt(site.getTypeName()));
            }
        }
        return lowestNonExecSiteId;
    }

    /**
     * Get a site that contains a copy of the given partition.  The site ID
     * returned may correspond to a site that is currently down.
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
     * Get a live site that contains a copy of the given partition.
     *
     * @param partition The id of a VoltDB partition.
     * @return The id of a VoltDB site containing a copy of
     * the requested partition.
     */
    public int getOneLiveSiteForPartition(int partition) {
        ArrayList<Integer> sites = m_partitionsToLiveSites.get(partition);
        assert(sites != null);
        return sites.get(0);
    }

    /**
     * Get the ids of all sites that contain a copy of the
     * partition specified.  The list will include sites that are down.
     *
     * @param partition A VoltDB partition id.
     * @return An array of VoltDB site ids.
     */
    public ArrayList<Integer> getAllSitesForPartition(int partition) {
        assert (m_partitionsToSites.containsKey(partition));
        return m_partitionsToSites.get(partition);
    }

    /**
     * Get the ids of all live sites that contain a copy of the
     * partition specified.
     *
     * @param partition A VoltDB partition id.
     * @return An array of VoltDB site ids.
     */
    public ArrayList<Integer> getLiveSitesForPartition(int partition) {
        assert (m_partitionsToLiveSites.containsKey(partition));
        return m_partitionsToLiveSites.get(partition);
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
            retval[i] = all_sites.get(i);
        }
        return retval;
    }

    /**
     * Get the ids of all sites that contain a copy of ANY of
     * the given partitions.
     * @param partitions as ArrayList
     */
    public ArrayList<Integer> getLiveSitesForEachPartitionAsList(int[]  partitions) {
        ArrayList<Integer> all_sites = new ArrayList<Integer>();
        for (int p : partitions) {
            ArrayList<Integer> sites = getLiveSitesForPartition(p);
            for (int site : sites)
            {
                all_sites.add(site);
            }
        }
        return all_sites;
    }

    /**
     * Get the ids of all live sites that contain a copy of ANY of
     * the given partitions.
     *
     * @param partitions A set of unique, non-null VoltDB
     * partition ids.
     * @return An array of VoltDB site ids.
     */
    public int[] getLiveSitesForEachPartition(int[] partitions) {
        ArrayList<Integer> all_sites = new ArrayList<Integer>();
        for (int p : partitions) {
            ArrayList<Integer> sites = getLiveSitesForPartition(p);
            for (int site : sites)
            {
                all_sites.add(site);
            }
        }
        int[] retval = new int[all_sites.size()];
        for (int i = 0; i < all_sites.size(); i++)
        {
            retval[i] = all_sites.get(i);
        }
        return retval;
    }

    /**
     * Get the list of all site IDs that are on a specific host ID
     * @param hostId
     * @return An ArrayList of VoltDB site IDs.
     */
    public ArrayList<Integer> getAllSitesForHost(int hostId)
    {
        if (!m_hostsToSites.containsKey(hostId)) {
            System.out.println("Couldn't find sites for host " + hostId);
            assert m_hostsToSites.containsKey(hostId);
        }

        return m_hostsToSites.get(hostId);
    }

    /**
     * Get the host id for a specific site
     * @param siteid
     * @return Integer host id for that site
     */
    public Integer getHostForSite(Integer siteId) {
        assert m_sitesToHost.containsKey(siteId);
        return m_sitesToHost.get(siteId);
    }

    /**
     * Get the list of live execution site IDs on a specific host ID
     * @param hostId
     */
    public ArrayList<Integer> getLiveExecutionSitesForHost(int hostId)
    {
        assert m_hostsToSites.containsKey(hostId);
        ArrayList<Integer> retval = new ArrayList<Integer>();
        for (Integer site_id : m_hostsToSites.get(hostId))
        {
            if (m_sites.get(Integer.toString(site_id)).getIsexec() &&
                m_sites.get(Integer.toString(site_id)).getIsup())
            {
                retval.add(site_id);
            }
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
     * @return An ArrayDeque containing references for the catalog Site
     *         objects which are currrently up.  This includes both
     *         execution sites and non-execution sites (initiators, basically)
     */
    public ArrayDeque<Site> getUpSites()
    {
        ArrayDeque<Site> retval = new ArrayDeque<Site>();
        for (Site site : m_sites)
        {
            if (site.getIsup())
            {
                retval.add(site);
            }
        }
        return retval;
    }

    public HashSet<Integer> getNonExecSitesForHost(int hostId) {
        return m_nonExecSitesForHost.get(hostId);
    }

    /**
     * @return An array containing siteds for all up, Isexec() sites.
     */
    public int[] getUpExecutionSites()
    {
        ArrayDeque<Integer> tmplist = new ArrayDeque<Integer>();
        for  (Site site : m_sites) {
            if (site.getIsup() && site.getIsexec()) {
                tmplist.add(Integer.parseInt(site.getTypeName()));
            }
        }
        int[] retval = new int[tmplist.size()];
        for (int i=0; i < retval.length; ++i) {
            retval[i] = tmplist.poll();
        }
        return retval;
    }

    public int[] getUpExecutionSitesExcludingSite(int excludedSite) {
        int sites[] = m_upExecutionSitesExcludingSite.get(excludedSite);
        if (sites == null) {
            ArrayList<Integer> list = new ArrayList<Integer>();
            for (Integer site : getUpExecutionSites()) {
                if (site.intValue() != excludedSite) {
                    list.add(site);
                }
            }
            sites = new int[list.size()];
            int ii = 0;
            for (int site : list) {
                sites[ii++] = site;
            }
            m_upExecutionSitesExcludingSite.put( excludedSite, sites);
        }
        return sites;
    }

    /**
     * @return A Set of all the execution site IDs in the cluster.
     */
    public Set<Integer> getExecutionSiteIds()
    {
        Set<Integer> exec_sites = new HashSet<Integer>(m_sites.size());
        for (Site site : m_sites)
        {
            if (site.getIsexec())
            {
                exec_sites.add(Integer.parseInt(site.getTypeName()));
            }
        }
        return exec_sites;
    }

    /**
     * @return A list containing the partition IDs of any partitions
     * which are not currently present on any live execution sites
     */
    public ArrayList<Integer> getFailedPartitions()
    {
        ArrayList<Integer> retval = new ArrayList<Integer>();
        for (Integer partition : m_partitionsToLiveSites.keySet())
        {
            if (m_partitionsToLiveSites.get(partition).size() == 0)
            {
                retval.add(partition);
            }
        }
        return retval;
    }

    /**
     * @param hostId
     * @return the ID of the lowest execution site on the given hostId
     */
    public Integer getLowestLiveExecSiteIdForHost(int hostId)
    {
        ArrayList<Integer> sites = getLiveExecutionSitesForHost(hostId);
        return Collections.min(sites);
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

    /*
     * Get an array of local sites that need heartbeats. This will get individually generated heartbeats.
     */
    public int[] getLocalHeartbeatTargets() {
        return m_localHeartbeatTargets;
    }

    /*
     * An array per up host, there will be no entry for this host
     */
    public int[][] getRemoteHeartbeatTargets() {
        return m_remoteHeartbeatTargets;
    }

    public int getFirstNonExecSiteForHost(int hostId) {
        return m_firstNonExecSiteForHost[hostId];
    }
}
