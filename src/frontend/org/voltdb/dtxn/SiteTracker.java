/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.util.List;
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

    MailboxTracker m_mailboxTracker = null;

    int m_liveSiteCount = 0;
    int m_liveInitiatorCount = 0;

    // Cache a reference to the Catalog sites list
    CatalogMap<Site> m_sites;

    // a map of site ids (index) to partition ids (value)
    Map<Long, Integer> m_sitesToPartitions = new HashMap<Long, Integer>();

    Map<Integer, ArrayList<Long>> m_partitionsToSites =
        new HashMap<Integer, ArrayList<Long>>();

    Map<Integer, ArrayList<Long>> m_partitionsToLiveSites =
        new HashMap<Integer, ArrayList<Long>>();

    Map<Integer, ArrayList<Long>> m_hostsToSites =
        new HashMap<Integer, ArrayList<Long>>();

    Map<Integer, HashSet<Long>> m_nonExecSitesForHost = new HashMap<Integer, HashSet<Long>>();

    // maps <site:timestamp> of the last message sent to each sites
    HashMap<Long, Long> m_lastHeartbeatTime = new HashMap<Long, Long>();

    Set<Integer> m_liveHostIds = new TreeSet<Integer>();

    Set<Integer> m_downHostIds = new TreeSet<Integer>();

    private final Site m_allSites[];

    private final long[] m_firstNonExecSiteForHost;

    private final long[] m_localHeartbeatTargets;

    private final long[][] m_remoteHeartbeatTargets;

    // scratch value used to compute the out of date sites
    long[] m_tempOldSitesScratch = null;

    private final HashMap<Long, long[]> m_upExecutionSitesExcludingSite =
        new HashMap<Long, long[]>();

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
            long siteId = Integer.parseInt(site.getTypeName());
            allSites.add(site);
            int hostId = Integer.parseInt(site.getHost().getTypeName());
            if (!m_hostsToSites.containsKey(hostId))
            {
                m_hostsToSites.put(hostId, new ArrayList<Long>());
            }
            m_hostsToSites.get(hostId).add(siteId);
            if (!m_nonExecSitesForHost.containsKey(hostId)) {
                m_nonExecSitesForHost.put(hostId, new HashSet<Long>());
            }

            // don't put non-exec (has ee) sites in the list.
            if (site.getIsexec() == false)
            {
                m_nonExecSitesForHost.get(hostId).add(siteId);
                if (site.getIsup())
                {
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
                                            new ArrayList<Long>());
                }
                m_partitionsToSites.get(partitionId).add(siteId);

                if (!m_partitionsToLiveSites.containsKey(partitionId))
                {
                    m_partitionsToLiveSites.put(partitionId,
                                                new ArrayList<Long>());
                }
                if (site.getIsup() == true)
                {
                    m_liveSiteCount++;
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
        m_tempOldSitesScratch = new long[m_sites.size()];

        for (long siteId : m_sitesToPartitions.keySet()) {
            if (getSiteForId(siteId).getIsup()) {
                m_lastHeartbeatTime.put(siteId, -1L);
            }
        }

        /*
         *  Calculate exec sites for each up host, these will be the heartbeat targets
         */
        HashMap<Integer, ArrayList<Long>> upHostsToExecSites = new HashMap<Integer, ArrayList<Long>>();
        for (Integer host : m_liveHostIds) {
            ArrayList<Long> sites = new ArrayList<Long>(m_hostsToSites.get(host));
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
            m_remoteHeartbeatTargets = new long[0][];
        } else {
            m_remoteHeartbeatTargets = new long[upHostsToExecSites.size() - 1][];
        }
        long tempLocalHeartbeatTargets[] = new long[0];
        for (Map.Entry<Integer, ArrayList<Long>> entry : upHostsToExecSites.entrySet()) {
            if (entry.getKey() == myHostId) {
                tempLocalHeartbeatTargets = longArrayListToArray(entry.getValue());
            } else {
                m_remoteHeartbeatTargets[ii++] = longArrayListToArray(entry.getValue());
            }
        }
        m_localHeartbeatTargets = tempLocalHeartbeatTargets;
        m_firstNonExecSiteForHost = new long[m_hostsToSites.size()];
        java.util.Arrays.fill(m_firstNonExecSiteForHost, -1);
        for (ii = 0; ii < m_firstNonExecSiteForHost.length; ii++) {
            HashSet<Long> set = getNonExecSitesForHost(ii);
            if (set != null) {
                if (set.iterator().hasNext()) {
                    m_firstNonExecSiteForHost[ii] = set.iterator().next();
                }
            }
        }
    }

    private long[] longArrayListToArray(ArrayList<Long> longs) {
        long retval[] = new long[longs.size()];
        for (int ii = 0; ii < retval.length; ii++) {
            retval[ii] = longs.get(ii);
        }
        return retval;
    }

    public Set<Long> getAllLiveSites() {
        return m_mailboxTracker.getAllSites();
    }

    public Set<Integer> getAllLiveHosts() {
        return m_mailboxTracker.getAllHosts();
    }

    public Set<Integer> getAllDownHosts() {
        return m_downHostIds;
    }

    /**
     * @return Site object for the corresponding siteId
     */
    public Site getSiteForId(long siteId) {
        return m_sites.get(Long.toString(siteId));
    }

    /**
     * @return The number of live executions sites currently in the cluster.
     */
    public int getLiveSiteCount()
    {
        return m_mailboxTracker.getAllSites().size();
    }

    public Site[] getAllSites() {
        return m_allSites;
    }

    /**
     * @return the lowest site ID across the live non-execution sites in the
     *         cluster
     */
    public long getLowestLiveNonExecSiteId()
    {
        long lowestNonExecSiteId = Long.MAX_VALUE;
        Set<Long> initiators = m_mailboxTracker.getAllInitiators();
        for (long initiator : initiators) {
            lowestNonExecSiteId = Math.min(lowestNonExecSiteId, initiator);
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
    public long getOneSiteForPartition(int partition) {
        ArrayList<Long> sites = m_partitionsToSites.get(partition);
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
    public long getOneLiveSiteForPartition(int partition) {
        List<Long> sites = m_mailboxTracker.getSitesForPartition(partition);
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
    public ArrayList<Long> getAllSitesForPartition(int partition) {
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
    public List<Long> getLiveSitesForPartition(int partition) {
        return m_mailboxTracker.getSitesForPartition(partition);
    }

    /**
     * Get one site id for each of the partition ids given.
     *
     * @param partitions A set of unique, non-null VoltDB
     * @return An array of VoltDB site ids.
     */
    public long[] getOneSiteForEachPartition(int[] partitions) {
        long[] retval = new long[partitions.length];
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
    public long[] getAllSitesForEachPartition(int[] partitions) {
        ArrayList<Long> all_sites = new ArrayList<Long>();
        for (int p : partitions) {
            ArrayList<Long> sites = getAllSitesForPartition(p);
            for (long site : sites)
            {
                all_sites.add(site);
            }
        }

        long[] retval = new long[all_sites.size()];
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
    public ArrayList<Long> getLiveSitesForEachPartitionAsList(int[]  partitions) {
        ArrayList<Long> all_sites = new ArrayList<Long>();
        for (int p : partitions) {
            List<Long> sites = getLiveSitesForPartition(p);
            for (long site : sites)
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
    public long[] getLiveSitesForEachPartition(int[] partitions) {
        ArrayList<Long> all_sites = new ArrayList<Long>();
        for (int p : partitions) {
            List<Long> sites = getLiveSitesForPartition(p);
            for (long site : sites)
            {
                all_sites.add(site);
            }
        }
        long[] retval = new long[all_sites.size()];
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
    public ArrayList<Long> getAllSitesForHost(int hostId)
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
    public Integer getHostForSite(Long siteId) {
        return MailboxTracker.getHostForHSId(siteId);
    }

    /**
     * Get the list of live execution site IDs on a specific host ID
     * @param hostId
     */
    public List<Long> getLiveExecutionSitesForHost(int hostId)
    {
        return m_mailboxTracker.getSitesForHost(hostId);
    }

    /**
     * Return the id of the partition stored by a given site.
     *
     * @param siteId The id of a VoltDB site.
     * @return The id of the partition stored at the given site.
     */
    public int getPartitionForSite(long siteId)
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

    public HashSet<Long> getNonExecSitesForHost(int hostId) {
        return m_nonExecSitesForHost.get(hostId);
    }

    /**
     * @return An array containing siteds for all up, Isexec() sites.
     */
    public long[] getUpExecutionSites()
    {
        Set<Long> tmplist = m_mailboxTracker.getAllSites();
        long[] retval = new long[tmplist.size()];
        int i = 0;
        for (long id : tmplist) {
            retval[i++] = id;
        }
        return retval;
    }

    public long[] getUpExecutionSitesExcludingSite(long excludedSite) {
        long sites[] = m_upExecutionSitesExcludingSite.get(excludedSite);
        if (sites == null) {
            ArrayList<Long> list = new ArrayList<Long>();
            for (Long site : getUpExecutionSites()) {
                if (site.longValue() != excludedSite) {
                    list.add(site);
                }
            }
            sites = new long[list.size()];
            int ii = 0;
            for (long site : list) {
                sites[ii++] = site;
            }
            m_upExecutionSitesExcludingSite.put( excludedSite, sites);
        }
        return sites;
    }

    /**
     * @return A Set of all the execution site IDs in the cluster.
     */
    public Set<Long> getExecutionSiteIds()
    {
        Set<Long> exec_sites = new HashSet<Long>(m_sites.size());
        for (Site site : m_sites)
        {
            if (site.getIsexec())
            {
                exec_sites.add(Long.parseLong(site.getTypeName()));
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
    public Long getLowestLiveExecSiteIdForHost(int hostId)
    {
        List<Long> sites = getLiveExecutionSitesForHost(hostId);
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
    void noteSentMessage(long time, long... siteIds) {
        for (long id : siteIds)
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
    long[] getSitesWhichNeedAHeartbeat(long time, long timeout) {
        int index = 0;
        for (Entry<Long, Long> e : m_lastHeartbeatTime.entrySet()) {
            long siteTime = e.getValue();
            if ((time - siteTime) >= timeout)
                m_tempOldSitesScratch[index++] = e.getKey();
        }

        long[] retval = new long[index];
        for (int i = 0; i < index; i++)
            retval[i] = m_tempOldSitesScratch[i];

        return retval;
    }

    /*
     * Get an array of local sites that need heartbeats. This will get individually generated heartbeats.
     */
    public long[] getLocalHeartbeatTargets() {
        return m_localHeartbeatTargets;
    }

    /*
     * An array per up host, there will be no entry for this host
     */
    public long[][] getRemoteHeartbeatTargets() {
        return m_remoteHeartbeatTargets;
    }

    public long getFirstNonExecSiteForHost(int hostId) {
        return m_firstNonExecSiteForHost[hostId];
    }

    public void setMailboxTracker(MailboxTracker mailboxTracker) {
        m_mailboxTracker = mailboxTracker;
    }

    public MailboxTracker getMailboxTracker() {
        return m_mailboxTracker;
    }
}
