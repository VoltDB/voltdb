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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    private long[] longListToArray(List<Long> longs) {
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
     * Whether we are the leader. It doesn't mean that we have the lowest host
     * ID, it just guarantees that there is only one node in the cluster that is
     * the leader.
     *
     * @return
     */
    public boolean isFirstHost() {
        return m_mailboxTracker.isFirstHost();
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
    public List<Long> getAllSitesForHost(int hostId)
    {
        // TODO: used to return all sites, now only returns exec sites
        return m_mailboxTracker.getSitesForHost(hostId);
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
        return m_mailboxTracker.getPartitionForSite(siteId);
    }

    public List<Long> getNonExecSitesForHost(int hostId) {
        return m_mailboxTracker.getInitiatorForHost(hostId);
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
        Set<Long> tmplist = m_mailboxTracker.getAllSites();
        int size = tmplist.size();
        if (tmplist.contains(excludedSite))
            size--;
        long[] retval = new long[size];
        int i = 0;
        for (long id : tmplist) {
            if (id != excludedSite)
                retval[i++] = id;
        }

        return retval;
    }

    /**
     * @return A Set of all the execution site IDs in the cluster.
     */
    public Set<Long> getExecutionSiteIds()
    {
        return m_mailboxTracker.getAllSites();
    }

    /**
     * TODO: needs work
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

    /*
     * Get an array of local sites that need heartbeats. This will get individually generated heartbeats.
     */
    public long[] getLocalHeartbeatTargets() {
        int hostId = VoltDB.instance().getHostMessenger().getHostId();
        return longListToArray(m_mailboxTracker.getSitesForHost(hostId));
    }

    /*
     * An array per up host, there will be no entry for this host
     */
    public long[][] getRemoteHeartbeatTargets() {
        int localhost = VoltDB.instance().getHostMessenger().getHostId();
        Set<Integer> hosts = m_mailboxTracker.getAllHosts();
        long[][] retval = new long[hosts.size() - 1][];
        int i = 0;
        for (int host : hosts) {
            if (host != localhost) {
                retval[i++] = longListToArray(m_mailboxTracker.getSitesForHost(host));
            }
        }
        return retval;
    }

    public long getFirstNonExecSiteForHost(int hostId) {
        List<Long> initiators = m_mailboxTracker.getInitiatorForHost(hostId);
        return initiators.get(0);
    }

    public Set<Long> getLiveNonExecSites() {
        return m_mailboxTracker.getAllInitiators();
    }

    public void setMailboxTracker(MailboxTracker mailboxTracker) {
        m_mailboxTracker = mailboxTracker;
    }

    public MailboxTracker getMailboxTracker() {
        return m_mailboxTracker;
    }
}
