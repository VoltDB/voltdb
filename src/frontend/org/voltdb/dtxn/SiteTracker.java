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
import java.util.Map.Entry;

import org.voltcore.utils.MiscUtils;
import org.voltdb.MailboxNodeContent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.utils.NotImplementedException;

public class SiteTracker {
    private final int m_hostId;
    private boolean m_isFirstHost;

    private final Set<Integer> m_allHosts = new HashSet<Integer>();
    private final Set<Long> m_allSites = new HashSet<Long>();
    private final Set<Long> m_allInitiators = new HashSet<Long>();

    private final Map<Integer, ArrayList<Long>> m_hostsToSites =
            new HashMap<Integer, ArrayList<Long>>();
    private final Map<Integer, ArrayList<Integer>> m_hostsToPartitions =
            new HashMap<Integer, ArrayList<Integer>>();
    private final Map<Integer, ArrayList<Long>> m_partitionsToSites =
            new HashMap<Integer, ArrayList<Long>>();
    private final Map<Long, Integer> m_sitesToPartitions =
            new HashMap<Long, Integer>();
    private final Map<Integer, ArrayList<Long>> m_hostsToInitiators =
            new HashMap<Integer, ArrayList<Long>>();
    private final Map<MailboxType, ArrayList<Long>> m_otherHSIds =
            new HashMap<MailboxType, ArrayList<Long>>();
    private final Map<MailboxType, Map<Integer, ArrayList<Long>>> m_hostsToOtherHSIds =
            new HashMap<MailboxType, Map<Integer, ArrayList<Long>>>();
    private long m_statsAgents[];

    public SiteTracker(int hostId, Map<MailboxType, List<MailboxNodeContent>> mailboxes) {
        m_hostId = hostId;

        for (Entry<MailboxType, List<MailboxNodeContent>> e : mailboxes.entrySet()) {
            if (e.getKey().equals(MailboxType.ExecutionSite)) {
                populateSites(e.getValue());
            } else if (e.getKey().equals(MailboxType.Initiator)) {
                populateInitiators(e.getValue());
            } if (e.getKey().equals(MailboxType.StatsAgent)) {
                populateStatsAgents(e.getValue());
            } else {
                populateOtherHSIds(e.getKey(), e.getValue());
            }
        }
    }

    private void populateStatsAgents(List<MailboxNodeContent> value) {
        m_statsAgents = new long[value.size()];
        int ii = 0;
        for (MailboxNodeContent mnc : value) {
            m_statsAgents[ii] = mnc.HSId;
            ii++;
        }
    }

    public long[] getStatsAgents() {
        return m_statsAgents;
    }

    private void populateSites(List<MailboxNodeContent> objs) {
        int firstHostId = -1;
        for (MailboxNodeContent obj : objs) {
            int hostId = MiscUtils.getHostIdFromHSId(obj.HSId);

            if (firstHostId == -1) {
                firstHostId = hostId;
            }

            ArrayList<Long> hostSiteList = m_hostsToSites.get(hostId);
            if (hostSiteList == null)
            {
                hostSiteList = new ArrayList<Long>();
                m_hostsToSites.put(hostId, hostSiteList);
            }
            hostSiteList.add(obj.HSId);

            ArrayList<Integer> hostPartList = m_hostsToPartitions.get(hostId);
            if (hostPartList == null) {
                hostPartList = new ArrayList<Integer>();
                m_hostsToPartitions.put(hostId, hostPartList);
            }
            hostPartList.add(obj.partitionId);

            ArrayList<Long> partSiteList = m_partitionsToSites.get(obj.partitionId);
            if (partSiteList == null) {
                partSiteList = new ArrayList<Long>();
                m_partitionsToSites.put(obj.partitionId, partSiteList);
            }
            partSiteList.add(obj.HSId);


            m_allHosts.add(hostId);
            m_allSites.add(obj.HSId);
            m_sitesToPartitions.put(obj.HSId, obj.partitionId);
        }
        m_isFirstHost = (m_hostId == firstHostId);
    }

    private void populateInitiators(List<MailboxNodeContent> objs) {
        for (MailboxNodeContent obj : objs) {
            int hostId = MiscUtils.getHostIdFromHSId(obj.HSId);

            ArrayList<Long> initiators = m_hostsToInitiators.get(hostId);
            if (initiators == null) {
                initiators = new ArrayList<Long>();
                m_hostsToInitiators.put(hostId, initiators);
            }
            initiators.add(obj.HSId);

            m_allInitiators.add(obj.HSId);
            // TODO: needs to determine if it's the master or replica
        }
    }

    private void populateOtherHSIds(MailboxType type, List<MailboxNodeContent> objs) {
        ArrayList<Long> hsids = m_otherHSIds.get(type);
        if (hsids == null) {
            hsids = new ArrayList<Long>();
            m_otherHSIds.put(type, hsids);
        }

        Map<Integer, ArrayList<Long>> hostToIds = m_hostsToOtherHSIds.get(type);
        if (hostToIds == null) {
            hostToIds = new HashMap<Integer, ArrayList<Long>>();
            m_hostsToOtherHSIds.put(type, hostToIds);
        }

        for (MailboxNodeContent obj : objs) {
            int hostId = MiscUtils.getHostIdFromHSId(obj.HSId);

            hsids.add(obj.HSId);

            ArrayList<Long> hostIdList = hostToIds.get(hostId);
            if (hostIdList == null) {
                hostIdList = new ArrayList<Long>();
                hostToIds.put(hostId, hostIdList);
            }
            hostIdList.add(obj.HSId);
        }
    }

    public static long[] longListToArray(List<Long> longs) {
        long retval[] = new long[longs.size()];
        for (int ii = 0; ii < retval.length; ii++) {
            retval[ii] = longs.get(ii);
        }
        return retval;
    }

    public Set<Long> getAllSites() {
        return m_allSites;
    }

    public Set<Integer> getAllHosts() {
        return m_allHosts;
    }

    public Set<Long> getAllInitiators() {
        return m_allInitiators;
    }

    /**
     * Get the ids of all live sites that contain a copy of the
     * partition specified.
     *
     * @param partition A VoltDB partition id.
     * @return An array of VoltDB site ids.
     */
    public List<Long> getSitesForPartition(int partition) {
        return m_partitionsToSites.get(partition);
    }

    /**
     * Get the ids of all sites that contain a copy of ANY of
     * the given partitions.
     * @param partitions as ArrayList
     */
    public List<Long> getSitesForPartitions(int[] partitions) {
        ArrayList<Long> all_sites = new ArrayList<Long>();
        for (int p : partitions) {
            List<Long> sites = getSitesForPartition(p);
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
        return m_isFirstHost;
    }

    /**
     * Get the ids of all live sites that contain a copy of ANY of
     * the given partitions.
     *
     * @param partitions A set of unique, non-null VoltDB
     * partition ids.
     * @return An array of VoltDB site ids.
     */
    public long[] getSitesForPartitionsAsArray(int[] partitions) {
        ArrayList<Long> all_sites = new ArrayList<Long>();
        for (int p : partitions) {
            List<Long> sites = getSitesForPartition(p);
            for (long site : sites)
            {
                all_sites.add(site);
            }
        }
        return longListToArray(all_sites);
    }

    /**
     * Get the list of all site IDs that are on a specific host ID
     * @param hostId
     * @return An ArrayList of VoltDB site IDs.
     */
    public List<Long> getSitesForHost(int hostId)
    {
        return m_hostsToSites.get(hostId);
    }

    /**
     * Get the host id for a specific site
     * @param siteid
     * @return Integer host id for that site
     */
    public static int getHostForSite(long siteId) {
        return MiscUtils.getHostIdFromHSId(siteId);
    }

    /**
     * Return the id of the partition stored by a given site.
     *
     * @param siteId The id of a VoltDB site.
     * @return The id of the partition stored at the given site.
     */
    public int getPartitionForSite(long siteId)
    {
        return m_sitesToPartitions.get(siteId);
    }

    /**
     * @param hostId
     * @return the ID of the lowest execution site on the given hostId
     */
    public Long getLowestSiteForHost(int hostId)
    {
        List<Long> sites = getSitesForHost(hostId);
        return Collections.min(sites);
    }

    /*
     * Get an array of local sites that need heartbeats. This will get individually generated heartbeats.
     */
    public long[] getLocalSites() {
        int hostId = VoltDB.instance().getHostMessenger().getHostId();
        return longListToArray(m_hostsToSites.get(hostId));
    }

    /*
     * An array per up host, there will be no entry for this host
     */
    public long[][] getRemoteSites() {
        int localhost = VoltDB.instance().getHostMessenger().getHostId();
        Set<Integer> hosts = m_allHosts;
        long[][] retval = new long[hosts.size() - 1][];
        int i = 0;
        for (int host : hosts) {
            if (host != localhost) {
                retval[i++] = longListToArray(m_hostsToSites.get(host));
            }
        }
        return retval;
    }

    public List<Long> getInitiatorsForHost(int host) {
        return m_hostsToInitiators.get(host);
    }

    public Map<Long, Integer> getSitesToPartitions() {
        return m_sitesToPartitions;
    }

    public List<Integer> getPartitionsForHost(int host) {
        return m_hostsToPartitions.get(host);
    }

    public List<Long> getHSIdsForHost(MailboxType type, int host) {
        Map<Integer, ArrayList<Long>> hostIdList = m_hostsToOtherHSIds.get(type);
        if (hostIdList == null) {
            return new ArrayList<Long>();
        }
        return hostIdList.get(host);
    }

    /**
     * XXX: remove this
     * @return
     */
    public List<Integer> getFailedPartitions() {
        throw new NotImplementedException("This method should be removed");
    }

    /**
     * XXX: remove this
     * @return
     */
    public Set<Integer> getAllDownHosts() {
        throw new NotImplementedException("This method should be removed");
    }
}
