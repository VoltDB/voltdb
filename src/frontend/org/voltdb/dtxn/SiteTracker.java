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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.voltcore.utils.CoreUtils;
import org.voltdb.MailboxNodeContent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK.MailboxType;

public class SiteTracker {
    private final int m_hostId;
    private boolean m_isFirstHost;

    public final int m_version;

    private final Set<Integer> m_allHosts = new HashSet<Integer>();
    public final Set<Integer> m_allHostsImmutable = Collections.unmodifiableSet(m_allHosts);

    /*
     * Includes initiator sites, execution sites, and stats agents. Not really "all"
     */
    private final Set<Long> m_allSites = new HashSet<Long>();
    public final Set<Long> m_allSitesImmutable = Collections.unmodifiableSet(m_allSites);

    private final Set<Long> m_allExecutionSites = new HashSet<Long>();
    public final Set<Long> m_allExecutionSitesImmutable = Collections.unmodifiableSet(m_allExecutionSites);
    private final long m_allExecutionSitesArray[];

    private final Set<Long> m_allInitiators = new HashSet<Long>();
    public final Set<Long> m_allInitiatorsImmutable = Collections.unmodifiableSet(m_allInitiators);

    private final Set<Long> m_allIv2Initiators = new HashSet<Long>();
    public final Set<Long> m_allIv2InitiatorsImmutable = Collections.unmodifiableSet(m_allIv2Initiators);

    public final Map<Integer, List<Long>> m_hostsToSitesImmutable;


    public final Map<Integer, List<Integer>> m_hostsToPartitionsImmutable;


    public final Map<Integer, List<Long>> m_partitionsToSitesImmutable;

    public final Map<Integer, List<Long>> m_partitionToInitiatorsImmutable;

    public final Map<Long, Integer> m_sitesToPartitionsImmutable;


    public final Map<Integer, List<Long>> m_hostsToInitiatorsImmutable;

    public final Map<MailboxType, List<Long>> m_otherHSIdsImmutable;

    public final Map<MailboxType, Map<Integer, List<Long>>> m_hostsToOtherHSIdsImmutable;

    public final int m_numberOfPartitions;
    public final int m_numberOfHosts;
    public final int m_numberOfExecutionSites;

    private final int m_allPartitions[];

    private long m_statsAgents[];

    public SiteTracker() {
        m_allExecutionSitesArray = null;
        m_allPartitions = null;
        m_hostId = 0;
        m_hostsToInitiatorsImmutable = null;
        m_hostsToOtherHSIdsImmutable = null;
        m_partitionsToSitesImmutable = null;
        m_hostsToPartitionsImmutable = null;
        m_hostsToSitesImmutable = null;
        m_numberOfHosts = 1;
        m_numberOfExecutionSites = 0;
        m_numberOfPartitions = 0;
        m_otherHSIdsImmutable = null;
        m_sitesToPartitionsImmutable = null;
        m_partitionToInitiatorsImmutable = null;
        m_version = 0;
    }

    public SiteTracker(int hostId, Map<MailboxType, List<MailboxNodeContent>> mailboxes) {
        this(hostId, mailboxes, 0);
    }

    public SiteTracker(int hostId, Map<MailboxType, List<MailboxNodeContent>> mailboxes, int version) {
        m_version = version;
        m_hostId = hostId;
        Map<Integer, List<Long>> hostsToSites =
            new HashMap<Integer, List<Long>>();
        Map<Integer, List<Integer>> hostsToPartitions =
            new HashMap<Integer, List<Integer>>();
        Map<Integer, List<Long>> partitionsToSites =
            new HashMap<Integer, List<Long>>();
        Map<Long, Integer> sitesToPartitions =
            new HashMap<Long, Integer>();
        Map<Integer, List<Long>> hostsToInitiators =
            new HashMap<Integer, List<Long>>();
        Map<MailboxType, List<Long>> otherHSIds =
            new HashMap<MailboxType, List<Long>>();
        Map<Integer, List<Long>> partitionToInitiators =
            new HashMap<Integer, List<Long>>();

        Map<MailboxType, Map<Integer, List<Long>>> hostsToOtherHSIds =
            new HashMap<MailboxType, Map<Integer, List<Long>>>();
        for (Entry<MailboxType, List<MailboxNodeContent>> e : mailboxes.entrySet()) {
            if (e.getKey().equals(MailboxType.ExecutionSite)) {
                populateSites(e.getValue(), hostsToSites, hostsToPartitions, partitionsToSites, sitesToPartitions);
            } else if (e.getKey().equals(MailboxType.Initiator)) {
                populateInitiators(e.getValue(), hostsToInitiators,
                                   partitionToInitiators);
            } if (e.getKey().equals(MailboxType.StatsAgent)) {
                populateStatsAgents(e.getValue());
            } else {
                populateOtherHSIds(e.getKey(), e.getValue(), otherHSIds, hostsToOtherHSIds);
            }
        }

        m_hostsToSitesImmutable = CoreUtils.unmodifiableMapCopy(hostsToSites);
        m_hostsToPartitionsImmutable = CoreUtils.unmodifiableMapCopy(hostsToPartitions);
        m_partitionsToSitesImmutable = CoreUtils.unmodifiableMapCopy(partitionsToSites);
        m_sitesToPartitionsImmutable = Collections.unmodifiableMap(sitesToPartitions);
        m_hostsToInitiatorsImmutable = CoreUtils.unmodifiableMapCopy(hostsToInitiators);
        m_otherHSIdsImmutable = CoreUtils.unmodifiableMapCopy(otherHSIds);
        m_partitionToInitiatorsImmutable =
            CoreUtils.unmodifiableMapCopy(partitionToInitiators);

        Map<MailboxType, Map<Integer, List<Long>>> hostsToOtherHSIdsReplacement =
            new HashMap<MailboxType, Map<Integer, List<Long>>>();
        for (Map.Entry<MailboxType, Map<Integer, List<Long>>> e : hostsToOtherHSIds.entrySet()) {
            hostsToOtherHSIds.put(e.getKey(), CoreUtils.unmodifiableMapCopy(e.getValue()));
        }
        m_hostsToOtherHSIdsImmutable = Collections.unmodifiableMap(hostsToOtherHSIdsReplacement);
        m_allExecutionSitesArray = new long[m_allExecutionSites.size()];
        int ii = 0;
        for (Long site : m_allExecutionSites) {
            m_allExecutionSitesArray[ii++] = site;
        }
        m_numberOfPartitions = m_partitionsToSitesImmutable.keySet().size();
        m_numberOfHosts = m_hostsToSitesImmutable.keySet().size();
        m_numberOfExecutionSites = m_sitesToPartitionsImmutable.keySet().size();
        m_allPartitions = new int[m_numberOfPartitions];
        ii = 0;
        for (Integer partition : m_partitionsToSitesImmutable.keySet()) {
            m_allPartitions[ii++] = partition;
        }
        m_allSites.addAll(m_allExecutionSites);
        m_allSites.addAll(m_allInitiators);
        m_allSites.addAll(m_allIv2Initiators);
        for (List<Long> siteIds : otherHSIds.values()) {
            m_allSites.addAll(siteIds);
        }
    }

    public int[] getAllPartitions() {
        return Arrays.copyOf(m_allPartitions, m_allPartitions.length);
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
        return Arrays.copyOf(m_statsAgents, m_statsAgents.length);
    }

    private void populateSites(
            List<MailboxNodeContent> objs,
            Map<Integer, List<Long>> hostsToSites,
            Map<Integer, List<Integer>> hostsToPartitions,
            Map<Integer, List<Long>> partitionsToSites,
            Map<Long, Integer> sitesToPartitions) {
        int firstHostId = -1;
        for (MailboxNodeContent obj : objs) {
            int hostId = CoreUtils.getHostIdFromHSId(obj.HSId);

            if (firstHostId == -1) {
                firstHostId = hostId;
            }

            List<Long> hostSiteList = hostsToSites.get(hostId);
            if (hostSiteList == null)
            {
                hostSiteList = new ArrayList<Long>();
                hostsToSites.put(hostId, hostSiteList);
            }
            hostSiteList.add(obj.HSId);

            List<Integer> hostPartList = hostsToPartitions.get(hostId);
            if (hostPartList == null) {
                hostPartList = new ArrayList<Integer>();
                hostsToPartitions.put(hostId, hostPartList);
            }
            hostPartList.add(obj.partitionId);

            List<Long> partSiteList = partitionsToSites.get(obj.partitionId);
            if (partSiteList == null) {
                partSiteList = new ArrayList<Long>();
                partitionsToSites.put(obj.partitionId, partSiteList);
            }
            partSiteList.add(obj.HSId);


            m_allHosts.add(hostId);
            m_allExecutionSites.add(obj.HSId);
            sitesToPartitions.put(obj.HSId, obj.partitionId);
        }
        m_isFirstHost = (m_hostId == firstHostId);
    }

    private void populateInitiators(List<MailboxNodeContent> objs,
                                    Map<Integer, List<Long>> hostsToInitiators,
                                    Map<Integer, List<Long>> partitionToInitiators)
    {
        for (MailboxNodeContent obj : objs) {
            int hostId = CoreUtils.getHostIdFromHSId(obj.HSId);

            List<Long> initiators = hostsToInitiators.get(hostId);
            if (initiators == null) {
                initiators = new ArrayList<Long>();
                hostsToInitiators.put(hostId, initiators);
            }
            if (obj.partitionId == null) {
                initiators.add(obj.HSId);
                m_allInitiators.add(obj.HSId);
            } else {
                List<Long> initiators_for_part =
                    partitionToInitiators.get(obj.partitionId);
                if (initiators_for_part == null) {
                    initiators_for_part = new ArrayList<Long>();
                    partitionToInitiators.put(obj.partitionId,
                                              initiators_for_part);
                }
                initiators_for_part.add(obj.HSId);
                m_allIv2Initiators.add(obj.HSId);
            }
        }
    }

    private void populateOtherHSIds(MailboxType type, List<MailboxNodeContent> objs,
            Map<MailboxType, List<Long>> otherHSIds,
            Map<MailboxType, Map<Integer, List<Long>>> hostsToOtherHSIds) {
        List<Long> hsids = otherHSIds.get(type);
        if (hsids == null) {
            hsids = new ArrayList<Long>();
            otherHSIds.put(type, hsids);
        }

        Map<Integer, List<Long>> hostToIds = hostsToOtherHSIds.get(type);
        if (hostToIds == null) {
            hostToIds = new HashMap<Integer, List<Long>>();
            hostsToOtherHSIds.put(type, hostToIds);
        }

        for (MailboxNodeContent obj : objs) {
            int hostId = CoreUtils.getHostIdFromHSId(obj.HSId);

            hsids.add(obj.HSId);

            List<Long> hostIdList = hostToIds.get(hostId);
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
        return m_allExecutionSitesImmutable;
    }

    public long[] getAllSitesExcluding(long site) {
        long allSitesMinusOne[] = new long[m_allExecutionSitesArray.length - 1];
        int zz = 0;
        for (int ii = 0; ii < m_allExecutionSitesArray.length; ii++) {
            if (m_allExecutionSitesArray[ii] == site) {
                continue;
            }
            allSitesMinusOne[zz++] =  m_allExecutionSitesArray[ii];
        }
        return allSitesMinusOne;
    }

    public Set<Integer> getAllHosts() {
        return m_allHostsImmutable;
    }

    public Set<Long> getAllInitiators() {
        return m_allInitiatorsImmutable;
    }

    /**
     * Get the ids of all live sites that contain a copy of the
     * partition specified.
     *
     * @param partition A VoltDB partition id.
     * @return An array of VoltDB site ids.
     */
    public List<Long> getSitesForPartition(int partition) {
        return m_partitionsToSitesImmutable.get(partition);
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
        return m_hostsToSitesImmutable.get(hostId);
    }

    /**
     * Get the host id for a specific site
     * @param siteid
     * @return Integer host id for that site
     */
    public static int getHostForSite(long siteId) {
        return CoreUtils.getHostIdFromHSId(siteId);
    }

    /**
     * Return the id of the partition stored by a given site.
     *
     * @param siteId The id of a VoltDB site.
     * @return The id of the partition stored at the given site.
     */
    public int getPartitionForSite(long siteId)
    {
        return m_sitesToPartitionsImmutable.get(siteId);
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
        return longListToArray(m_hostsToSitesImmutable.get(hostId));
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
                retval[i++] = longListToArray(m_hostsToSitesImmutable.get(host));
            }
        }
        return retval;
    }

    public List<Long> getInitiatorsForHost(int host) {
        return m_hostsToInitiatorsImmutable.get(host);
    }

    public Map<Long, Integer> getSitesToPartitions() {
        return m_sitesToPartitionsImmutable;
    }

    public List<Integer> getPartitionsForHost(int host) {
        return m_hostsToPartitionsImmutable.get(host);
    }

    public List<Long> getHSIdsForHost(MailboxType type, int host) {
        Map<Integer, List<Long>> hostIdList = m_hostsToOtherHSIdsImmutable.get(type);
        if (hostIdList == null) {
            return new ArrayList<Long>();
        }
        return hostIdList.get(host);
    }
}
