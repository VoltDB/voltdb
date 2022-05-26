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

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.ParameterSet;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTableRow;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SysProcFragmentId;



public class PartitionedTableSaveFileState extends TableSaveFileState
{
    private static final VoltLogger LOG = new VoltLogger(PartitionedTableSaveFileState.class.getName());
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    public PartitionedTableSaveFileState(String tableName, long txnId)
    {
        super(tableName, txnId);
    }

    @Override
    void addHostData(VoltTableRow row) throws IOException
    {
        assert(row.getString("TABLE").equals(getTableName()));

        if (m_totalPartitions == 0)
        {
            // XXX this cast should be okay unless we exceed MAX_INT partitions
            m_totalPartitions = (int) row.getLong("TOTAL_PARTITIONS");
        }
        checkSiteConsistency(row); // throws if inconsistent

        int originalPartitionId = (int) row.getLong("PARTITION");
        m_partitionsSeen.add(originalPartitionId);
        int currentHostId = (int) row.getLong("CURRENT_HOST_ID");
        Set<Pair<Integer, Integer>> partitions_at_host = null;
        if (!(m_partitionsAtHost.containsKey(currentHostId))) {
            partitions_at_host = new HashSet<Pair<Integer, Integer>>();
            m_partitionsAtHost.put( currentHostId, partitions_at_host);
        }
        partitions_at_host = m_partitionsAtHost.get(currentHostId);

        partitions_at_host.add(
                Pair.of(
                        originalPartitionId,
                        (int) row.getLong("ORIGINAL_HOST_ID")));
    }

    @Override
    public String debug() {
        StringBuilder builder = new StringBuilder("Partitioned table ");
        builder.append(getTableName()).append("\n");
        for (Entry<Integer, Set<Pair<Integer, Integer>>> entry : m_partitionsAtHost.entrySet()) {
            int hostId = entry.getKey();
            builder.append("Host ").append(hostId).append(" got (originalPartitionId, originalHostId): ");
            for (Pair<Integer, Integer> pair : entry.getValue()) {
                builder.append("(").append(pair.getFirst()).append(",")
                .append(pair.getSecond()).append(") ");
            }
        }
        return builder.toString();
    }

    @Override
    public boolean isConsistent()
    {
        boolean consistent =
            ((m_partitionsSeen.size() == m_totalPartitions) &&
             (m_partitionsSeen.first() == 0) &&
             (m_partitionsSeen.last() == m_totalPartitions - 1));
        if (!consistent)
        {
            m_consistencyResult = "Table: " + getTableName() +
                " is missing " + (m_totalPartitions - m_partitionsSeen.size()) +
                " out of " + m_totalPartitions + " total partitions" +
                " (partitions seen: " + m_partitionsSeen + ")";

        }
        else
        {
            m_consistencyResult = "Table: " + getTableName() +
                " has consistent savefile state.";
        }
        return consistent;
    }

    int getTotalPartitions()
    {
        return m_totalPartitions;
    }

    @Override
    public SynthesizedPlanFragment[]
            generateRestorePlan(SnapshotTableInfo table, SiteTracker st)
    {
        LOG.info("Total partitions for Table: " + getTableName() + ": " +
                 getTotalPartitions());
        return table.isReplicated() ? generatePartitionedToReplicatedPlan(st)
                : generatePartitionedToPartitionedPlan(st);
    }

    private void checkSiteConsistency(VoltTableRow row) throws IOException
    {
        if (!row.getString("IS_REPLICATED").equals("FALSE"))
        {
            String error = "Table: " + getTableName() + " was partitioned " +
            "but has a savefile which indicates replication at site: " +
            row.getLong("CURRENT_HOST_ID");
            m_consistencyResult = error;
            throw new IOException(error);
        }

        if ((int) row.getLong("TOTAL_PARTITIONS") != getTotalPartitions())
        {
            String error = "Table: " + getTableName() + " has a savefile " +
            " with an inconsistent number of total partitions: " +
            row.getLong("TOTAL_PARTITIONS") + " (previous values were " +
            getTotalPartitions() + ") at site: " +
            row.getLong("CURRENT_HOST_ID");
            m_consistencyResult = error;
            throw new IOException(error);
        }
    }

    private SynthesizedPlanFragment[] generatePartitionedToReplicatedPlan(SiteTracker st) {
        ArrayList<SynthesizedPlanFragment> restorePlan = new ArrayList<SynthesizedPlanFragment>();
        Set<Integer> coveredPartitions = new HashSet<Integer>();

        Iterator<Entry<Integer, Set<Pair<Integer, Integer>>>> partitionAtHostItr =
                m_partitionsAtHost.entrySet().iterator();

        // looping through all current hosts having .vpt files of this table
        while(partitionAtHostItr.hasNext()) {
            Entry<Integer, Set<Pair<Integer, Integer>>> partitionAtHost = partitionAtHostItr.next();
            Integer host = partitionAtHost.getKey();
            List<Integer> loadPartitions = new ArrayList<Integer>();
            List<Integer> loadOrigHosts = new ArrayList<Integer>();
            Set<Pair<Integer, Integer>> partitionAndOrigHostSet = partitionAtHost.getValue();
            Iterator<Pair<Integer, Integer>> itr = partitionAndOrigHostSet.iterator();

            // calculate which available partitions not yet been covered and put
            // its partition_id and orig_host_id in loadPartitions and loadOrigHosts
            while(itr.hasNext()) {
                Pair<Integer, Integer> pair = itr.next();
                if(!coveredPartitions.contains(pair.getFirst())) {
                    loadPartitions.add(pair.getFirst());
                    loadOrigHosts.add(pair.getSecond());
                    coveredPartitions.add(pair.getFirst());
                }
            }

            // if there are some work to do
            if(loadPartitions.size() > 0){
                int[] relevantPartitionIds = com.google_voltpatches.common.primitives.Ints.toArray(loadPartitions);
                int[] originalHosts = com.google_voltpatches.common.primitives.Ints.toArray(loadOrigHosts);
                List<Long> sitesAtHost = st.getSitesForHost(host);

                // for each site of this host, generate one work fragment and let them execute in parallel
                for(Long site : sitesAtHost) {
                    restorePlan.add(constructDistributePartitionedTableFragment(
                            site, relevantPartitionIds, originalHosts, true));
                }
            }
        }
        restorePlan.add(constructDistributePartitionedTableAggregatorFragment(true));
        assert(coveredPartitions.size() == m_partitionsSeen.size());
        return restorePlan.toArray(new SynthesizedPlanFragment[restorePlan.size()]);
    }

    private SynthesizedPlanFragment[] generatePartitionedToPartitionedPlan(SiteTracker st) {
        LOG.info("Partition set: " + m_partitionsSeen);
        ArrayList<SynthesizedPlanFragment> restorePlan = new ArrayList<SynthesizedPlanFragment>();
        HashSet<Integer> coveredPartitions = new HashSet<Integer>();

        HashMap<Integer, ArrayList<Integer>> hostsToUncoveredPartitions = new HashMap<Integer, ArrayList<Integer>>();
        HashMap<Integer, ArrayList<Integer>> hostsToOriginalHosts = new HashMap<Integer, ArrayList<Integer>>();

        for (Integer host : m_partitionsAtHost.keySet()) {
            hostsToUncoveredPartitions.put(host, new ArrayList<Integer>());
            hostsToOriginalHosts.put(host, new ArrayList<Integer>());
        }

        /*
         * Loop through the list of hosts repeatedly. Each time pick only one
         * partition to distribute from each host. This ensures some load
         * balancing.
         */
        while (!coveredPartitions.containsAll(m_partitionsSeen)) {
            Iterator<Integer> hosts = m_partitionsAtHost.keySet().iterator();
            // Track if progress was made, if nothing to distribute
            // was found and we aren't covering then it is missing partitions
            int numPartitionsUsed = 0;
            while (hosts.hasNext()) {
                /**
                 * Get the list of partitions on this host and remove all that
                 * were covered already
                 */
                Integer nextHost = hosts.next();
                Set<Pair<Integer, Integer>> partitionsAndOrigHosts = new HashSet<Pair<Integer, Integer>>(
                        m_partitionsAtHost.get(nextHost));
                Iterator<Pair<Integer, Integer>> removeCoveredIterator = partitionsAndOrigHosts
                        .iterator();

                List<Integer> uncoveredPartitionsAtHostList = hostsToUncoveredPartitions
                        .get(nextHost);
                ArrayList<Integer> originalHosts = hostsToOriginalHosts
                        .get(nextHost);
                while (removeCoveredIterator.hasNext()) {
                    Pair<Integer, Integer> p = removeCoveredIterator.next();
                    if (coveredPartitions.contains(p.getFirst())) {
                        removeCoveredIterator.remove();
                    }
                }

                /*
                 * If there is a partition left that isn't covered select it for
                 * distribution
                 */
                Iterator<Pair<Integer, Integer>> candidatePartitions = partitionsAndOrigHosts
                        .iterator();
                if (candidatePartitions.hasNext()) {
                    Pair<Integer, Integer> p = candidatePartitions.next();
                    coveredPartitions.add(p.getFirst());
                    uncoveredPartitionsAtHostList.add(p.getFirst());
                    originalHosts.add(p.getSecond());
                    numPartitionsUsed++;
                }
            }
            if (numPartitionsUsed == 0
                    && !coveredPartitions.containsAll(m_partitionsSeen)) {
                LOG.error("Could not find a host to distribute some partitions");
                return null;
            }
        }

        SNAP_LOG.info("Distribution plan for table " + getTableName());
        for (Integer host : m_partitionsAtHost.keySet()) {
            List<Integer> uncoveredPartitionsAtHostList = hostsToUncoveredPartitions
                    .get(host);
            ArrayList<Integer> originalHosts = hostsToOriginalHosts.get(host);

            List<Long> sitesAtHost = VoltDB.instance().getSiteTrackerForSnapshot()
                    .getSitesForHost(host);

            int originalHostsArray[] = new int[originalHosts.size()];
            int qq = 0;
            for (int originalHostId : originalHosts) {
                originalHostsArray[qq++] = originalHostId;
            }
            int uncoveredPartitionsAtHost[] = new int[uncoveredPartitionsAtHostList
                    .size()];
            for (int ii = 0; ii < uncoveredPartitionsAtHostList.size(); ii++) {
                uncoveredPartitionsAtHost[ii] = uncoveredPartitionsAtHostList
                        .get(ii);
            }

            StringBuilder sb = new StringBuilder();
            sb.append("\tHost ").append(host)
                    .append(" will distribute partitions ");
            for (Integer partition : uncoveredPartitionsAtHostList) {
                sb.append(partition).append(' ');
            }
            SNAP_LOG.info(sb.toString());

            /*
             * Assigning the FULL workload to each site. At the actual host
             * static synchronization in the procedure will ensure the work is
             * distributed across every ES in a meaningful way.
             */
            for (Long site : sitesAtHost) {
                restorePlan.add(constructDistributePartitionedTableFragment(
                        site, uncoveredPartitionsAtHost, originalHostsArray, false));
            }
        }
        restorePlan
                .add(constructDistributePartitionedTableAggregatorFragment(false));
        return restorePlan.toArray(new SynthesizedPlanFragment[restorePlan.size()]);
    }

    private SynthesizedPlanFragment
    constructDistributePartitionedTableFragment(
            long distributorSiteId,     // site which will execute this plan fragment
            int uncoveredPartitionsAtHost[],    // which partitions' data in the .vpt files will be extracted as TableSaveFile
            int originalHostsArray[],           // used to locate .vpt files
            boolean asReplicated)
    {
        int fragmentId = (asReplicated ? SysProcFragmentId.PF_restoreDistributePartitionedTableAsReplicated
                : SysProcFragmentId.PF_restoreDistributePartitionedTableAsPartitioned);
        int resultDependencyId = getNextDependencyId();
        ParameterSet params = ParameterSet.fromArrayNoCopy(getTableName(), originalHostsArray,
                uncoveredPartitionsAtHost, resultDependencyId, getIsRecoverParam());

        return new SynthesizedPlanFragment(distributorSiteId, fragmentId, resultDependencyId, false, params);
    }

    private SynthesizedPlanFragment
    constructDistributePartitionedTableAggregatorFragment(boolean asReplicated)
    {
        int resultDependencyId = getNextDependencyId();
        setRootDependencyId(resultDependencyId);
        ParameterSet parameters = ParameterSet.fromArrayNoCopy(
                resultDependencyId,
                (asReplicated ?
                        "Aggregating partitioned-to-replicated table restore results"
                        : "Aggregating partitioned table restore results"),
                getIsRecoverParam());
        return new SynthesizedPlanFragment(SysProcFragmentId.PF_restoreReceiveResultTables, resultDependencyId,
                false, parameters);
    }

    // XXX-BLAH should this move to SiteTracker?
    public Set<Pair<Integer, Integer>> getPartitionsAtHost(int hostId) {
        return m_partitionsAtHost.get(hostId);
    }

    Set<Integer> getPartitionSet()
    {
        return m_partitionsSeen;
    }

    /**
     * Set of original PartitionId
     */
    private final TreeSet<Integer> m_partitionsSeen =
          new TreeSet<Integer>();

    /**
     * Map from a current host id to a pair of an original
     * partition id and the original host id
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> m_partitionsAtHost =
        new HashMap<Integer, Set<Pair<Integer, Integer>>>();
    private int m_totalPartitions = 0;
}
