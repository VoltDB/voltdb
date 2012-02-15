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

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Table;
import org.voltcore.logging.VoltLogger;
import org.voltdb.sysprocs.SysProcFragmentId;
import org.voltcore.utils.Pair;



public class PartitionedTableSaveFileState extends TableSaveFileState
{
    private static final VoltLogger LOG = new VoltLogger(PartitionedTableSaveFileState.class.getName());
    private static final VoltLogger hostLog = new VoltLogger("HOST");

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
    generateRestorePlan(Table catalogTable)
    {
        SynthesizedPlanFragment[] restore_plan = null;
        LOG.info("Total partitions for Table: " + getTableName() + ": " +
                 getTotalPartitions());
        if (!catalogTable.getIsreplicated())
        {
            restore_plan = generatePartitionedToPartitionedPlan();
        }
        else
        {
            // XXX Not implemented until we're going to support catalog changes
        }
        return restore_plan;
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

    private SynthesizedPlanFragment[] generatePartitionedToPartitionedPlan() {
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

        hostLog.info("Distribution plan for table " + getTableName());
        for (Integer host : m_partitionsAtHost.keySet()) {
            List<Integer> uncoveredPartitionsAtHostList = hostsToUncoveredPartitions
                    .get(host);
            ArrayList<Integer> originalHosts = hostsToOriginalHosts.get(host);

            List<Long> sitesAtHost = VoltDB.instance().getSiteTracker()
                    .getSitesForHost(host);

            int originalHostsArray[] = new int[originalHosts.size()];
            int qq = 0;
            for (int originalHostId : originalHosts)
                originalHostsArray[qq++] = originalHostId;
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
            hostLog.info(sb.toString());

            /*
             * Assigning the FULL workload to each site. At the actual host
             * static synchronization in the procedure will ensure the work is
             * distributed across every ES in a meaningful way.
             */
            for (Long site : sitesAtHost) {
                restorePlan.add(constructDistributePartitionedTableFragment(
                        site, uncoveredPartitionsAtHost, originalHostsArray));
            }
        }
        restorePlan
                .add(constructDistributePartitionedTableAggregatorFragment());
        return restorePlan.toArray(new SynthesizedPlanFragment[0]);
    }

    private SynthesizedPlanFragment
    constructDistributePartitionedTableFragment(
            long distributorSiteId,
            int uncoveredPartitionsAtHost[],
            int originalHostsArray[])
    {
        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId =
            SysProcFragmentId.PF_restoreDistributePartitionedTable;
        plan_fragment.multipartition = false;
        plan_fragment.siteId = distributorSiteId;
        plan_fragment.outputDepId = result_dependency_id;
        plan_fragment.inputDepIds = new int[] {};
        addPlanDependencyId(result_dependency_id);
        ParameterSet params = new ParameterSet();
        params.setParameters(getTableName(),
                             originalHostsArray,
                             uncoveredPartitionsAtHost,
                             result_dependency_id);
        plan_fragment.parameters = params;
        return plan_fragment;
    }

    private SynthesizedPlanFragment
    constructDistributePartitionedTableAggregatorFragment()
    {
        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId =
            SysProcFragmentId.PF_restoreDistributePartitionedTableResults;
        plan_fragment.multipartition = false;
        plan_fragment.outputDepId = result_dependency_id;
        plan_fragment.inputDepIds = getPlanDependencyIds();
        setRootDependencyId(result_dependency_id);
        ParameterSet params = new ParameterSet();
        params.setParameters(result_dependency_id);
        plan_fragment.parameters = params;
        return plan_fragment;
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
