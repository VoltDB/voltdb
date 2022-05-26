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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.ParameterSet;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTableRow;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SysProcFragmentId;

public class ReplicatedTableSaveFileState extends TableSaveFileState {
    ReplicatedTableSaveFileState(String tableName, long txnId) {
        super(tableName, txnId);
    }

    @Override
    void addHostData(VoltTableRow row) throws IOException {
        assert(row.getString("TABLE").equals(getTableName()));
        checkSiteConsistency(row); // throws if inconsistent
        // this cast should be safe; site_ids are ints but get
        // promoted to long in the VoltTable.row.getLong return
        m_hostsWithThisTable.add((int) row.getLong("CURRENT_HOST_ID"));
    }

    @Override
    public String debug() {
        StringBuilder builder = new StringBuilder("Replicated table ");
        builder.append(getTableName()).append(" exists on host ");
        for (Integer host : m_hostsWithThisTable) {
            builder.append(host).append(", ");
        }
        builder.setLength(builder.length() - 2);
        return builder.toString();
    }

    @Override
    public boolean isConsistent() {
        // right now there is nothing to check across all rows
        m_consistencyResult = "Table: " + getTableName() +
            " has consistent savefile state.";
        return true;
    }

    public Set<Integer> getHostsWithThisTable() {
        return m_hostsWithThisTable;
    }

    @Override public SynthesizedPlanFragment[]
            generateRestorePlan(SnapshotTableInfo table, SiteTracker st) {
        for (int hostId : m_hostsWithThisTable) {
            m_sitesWithThisTable.addAll(st.getSitesForHost(hostId));
        }

        return table.isReplicated() ? generateReplicatedToReplicatedPlan(st) : generateReplicatedToPartitionedPlan(st);
    }

    private void checkSiteConsistency(VoltTableRow row) throws IOException {
        if (! row.getString("IS_REPLICATED").equals("TRUE")) {
            String error = "Table: " + getTableName() + " was replicated " +
            "but has a savefile which indicates partitioning at site: " +
            row.getLong("CURRENT_HOST_ID");
            m_consistencyResult = error;
            throw new IOException(error);
        }
    }

    private SynthesizedPlanFragment[] generateReplicatedToPartitionedPlan(SiteTracker st) {
        SynthesizedPlanFragment[] restore_plan = null;

        Integer host = m_hostsWithThisTable.iterator().next();

        // replicated table is small enough for only one site to distribute the task
        restore_plan = new SynthesizedPlanFragment[2];
        int restore_plan_index = 0;
        assert(st.getSitesForHost(host).size() > 0);
        long site = st.getSitesForHost(host).get(0);
        restore_plan[restore_plan_index] = constructDistributeReplicatedTableAsPartitionedFragment(site);
        ++restore_plan_index;
        restore_plan[restore_plan_index] = constructLoadReplicatedTableAggregatorFragment(true);

        return restore_plan;
    }

    private SynthesizedPlanFragment
    constructDistributeReplicatedTableAsPartitionedFragment(long siteId) {
        int resultDependencyId = getNextDependencyId();
        ParameterSet parameters = ParameterSet.fromArrayNoCopy(
                getTableName(),
                resultDependencyId,
                getIsRecoverParam());
        return new SynthesizedPlanFragment(siteId, SysProcFragmentId.PF_restoreDistributeReplicatedTableAsPartitioned,
                resultDependencyId, false, parameters);
    }

    private SynthesizedPlanFragment[]
    generateReplicatedToReplicatedPlan(SiteTracker siteTracker) {
        Set<Long> allSiteIds = siteTracker.getAllSites();
        Set<Integer> hostsMissingTable = getHostsMissingTable(allSiteIds);
        // send a fragment to all the sites with this table to load the local copy of this table.
        // send a fragment to selected sites to send the table data to all the HOSTS
        // (not sites) without this table to load.
        int planFragmentCount = m_sitesWithThisTable.size() + hostsMissingTable.size() + 1;
        SynthesizedPlanFragment[] restorePlan = new SynthesizedPlanFragment[planFragmentCount];
        int planIndex = 0;
        for (Long siteId : m_sitesWithThisTable) {
            restorePlan[planIndex] = constructLoadReplicatedTableFragment();
            restorePlan[planIndex].siteId = siteId;
            ++planIndex;
        }
        Iterator<Long> sourceSitePicker = m_sitesWithThisTable.iterator();
        for (Integer hostId : hostsMissingTable) {
            if (! sourceSitePicker.hasNext()) {
                sourceSitePicker = m_sitesWithThisTable.iterator();
            }
            // pick a site to be responsible for sending the data.
            long sourceSiteId = sourceSitePicker.next();
            restorePlan[planIndex] =
                constructDistributeReplicatedTableAsReplicatedFragment(
                        sourceSiteId, hostId);
            ++planIndex;
        }
        assert(planIndex == planFragmentCount - 1);
        restorePlan[planIndex] =
            constructLoadReplicatedTableAggregatorFragment(false);
        return restorePlan;
    }

    private Set<Integer> getHostsMissingTable(Set<Long> clusterSiteIds) {
        Set<Integer> hostsMissingTable = new HashSet<Integer>();
        for (long siteId : clusterSiteIds) {
            if (! m_sitesWithThisTable.contains(siteId)) {
                hostsMissingTable.add(SiteTracker.getHostForSite(siteId));
            }
        }
        return hostsMissingTable;
    }

    private SynthesizedPlanFragment
    constructLoadReplicatedTableFragment() {
        int resultDependencyId = getNextDependencyId();
        ParameterSet parameters = ParameterSet.fromArrayNoCopy(
                getTableName(),
                resultDependencyId,
                getIsRecoverParam());
        return new SynthesizedPlanFragment(SysProcFragmentId.PF_restoreLoadReplicatedTable, resultDependencyId,
                false, parameters);
    }

    private SynthesizedPlanFragment
    constructDistributeReplicatedTableAsReplicatedFragment(long sourceSiteId,
                                                           int destHostId) {
        int resultDepId = getNextDependencyId();
        ParameterSet parameters = ParameterSet.fromArrayNoCopy(
                getTableName(),
                destHostId,
                resultDepId,
                getIsRecoverParam());
        return new SynthesizedPlanFragment(sourceSiteId,
                SysProcFragmentId.PF_restoreDistributeReplicatedTableAsReplicated, resultDepId, false,
                parameters);
    }

    private SynthesizedPlanFragment
    constructLoadReplicatedTableAggregatorFragment(boolean asPartitioned) {
        int resultDependencyId = getNextDependencyId();
        setRootDependencyId(resultDependencyId);
        ParameterSet parameters = ParameterSet.fromArrayNoCopy(resultDependencyId,
                (asPartitioned ? "Aggregating replicated-to-partitioned table restore results"
                               : "Aggregating replicated table restore results"),
                getIsRecoverParam());
        return new SynthesizedPlanFragment(SysProcFragmentId.PF_restoreReceiveResultTables, resultDependencyId, false,
                parameters);
    }

    private final Set<Integer> m_hostsWithThisTable = new HashSet<Integer>();
    private final Set<Long> m_sitesWithThisTable = new TreeSet<Long>();
}
