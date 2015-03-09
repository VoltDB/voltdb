/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.ParameterSet;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SysProcFragmentId;

public class ReplicatedTableSaveFileState extends TableSaveFileState
{
    ReplicatedTableSaveFileState(String tableName, long txnId)
    {
        super(tableName, txnId);
    }

    @Override
    void addHostData(VoltTableRow row) throws IOException
    {
        assert(row.getString("TABLE").equals(getTableName()));

        checkSiteConsistency(row); // throws if inconsistent
        // this cast should be safe; site_ids are ints but get
        // promoted to long in the VoltTable.row.getLong return
        m_hostsWithThisTable.add((int) row.getLong("CURRENT_HOST_ID"));
    }

    @Override
    public boolean isConsistent()
    {
        // right now there is nothing to check across all rows
        m_consistencyResult = "Table: " + getTableName() +
            " has consistent savefile state.";
        return true;
    }

    public Set<Integer> getHostsWithThisTable() {
        return m_hostsWithThisTable;
    }

    @Override
    public SynthesizedPlanFragment[]
    generateRestorePlan(Table catalogTable, SiteTracker st)
    {
        for (int hostId : m_hostsWithThisTable) {
            m_sitesWithThisTable.addAll(st.getSitesForHost(hostId));
        }

        SynthesizedPlanFragment[] restore_plan = null;
        if (catalogTable.getIsreplicated())
        {
            restore_plan = generateReplicatedToReplicatedPlan(st);
        }
        else
        {
            restore_plan = generateReplicatedToPartitionedPlan(st);
        }
        return restore_plan;
    }

    private void checkSiteConsistency(VoltTableRow row) throws IOException
    {
        if (!row.getString("IS_REPLICATED").equals("TRUE"))
        {
            String error = "Table: " + getTableName() + " was replicated " +
            "but has a savefile which indicates partitioning at site: " +
            row.getLong("CURRENT_HOST_ID");
            m_consistencyResult = error;
            throw new IOException(error);
        }
    }

    private SynthesizedPlanFragment[]
    generateReplicatedToPartitionedPlan(SiteTracker st)
    {
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
    constructDistributeReplicatedTableAsPartitionedFragment(long siteId)
    {
        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId =
            SysProcFragmentId.PF_restoreDistributeReplicatedTableAsPartitioned;
        plan_fragment.multipartition = false;
        plan_fragment.siteId = siteId;
        plan_fragment.outputDepId = result_dependency_id;
        plan_fragment.inputDepIds = new int[] {};
        addPlanDependencyId(result_dependency_id);
        plan_fragment.parameters = ParameterSet.fromArrayNoCopy(
                getTableName(),
                result_dependency_id);
        return plan_fragment;
    }

    private SynthesizedPlanFragment[]
    generateReplicatedToReplicatedPlan(SiteTracker st)
    {
        SynthesizedPlanFragment[] restore_plan = null;
        Set<Long> execution_site_ids =
            st.getAllSites();
        Set<Long> sites_missing_table =
            getSitesMissingTable(execution_site_ids);
        // not sure we want to deal with handling expected load failures,
        // so let's send an individual load to each site with the table
        // and then pick sites to send the table to those without it
        restore_plan =
            new SynthesizedPlanFragment[execution_site_ids.size() + 1];
        int restore_plan_index = 0;
        for (Long site_id : m_sitesWithThisTable)
        {
            restore_plan[restore_plan_index] =
                constructLoadReplicatedTableFragment();
            restore_plan[restore_plan_index].siteId = site_id;
            ++restore_plan_index;
        }
        for (Long site_id : sites_missing_table)
        {
            long source_site_id =
                m_sitesWithThisTable.iterator().next();
            restore_plan[restore_plan_index] =
                constructDistributeReplicatedTableAsReplicatedFragment(source_site_id,
                                                           site_id);
            ++restore_plan_index;
        }
        assert(restore_plan_index == execution_site_ids.size());
        restore_plan[restore_plan_index] =
            constructLoadReplicatedTableAggregatorFragment(false);
        return restore_plan;
    }

    private Set<Long> getSitesMissingTable(Set<Long> clusterSiteIds)
    {
        Set<Long> sites_missing_table = new HashSet<Long>();
        for (long site_id : clusterSiteIds)
        {
            if (!m_sitesWithThisTable.contains(site_id))
            {
                sites_missing_table.add(site_id);
            }
        }
        return sites_missing_table;
    }

    private SynthesizedPlanFragment
    constructLoadReplicatedTableFragment()
    {
        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId =
            SysProcFragmentId.PF_restoreLoadReplicatedTable;
        plan_fragment.multipartition = false;
        plan_fragment.outputDepId = result_dependency_id;
        plan_fragment.inputDepIds = new int[] {};
        addPlanDependencyId(result_dependency_id);
        plan_fragment.parameters = ParameterSet.fromArrayNoCopy(
                getTableName(),
                result_dependency_id);
        return plan_fragment;
    }

    private SynthesizedPlanFragment
    constructDistributeReplicatedTableAsReplicatedFragment(long sourceSiteId,
                                                           long destinationSiteId)
    {
        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId =
            SysProcFragmentId.PF_restoreDistributeReplicatedTableAsReplicated;
        plan_fragment.multipartition = false;
        plan_fragment.siteId = sourceSiteId;
        plan_fragment.outputDepId = result_dependency_id;
        plan_fragment.inputDepIds = new int[] {};
        addPlanDependencyId(result_dependency_id);
        plan_fragment.parameters = ParameterSet.fromArrayNoCopy(
                getTableName(),
                destinationSiteId,
                result_dependency_id);
        return plan_fragment;
    }

    private SynthesizedPlanFragment
    constructLoadReplicatedTableAggregatorFragment(boolean asPartitioned)
    {
        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId =
            SysProcFragmentId.PF_restoreReceiveResultTables;
        plan_fragment.multipartition = false;
        plan_fragment.outputDepId = result_dependency_id;
        plan_fragment.inputDepIds = getPlanDependencyIds();
        setRootDependencyId(result_dependency_id);
        plan_fragment.parameters = ParameterSet.fromArrayNoCopy(result_dependency_id,
                (asPartitioned ? "Aggregating replicated-to-partitioned table restore results"
                               : "Aggregating replicated table restore results"));
        return plan_fragment;
    }

    private final Set<Integer> m_hostsWithThisTable = new HashSet<Integer>();
    private final Set<Long> m_sitesWithThisTable = new TreeSet<Long>();
}
