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

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.VoltTableRow;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;

public abstract class TableSaveFileState
{
    // XXX This look a lot like similar stuff hiding in PlanAssembler.  I bet
    // there's no easy way to consolidate it, though.
    private static int NEXT_DEPENDENCY_ID = 1;

    public synchronized static int getNextDependencyId()
    {
        return NEXT_DEPENDENCY_ID++;
    }

    // XXX THIS METHOD DOES NOT BELONG HERE BUT SHOULD PROBABLY LIVE IN THE
    // CATALOG SOMEWHERE.  HOWEVER, I DON'T FEEL LIKE FINDING A HOME FOR IT
    // AT THE MOMENT --izzy
    static Set<Integer> getExecutionSiteIds(CatalogMap<Site> clusterSites)
    {
        Set<Integer> exec_sites = new HashSet<Integer>(clusterSites.size());
        for (Site site : clusterSites)
        {
            if (site.getIsexec())
            {
                exec_sites.add(Integer.parseInt(site.getTypeName()));
            }
        }
        return exec_sites;
    }

    TableSaveFileState(String tableName, int allowELT)
    {
        m_tableName = tableName;
        m_planDependencyIds = new HashSet<Integer>();
        m_allowELT = allowELT;
    }

    abstract public SynthesizedPlanFragment[]
    generateRestorePlan(Table catalogTable,
                        CatalogMap<Site> clusterSites);

    String getTableName()
    {
        return m_tableName;
    }

    abstract void addHostData(VoltTableRow row) throws IOException;

    public abstract boolean isConsistent();

    void addPlanDependencyId(int dependencyId)
    {
        m_planDependencyIds.add(dependencyId);
    }

    int[] getPlanDependencyIds()
    {
        int[] unboxed_ids = new int[m_planDependencyIds.size()];
        int id_index = 0;
        for (int id : m_planDependencyIds)
        {
            unboxed_ids[id_index] = id;
            id_index++;
        }
        return unboxed_ids;
    }

    void setRootDependencyId(int dependencyId)
    {
        m_rootDependencyId = dependencyId;
    }

    public int getRootDependencyId()
    {
        return m_rootDependencyId;
    }

    protected HashMap<Integer, List<Integer>> getHostToSitesMap(
            CatalogMap<Site> clusterSites) {
        HashMap<Integer, List<Integer>> hostToSitesMap =
            new HashMap<Integer, List<Integer>>();
        for (Site s : clusterSites) {
            int hostId = Integer.parseInt(s.getHost().getTypeName());
            List<Integer> sites = hostToSitesMap.get(hostId);
            if (sites == null) {
                sites = new ArrayList<Integer>();
                hostToSitesMap.put(hostId, sites);
            }
            if (s.getIsexec()) {
                sites.add(Integer.parseInt(s.getTypeName()));
            }
        }
        return hostToSitesMap;
    }

    private String m_tableName;
    private Set<Integer> m_planDependencyIds;
    final int m_allowELT;
    int m_rootDependencyId;
}
