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

import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;

public abstract class TableSaveFileState
{
    // XXX This look a lot like similar stuff hiding in PlanAssembler.  I bet
    // there's no easy way to consolidate it, though.
    private static int NEXT_DEPENDENCY_ID = 1;

    public synchronized static int getNextDependencyId()
    {
        return NEXT_DEPENDENCY_ID++;
    }

    TableSaveFileState(String tableName, long txnId)
    {
        m_tableName = tableName;
        m_txnId = txnId;
        m_consistencyResult = "Table: " + m_tableName + " not yet processed";
        m_planDependencyIds = new HashSet<Integer>();
    }

    abstract public SynthesizedPlanFragment[]
    generateRestorePlan(Table catalogTable, SiteTracker st);

    String getTableName()
    {
        return m_tableName;
    }

    long getTxnId()
    {
        return m_txnId;
    }

    public String getConsistencyResult()
    {
        return m_consistencyResult;
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

    int m_rootDependencyId;
    protected String m_consistencyResult;
    private final String m_tableName;
    private final long m_txnId;
    private final Set<Integer> m_planDependencyIds;
}
