/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.HashMap;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.HsqlBackend;
import org.voltdb.OperationMode;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;

@ProcInfo(singlePartition = false)

public class Resume extends VoltSystemProcedure
{
    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
                     Procedure catProc, BackendTarget eeType, HsqlBackend hsql,
                     Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
    }

    @Override
    public DependencyPair executePlanFragment(
            HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        throw new RuntimeException("Resume was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    /**
     * Exit admin mode
     * @param ctx       Internal parameter. Not user-accessible.
     * @return          Standard STATUS table.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx)
    {
        // Choose the lowest site ID on this host to actually flip the bit
        int host_id = ctx.getExecutionSite().getCorrespondingHostId();
        Integer lowest_site_id =
            VoltDB.instance().getCatalogContext().siteTracker.
            getLowestLiveExecSiteIdForHost(host_id);
        if (ctx.getExecutionSite().getSiteId() == lowest_site_id)
        {
            VoltDB.instance().setMode(OperationMode.RUNNING);
        }

        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {t});
    }
}
