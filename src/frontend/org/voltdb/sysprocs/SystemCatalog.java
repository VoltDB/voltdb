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
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.logging.VoltLogger;

/**
 * Return VoltTable results that correspond to JDBC result sets for selected
 * methods on DatabaseMetaData
 */
@ProcInfo(
    singlePartition = true
)

public class SystemCatalog extends VoltSystemProcedure
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
                     Procedure catProc, BackendTarget eeType, HsqlBackend hsql,
                     Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies,
                                              long fragmentId,
                                              ParameterSet params,
                                              SystemProcedureExecutionContext context)
    {
        // Never called, we do all the work in run()
        return null;
    }

    /**
     * Returns the cluster info requested by the provided selector
     * @param ctx          Internal. Not exposed to the end-user.
     * @param selector     Selector requested
     * @return             The property/value table for the provided selector
     * @throws VoltAbortException
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String selector) throws VoltAbortException
    {
        VoltTable[] results = new VoltTable[1];
        results[0] = VoltDB.instance().getCatalogContext().m_jdbc.getMetaData(selector);
        if (results[0] == null)
        {
            throw new VoltAbortException("Invalid @SystemCatalog selector: " + selector);
        }
        return results;
    }

}