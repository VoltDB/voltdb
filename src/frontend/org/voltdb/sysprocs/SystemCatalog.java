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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltcore.logging.VoltLogger;

/**
 * Return VoltTable results that correspond to JDBC result sets for selected
 * methods on DatabaseMetaData
 */
@ProcInfo(
    singlePartition = true
)

public class SystemCatalog extends VoltSystemProcedure
{
    @SuppressWarnings("unused")
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    @Override
    public void init() {}

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
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
        VoltDB.crashLocalVoltDB("This should never be called.", false, null);
        return null;
    }

}
