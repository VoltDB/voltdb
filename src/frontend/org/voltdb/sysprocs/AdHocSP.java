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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
    singlePartition = true,
    partitionInfo = "DUMMY: 4"
)
public class AdHocSP extends VoltSystemProcedure {

    @Override
    public void init() {}

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // This code should never be called.
        assert(false);
        return null;
    }

    public VoltTable run(SystemProcedureExecutionContext ctx,
            String aggregatorFragment, String collectorFragment,
            String sql, int isReplicatedTableDML) {

        assert(collectorFragment == null);

        SiteProcedureConnection spc = ctx.getSiteProcedureConnection();
        VoltTable t;

        t = spc.executeCustomPlanFragment(aggregatorFragment, -1,
                                          getTransactionId());

        return t;
    }
}
