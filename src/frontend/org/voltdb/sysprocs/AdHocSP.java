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

import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.jni.ExecutionEngine;

@ProcInfo(
    singlePartition = true,
    partitionInfo = "DUMMY: 4"
)
public class AdHocSP extends VoltSystemProcedure {

    @Override
    public DependencyPair executePlanFragment(
            HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // This code should never be called.
        assert(false);
        return null;
    }

    public VoltTable run(SystemProcedureExecutionContext ctx,
            String aggregatorFragment, String collectorFragment,
            String sql, int isReplicatedTableDML) {

        assert(collectorFragment == null);

        ExecutionEngine ee = ctx.getExecutionEngine();
        VoltTable t;

        t = ee.executeCustomPlanFragment(aggregatorFragment, 1, -1, getTransactionId(),
                                         ctx.getLastCommittedTxnId(),
                                         ctx.getNextUndo());

        return t;
    }
}
