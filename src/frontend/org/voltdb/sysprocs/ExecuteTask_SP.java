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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.jni.ExecutionEngine.TaskType;

public class ExecuteTask_SP extends VoltSystemProcedure {

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // Never called, we do all the work in run()
        return null;
    }

    /**
     * System procedure run hook.
     * Use the base class implementation.
     *
     * @param ctx  execution context
     * @param partitionParam  key for routing stored procedure to correct site
     * @param params          additional parameter(s) for the task to execute, first one is always task type
     */
    public void run(SystemProcedureExecutionContext ctx, byte[] partitionParam, byte[] params)
    {
        assert params.length > 0;
        byte taskId = params[0];
        TaskType taskType = TaskType.values()[taskId];
        switch (taskType) {
        case RESET_DR_APPLIED_TRACKER_SINGLE:
            assert params.length == 2;
            byte clusterId = params[1];
            ctx.resetDrAppliedTracker(clusterId);
            break;
        default:
            throw new VoltAbortException("Unable to find the task associated with the given task id");
        }
    }
}
