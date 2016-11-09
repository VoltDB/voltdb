/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.jni.ExecutionEngine.TaskType;

public class ExecuteTask_SP extends VoltSystemProcedure {

    @Override
    public void init() {
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
     * @param taskId          type of the task to execute
     */
    public void run(SystemProcedureExecutionContext ctx, byte[] partitionParam, byte taskId)
    {
        TaskType taskType = TaskType.values()[taskId];
        switch (taskType) {
        case SP_JAVA_GET_DRID_TRACKER:
            Map<Integer, Map<Integer, DRConsumerDrIdTracker>> drIdTrackers = ctx.getDrAppliedTrackers();
            Pair<Long, Long> lastConsumerUniqueIds = ctx.getDrLastAppliedUniqueIds();
            try {
                setAppStatusString(jsonifyTrackedDRData(lastConsumerUniqueIds, drIdTrackers));
            } catch (JSONException e) {
                throw new VoltAbortException("DRConsumerDrIdTracker could not be converted to JSON");
            }

            break;
        default:
            throw new VoltAbortException("Unable to find the task associated with the given task id");
        }
    }

    public static String jsonifyTrackedDRData(Pair<Long, Long> lastConsumerUniqueIds,
                                              Map<Integer, Map<Integer, DRConsumerDrIdTracker>> allProducerTrackers)
    throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.keySymbolValuePair("lastConsumerSpUniqueId", lastConsumerUniqueIds.getFirst());
        stringer.keySymbolValuePair("lastConsumerMpUniqueId", lastConsumerUniqueIds.getSecond());
        stringer.key("trackers").object();
        if (allProducerTrackers != null) {
            for (Map.Entry<Integer, Map<Integer, DRConsumerDrIdTracker>> clusterTrackers : allProducerTrackers.entrySet()) {
                stringer.key(Integer.toString(clusterTrackers.getKey())).object();
                for (Map.Entry<Integer, DRConsumerDrIdTracker> e : clusterTrackers.getValue().entrySet()) {
                    stringer.key(e.getKey().toString());
                    stringer.value(e.getValue().toJSON());
                }
                stringer.endObject();
            }
        }
        stringer.endObject();
        stringer.endObject();
        return stringer.toString();
    }
}
