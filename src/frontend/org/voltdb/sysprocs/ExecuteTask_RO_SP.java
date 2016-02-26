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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.DependencyPair;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.jni.ExecutionEngine.TaskType;

public class ExecuteTask_RO_SP extends VoltSystemProcedure {

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
     * @param payload  serialized task-specific parameters
     * @return  results as VoltTable array
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte[] partitionParam, byte[] payload)
    {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int taskId = buffer.getInt();
        TaskType taskType = TaskType.values()[taskId]; // Param(1) in payload
        switch (taskType) {
        case SP_JAVA_GET_DRID_TRACKER:
            ParameterSet params = ParameterSet.fromArrayNoCopy(new Object[] { payload });
            int producerClusterId = (int)params.getParam(1);
            Map<Integer, Map<Integer, DRConsumerDrIdTracker>> drIdTrackers = ctx.getDrAppliedTrackers();
            Pair<Long, Long> lastConsumerUniqueIds = ctx.getDrLastAppliedUniqueIds();
            Map<Integer, DRConsumerDrIdTracker> producerPartitionMap = drIdTrackers.get(producerClusterId);
            if (producerPartitionMap == null) {
                producerPartitionMap = new HashMap<Integer, DRConsumerDrIdTracker>();
            }
            JSONStringer stringer = new JSONStringer();
            try {
                stringer.key(Integer.toString(producerClusterId));
                stringer.object();
                for (Map.Entry<Integer, DRConsumerDrIdTracker> e : producerPartitionMap.entrySet()) {
                    stringer.key(e.getKey().toString());
                    stringer.object();
                    stringer.key("lastConsumerSpUniqueId").value(lastConsumerUniqueIds.getFirst());
                    stringer.key("lastConsumerMpUniqueId").value(lastConsumerUniqueIds.getSecond());
                    ExtensibleSnapshotDigestData.serializeConsumerDrIdTrackerToJSON(stringer, e.getValue());
                    stringer.endObject();
                }
                stringer.endObject();
            } catch (JSONException e) {
                throw new VoltAbortException("DRConsumerDrIdTracker could not be converted to JSON");
            }
            setAppStatusString(stringer.toString());

        default:
            throw new VoltAbortException("Unable to find the task associated with the given task id");
        }
    }

}
