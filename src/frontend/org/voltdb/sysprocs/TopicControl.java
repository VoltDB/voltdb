/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import org.hsqldb_voltpatches.lib.StringUtil;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTableUtil;

/**
 * Release a topic that may be in BLOCKED state
 */
public class TopicControl extends VoltSystemProcedure {

    private static final VoltLogger LOG = new VoltLogger("KIPLING");

    // Supported operations
    public static enum TopicControlOperation {
        RELEASE
    }

    /* (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#getPlanFragmentIds()
     */
    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
                SysProcFragmentId.PF_topicControl,
                SysProcFragmentId.PF_topicControlAggregate
        };
    }

    /* (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#executePlanFragment(java.util.Map, long, org.voltdb.ParameterSet, org.voltdb.SystemProcedureExecutionContext)
     */
    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        if (fragmentId == SysProcFragmentId.PF_topicControl) {
            VoltTable results = new VoltTable(
                    new ColumnInfo("TOPIC", VoltType.STRING),
                    new ColumnInfo("PARTITIONID", VoltType.BIGINT),
                    new ColumnInfo("STATUS", VoltType.STRING),
                    new ColumnInfo("MESSAGE", VoltType.STRING));

            if (context.isLowestSiteId()) {
                assert(params.toArray()[0] != null);
                assert(params.toArray()[1] != null);
                final String topic = (String) params.toArray()[0];
                final String operationMode = (String) params.toArray()[1];
                ExportManagerInterface.instance().processTopicControl(topic,
                        TopicControlOperation.valueOf(operationMode.toUpperCase()), results);
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_topicControl, results);
        }
        else if (fragmentId == SysProcFragmentId.PF_topicControlAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_topicControl));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_topicControlAggregate, result);
        }
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String topic, String operationMode) throws Exception {
        VoltTable results = new VoltTable(
                new ColumnInfo("TOPIC", VoltType.STRING),
                new ColumnInfo("PARTITIONID", VoltType.BIGINT),
                new ColumnInfo("STATUS", VoltType.STRING),
                new ColumnInfo("MESSAGE", VoltType.STRING));
        try {
            TopicControlOperation.valueOf(operationMode.toUpperCase());
        } catch (IllegalArgumentException e){
            results.addRow("", -1, "FAILURE", e.getMessage());
            return new VoltTable[] {results};
        }

        topic = topic == null ? "" : topic;
        if (!StringUtil.isEmpty(topic)) {
            if (!CatalogUtil.isTopic(ctx.getDatabase(), topic)) {
                results.addRow(topic, -1,"FAILED", "Topic stream " + topic + " does not exist.");
                return new VoltTable[] {results};
            }
        }
        LOG.info("Topic " + operationMode + " on topic:" + topic);
        return performTopicControl(topic, operationMode);
    }

    private final VoltTable[] performTopicControl(String topic, String operationMode) {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_topicControl,
                SysProcFragmentId.PF_topicControlAggregate, topic, operationMode);
    }
}
