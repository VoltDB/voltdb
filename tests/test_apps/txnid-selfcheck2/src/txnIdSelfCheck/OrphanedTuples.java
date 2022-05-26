/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package txnIdSelfCheck;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.net.UnknownHostException;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltcore.utils.CoreUtils;
import org.voltdb.task.Action;
import org.voltdb.task.ActionGenerator;
import org.voltdb.task.ActionResult;
import org.voltdb.task.TaskHelper;
import org.voltdb.utils.CompoundErrors;

/**
 * Task for polling the EXPORT statistics and reporting when disabled export streams have pending tuple counts.
 * Example task creation:
 *
 * CREATE TASK orphaned_tuples ON SCHEDULE EVERY 1 MINUTES
 *     PROCEDURE FROM CLASS txnIdSelfCheck.OrphanedTuples
 *     ON ERROR LOG;
 */
public class OrphanedTuples implements ActionGenerator {
    private TaskHelper helper;

    public void initialize(TaskHelper helper) {
        this.helper = helper;
    }

    @Override
    public Action getFirstAction() {
        return Action.procedureCall(this::handleStatisticsResult, "@Statistics", "EXPORT", 0);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Collection<String> getDependencies() {
        // This class has no external dependencies outside of java standard library and voltdb
        return Collections.emptyList();
    }

    /**
     * Callback to handle the result of the statistics query and return the next statistics call
     *
     * @param result of statistics query
     * @return next statistics action
     */
    private Action handleStatisticsResult(ActionResult result) {
        ClientResponse response = result.getResponse();

        if (response.getStatus() == ClientResponse.SUCCESS) {
            VoltTable stats = response.getResults()[0];
            processStats(stats);
        }
        return Action.procedureCall(this::handleStatisticsResult, "@Statistics", "EXPORT", 0);
    }

    /**
     * Process the export stats
     *
     */
    private void processStats(VoltTable stats) {
        Map<String, Long> partitionMap = new HashMap<String, Long>();
        Map<String, Long> tableTupleCount = new HashMap<String, Long>();

        while (stats.advanceRow()) {
            long partitionid = stats.getLong("PARTITION_ID");
            String source = stats.getString("SOURCE");
            String active = stats.getString("ACTIVE");
            long tuplePending = stats.getLong("TUPLE_PENDING");
            String tablePart = source + "_" + partitionid;
            if (! partitionMap.containsKey(tablePart)) {
                // only put this table+partition count in the map once
                partitionMap.put(tablePart, tuplePending);
                if ("FALSE".equalsIgnoreCase(active) && tuplePending > 0)
                    if (! tableTupleCount.containsKey(source))
                        tableTupleCount.put(source, tuplePending);
                    else
                        tableTupleCount.put(source, tableTupleCount.get(source) + tuplePending);
            }
        }

        // InetAddress ip;
        String hostName = CoreUtils.getHostnameAndAddress();
        for (Map.Entry<String, Long> t: tableTupleCount.entrySet()) {
            helper.logWarning(
                    "Host: " + hostName + " Stream " + t.getKey() + " has " + t.getValue() + " tuples pending but is not actively exporting anything");
        }
    }
}
