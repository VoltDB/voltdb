/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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


package ddlwindowing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import ddlwindowing.WindowingApp.PartitionInfo;

/**
 * <p>Runnable-implementor that fetches some per-partition information
 * from a running VoltDB procedure using VoltDB system procedures.
 * First, it uses "@GetPartitionKeys" to get a value that will partition
 * to each partition (used for targeting single-partition procedure call).
 * Then it uses "@Statistics" with the "TABLE" selector to get the number
 * of tuples in the 'timedata' table at each partition.</p>
 *
 * <p>Every time that it's called, it updates a global data structure. This
 * structure is primarily used by Reporter to log per-partition statistics.</p>
 *
 * <p>This code is pretty adaptable to other applications without much
 * modification.</p>
 *
 */
public class PartitionDataTracker implements Runnable {

    // Global state
    final WindowingApp app;

    // track failures for reporting
    final AtomicLong failureCount = new AtomicLong(0);

    public PartitionDataTracker(WindowingApp app) {
        this.app = app;
    }

    @Override
    public void run() {
        Map<Long, PartitionInfo> partitionData = new HashMap<Long, PartitionInfo>();

        VoltTable partitionKeys = null, tableStats = null;

        try {
            tableStats = app.client.callProcedure("@Statistics", "TABLE").getResults()[0];
            partitionKeys = app.client.callProcedure("@GetPartitionKeys", "STRING").getResults()[0];
        }
        catch (IOException | ProcCallException e) {
            // Track failures in a simplistic way.
            failureCount.incrementAndGet();

            // No worries. Will be scheduled again soon.
            return;
        }

        while (tableStats.advanceRow()) {
            if (!tableStats.getString("TABLE_NAME").equalsIgnoreCase("timedata")) {
                continue;
            }

            PartitionInfo pinfo = new PartitionInfo();
            long partitionId = tableStats.getLong("PARTITION_ID");
            pinfo.tupleCount = tableStats.getLong("TUPLE_COUNT");
            pinfo.partitionKey = null;

            // If redundancy (k-safety) is enabled, this will put k+1 times per partition,
            // but the tuple count will be the same so it will be ok.
            partitionData.put(partitionId, pinfo);
        }

        while (partitionKeys.advanceRow()) {
            long partitionId = partitionKeys.getLong("PARTITION_ID");
            PartitionInfo pinfo = partitionData.get(partitionId);
            if (pinfo == null) {
                // The set of partitions from the two calls don't match.
                // Try again next time this is called... Maybe things
                // will have settled down.
                return;
            }

            pinfo.partitionKey = partitionKeys.getString("PARTITION_KEY");

            try {
                // Find the age of the oldest and youngest tuples in this partition to
                // demonstrate that we're both accepting new tuples and aging out
                // old tuples at the appropriate time.
                ClientResponse cr = app.client.callProcedure("AgeOfOldest", pinfo.partitionKey);
                pinfo.oldestTupleAge = cr.getResults()[0].asScalarLong();
                cr = app.client.callProcedure("AgeOfYoungest", pinfo.partitionKey);
                pinfo.youngestTupleAge = cr.getResults()[0].asScalarLong();
            } catch (IOException | ProcCallException e) {
                failureCount.incrementAndGet();
                return;
            }
        }

        // This is a sanity check to see that every partition has
        // a partition value
        boolean allMatched = true;
        for (PartitionInfo pinfo : partitionData.values()) {
            // a partition has a count, but no key
            if (pinfo.partitionKey == null) {
                allMatched = false;
            }
        }
        if (!allMatched) {
            // The set of partitions from the two calls don't match.
            // Try again next time this is called... Maybe things
            // will have settled down.
            return;
        }

        // atomically update the new map for the old one
        app.updatePartitionInfo(partitionData);
    }
}
