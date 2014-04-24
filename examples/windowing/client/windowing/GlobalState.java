/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


package windowing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.client.Client;

public class GlobalState {

    private final AtomicLong totalDeletes = new AtomicLong(0);
    private final AtomicLong deletesSinceLastChecked = new AtomicLong(0);

    // these values are updated by the UpdatePartitionData class each time it is run
    private final AtomicReference<Map<Long, PartitionInfo>> partitionData =
            new AtomicReference<Map<Long, PartitionInfo>>(new HashMap<Long, PartitionInfo>());
    private final AtomicLong redundancy = new AtomicLong(1);

    GlobalState(WindowingConfig config, Client client) {
        this.config = config;
        this.client = client;
    }

    /////
    // PACKAGE VISIBLE SHARED STATE ACCESS BELOW
    /////

    static final class PartitionInfo {
        String partitionKey;
        long tupleCount;
    }

    // Reference to the database connection we will use
    final Client client;

    // validated command line configuration
    final WindowingConfig config;

    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    final long benchmarkStartTS = System.currentTimeMillis();

    Map<Long, PartitionInfo> getPartitionData() {
        return partitionData.get();
    }

    long getPartitionCount() {
        return partitionData.get().size();
    }

    long getRedundancy() {
        return redundancy.get();
    }

    void updatePartitionInfoAndRedundancy(Map<Long, PartitionInfo> partitionData, long redundancy) {
        this.partitionData.set(partitionData);
        this.redundancy.set(redundancy);
    }

    void addToDeletedTuples(long count) {
        totalDeletes.addAndGet(count);
        deletesSinceLastChecked.addAndGet(count);
    }

    void shutdown() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            // block until all outstanding txns return
            client.drain();
            // close down the client connections
            client.close();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
