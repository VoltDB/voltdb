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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

public class ContinuousDeleter {

    class PartitionInfo {
        String partitionKey;
        long tupleCount;
    }

    AtomicBoolean shouldContinue = new AtomicBoolean(true);
    final Client client;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicReference<Map<Long, PartitionInfo>> partitionData =
            new AtomicReference<Map<Long, PartitionInfo>>(new HashMap<Long, PartitionInfo>());
    private final AtomicLong globalTupleCount = new AtomicLong(0);
    private final AtomicLong redundancy = new AtomicLong(1);

    private final AtomicLong totalDeletes = new AtomicLong(0);
    private final AtomicLong deletesSinceLastChecked = new AtomicLong(0);

    long getTotalDeletes() {
        return totalDeletes.get();
    }

    long getDeletesSinceLastChecked() {
        return deletesSinceLastChecked.getAndSet(0);
    }

    class UpdatePatitionData implements Runnable {

        @Override
        public void run() {
            try {
                Map<Long, PartitionInfo> partitionDataTemp = new HashMap<Long, PartitionInfo>();

                VoltTable partitionKeys = null, tableStats = null;

                try {
                    tableStats = client.callProcedure("@Statistics", "TABLE").getResults()[0];
                    partitionKeys = client.callProcedure("@GetPartitionKeys", "STRING").getResults()[0];
                }
                catch (IOException | ProcCallException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
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
                    partitionDataTemp.put(partitionId, pinfo);
                }

                while (partitionKeys.advanceRow()) {
                    long partitionId = partitionKeys.getLong("PARTITION_ID");
                    PartitionInfo pinfo = partitionDataTemp.get(partitionId);
                    if (pinfo == null) {
                        // The set of partitions from the two calls don't match.
                        // Try again next time this is called... Maybe things
                        // will have settled down.
                        return;
                    }

                    pinfo.partitionKey = partitionKeys.getString("PARTITION_KEY");
                }

                // this is a sanity check to see that every partition has
                // a partition value
                long globalTupleCountTemp = 0;
                boolean allMatched = true;
                for (PartitionInfo pinfo : partitionDataTemp.values()) {
                    globalTupleCountTemp += pinfo.tupleCount;

                    // a partition has a count, but no key
                    if (pinfo.partitionKey == null) {
                        allMatched = false;
                    }
                }
                globalTupleCount.set(globalTupleCountTemp);
                redundancy.set(tableStats.getRowCount() / globalTupleCountTemp);
                if (!allMatched) {
                    // The set of partitions from the two calls don't match.
                    // Try again next time this is called... Maybe things
                    // will have settled down.
                    return;
                }

                // atomically update the new map for the old one
                partitionData.set(partitionDataTemp);
            }
            catch (Throwable t) {
                t.printStackTrace();
                throw t;
            }
        }

    }

    class DeleteToTarget implements Runnable {

        static final long CHUNK_SIZE = 1000;

        CountDownLatch latch = null;
        TimestampType dateTarget = new TimestampType((System.currentTimeMillis() - 30 * 1000) * 1000);
        AtomicLong partitionsUnfinished = new AtomicLong(0);

        class Callback implements ProcedureCallback {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                    long tuplesDeleted = clientResponse.getResults()[0].asScalarLong();
                    totalDeletes.addAndGet(tuplesDeleted);
                    deletesSinceLastChecked.addAndGet(tuplesDeleted);
                    if (tuplesDeleted >= CHUNK_SIZE) {
                        partitionsUnfinished.incrementAndGet();
                    }
                }
                else {
                    System.err.println(clientResponse.getStatusString());
                }
                // failure cases don't increment partitionsUnfinished because it is desireable to wait
                // a bit before trying again
                latch.countDown();
            }
        }

        @Override
        public void run() {
            try {
                Map<Long, PartitionInfo> currentPartitionInfo = partitionData.get();
                long partitionCount = currentPartitionInfo.size();

                latch = new CountDownLatch((int) partitionCount);

                for (PartitionInfo pinfo : currentPartitionInfo.values()) {
                    try {
                        client.callProcedure(new Callback(), "DeleteAfterDate", pinfo.partitionKey, dateTarget, CHUNK_SIZE);
                    } catch (Exception e) {
                        e.printStackTrace();
                        latch.countDown();
                    }
                }

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                try {
                    if (partitionsUnfinished.get() > 0) {
                        scheduler.execute(new DeleteToTarget());
                    }
                    else {
                        scheduler.schedule(new DeleteToTarget(), 100, TimeUnit.MILLISECONDS);
                    }
                }
                catch (RejectedExecutionException e) {
                    // ignore this... presumably the executor service has shutdown
                }
            }
            catch (Throwable t) {
                t.printStackTrace();
                throw t;
            }
        }

    }

    public ContinuousDeleter(Client client) {
        this.client = client;
    }

    public void stop() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void initialize() {
        // update the partition key set once per second
        scheduler.scheduleAtFixedRate(new UpdatePatitionData(), 1, 1, TimeUnit.SECONDS);
    }

    public final void start() {
        initialize();

        scheduler.execute(new DeleteToTarget());
    }

}
