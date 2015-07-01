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

package windowing;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import windowing.WindowingApp.PartitionInfo;

/**
 * Runnable-implementor that runs a stored procedure at every partition.
 * Each stored procedure deletes tuples that are older than a given
 * cutoff date or past the maximum desired row count for the partition.
 * All stored procedures are run concurrently using asynchronous calls.
 *
 */
public class ContinuousDeleter implements Runnable {

    // Global state
    final WindowingApp app;

    // track failures for reporting
    final AtomicLong failureCount = new AtomicLong(0);

    ContinuousDeleter(WindowingApp app) {
        this.app = app;
    }

    /**
     * Callback for the asynchronous procedure that deletes tuples.
     *
     */
    class Callback implements ProcedureCallback {

        final CountDownLatch latch;
        final AtomicBoolean unifinished;

        Callback(CountDownLatch latch, AtomicBoolean unfinished) {
            this.latch = latch;
            this.unifinished = unfinished;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                long tuplesDeleted = clientResponse.getResults()[0].asScalarLong();
                app.addToDeletedTuples(tuplesDeleted);
                // If the procedue deleted up to its limit, reduce the time before
                // the deletes process runs again.
                if (tuplesDeleted >= app.config.deletechunksize) {
                    unifinished.set(true);
                }
            }
            else {
                // Un-comment to see problems on the console
                //System.err.println(clientResponse.getStatusString());

                // Track failures in a simplistic way.
                failureCount.incrementAndGet();
            }
            // failure cases don't increment partitionsUnfinished because it is desirable to wait
            // a bit before trying again
            latch.countDown();
        }
    }

    @Override
    public void run() {
        // Set of partition keys and row counts for each partition, updated
        // periodically by the PartitionTracker class.
        final Map<Long, PartitionInfo> currentPartitionInfo = app.getPartitionData();
        final long partitionCount = currentPartitionInfo.size();

        // Used to block until all asynchronous procedure calls have completed.
        final CountDownLatch latch = new CountDownLatch((int) partitionCount);

        // Will be set to true if any partition needs to delete more rows to
        // meet the goals.
        AtomicBoolean unfinished = new AtomicBoolean(false);

        // Default yield time between deletes. May be shortened if there are a ton of
        // tuples that need deleting.
        long yieldTimeMs = app.config.deleteyieldtime;

        try {
            // Get the targets from the main app class.
            // Only one will be used, depending on whether the user is deleting
            // old rows by date or by row count.
            TimestampType dateTarget = app.getTargetDate();
            long rowTarget = app.getTargetRowsPerPartition();

            // Send one procedure invocation to each partition. This is done
            // by using the partitioning keys retrieved by the PartitionTracker
            // class. It uses the @GetPartitionKeys system procedure to get
            // a value that can be used as a partitioning value to target each
            // partition.
            for (PartitionInfo pinfo : currentPartitionInfo.values()) {
                try {
                    // Deleting all rows older than date...
                    if (app.config.historyseconds > 0) {
                        app.client.callProcedure(new Callback(latch, unfinished),
                                                 "DeleteAfterDate",
                                                 pinfo.partitionKey,
                                                 dateTarget,
                                                 app.config.deletechunksize);
                    }
                    // Deleting all rows beyond a given rowcount...
                    else /* if (app.config.maxrows > 0) */ {
                        app.client.callProcedure(new Callback(latch, unfinished),
                                                 "DeleteOldestToTarget",
                                                 pinfo.partitionKey,
                                                 rowTarget,
                                                 app.config.deletechunksize);
                    }
                }
                catch (Exception e) {
                    // Track failures in a simplistic way.
                    failureCount.incrementAndGet();
                    // Make sure the latch is released for this call, even if failed
                    latch.countDown();
                }
            }

            // Wait for all of the calls to return.
            latch.await();

            // If this round of deletes didn't remove all of the tuples
            // that could have been removed, reduce the pause time before the
            // next round to zero.
            // If this process still can't keep up, then
            // you can always add a second ContinuousDeleter instance in
            // WindowingApp and schedule two of them to run concurrently.
            if (unfinished.get()) {
                yieldTimeMs = 0;
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
            // Live dangerously and ignore this.
        }
        finally {
            // Use a finally block to ensure this task is re-scheduled.
            app.scheduler.schedule(this, yieldTimeMs, TimeUnit.MILLISECONDS);
        }
    }
}
