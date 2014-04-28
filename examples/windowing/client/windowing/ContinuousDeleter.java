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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import windowing.WindowingApp.PartitionInfo;

/**
 *
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

    @Override
    public void run() {
        deleteSomeTuples(1);
    }

    class Callback implements ProcedureCallback {

        final CountDownLatch latch;
        final AtomicLong totalTuplesNotDeleted;

        Callback(CountDownLatch latch, AtomicLong totalTuplesNotDeleted) {
            this.latch = latch;
            this.totalTuplesNotDeleted = totalTuplesNotDeleted;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                VoltTable results = clientResponse.getResults()[0];
                long tuplesDeleted = results.fetchRow(0).getLong("deleted");
                long tuplesNotDeleted = results.fetchRow(0).getLong("not_deleted");

                app.addToDeletedTuples(tuplesDeleted);
                totalTuplesNotDeleted.addAndGet(tuplesNotDeleted);
            }
            else {
                // Uncomment to see problems on the console
                //System.err.println(clientResponse.getStatusString());

                // Track failures in a simplistic way.
                failureCount.incrementAndGet();
            }
            // failure cases don't increment partitionsUnfinished because it is desirable to wait
            // a bit before trying again
            latch.countDown();
        }
    }

    protected void deleteSomeTuples(int rounds) {
        final Map<Long, PartitionInfo> currentPartitionInfo = app.getPartitionData();
        final long partitionCount = currentPartitionInfo.size();

        final CountDownLatch latch = new CountDownLatch((int) partitionCount * rounds);
        AtomicLong tuplesNotDeleted = null;

        try {
            for (int round = 0; round < rounds; round++) {
                final AtomicLong tuplesNotDeletedForRound = new AtomicLong(0);

                TimestampType dateTarget = app.getTargetDate();
                long rowTarget = app.getTargetRowsPerPartition();

                for (PartitionInfo pinfo : currentPartitionInfo.values()) {
                    try {
                        if (app.config.historyseconds > 0) {
                            app.client.callProcedure(new Callback(latch, tuplesNotDeletedForRound),
                                                     "DeleteAfterDate",
                                                     pinfo.partitionKey,
                                                     dateTarget,
                                                     app.config.deletechunksize);
                        }
                        else /* if (app.config.maxrows > 0) */ {
                            app.client.callProcedure(new Callback(latch, tuplesNotDeletedForRound),
                                                     "DeleteOldestToTarget",
                                                     pinfo.partitionKey,
                                                     rowTarget,
                                                     app.config.deletechunksize);
                        }
                    } catch (Exception e) {
                        // Track failures in a simplistic way.
                        failureCount.incrementAndGet();
                        // Make sure the latch is released for this call, even if failed
                        latch.countDown();
                    }
                }

                tuplesNotDeleted = tuplesNotDeletedForRound;
            }

            // Wait for all of the calls to return.
            latch.await();

            long finalTuplesNotDeleted = tuplesNotDeleted.get();
            if (tuplesNotDeleted.get() > 0) {
                double avgOutstandingPerPartition = finalTuplesNotDeleted / (double) app.getPartitionCount();
                int desiredRounds = (int) Math.ceil(avgOutstandingPerPartition / app.config.deletechunksize);
                assert(desiredRounds > 0);
                app.scheduler.execute(getRunnableForMulitpleRounds(desiredRounds));
                // Done once re-scheduled.
                return;
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
            // Live dangerously and ignore this.
        }

        // This is after the try/catch to ensure it gets scheduled again
        // It's not a 'finally' block because this should only run if the other
        // scheduling code above doesn't run.
        app.scheduler.schedule(this, app.config.deleteyieldtime, TimeUnit.MILLISECONDS);
    }

    protected Runnable getRunnableForMulitpleRounds(final int rounds) {
        return new Runnable() {
            @Override
            public void run() {
                deleteSomeTuples(rounds);
            }
        };
    }
}
