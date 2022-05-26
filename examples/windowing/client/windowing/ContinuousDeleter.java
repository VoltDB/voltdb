/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.types.TimestampType;

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

    @Override
    public void run() {

        // Will be set to true if any partition needs to delete more rows to meet the goals.
        AtomicBoolean unfinished = new AtomicBoolean(false);

        // Default yield time between deletes. May be shortened if there are a ton of tuples that need deleting.
        long yieldTimeMs = app.config.deleteyieldtime;

        try {
            // Get the targets from the main app class. Only one will be used, depending on whether the user is deleting
            // old rows by date or by row count.
            TimestampType dateTarget = app.getTargetDate();
            long rowTarget = app.getTargetRowsPerPartition();
            // Send the procedure call to all partitions and get results from each partitions.
            ClientResponseWithPartitionKey[] responses;
            if (app.config.historyseconds > 0) {
                responses = app.client.callAllPartitionProcedure("DeleteAfterDate", dateTarget, app.config.deletechunksize);
            }
            else /* if (app.config.maxrows > 0) */ {
                responses = app.client.callAllPartitionProcedure("DeleteOldestToTarget", rowTarget, app.config.deletechunksize);
            }

            app.updatePartitionCount(responses.length);
            for (ClientResponseWithPartitionKey resp: responses) {
                if (resp.response.getStatus() == ClientResponse.SUCCESS) {
                    long tuplesDeleted = resp.response.getResults()[0].asScalarLong();
                    app.addToDeletedTuples(tuplesDeleted);

                    // If the procedure deleted up to its limit, reduce the time before the deletes process runs again.
                    if (tuplesDeleted >= app.config.deletechunksize) {
                        unfinished.set(true);
                    }
                } else {
                    failureCount.incrementAndGet();
                }
            }
            // If this round of deletes didn't remove all of the tuples that could have been removed, reduce the pause time before the
            // next round to zero. If this process still can't keep up, then you can always add a second ContinuousDeleter instance in
            // WindowingApp and schedule two of them to run concurrently.
            if (unfinished.get()) {
                yieldTimeMs = 0;
            }

        } catch (Exception e) {
            failureCount.incrementAndGet();
        } catch (Throwable t) {
            t.printStackTrace();
            // Live dangerously and ignore this.
        } finally {
            // Use a finally block to ensure this task is re-scheduled.
            app.scheduler.schedule(this, yieldTimeMs, TimeUnit.MILLISECONDS);
        }
    }
}
