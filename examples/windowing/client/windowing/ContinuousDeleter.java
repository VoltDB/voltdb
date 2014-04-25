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

public class ContinuousDeleter implements Runnable {

    final WindowingApp app;

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
                System.err.println(clientResponse.getStatusString());
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
                    e.printStackTrace();
                    latch.countDown();
                }
            }

            tuplesNotDeleted = tuplesNotDeletedForRound;
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        long finalTuplesNotDeleted = tuplesNotDeleted.get();
        if (tuplesNotDeleted.get() > 0) {
            double avgOutstandingPerPartition = finalTuplesNotDeleted / (double) app.getPartitionCount();
            int desiredRounds = (int) Math.ceil(avgOutstandingPerPartition / app.config.deletechunksize);
            assert(desiredRounds > 0);
            app.scheduler.execute(getRunnableForMulitpleRounds(desiredRounds));
        }
        else {
            app.scheduler.schedule(this, app.config.deleteyieldtime, TimeUnit.MILLISECONDS);
        }
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
