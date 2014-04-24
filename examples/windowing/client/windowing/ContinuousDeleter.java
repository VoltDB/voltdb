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

public class ContinuousDeleter implements Runnable {

    static final long CHUNK_SIZE = 100;
    static final int DELETE_YIELD_MS = 50;

    final GlobalState state;

    ContinuousDeleter(GlobalState state) {
        this.state = state;
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

                state.addToDeletedTuples(tuplesDeleted);
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
        final Map<Long, GlobalState.PartitionInfo> currentPartitionInfo = state.getPartitionData();
        final long partitionCount = currentPartitionInfo.size();

        final CountDownLatch latch = new CountDownLatch((int) partitionCount * rounds);
        AtomicLong tuplesNotDeleted = null;

        for (int round = 0; round < rounds; round++) {
            final AtomicLong tuplesNotDeletedForRound = new AtomicLong(0);

            TimestampType dateTarget = new TimestampType((System.currentTimeMillis() - 30 * 1000) * 1000);

            for (GlobalState.PartitionInfo pinfo : currentPartitionInfo.values()) {
                try {
                    state.client.callProcedure(new Callback(latch, tuplesNotDeletedForRound),
                                         "DeleteAfterDate",
                                         pinfo.partitionKey,
                                         dateTarget,
                                         CHUNK_SIZE);
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
            double avgOutstandingPerPartition = finalTuplesNotDeleted / (double) state.getPartitionCount();
            int desiredRounds = (int) Math.ceil(avgOutstandingPerPartition / ContinuousDeleter.CHUNK_SIZE);
            assert(desiredRounds > 0);
            state.scheduler.execute(getRunnableForMulitpleRounds(desiredRounds));
        }
        else {
            state.scheduler.schedule(this, DELETE_YIELD_MS, TimeUnit.MILLISECONDS);
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
