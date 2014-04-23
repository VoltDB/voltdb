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
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import windowing.PartitionDataTracker.PartitionInfo;

public class ContinuousDeleter {

    static final long CHUNK_SIZE = 1000;

    class Callback implements ProcedureCallback {

        final CountDownLatch latch;
        final AtomicLong totalTuplesDeleted;
        final AtomicLong partitionsUnfinished;

        Callback(CountDownLatch latch, AtomicLong totalTuplesDeleted, AtomicLong partitionsUnfinished) {
            this.latch = latch;
            this.totalTuplesDeleted = totalTuplesDeleted;
            this.partitionsUnfinished = partitionsUnfinished;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                long tuplesDeleted = clientResponse.getResults()[0].asScalarLong();
                totalTuplesDeleted.addAndGet(tuplesDeleted);
                if (tuplesDeleted >= CHUNK_SIZE) {
                    partitionsUnfinished.incrementAndGet();
                }
            }
            else {
                System.err.println(clientResponse.getStatusString());
            }
            // failure cases don't increment partitionsUnfinished because it is desirable to wait
            // a bit before trying again
            latch.countDown();
        }
    }

    public long[] deleteSomeTuples(Client client, PartitionDataTracker partitionData) {
        final Map<Long, PartitionInfo> currentPartitionInfo = partitionData.getPartitionInfo();
        final long partitionCount = currentPartitionInfo.size();

        final CountDownLatch latch = new CountDownLatch((int) partitionCount);
        final AtomicLong totalTuplesDeleted = new AtomicLong(0);
        final AtomicLong partitionsUnfinished = new AtomicLong(0);

        TimestampType dateTarget = new TimestampType((System.currentTimeMillis() - 30 * 1000) * 1000);

        for (PartitionInfo pinfo : currentPartitionInfo.values()) {
            try {
                client.callProcedure(new Callback(latch, totalTuplesDeleted, partitionsUnfinished),
                                     "DeleteAfterDate",
                                     pinfo.partitionKey,
                                     dateTarget,
                                     CHUNK_SIZE);
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

        return new long[] { totalTuplesDeleted.get(), partitionsUnfinished.get() };
    }
}
