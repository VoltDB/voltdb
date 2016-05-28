/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.ClientResponseImpl;
import org.voltdb.OperationMode;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class CappedTableLoader extends BenchmarkThread {

    final Client client;
    final long targetCount;
    final String tableName;
    final int rowSize;
    final int batchSize;
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore m_permits;
    long insertsTried = 0;
    long rowsLoaded = 0;
    float mpRatio;
    byte cnt = 0;

    CappedTableLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits, float mpRatio) {
        setName("CappedTableLoader");
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        m_permits = permits;
        this.mpRatio = mpRatio;

        // make this run more than other threads
        setPriority(getPriority() + 1);

        log.info("CappedTableLoader table: "+ tableName + " targetCount: " + targetCount);
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    class InsertCallback implements ProcedureCallback {

        CountDownLatch latch;

        InsertCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception { // fix this
            byte status = clientResponse.getStatus();
            if (status == ClientResponse.GRACEFUL_FAILURE) {
                // This case is what happens when the table fails to delete enough rows to make room for the next insert.
                // log.info("CappedTableLoader acceptably failed to insert into " + tableName + ". ");
            } else if ( status == ClientResponse.USER_ABORT) {
                log.error("User abort while attempting to insert into table "+ tableName );
            } else
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.warn("CappedTableLoader ungracefully failed to insert into table " + tableName);
                log.warn(((ClientResponseImpl) clientResponse).toJSONString());
            }
            else {
                Benchmark.txnCount.incrementAndGet();
                rowsLoaded++;
            }
            latch.countDown();
        }
    }

    @Override
    public void run() {
        byte[] data = new byte[rowSize];
        byte shouldRollback = 0;
        long currentRowCount = 0;
        while (m_shouldContinue.get()) {
            r.nextBytes(data);

            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }

            try {
                // insert some batches...
                int tc = batchSize * r.nextInt(99);
                while ((currentRowCount < tc) && (m_shouldContinue.get())) {
                    CountDownLatch latch = new CountDownLatch(batchSize);
                    // try to insert batchSize random rows
                    for (int i = 0; i < batchSize; i++) {
                        long p = Math.abs(r.nextLong());
                        m_permits.acquire();
                        insertsTried++;
                        client.callProcedure(new InsertCallback(latch), tableName.toUpperCase() + "TableInsert", p, data);
                    }
                    latch.await(10, TimeUnit.SECONDS);
                    long nextRowCount = -1;
                    try {
                        nextRowCount = TxnId2Utils.getRowCount(client, tableName);
                    } catch (Exception e) {
                        hardStop("getrowcount exception", e);
                    }
                    // if no progress, throttle a bit
                    if (nextRowCount == currentRowCount) {
                        try { Thread.sleep(1000); } catch (Exception e2) {}
                    }
                    currentRowCount = nextRowCount;
                    if (exceedsCappedLimit())
                        hardStop("Capped table exceeds 10 rows, this shoudln't happen. Exiting. ");
                }
            }
            catch (Exception e) {
                // on exception, log and end the thread, but don't kill the process
                log.warn("CappedTableLoader failed a TableInsert procedure call for table '" + tableName + "', exception msg: " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) { }
            }
        }
        log.info("CappedTableLoader normal exit for table " + tableName + " rows sent: " + insertsTried + " inserted: " + rowsLoaded);
    }

    private boolean exceedsCappedLimit() throws NoConnectionsException, IOException, ProcCallException {
        boolean ret = false;
        VoltTable partitions = client.callProcedure("@GetPartitionKeys",
                "INTEGER").getResults()[0];
        long count = TxnId2Utils.doAdHoc(client,"SELECT COUNT(*) FROM capr;").getResults()[0].fetchRow(0).getLong(0);
        if (count > 10) {
            log.error("Replicated table CAPR has more rows ("+count+") than the limit set by capped collections (10)");
            ret = true;
        }
        while (partitions.advanceRow()) {
            long id = partitions.getLong(0);
            long key = partitions.getLong(1);
            count = client.callProcedure("CAPPCountPartitionRows",key).getResults()[0].fetchRow(0).getLong(0);
            if (count > 10) {
                log.error("Replicated table CAPP has more rows ("+count+") than the limit set by capped collections (10) on partition "+id);
                ret = true;
            }
        }
        return ret;
    }
}
