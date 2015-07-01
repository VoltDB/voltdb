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

package txnIdSelfCheck;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class TruncateTableLoader extends BenchmarkThread {


    final Client client;
    final long targetCount;
    final String tableName;
    final int rowSize;
    final int batchSize;
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore m_permits;
    String truncateProcedure = "TruncateTable";
    String scanAggProcedure = "ScanAggTable";
    long insertsTried = 0;
    long rowsLoaded = 0;
    long nTruncates = 0;
    float mpRatio;

    TruncateTableLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits, float mpRatio) {
        setName("TruncateTableLoader");
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        m_permits = permits;
        this.mpRatio = mpRatio;

        // make this run more than other threads
        setPriority(getPriority() + 1);

        log.info("TruncateTableLoader table: "+ tableName + " targetCount: " + targetCount);
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
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            byte status = clientResponse.getStatus();
            if (status == ClientResponse.GRACEFUL_FAILURE ||
                    status == ClientResponse.USER_ABORT) {
                // log what happened
                hardStop("TruncateTableLoader gracefully failed to insert into table " + tableName + " and this shoudn't happen. Exiting.", clientResponse);
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.error("TruncateTableLoader ungracefully failed to insert into table " + tableName);
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
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
                }
            }
            catch (Exception e) {
                // on exception, log and end the thread, but don't kill the process
                log.error("TruncateTableLoader failed a TableInsert procedure call for table '" + tableName + "' " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }


            // truncate the table, check for zero rows
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }

            try {
                log.debug("TruncateTableLoader truncate table..." + tableName + " current row count is " + currentRowCount);
                shouldRollback = (byte) (r.nextInt(10) == 0 ? 1 : 0);
                long p = Math.abs(r.nextLong());
                String tp = this.truncateProcedure;
                if (tableName == "trup")
                    tp += r.nextInt(100) < mpRatio * 100. ? "MP" : "SP";
                ClientResponse clientResponse = client.callProcedure(tableName.toUpperCase() + tp, p, shouldRollback);
                byte status = clientResponse.getStatus();
                if (status == ClientResponse.GRACEFUL_FAILURE ||
                        (shouldRollback == 0 && status == ClientResponse.USER_ABORT)) {
                    hardStop("TruncateTableLoader gracefully failed to truncate table " + tableName + " and this shoudn't happen. Exiting.", clientResponse);
                }
                if (status != ClientResponse.SUCCESS) {
                    // log what happened
                    log.error("TruncateTableLoader ungracefully failed to truncate table " + tableName);
                    log.error(((ClientResponseImpl) clientResponse).toJSONString());
                }
                else {
                    Benchmark.txnCount.incrementAndGet();
                    nTruncates++;
                }
                shouldRollback = 0;
            }
            catch (ProcCallException e) {
                ClientResponseImpl cri = (ClientResponseImpl) e.getClientResponse();
                if (shouldRollback == 0) {
                    // this implies bad data and is fatal
                    if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE) ||
                            (cri.getStatus() == ClientResponse.USER_ABORT)) {
                        // on exception, log and end the thread, but don't kill the process
                        hardStop("TruncateTableLoader failed a TruncateTable ProcCallException call for table '" + tableName + "' " + e.getMessage());
                    }
                }
            }
            catch (NoConnectionsException e) {
                // on exception, log and end the thread, but don't kill the process
                log.error("TruncateTableLoader failed a non-proc call exception for table '" + tableName + "' " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }
            catch (IOException e) {
                // just need to fall through and get out
                throw new RuntimeException(e);
            }

            // scan-agg table
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }

            try {
                log.debug("TruncateTableLoader scan agg table..." + tableName + " current row count is " + currentRowCount);
                shouldRollback = (byte) (r.nextInt(10) == 0 ? 1 : 0);
                long p = Math.abs(r.nextLong());
                String sp = this.scanAggProcedure;
                if (tableName == "trup")
                    sp += r.nextInt(100) < mpRatio * 100. ? "MP" : "SP";
                ClientResponse clientResponse = client.callProcedure(tableName.toUpperCase() + sp, p, shouldRollback);
                byte status = clientResponse.getStatus();
                if (status == ClientResponse.GRACEFUL_FAILURE ||
                        (shouldRollback == 0 && status == ClientResponse.USER_ABORT)) {
                    hardStop("TruncateTableLoader gracefully failed to scan-agg table " + tableName + " and this shoudn't happen. Exiting.", clientResponse);
                }
                if (status != ClientResponse.SUCCESS) {
                    // log what happened
                    log.error("TruncateTableLoader ungracefully failed to scan-agg table " + tableName);
                    log.error(((ClientResponseImpl) clientResponse).toJSONString());
                }
                else
                    Benchmark.txnCount.incrementAndGet();
                shouldRollback = 0;
            }
            catch (ProcCallException e) {
                ClientResponseImpl cri = (ClientResponseImpl) e.getClientResponse();
                if (shouldRollback == 0) {
                    // this implies bad data and is fatal
                    if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE) ||
                            (cri.getStatus() == ClientResponse.USER_ABORT)) {
                        // on exception, log and end the thread, but don't kill the process
                        hardStop("TruncateTableLoader failed a ScanAgg ProcCallException call for table '" + tableName + "' " + e.getMessage());
                    }
                }
            }
            catch (NoConnectionsException e) {
                // on exception, log and end the thread, but don't kill the process
                log.error("TruncateTableLoader failed a non-proc call exception for table '" + tableName + "' " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }
            catch (IOException e) {
                // just need to fall through and get out
                throw new RuntimeException(e);
            }
        }
        log.info("TruncateTableLoader normal exit for table " + tableName + " rows sent: " + insertsTried + " inserted: " + rowsLoaded + " truncates: " + nTruncates);
    }

}
