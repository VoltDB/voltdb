/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import java.io.InterruptedIOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NibbleDeleteLoader extends BenchmarkThread {

    final Client client;
    final long targetCount;
    final String tableName;
    final int rowSize;
    final int batchSize;
    final int partitionCount;
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore m_permits;
    long insertsTried = 0;
    long rowsLoaded = 0;
    long deletesTried = 0;
    long deletesSucceeded = 0;
    NibbleDeleter deleter = null;
    long rowsDeletedTotal = 0;

    NibbleDeleteLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits, int partitionCount) {
        setName("NibbleDeleteLoader-"+tableName);
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        m_permits = permits;
        this.partitionCount = partitionCount;

        // make this run more than other threads
        setPriority(getPriority() + 1);
        log.info("NibbleDeleteLoader table: "+ tableName + " targetCount: " + targetCount + " storage required: " + targetCount*rowSize + " bytes");
        this.deleter = new NibbleDeleter(this.tableName, "ts", "75", ">", 1000, 2000);
    }

    void shutdown() {
        m_shouldContinue.set(false);
        log.info("NibbleDeleteLoader " + tableName + " shutdown: inserts tried: " + insertsTried + " rows loaded: " + rowsLoaded +
                " deletes tried: " + deletesTried + " deletes succeeded: " + deletesSucceeded);
        this.interrupt();
    }

    //"@LowImpactDelete", [FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_BIGINT,FastSerializer.VOLTTYPE_BIGINT],
    //        [tablename,columnName,columnThresholdInMicros,comparisonOp,chunksize,timeoutms]

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
                hardStop("NibbleDeleteLoader gracefully failed to insert into table " + tableName + " and this shoudn't happen. Exiting.");
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.warn("NibbleDeleteLoader ungracefully failed to insert into table " + tableName);
                log.warn(((ClientResponseImpl) clientResponse).toJSONString());
            }
            else {
                Benchmark.txnCount.incrementAndGet();
                rowsLoaded++;
            }
            latch.countDown();
        }
    }


    class NibbleDeleter extends Thread {

        final String columnName;
        final String tableName;
        final String columnThresholdInMicros;
        final String comparisonOp;
        final int chunksize;
        final int timeoutms;

        NibbleDeleter(String tableName, String columnName, String columnThresholdInMicros, String comparisonOp, int chunksize, int timeoutms) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.columnThresholdInMicros = columnThresholdInMicros;
            this.comparisonOp = comparisonOp;
            this.chunksize = chunksize;
            this.timeoutms = timeoutms;
        }

        @Override
        public void run() {
            while (m_shouldContinue.get()) {
                try { Thread.sleep(1000); } catch (Exception e) { }
                try {
                    m_permits.acquire(1000);
                } catch (InterruptedException e) {
                    if (!m_shouldContinue.get()) {
                        return;
                    }
                    log.error("NibbleDeleter thread interrupted while waiting for permits. " + e.getMessage());
                }
                try {
                    deletesTried++;
                    // this sysproc is synchronous
                    ClientResponse response = TxnId2Utils.doProcCall(client, "@LowImpactDelete", tableName, columnName, columnThresholdInMicros, comparisonOp, chunksize, timeoutms);
                    if (response.getStatus() == ClientResponse.SUCCESS) {
                        VoltTable[] t = response.getResults();
                        VoltTable data = t[0];
                        if (data.getRowCount() <= 0) {
                            hardStop("No rows");
                        }
                        VoltTableRow row = data.fetchRow(0);
                        long rowsDeleted = row.getLong(0);
                        long rowsLeft = row.getLong(1);
                        long rounds = row.getLong(2);
                        long deletedLastRound = row.getLong(3);
                        String note = row.getString(4);
                        deletesSucceeded++;
                        rowsDeletedTotal += rowsDeleted;
                    } else {

                    }
                } catch (ProcCallException e) {
                    if (! m_shouldContinue.get())
                        return;
                    hardStop("NibbleDeleter failed a '@LowImpactDelete' procedure call for table '" + tableName + "' " + e.getMessage());
                } catch (Exception e) {
                    hardStop("NibbleDeleter failed a '@LowImpactDelete' for table '" + tableName + "' " + e.getMessage());
                }
            }
        }
    }


    @Override
    public void run() {
        this.deleter.start();  // start the nibbler
        byte[] data = new byte[rowSize];
        long currentRowCount;
        while (m_shouldContinue.get()) {
            r.nextBytes(data);
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
                // insert some batches...
                while ((currentRowCount < targetCount) && (m_shouldContinue.get())) {
                    CountDownLatch latch = new CountDownLatch(batchSize);
                    // try to insert batchSize random rows
                    for (int i = 0; i < batchSize; i++) {
                        // introduce skew in the partition data (also need an empty partition
                        long p = Math.abs((long)(r.nextGaussian() * this.partitionCount-1));
                        try {
                            m_permits.acquire();
                        } catch (InterruptedException e) {
                            if (!m_shouldContinue.get()) {
                                return;
                            }
                            log.error("NibbleDeleteLoader thread interrupted while waiting for permit. " + e.getMessage());
                        }
                        insertsTried++;
                        client.callProcedure(new InsertCallback(latch), tableName.toUpperCase() + "TableInsert", p, data);
                    }
                    try {
                        latch.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        if (!m_shouldContinue.get()) {
                            return;
                        }
                        log.error("NibbleDeleteLoader thread interrupted while waiting." + e.getMessage());
                    }
                    long nextRowCount = TxnId2Utils.getRowCount(client, tableName);
                    // if no progress, throttle a bit
                    if (nextRowCount == currentRowCount) {
                        try { Thread.sleep(1000); } catch (Exception e2) {}
                    }
                    currentRowCount = nextRowCount;
                    log.info("NibbleDeleteLoader " + tableName.toUpperCase() + " current count: " + currentRowCount + " rows deleted: " + rowsDeletedTotal);
                }
            }
            catch (Exception e) {
                if ( e instanceof InterruptedIOException && ! m_shouldContinue.get()) {
                    continue;
                }
                // on exception, log and end the thread, but don't kill the process
                hardStop("NibbleDeleteLoader failed a 'TableInsert' procedure call for table '" + tableName + "' " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }
        }
        log.info("NibbleDeleteLoader exit for table " + tableName + " rows sent: " + insertsTried + " inserted: " + rowsLoaded);
        hardStop("NibbleDeleteLoader exit");
    }
}
