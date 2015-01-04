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

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class BigTableLoader extends Thread {

    static VoltLogger log = new VoltLogger("HOST");

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
    long nTruncates = 0;

    BigTableLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits, int partitionCount) {
        setName("BigTableLoader");
        setDaemon(true);

        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        m_permits = permits;
        this.partitionCount = partitionCount;

        // make this run more than other threads
        setPriority(getPriority() + 1);
        log.info("BigTableLoader table: "+ tableName + " targetCount: " + targetCount + " storage required: " + targetCount*rowSize + " bytes");
    }

    long getRowCount() throws NoConnectionsException, IOException, ProcCallException {
        // XXX/PSR maybe we don't care (so much) about mp reads relative to mpRatio control?
        VoltTable t = client.callProcedure("@AdHoc", "select count(*) from " + tableName + ";").getResults()[0];
        return t.asScalarLong();
    }

    void shutdown() {
        m_shouldContinue.set(false);
        log.info("BigTableLoader " + tableName + " shutdown: inserts tried " + insertsTried + " rows loaded " + rowsLoaded +
                    " truncates " + nTruncates);
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
                log.error("BigTableLoader gracefully failed to insert into table " + tableName + " and this shoudn't happen. Exiting.");
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
                Benchmark.printJStack();
                // stop the world
                System.exit(-1);
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.error("BigTableLoader ungracefully failed to insert into table " + tableName);
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
            }
            else {
                rowsLoaded++;
            }
            latch.countDown();
        }
    }

    @Override
    public void run() {
        byte[] data = new byte[rowSize];
        long currentRowCount;
        while (m_shouldContinue.get()) {
            r.nextBytes(data);

            try {
                currentRowCount = getRowCount();
                // insert some batches...
                while ((currentRowCount < targetCount) && (m_shouldContinue.get())) {
                    CountDownLatch latch = new CountDownLatch(batchSize);
                    // try to insert batchSize random rows
                    for (int i = 0; i < batchSize; i++) {
                        long p = Math.abs((long)(r.nextGaussian() * this.partitionCount));
                        try {
                            m_permits.acquire();
                        } catch (InterruptedException e) {
                            if (!m_shouldContinue.get()) {
                                return;
                            }
                            log.error("BigTableLoader thread interrupted while waiting for permit.", e);
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
                        log.error("BigTableLoader thread interrupted while waiting.", e);
                    }
                    long nextRowCount = getRowCount();
                    // if no progress, throttle a bit
                    if (nextRowCount == currentRowCount) {
                        try { Thread.sleep(1000); } catch (Exception e2) {}
                    }
                    currentRowCount = nextRowCount;
                    log.debug("BigTableLoader " + tableName.toUpperCase() + " count " + currentRowCount);
                }
            }
            catch (Exception e) {
                if ( e instanceof InterruptedIOException && ! m_shouldContinue.get()) {
                    continue;
                }
                // on exception, log and end the thread, but don't kill the process
                log.error("BigTableLoader failed a TableInsert procedure call for table " + tableName, e);
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }
        }
        log.info("BigTableLoader normal exit for table " + tableName + " rows sent: " + insertsTried + " inserted: " + rowsLoaded + " truncates: " + nTruncates);
    }

}
