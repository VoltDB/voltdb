/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
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
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);

    BigTableLoader(Client client, String tableName, int targetCount, int rowSize, int batchSize) {
        setName("BigTableLoader");

        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;

        // make this run more than other threads
        setPriority(getPriority() + 1);
    }

    long getRowCount() throws NoConnectionsException, IOException, ProcCallException {
        VoltTable t = client.callProcedure("@AdHoc", "select count(*) from " + tableName + ";").getResults()[0];
        return t.asScalarLong();
    }

    void shutdown() {
        m_shouldContinue.set(false);
    }

    class InsertCallback implements ProcedureCallback {

        CountDownLatch latch;

        InsertCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            byte status = clientResponse.getStatus();
            if (status == ClientResponse.GRACEFUL_FAILURE) {
                // log what happened
                log.error("BigTableLoader gracefully failed to insert into table " + tableName + " and this shoudn't happen. Exiting.");
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
                // stop the world
                System.exit(-1);
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.error("BigTableLoader ungracefully failed to insert into table " + tableName);
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
                // stop the loader
                m_shouldContinue.set(false);
            }
            latch.countDown();
        }
    }

    @Override
    public void run() {
        byte[] data = new byte[rowSize];
        r.nextBytes(data);

        try {
            long currentRowCount = getRowCount();
            while ((currentRowCount < targetCount) && (m_shouldContinue.get())) {
                CountDownLatch latch = new CountDownLatch(batchSize);
                // try to insert batchSize random rows
                for (int i = 0; i < batchSize; i++) {
                    long p = Math.abs(r.nextLong());
                    client.callProcedure(new InsertCallback(latch), tableName.toUpperCase() + "TableInsert", p, data);
                }
                latch.await(10, TimeUnit.SECONDS);
                long nextRowCount = getRowCount();
                // if no progress, throttle a bit
                if (nextRowCount == currentRowCount) {
                    Thread.sleep(1000);
                }
                currentRowCount = nextRowCount;
            }

        }
        catch (Exception e) {
            // on exception, log and end the thread, but don't kill the process
            log.error("BigTableLoader failed a procedure call for table " + tableName +
                    " and the thread will now stop.", e);
            return;
        }
    }

}
