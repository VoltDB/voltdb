/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.voltcore.logging.Level;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class TruncateTableLoader extends BenchmarkThread {

    // Setting this to true ensures that we are testing the @SwapTables system
    // stored procedure being added to VoltDB V7.1; some of the code below was
    // instead written when we planned to add a DML statement of the form:
    //     "SWAP TABLE T1 WITH T2"
    // This setting makes sure that we are not using that code, which is retained
    // below in case we ever do support that DML version of Swap Tables.
    final boolean USE_AT_SWAP_TABLES_PROC = true;

    final Client client;
    final long targetCount;
    final String tableName;
    final String swapTableName;
    final int rowSize;
    final int batchSize;
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore m_permits;
    String truncateProcedure = "TruncateTable";
    String swapProcedure = "SwapTables";
    String scanAggProcedure = "ScanAggTable";
    long insertsTried = 0;
    long rowsLoaded = 0;
    long nTruncates = 0;
    long nSwaps = 0;
    float mpRatio;
    float swapRatio;

    TruncateTableLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits, float mpRatio, float swapRatio) {
        setName("TruncateTableLoader");
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        this.m_permits = permits;
        this.mpRatio = mpRatio;
        this.swapRatio = swapRatio;

        if ("trup".equalsIgnoreCase(tableName)) {
            this.swapTableName = "swapp";
        } else {
            this.swapTableName = "swapr";
        }

        // make this run more than other threads
        setPriority(getPriority() + 1);

        log.info("TruncateTableLoader table: "+ tableName + " targetCount: " + targetCount);

        // To get more detailed output, uncomment this:
        //log.setLevel(Level.DEBUG);
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
            if (isStatusSuccess(clientResponse, (byte)0, "insert into", tableName)) {
                Benchmark.txnCount.incrementAndGet();
                rowsLoaded++;
            }
            latch.countDown();
        }
    }

    private boolean isStatusSuccess(ClientResponse clientResponse,
            byte shouldRollback, String truncateOrSwap, String tableName) {
        byte status = clientResponse.getStatus();
        if (status == ClientResponse.GRACEFUL_FAILURE ||
                (shouldRollback == 0 && status == ClientResponse.USER_ABORT)) {
            hardStop("TruncateTableLoader gracefully failed to " + truncateOrSwap + " table "
                + tableName + " and this shoudn't happen. Exiting.", clientResponse);
        }
        if (status == ClientResponse.SUCCESS) {
            return true;
        } else {
            // log what happened
            log.warn("TruncateTableLoader ungracefully failed to " + truncateOrSwap + " table " + tableName);
            log.warn(((ClientResponseImpl) clientResponse).toJSONString());
            return false;
        }
    }

    private long[] swapTables(byte shouldRollback, String sp) throws IOException, ProcCallException {
        long[] rowCounts = new long[2];

        // Get the row counts, before doing the table-swap
        long tableRowCount = -1;
        long swapRowCount  = -1;
        try {
            tableRowCount = TxnId2Utils.getRowCount(client, tableName);
            swapRowCount  = TxnId2Utils.getRowCount(client, swapTableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }

        // Perform the table-swap
        ClientResponse clientResponse = null;
        String swapProcName = "@SwapTables";
        if (USE_AT_SWAP_TABLES_PROC) {  // Tests @SwapTables stored proc
            if (shouldRollback != 0) {
                clientResponse = client.callProcedure(swapProcName, tableName.toUpperCase(), "NONEXISTENT_TABLE");
            } else {
                clientResponse = client.callProcedure(swapProcName, tableName.toUpperCase(), swapTableName.toUpperCase());
            }
        } else {  // Tests "SWAP TABLE T1 WITH T2" DML - not currently supported
            swapProcName = tableName.toUpperCase() + sp;
            long p = Math.abs(r.nextLong());
            clientResponse = client.callProcedure(swapProcName, p, shouldRollback);
        }
        if (isStatusSuccess(clientResponse, shouldRollback, "swap", tableName)) {
            Benchmark.txnCount.incrementAndGet();
            nSwaps++;
        }

        // Confirm that the table-swap worked correctly, by checking the row counts
        try {
            rowCounts[0] = TxnId2Utils.getRowCount(client, tableName);
            rowCounts[1] = TxnId2Utils.getRowCount(client, swapTableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }
        if (rowCounts[0] != swapRowCount || rowCounts[1] != tableRowCount) {
            String message = swapProcName+" on "+tableName+", "+swapTableName
                    + " failed to swap row counts correctly: went from " + tableRowCount
                    + ", " + swapRowCount + " to " + rowCounts[0] + ", " + rowCounts[1];
            if ("TRUPSwapTablesSP".equals(swapProcName)) {
                // TRUPSwapTablesSP, being SP, does not swap the two tables
                // entirely, so this situation is expected, and not fatal
                log.warn(message);
            } else {
                hardStop("TruncateTableLoader: " + message);
            }
        }
        return rowCounts;
    }

    private void truncateTable(byte shouldRollback, String tp) throws IOException, ProcCallException {
        long p = Math.abs(r.nextLong());

        // Perform the truncate
        ClientResponse clientResponse = client.callProcedure(tableName.toUpperCase() + tp, p, shouldRollback);
        if (isStatusSuccess(clientResponse, shouldRollback, "truncate", tableName)) {
            Benchmark.txnCount.incrementAndGet();
            nTruncates++;
        }

        // Confirm that the truncate worked correctly, by checking the row count
        // (even though the stored procedures themselves also check this)
        long rowCount = -1;
        try {
            rowCount = TxnId2Utils.getRowCount(client, tableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }
        if (rowCount != 0) {
            String truncateProcName = tableName.toUpperCase() + tp;
            String message = "Table '"+tableName+"' has "+rowCount+" rows after truncate "
                    + "(by stored proc "+truncateProcName+"): non-zero";
            if ("TRUPTruncateTableSP".equals(truncateProcName)) {
                // TRUPTruncateTableSP, being SP, does not truncate the entire
                // table, so this situation is expected, and not fatal
                log.warn(message + " (OK for SP)");
            } else {
                hardStop("TruncateTableLoader: " + message + "!");
            }
        }
    }

    @Override
    public void run() {
        byte[] data = new byte[rowSize];
        byte shouldRollback = 0;
        long currentRowCount = 0;
        long swapRowCount = 0;
        while (m_shouldContinue.get()) {
            r.nextBytes(data);

            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }
            CountDownLatch latch = new CountDownLatch(0);
            try {
                // insert some batches...
                int tc = batchSize * r.nextInt(99);
                while ((currentRowCount < tc) && (m_shouldContinue.get())) {
                    latch = new CountDownLatch(batchSize);
                    // try to insert batchSize random rows
                    for (int i = 0; i < batchSize; i++) {
                        long p = Math.abs(r.nextLong());
                        m_permits.acquire();
                        insertsTried++;
                        client.callProcedure(new InsertCallback(latch), tableName.toUpperCase() + "TableInsert", p, data);
                    }
                    latch.await();
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
                log.error("TruncateTableLoader failed a TableInsert procedure call for table '" + tableName + "': " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }

            if (latch.getCount() != 0) {
                hardStop("latch not zero " + latch.getCount());
            }

            // check the initial table counts, prior to truncate and/or swap
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
                swapRowCount = TxnId2Utils.getRowCount(client, swapTableName);
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }

            try {
                log.debug(tableName + " current row count is " + currentRowCount
                         + "; " + swapTableName + " row count is " + swapRowCount);
                String tp = this.truncateProcedure;
                String sp = this.swapProcedure;
                if (tableName == "trup") {
                    tp += r.nextInt(100) < mpRatio * 100. ? "MP" : "SP";
                    sp += r.nextInt(100) < mpRatio * 100. ? "MP" : "SP";
                }

                // perhaps swap tables, before truncating: we swap both before
                // and/or after truncating (but not always), so that we are
                // sometimes swapping two empty tables, sometimes two "full"
                // (non-empty) tables, and sometimes one of each
                if (r.nextInt(100) < swapRatio * 100.) {
                    shouldRollback = (byte) (r.nextInt(10) == 0 ? 1 : 0);
                    long[] rowCounts = swapTables(shouldRollback, sp);
                    currentRowCount = rowCounts[0];
                    swapRowCount = rowCounts[1];
                }

                // truncate the (trur or trup) table
                shouldRollback = (byte) (r.nextInt(10) == 0 ? 1 : 0);
                if (shouldRollback != 0) {
                    currentRowCount = TxnId2Utils.getRowCount(client, tableName);
                    swapRowCount = TxnId2Utils.getRowCount(client, swapTableName);
                }
                truncateTable(shouldRollback, tp);

                // perhaps swap tables, after truncating (why? see comment above)
                if (r.nextInt(100) < swapRatio * 100.) {
                    shouldRollback = (byte) (r.nextInt(10) == 0 ? 1 : 0);
                    if (shouldRollback != 0) {
                        currentRowCount = TxnId2Utils.getRowCount(client, tableName);
                        swapRowCount = TxnId2Utils.getRowCount(client, swapTableName);
                    }
                    long[] rowCounts = swapTables(shouldRollback, sp);
                    currentRowCount = rowCounts[0];
                    swapRowCount = rowCounts[1];
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
                        hardStop("TruncateTableLoader failed a TruncateTable or SwapTable ProcCallException call for table '"
                                + tableName + "': " + e.getMessage());
                    }
                }
                if (!TxnId2Utils.isConnectionTransactionCatalogOrServerUnavailableIssue(e.getClientResponse().getStatusString())) {
                    long newRowCount = -1;
                    try {
                        newRowCount = TxnId2Utils.getRowCount(client, tableName);
                    } catch (Exception e2) {
                        hardStop("getrowcount exception", e2);
                    }
                    if (newRowCount != currentRowCount) {
                        hardStop("TruncateTableLoader call to TruncateTable or SwapTable changed row count from " + currentRowCount
                                + " to "+newRowCount+", despite rollback, for table '" + tableName + "': " + e.getMessage());
                    }
                }
            }
            catch (NoConnectionsException e) {
                // on exception, log and end the thread, but don't kill the process
                log.warn("TruncateTableLoader failed a non-proc call exception for table '" + tableName + "': " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }
            catch (IOException e) {
                // just need to fall through and get out
                throw new RuntimeException(e);
            }

            // scan-agg table
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
                swapRowCount = TxnId2Utils.getRowCount(client, swapTableName);
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }

            try {
                log.debug("scan agg: " + tableName + " current row count is " + currentRowCount
                         + "; " + swapTableName + " row count is " + swapRowCount);
                shouldRollback = (byte) (r.nextInt(10) == 0 ? 1 : 0);
                long p = Math.abs(r.nextLong());
                String sp = this.scanAggProcedure;
                if (tableName == "trup")
                    sp += r.nextInt(100) < mpRatio * 100. ? "MP" : "SP";
                ClientResponse clientResponse = client.callProcedure(tableName.toUpperCase() + sp, p, shouldRollback);

                if (isStatusSuccess(clientResponse, shouldRollback, "scan-agg", tableName)) {
                    Benchmark.txnCount.incrementAndGet();
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
                        hardStop("TruncateTableLoader failed a ScanAgg ProcCallException call for table '" + tableName + "': " + e.getMessage());
                    }
                }
            }
            catch (NoConnectionsException e) {
                // on exception, log and end the thread, but don't kill the process
                log.warn("TruncateTableLoader failed a non-proc call exception for table '" + tableName + "': " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }
            catch (IOException e) {
                // just need to fall through and get out
                throw new RuntimeException(e);
            }
            log.info("table: " + tableName + "; rows sent: " + insertsTried + "; inserted: "
                    + rowsLoaded + "; truncates: " + nTruncates + "; swaps: " + nSwaps);
        }
        log.info("TruncateTableLoader normal exit for table " + tableName + "; rows sent: " + insertsTried
                + "; inserted: " + rowsLoaded + "; truncates: " + nTruncates + "; swaps: " + nSwaps);
    }

}
