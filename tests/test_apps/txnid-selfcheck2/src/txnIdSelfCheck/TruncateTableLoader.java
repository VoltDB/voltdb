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

package txnIdSelfCheck;

import org.voltdb.ClientResponseImpl;
import org.voltdb.client.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TruncateTableLoader extends BenchmarkThread {

    // Setting this to true ensures that we are testing the @SwapTables system
    // stored procedure being added to VoltDB V7.1; some of the code below was
    // instead written when we planned to add a DML statement of the form:
    //     "SWAP TABLE T1 WITH T2"
    // This setting makes sure that we are not using that code, which is retained
    // below in case we ever do support that DML version of Swap Tables.
    final boolean USE_SWAP_TABLES_SYSPROC = true;

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
    boolean swaptables;

    TruncateTableLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits, float mpRatio, float swapRatio) {
        setName("TruncateTableLoader");
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        this.m_permits = permits;
        this.mpRatio = mpRatio;
        this.swaptables = (swapRatio <= 0.0) ? false : true;
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

        AtomicInteger latch;

        InsertCallback(AtomicInteger latch) {
            this.latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (isStatusSuccess(clientResponse, (byte)0, "insert into", tableName)) {
                Benchmark.txnCount.incrementAndGet();
                rowsLoaded++;
            }
            latch.decrementAndGet();
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
        if (USE_SWAP_TABLES_SYSPROC)
            return swapTables_sysproc(shouldRollback, sp);
        else
            return swapTables_dml(shouldRollback, sp);
    }

    private long[] swapTables_sysproc(byte shouldRollback, String sp) throws IOException, ProcCallException {
        long[] b4RowCounts = new long[] {-1, -1};
        long[] afterRowCounts = new long[] {-1, -1};

        // Get the row counts, before doing the table-swap
        try {
            b4RowCounts[0] = TxnId2Utils.getRowCount(client, tableName);
            b4RowCounts[1]  = TxnId2Utils.getRowCount(client, swapTableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }

        // Perform the table-swap
        ClientResponse clientResponse = null;
        String swapProcName = "@SwapTables";
        // swaptables sysproc is ALWAYS MP
        // we use one-shot here to bypass the retry mechanisms of doProcCall,
        // then we evaluate the response and check the behavior
        clientResponse = TxnId2Utils.doProcCallOneShot(client, swapProcName,
                tableName.toUpperCase(),
                shouldRollback == 0 ? swapTableName.toUpperCase() : "NONEXISTENT_TABLE");
        // Fake set shouldRollback here so that USER_ABORTs generated by SwapTables using AdHoc behind the scenes
        // do not get interpreted as aberrant rollback behavior.
        Boolean result = isStatusSuccess(clientResponse, (byte) 1, "swap", tableName);

        /* check the table counts and various result cases
         * success: tables should be swapped (counts appear swapped)
         * failed: tables should not be swapped
         * indeterminate: tables should either be swapped or not
         */
        try {
            afterRowCounts[0] = TxnId2Utils.getRowCount(client, tableName);
            afterRowCounts[1] = TxnId2Utils.getRowCount(client, swapTableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }

        // if we can't tell if the txn committed or not
        if (!result && TxnId2Utils.isTransactionStateIndeterminate(clientResponse)) {
            // the best we can do is check that neither table has been mutated is some unexpected way
            if ( ! (afterRowCounts[0] == b4RowCounts[0] || afterRowCounts[0] == b4RowCounts[1])
                    || ! (afterRowCounts[1] == b4RowCounts[0] || afterRowCounts[1] == b4RowCounts[1])) {
                String message = swapProcName + " on " + tableName + ", " + swapTableName
                        + " count(s) are not as expected after an indeterminate result: before: " + b4RowCounts[0] + ", " + b4RowCounts[1]
                        + " after: " + afterRowCounts[0] + ", " + afterRowCounts[1] + ", rollback: " + shouldRollback;
                hardStop("TruncateTableLoader: " + message);
            }
        } else {
            // either succeeded or failed or rollback deterministic response, tables should NOT be swapped
            // if shouldRollback is set the operation will fail (ie. result will be false)
            // z==0 compares the counts in the swapped (success) configuration
            int z = result ? 0 : 1;
            if (afterRowCounts[(z + 0) & 1] != b4RowCounts[1] || afterRowCounts[(z + 1) & 1] != b4RowCounts[0]) {
                String message = swapProcName + " on " + tableName + ", " + swapTableName
                        + " failed to swap row counts correctly: went from " + b4RowCounts[0] + ", " + b4RowCounts[1]
                        + " to " + afterRowCounts[0] + ", " + afterRowCounts[1] + ", rollback: " + shouldRollback;
                hardStop("TruncateTableLoader: " + message);
            }
            if (result) {
                if (shouldRollback != 0)
                    hardStop("shouldRollback is set but operation passed");
                Benchmark.txnCount.incrementAndGet();
                nSwaps++;
            }
        }
        return afterRowCounts;
    }

    private long[] swapTables_dml(byte shouldRollback, String sp) throws IOException, ProcCallException {
        long[] rowCounts = new long[2];
        // Tests "SWAP TABLE T1 WITH T2" DML - NYI
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
        String swapProcName = tableName.toUpperCase() + sp;
        long p = Math.abs(r.nextLong());
        clientResponse = client.callProcedure(sp, p, shouldRollback);
        if (isStatusSuccess(clientResponse, shouldRollback, "swap", tableName)) {
            Benchmark.txnCount.incrementAndGet();
            nSwaps++;
        }
        // Confirm that the table-swap worked correctly, by checking the row counts
        // nb. swap is not supported with DR yet, with XDCR this check is likeley to fail
        // unless we take other action or change the test.
        try {
            rowCounts[0] = TxnId2Utils.getRowCount(client, tableName);
            rowCounts[1] = TxnId2Utils.getRowCount(client, swapTableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }
        if (rowCounts[0] != swapRowCount || rowCounts[1] != tableRowCount) {
            // XXX should check the row counts by partition in the SP case using select count(*) from ... where p=...
            String message = swapProcName+" on "+tableName+", "+swapTableName
                    + " failed to swap row counts correctly: went from " + tableRowCount
                    + ", " + swapRowCount + " to " + rowCounts[0] + ", " + rowCounts[1];
            hardStop("TruncateTableLoader: " + message);
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
        // while we would like to check for zero rows in the table outside the txn this test will fail
        // if, for example, with XDCR rows may be replicated from another cluster after the truncate txn.
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
            AtomicInteger latch = new AtomicInteger(0);
            try {
                // insert some batches...
                int tc = batchSize * r.nextInt(99);
                while ((currentRowCount < tc) && (m_shouldContinue.get())) {
                    latch = new AtomicInteger(0);
                    // try to insert batchSize random rows
                    for (int i = 0; i < batchSize; i++) {
                        long p = Math.abs(r.nextLong());
                        m_permits.acquire();
                        insertsTried++;
                        try {
                            client.callProcedure(new InsertCallback(latch), tableName.toUpperCase() + "TableInsert", p, data);
                            latch.incrementAndGet();
                        } catch (Exception e) {
                            // on exception, log and end the thread, but don't kill the process
                            log.error("TruncateTableLoader failed a TableInsert procedure call for table '" + tableName + "': " + e.getMessage());
                            try {
                                Thread.sleep(3000);
                            } catch (Exception e2) {
                            }
                        }
                    }
                    while (latch.get() > 0)
                        Thread.sleep(10);
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
            } catch (Exception e) {
                hardStop(e);
            }

            if (latch.get() != 0) {
                hardStop("latch not zero " + latch.get());
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

                // maybe swap tables before truncating
                // sinc @SwapTables is inherintly MP doesn't make much sense to do this in the MP thread
                if ( swaptables && (r.nextInt(100) < swapRatio * 100.) ) {
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
                hardStop(e);
            }
            log.info("table: " + tableName + "; rows sent: " + insertsTried + "; inserted: "
                    + rowsLoaded + "; truncates: " + nTruncates + "; swaps: " + nSwaps);
        }
        log.info("TruncateTableLoader normal exit for table " + tableName + "; rows sent: " + insertsTried
                + "; inserted: " + rowsLoaded + "; truncates: " + nTruncates + "; swaps: " + nSwaps);
    }

}
