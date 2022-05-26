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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class TaskLoader extends BenchmarkThread {

    final Client client;
    final long targetCount;
    final String tableName;
    final int rowSize;
    final int batchSize;
    final int partitionCount;
    /*
     * final String type; // "EXPORT" -- migrate to export stream, or "TTL" --
     * nibble delete
     */
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore m_permits;
    long insertsTried = 0;
    AtomicInteger rowsLoaded = new AtomicInteger(0);
    long deletesRemaining = 0;
    /* this is the task delay configured on each of the tasks in the ddl */
    long TASKDELAY = 10; // seconds

    TaskLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits,
            int partitionCount) {
        setName("TaskLoader-" + tableName);
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        m_permits = permits;
        this.partitionCount = partitionCount;

        // make this run more than other threads
        setPriority(getPriority() + 1);
    }

    void shutdown() {
        // log.debug("+++ In TaskLoader shutdown method");
        m_shouldContinue.set(false);
        log.info("TaskLoader " + tableName + " shutdown: inserts tried: " + insertsTried + " rows loaded: "
                + rowsLoaded.get() + " deletes remaining: " + deletesRemaining);
    }

    // "@LowImpactDelete",
    // [FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_STRING,FastSerializer.VOLTTYPE_BIGINT,FastSerializer.VOLTTYPE_BIGINT],
    // [tablename,columnName,columnThresholdInMicros,comparisonOp,chunksize,timeoutms]

    class InsertCallback implements ProcedureCallback {

        CountDownLatch latch;

        InsertCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            byte status = clientResponse.getStatus();
            if (status == ClientResponse.GRACEFUL_FAILURE || status == ClientResponse.USER_ABORT) {
                // log what happened
                Benchmark.hardStop("TaskLoader gracefully failed to insert into table " + tableName
                        + " and this shoudn't happen. Exiting.");
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.warn("TaskLoader ungracefully failed to insert into table " + tableName);
                log.warn(((ClientResponseImpl) clientResponse).toJSONString());
            } else {
                Benchmark.txnCount.incrementAndGet();
                rowsLoaded.incrementAndGet();
            }
            latch.countDown();
        }
    }

    class TaskMonitor extends Thread {
        final String tableName;
        final String tsColumnName;

        TaskMonitor(String tableName, String tsColumnName) {
            this.tableName = tableName;
            this.tsColumnName = tsColumnName;
            setName("TaskMonitor-" + tableName);
        }

        @Override
        public void run() {
            log.info("TaskMonitor is running");
            long totalDeleted = 0;
            int retries = 4;
            while (m_shouldContinue.get()) {
                try {
                    // give a little wiggle room when checking if rows should've been deleted.
                    long taskdelay = new Double(Math.ceil(TASKDELAY * 1.5)).longValue();
                    ClientResponse response = TxnId2Utils.doProcCall(client, "@AdHoc", "select count(*) from "
                            + tableName + " where " + tsColumnName + " < DATEADD(SECOND,-" + taskdelay + ",NOW) ");
                    long remainingRows = TxnId2Utils.doProcCall(client, "@AdHoc", "select count(*) from " + tableName)
                            .getResults()[0].asScalarLong();
                    Map<String, Object> stats;
                    try {
                        stats = getStats(tableName);
                        //log.info("+++ TASK STATS: " + stats);
                    } catch (ProcCallException e) {
                        // retry
                        continue;
                    }
                    if (response.getStatus() == ClientResponse.SUCCESS) {
                        long unDeletedRows = response.getResults()[0].asScalarLong();
                        // log.debug("+++ " + tableName + " rows: " + unDeletedRows);
                        // Object rows_left = (type == "TTL") ? stats.get("ROWS_LEFT") :
                        // stats.get("TUPLE_PENDING");
                        log.info("Total inserted rows:" + rowsLoaded.get() + " Rows behind from being deleted/migrated:"
                                + unDeletedRows);
                        // log.info("Stats Rows behind from being deleted/migrated:" + rows_left);
                        long currentRemainingRows = rowsLoaded.get() - remainingRows;
                        if (currentRemainingRows == totalDeleted && totalDeleted > 0) {
                            // nothing deleted in last round
                            retries--;
                            if (retries == 0) {
                                Benchmark.hardStop("Delete Task failed");
                            }
                        } else {
                            totalDeleted = currentRemainingRows;
                            retries = 0;
                        }
                    } else {
                        log.error("Response failed:" + response.getAppStatusString() + " status:"
                                + response.getStatusString());
                        Benchmark.hardStop("Delete Task failed");
                    }
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    log.info("Thread is interrupted");
                } catch (ProcCallException pe) {
                    pe.printStackTrace();
                    Benchmark.hardStop("Error executing procedure:" + pe.getMessage());
                }
            }
        }

        public Map<String, Object> getStats(String tableName) throws ProcCallException {
            Map<String, Object> stats = new HashMap<String, Object>();
            final String STATSTYPE = "TASK";
            ClientResponse cr = null;
            log.info("stats request, type: " + STATSTYPE);
            try {
                cr = client.callProcedure("@Statistics", STATSTYPE);
            } catch (IOException e) {
                log.error(e.getMessage());
                return stats;
            } catch (ProcCallException pe) {
                log.warn(pe.getMessage());
                throw pe;
            }
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                log.error("Failed to call Statistics " + STATSTYPE + " proc.");
                log.error(((ClientResponseImpl) cr).toJSONString());
                return stats;
            }

            VoltTable t = cr.getResults()[0];
            // log.info("+++ Stats:");
            // log.info(t.toFormattedString());
            t.resetRowPosition();
            stats.put("TABLE", tableName);
            /*
             * String task_name; long procedure_invokes = 0; while (t.advanceRow()) { if
             * (tableName.equalsIgnoreCase(t.getString("NAME"))) { deleted +=
             * t.getLong("PROCEDURE_INVOCATIONS"); left += t.getLong("ROWS_REMAINING"); } }
             * stats.put("ROWS_DELETED", deleted); stats.put("ROWS_LEFT", left);
             */
            return stats;
        }

        public long getRemainingRowCount(String tableName) {
            long remainingRows;
            try {
                remainingRows = TxnId2Utils.doProcCall(client, "@AdHoc", "select count(*) from " + tableName)
                        .getResults()[0].asScalarLong();
            } catch (ProcCallException e) {
                e.printStackTrace();
                return -1;
            }
            return remainingRows;
        }
    }

    @Override
    public void run() {
        TaskMonitor monitor = new TaskMonitor(this.tableName, "ts");
        monitor.start();
        log.info("TaskMonitor started.");
        byte[] data = new byte[rowSize];
        long currentRowCount;
        boolean diedEarly = false;
        while (m_shouldContinue.get()) {
            r.nextBytes(data);
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
                // insert some batches...
                boolean isDone = false;
                CountDownLatch latch = new CountDownLatch(batchSize);
                // try to insert batchSize random rows
                for (int i = 0; i < batchSize; i++) {
                    // introduce skew in the partition data (also need an empty partition
                    long p = Math.abs((long) (r.nextGaussian() * this.partitionCount - 1));
                    try {
                        m_permits.acquire();
                    } catch (InterruptedException e) {
                        if (!m_shouldContinue.get()) {
                            isDone = true;
                            break;
                        }
                        log.error("TaskLoader thread interrupted while waiting for permit. " + e.getMessage());
                    }
                    insertsTried++;
                    client.callProcedure(new InsertCallback(latch), tableName.toUpperCase() + "TableInsert", p, data);
                }
                if (isDone) {
                    break;
                }
                try {
                    latch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    if (!m_shouldContinue.get()) {
                        break;
                    }
                    log.error("TaskLoader thread interrupted while waiting." + e.getMessage());
                }
                long nextRowCount = TxnId2Utils.getRowCount(client, tableName);
                // if no progress, throttle a bit
                if (nextRowCount == currentRowCount) {
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e2) {
                    }
                }
                currentRowCount = nextRowCount;
                log.info("TaskLoader " + tableName.toUpperCase() + " current count: " + currentRowCount
                        + " total inserted rows: " + rowsLoaded.get());
                log.info("TaskLoader " + tableName.toUpperCase() + " has nothing to do and completed successfully");
            } catch (Exception e) {
                if (e instanceof InterruptedIOException && !m_shouldContinue.get()) {
                    continue;
                }
                diedEarly = true;
                // on exception, log and end the thread, but don't kill the process
                log.error("TaskLoader failed a 'TableInsert' procedure call for table '" + tableName + "' "
                        + e.getMessage());
                break;
            }
        }

        log.info("TaskLoader completed for table " + tableName + " rows inserted: " + rowsLoaded.get());
        long rowRemaining = monitor.getRemainingRowCount(tableName);
        if (!diedEarly) {
            final int TWELVE = 12;
            int retries = TWELVE;
            while (rowRemaining != 0) {
                rowRemaining = monitor.getRemainingRowCount(tableName);
                // log.debug("+++ Waiting for table to drain. Retry " + retries + ", rows remaining: " + rowRemaining);

                if (rowRemaining == 0) {
                    break;
                }
                try {
                    Thread.sleep(3000);
                } catch (Exception ex) {
                }
                retries -= 1;
                if (retries == 0) {
                    Benchmark.hardStop(
                            "Delete task hasn't finished on table '" + tableName + "' after " + TWELVE + " retries");
                }

            }
        }
        log.info("TaskLoader completed for table " + tableName + " rows remaining to be deleted:" + rowRemaining);
    }
}
