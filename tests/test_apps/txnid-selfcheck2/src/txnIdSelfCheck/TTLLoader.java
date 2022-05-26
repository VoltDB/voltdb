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
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import java.io.InterruptedIOException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TTLLoader extends BenchmarkThread {

    final Client client;
    final long targetCount;
    final String tableName;
    final int rowSize;
    final int batchSize;
    final int partitionCount;
    final String type;  // "EXPORT" -- migrate to export stream, or "TTL" -- nibble delete
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore m_permits;
    long insertsTried = 0;
    AtomicInteger rowsLoaded = new AtomicInteger(0);
    long deletesRemaining = 0;
    /* this is the ttl configured on each of the tables in the ddl */
    long TTL = 30;

    TTLLoader(Client client, String tableName, long targetCount, int rowSize, int batchSize, Semaphore permits, int partitionCount, String type) {
        setName("TTLLoader-"+tableName);
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.type = type;
        this.batchSize = batchSize;
        m_permits = permits;
        this.partitionCount = partitionCount;

        // make this run more than other threads
        setPriority(getPriority() + 1);
    }

    void shutdown() {
        m_shouldContinue.set(false);
        log.info("TTLLoader " + tableName + " shutdown: inserts tried: " + insertsTried + " rows loaded: " + rowsLoaded.get() +
                " deletes remaining: " + deletesRemaining);
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
                Benchmark.hardStop("TTLLoader gracefully failed to insert into table " + tableName + " and this shoudn't happen. Exiting.");
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.warn("TTLLoader ungracefully failed to insert into table " + tableName);
                log.warn(((ClientResponseImpl) clientResponse).toJSONString());
            }
            else {
                Benchmark.txnCount.incrementAndGet();
                rowsLoaded.incrementAndGet();
            }
            latch.countDown();
        }
    }

    class TTLMonitor extends Thread {
        final String tableName;
        final String tsColumnName;
        TTLMonitor(String tableName,String tsColumnName) {
            this.tableName = tableName;
            this.tsColumnName = tsColumnName;
            setName("TTLMonitor-"+tableName);
        }

        @Override
        public void run() {
            log.info("TTLMonitor is running");
            long totalDeleted = 0;
            int retries = 4;
            while (m_shouldContinue.get()) {
                try {
                    // give a little wiggle room when checking if rows should've been deleted.
                    long ttl = new Double(Math.ceil(TTL*1.5)).longValue();
                    ClientResponse response = TxnId2Utils.doProcCall(client, "@AdHoc", "select count(*) from "+tableName+" where "+tsColumnName+" < DATEADD(SECOND,-"+ttl+",NOW) ");
                    long remainingRows = TxnId2Utils.doProcCall(client, "@AdHoc", "select count(*) from "+tableName).getResults()[0].asScalarLong();
                    Map<String,Object> stats;
                    try {
                        stats = getStats(tableName, type);
                    } catch ( ProcCallException e ) {
                        // retry
                        continue;
                    }
                    if (response.getStatus() == ClientResponse.SUCCESS ) {
                        long unDeletedRows  = response.getResults()[0].asScalarLong();
                        Object rows_left = (type == "TTL") ? stats.get("ROWS_LEFT") : stats.get("TUPLE_PENDING");
                        log.info("Total inserted rows:" + rowsLoaded.get() + " Rows behind from being deleted/migrated:" + unDeletedRows);
                        log.info("Stats Rows behind from being deleted/migrated:" + rows_left);
                        long currentRemainingRows = rowsLoaded.get() - remainingRows;
                        if (currentRemainingRows == totalDeleted && totalDeleted > 0){
                            //nothing deleted in last round
                            retries--;
                            if (retries == 0) {
                                Benchmark.hardStop("Nibble Delete failed");
                            }
                        } else {
                            totalDeleted = currentRemainingRows;
                            retries = 0;
                        }
                    } else {
                        log.error("Response failed:"+response.getAppStatusString()+" status:"+response.getStatusString());
                        Benchmark.hardStop("Nibble Delete failed");
                    }
                    Thread.sleep(4000);
                } catch(InterruptedException e) {
                    log.info("Thread is interrupted");
                } catch(ProcCallException pe) {
                    pe.printStackTrace();
                    Benchmark.hardStop("Error executing procedure:"+pe.getMessage());
                }
            }
        }

        public Map<String,Object> getStats(String tableName, String type ) throws ProcCallException {
            Map<String,Object> stats = new HashMap<String,Object>();
            ClientResponse cr = null;
            log.info("stats request, type: " + type);
            try {
                cr = client.callProcedure("@Statistics", type);
            } catch(IOException e) {
                log.error(e.getMessage());
                return stats;
            } catch(ProcCallException pe) {
                    log.warn(pe.getMessage());
                    throw pe;
            }
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                log.error("Failed to call Statistics " + type + " proc.");
                log.error(((ClientResponseImpl) cr).toJSONString());
                return stats;
            }

            VoltTable t = cr.getResults()[0];
            System.out.println(t.toFormattedString());
            t.resetRowPosition();
            stats.put("TABLE", tableName);
            switch (type) {

            case "TTL" :
                long deleted = 0;
                long left = 0;
                while ( t.advanceRow() ) {
                    if ( tableName.equalsIgnoreCase(t.getString("TABLE_NAME")) ) {
                        deleted += t.getLong("ROWS_DELETED");
                        left += t.getLong("ROWS_REMAINING");
                    }
                }
                stats.put("ROWS_DELETED", deleted);
                stats.put("ROWS_LEFT",left);
                return stats;
            case "EXPORT":
                long tuple_count = 0;
                long tuple_pending = 0;
                while ( t.advanceRow() ) {
                    if ( tableName.equalsIgnoreCase(t.getString("SOURCE")) &&
                         t.getString("ACTIVE").equalsIgnoreCase("TRUE") ) {
                        tuple_count += t.getLong("TUPLE_COUNT");
                        tuple_pending += t.getLong("TUPLE_PENDING");
                    }
                }
                stats.put("TUPLE_COUNT", tuple_count);
                stats.put("TUPLE_PENDING",tuple_pending);
                return stats;
            default:
                log.info("Invalid type, " + type + " in getStats");
                break;
            }
            return stats;
        }

        public long getRemainingRowCount(String tableName, String type) {
            Map<String,Object> stats;
            try {
                stats = getStats(tableName, "TTL");
            } catch ( ProcCallException e) {
                return -1;
            }
            if (stats.isEmpty()) {
                return -1;
            }
            long row_count = (type == "TTL") ? ((Long)stats.get("ROWS_LEFT")).longValue() : ((Long)stats.get("TUPLE_PENDING")).longValue();
            return row_count;
        }
    }
    @Override
    public void run() {
        TTLMonitor monitor = new TTLMonitor(this.tableName,"ts");
        monitor.start();
        log.info("TTLMonitor started.");
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
                    long p = Math.abs((long)(r.nextGaussian() * this.partitionCount-1));
                    try {
                        m_permits.acquire();
                    } catch (InterruptedException e) {
                        if (!m_shouldContinue.get()) {
                            isDone = true;
                            break;
                        }
                        log.error("TTLLoader thread interrupted while waiting for permit. " + e.getMessage());
                    }
                    insertsTried++;
                    client.callProcedure(new InsertCallback(latch), tableName.toUpperCase() + "TableInsert", p, data);
                }
                if (isDone ) {
                    break;
                }
                try {
                    latch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    if (!m_shouldContinue.get()) {
                        break;
                    }
                    log.error("TTLLoader thread interrupted while waiting." + e.getMessage());
                }
                long nextRowCount = TxnId2Utils.getRowCount(client, tableName);
                // if no progress, throttle a bit
                if (nextRowCount == currentRowCount) {
                    try { Thread.sleep(1000); } catch (Exception e2) {}
                }
                currentRowCount = nextRowCount;
                log.info("TTLLoader " + tableName.toUpperCase() + " current count: " + currentRowCount + " total inserted rows:" + rowsLoaded.get() );
                log.info("TTLLoader " + tableName.toUpperCase() + " has nothing to do and completed successfully");
            } catch (Exception e) {
                if ( e instanceof InterruptedIOException && ! m_shouldContinue.get()) {
                    continue;
                }
                diedEarly = true;
                // on exception, log and end the thread, but don't kill the process
                log.error("TTLLoader failed a 'TableInsert' procedure call for table '" + tableName + "' " + e.getMessage());
                break;
            }
        }

        log.info("TTLLoader completed for table " + tableName + " rows inserted: " + rowsLoaded.get());
        long rowRemaining = monitor.getRemainingRowCount(tableName, type);
        if (!diedEarly){
            int retries = 12;
            while (rowRemaining != 0) {
                try { Thread.sleep(3000); } catch (Exception ex) {}
                rowRemaining = monitor.getRemainingRowCount(tableName, type);
                retries--;
                if (retries == 0 && rowRemaining != 0) {
                    Benchmark.hardStop("Nibble Delete/Migrate hasn't finished on table '" + tableName + "' after a TTL of "+TTL+" seconds" );
                }
            }
        }
        log.info("TTLLoader completed for table " + tableName + " rows remaining to be deleted:" + rowRemaining);
    }
}
