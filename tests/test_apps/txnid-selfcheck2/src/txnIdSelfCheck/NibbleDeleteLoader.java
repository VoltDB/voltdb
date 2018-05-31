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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
    long deletesRemaining = 0;
    long deletesSucceeded = 0;
    long rowsDeletedTotal = 0;
    NibbleDeleteMonitor monitor = null;
    /* this is the ttl configured on each of the tables in the ddl */
    long TTL = 30;

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
        this.monitor = new NibbleDeleteMonitor(this.tableName,"ts");
    }

    void shutdown() {
        m_shouldContinue.set(false);
        log.info("NibbleDeleteLoader " + tableName + " shutdown: inserts tried: " + insertsTried + " rows loaded: " + rowsLoaded +
                " deletes remaining: " + deletesRemaining);
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
                Benchmark.hardStop("NibbleDeleteLoader gracefully failed to insert into table " + tableName + " and this shoudn't happen. Exiting.");
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

    class NibbleDeleteMonitor extends Thread {
        final String tableName;
        final String tsColumnName;
        int deleteCnt = 0;
        int currentCnt = 0;

        NibbleDeleteMonitor(String tableName,String tsColumnName) {
            this.tableName = tableName;
            this.tsColumnName = tsColumnName;
            setName("NibbleDeleteMonitor-"+tableName);
        }

        @Override
        public void run() {
            try {
            long deleteCount = 0;
            int retries = 0;
            while ( true ) {
                // give a little wiggle room when checking if rows should've been deleted.
                long ttl = new Double(Math.ceil(TTL*1.5)).longValue();

                ClientResponse response = TxnId2Utils.doProcCall(client, "@AdHoc", "select *,now from "+tableName+" where "+tsColumnName+" < DATEADD(SECOND,-"+ttl+",NOW) ");
                Map<String,Object> stats = getTTLStats(tableName);
                if (response.getStatus() == ClientResponse.SUCCESS ) {
                    VoltTable[] t = response.getResults();
                    VoltTable data = t[0];
                    long unDeletedRows = data.getRowCount();
                    log.info("Rows behind from being deleted:"+unDeletedRows);
                    log.info("Stats Rows behind from being deleted:" + String.valueOf(stats.get("ROWS_REMAINING")));
                    // print some debugging info before we error out.
                    if ( unDeletedRows > 0 ) {
                        long deletes = (Long)stats.get("ROWS_DELETED");
                        //allow more time for the case that nodes are down and rejoined.
                        if ( retries <= 4 ) {
                            // We need to handle the case where we have just recovered and the nibble deleter hasn't caught up,
                            // don't fail completely unless we have failed two times consecutively after receiving valid responses
                            if (deletes > deleteCount) {
                                // only fail if we we don't increase delete count two times in a row.
                                log.info("nibble delete is making progress: "+String.valueOf(deletes)+" > "+String.valueOf(deleteCount));
                                deleteCount = deletes;
                                retries = 0;
                            } else {
                                log.info("nibble delete is not making progress, after attempt "+String.valueOf(retries)+" of 2");
                                retries++;
                            }
                            Thread.sleep(5000);
                            continue;
                        }
                        // nibble deletes are failing.
                        log.info("Nibble delete is too far behind but thinks it's done. TTL stats:");
                        log.info(stats);
                        Benchmark.hardStop("Nibble Delete failed to delete "+unDeletedRows+" from " + tableName + " and this shoudn't happen. Exiting.");
                    } else {
                        retries = 0;
                    }
                } else {
                   log.error("Response failed:"+response.getAppStatusString()+" status:"+response.getStatusString());
                   Benchmark.hardStop("Nibble Delete failed");
                }

                response = TxnId2Utils.doProcCall(client, "@AdHoc", "select count(*) from "+tableName);
                if (response.getStatus() == ClientResponse.SUCCESS ) {
                    VoltTable[] t = response.getResults();
                    VoltTable data = t[0];
                    VoltTableRow row = data.fetchRow(0);
                    deletesRemaining = row.getLong(0);
                    if ( deletesRemaining > 0 ) {
                        log.info("Nibble Delete has "+deletesRemaining+" rows from " + tableName + " to be deleted eventually.");
                    } else {
                        log.info("Nibble Delete has nothing to do");
                    }
                }

                Thread.sleep(2000);

            }
            } catch(InterruptedException e) {
                log.info("Thread is interrupted");
            } catch(ProcCallException pe) {
                pe.printStackTrace();
                Benchmark.hardStop("Error executing procedure:"+pe.getMessage());
            }

        }

        public Map<String,Object> getTTLStats(String tableName ) {
            Map<String,Object> stats = new HashMap<String,Object>();
            ClientResponse cr = null;
            try {
                cr = client.callProcedure("@Statistics", "TTL");
            } catch(IOException e) {
                log.error(e.getMessage());
                return stats;
            } catch(ProcCallException pe) {
                log.error(pe.getMessage());
            }
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                log.error("Failed to call Statistics TTL proc.");
                log.error(((ClientResponseImpl) cr).toJSONString());
                return stats;
            }

            VoltTable t = cr.getResults()[0];
            while ( t.advanceRow() ) {
                String table = t.getString(0);
                if ( tableName.equalsIgnoreCase(table) ) {
                    // ROWS_DELETED  ROUNDS  ROWS_DELETED_LAST_ROUND  ROWS_REMAINING
                    stats.put("TABLE", table);
                    stats.put("ROWS_DELETED", t.getLong(1));
                    stats.put("ROUNDS", t.getLong(2));
                    stats.put("ROWS_DELETED_LAST_ROUND", t.getLong(3));
                    stats.put("ROWS_REMAINING",t.getLong(4));
                    break;
                }
            }
            return stats;

        }
    }
    @Override
    public void run() {
        this.monitor.start();
        byte[] data = new byte[rowSize];
        long currentRowCount;
        boolean diedEarly = false;
        while (m_shouldContinue.get()) {
            r.nextBytes(data);
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, tableName);
                // insert some batches...
                boolean isDone = false;
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
                                isDone = true;
                                break;
                            }
                            log.error("NibbleDeleteLoader thread interrupted while waiting for permit. " + e.getMessage());
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
                        log.error("NibbleDeleteLoader thread interrupted while waiting." + e.getMessage());
                    }
                    long nextRowCount = TxnId2Utils.getRowCount(client, tableName);
                    // if no progress, throttle a bit
                    if (nextRowCount == currentRowCount) {
                        try { Thread.sleep(1000); } catch (Exception e2) {}
                    }
                    currentRowCount = nextRowCount;
                    log.info("NibbleDeleteLoader " + tableName.toUpperCase() + " current count: " + currentRowCount + " tried:" + insertsTried);
                }
                log.info("NibbleDeleteLoader " + tableName.toUpperCase() + " has nothing to do and completed successfully");
            } catch (Exception e) {
                if ( e instanceof InterruptedIOException && ! m_shouldContinue.get()) {
                    continue;
                }
                diedEarly = true;
                // on exception, log and end the thread, but don't kill the process
                log.error("NibbleDeleteLoader failed a 'TableInsert' procedure call for table '" + tableName + "' " + e.getMessage());
                break;
            }
        }

        log.info("NibbleDeleteLoader completed for table " + tableName + " rows sent: " + insertsTried + " inserted: " + rowsLoaded);
        long maxTime = System.nanoTime() + TTL*1000000+10;
        // only check if we all the rows were deleted if we ended gracefully
        if ( ! diedEarly ) {
            while ( deletesRemaining > 0 ) {
               if ( System.nanoTime() > maxTime) {
                    Benchmark.hardStop("Nibble Delete hasn't finished on table '" + tableName + "' after a TTL of "+TTL+" seconds" );
               }


               try { Thread.sleep(3000); } catch (Exception e2) {}
            }
        }
    }
}
