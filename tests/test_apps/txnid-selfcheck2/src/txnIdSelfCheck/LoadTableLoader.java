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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

/**
 * The LoadTableLoader gets passed in with tableName for which the thread does loading of data using sysproc. For MP
 * LoadMultiPartitionTable is used and for SP LaodSinglePartitionTable is used. The thread also launches a CopyAndDelete
 * thread which copies and deletes data from Load*Table tables 1/3rd of the time. This is to sprinkle other
 * transactions. The CopyAndDelete also servers the purpose of verification that the Load*Table data got indeed loaded.
 *
 * Any procedure failure will fail the test and exit.
 */
public class LoadTableLoader extends BenchmarkThread {

    final Client client;
    final long targetCount;
    final String m_tableName;
    final int batchSize;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final boolean m_isMP;
    final Semaphore m_permits;

    //Column information
    private VoltTable.ColumnInfo m_colInfo[];
    //Zero based index of the partitioned column
    private int m_partitionedColumnIndex = -1;
    //proc name
    final String m_procName;
    //proc name for load table row delete
    final String m_onlydelprocName;
    //Table that keeps building.
    final VoltTable m_table;
    final Random m_random;
    final Random g_random;
    final AtomicLong currentRowCount = new AtomicLong(0);

    // Q for cids which were inserted that should be copied
    final BlockingQueue<Long> cpyQueue = new LinkedBlockingQueue<Long>();
    // Q for cids that where copied that should be deleted
    final BlockingQueue<Long> cpDelQueue = new LinkedBlockingQueue<Long>();
    // Q for cids that were inserted (and not copied) that should be deleted
    final BlockingQueue<Long> onlyDelQueue = new LinkedBlockingQueue<Long>();
    // Q for cids that were inserted for which status is unknown
    final BlockingQueue<Long> unkQueue = new LinkedBlockingQueue<Long>();
    // Q for cids that were copied for which status is unknown
    final BlockingQueue<Long> cpyUnkQueue = new LinkedBlockingQueue<Long>();
    // Q for cids that were deleted for which status is unknown
    final BlockingQueue<Long> delUnkQueue = new LinkedBlockingQueue<Long>();

    private boolean m_slowFlight = false;
    static int slowDownDelayMs = 1000;
    private long[] loadTxnCount = {0};
    private long[] upsertTxnCount = {0};
    private long copyTxnCount = 0;
    private long deleteTxnCount = 0;


    LoadTableLoader(Client client, String tableName, long targetCount, int batchSize, Semaphore permits, boolean isMp, int pcolIdx)
            throws IOException, ProcCallException {
        setName("LoadTableLoader-" + tableName);
        this.client = client;
        this.m_tableName = tableName;
        this.targetCount = targetCount;
        this.batchSize = batchSize;
        m_permits = permits;
        m_partitionedColumnIndex = pcolIdx;
        m_isMP = isMp;
        //Build column info so we can build VoltTable
        m_colInfo = new VoltTable.ColumnInfo[3];
        m_colInfo[0] = new VoltTable.ColumnInfo("cid", VoltType.BIGINT);
        m_colInfo[1] = new VoltTable.ColumnInfo("txnid", VoltType.BIGINT);
        m_colInfo[2] = new VoltTable.ColumnInfo("rowid", VoltType.BIGINT);
        m_procName = (m_isMP ? "@LoadMultipartitionTable" : "@LoadSinglepartitionTable");
        m_onlydelprocName = (m_isMP ? "DeleteOnlyLoadTableMP" : "DeleteOnlyLoadTableSP");
        m_table = new VoltTable(m_colInfo);
        m_random = new Random();  // for the loadsptable thread
        g_random = new Random();  // for the data generator

        log.info("LoadTableLoader Table " + m_tableName + " Is : " + (m_isMP ? "MP" : "SP") + " Target Count: " + targetCount);
        // make this run more than other threads
        setPriority(getPriority() + 1);
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    private void setSlowFlight() {
        if (!m_slowFlight)
            log.info("slow flight set");
        m_slowFlight = true;
    }

    private void setFastFlight() {
        if (m_slowFlight)
            log.info("fast flight set");
        m_slowFlight = false;
    }

    class InsertCallback implements ProcedureCallback {

        final CountDownLatch latch;
        final long p;
        final byte shouldCopy;
        final BlockingQueue<Long> outQueue;
        final BlockingQueue<Long> unkQueue;
        long[] opCount;
        byte source;

        InsertCallback(CountDownLatch latch, long p, byte shouldCopy, BlockingQueue<Long> outQueue,
                       BlockingQueue<Long> unkQueue, long[] opCount, byte source) {
            this.latch = latch;
            this.p = p;
            this.shouldCopy = shouldCopy;
            this.outQueue = outQueue;
            this.unkQueue = unkQueue;
            this.opCount = opCount;
            this.source = source;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            latch.countDown();
            byte status = clientResponse.getStatus();
            if (status != ClientResponse.SUCCESS && !TxnId2Utils.isServerUnavailableStatus(status)) {
                // log what happened status will be logged in json error log.
                hardStop("LoadTableLoader failed to insert into table " + m_tableName
                        + " and this shoudn't happen, source "
                        + source + ". Exiting.", clientResponse);
            }
            if (status == ClientResponse.RESPONSE_UNKNOWN
                    || status == ClientResponse.CONNECTION_LOST
                    || status == ClientResponse.CONNECTION_TIMEOUT)
                unkQueue.add(p);
            //Connection loss node failure will come down here along with user aborts from procedure.
            else if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.warn("LoadTableLoader ungracefully failed to insert into table " + m_tableName + " lcid: " + p
                        + " source " + source);
                log.warn(((ClientResponseImpl) clientResponse).toJSONString());
                if (status == ClientResponse.SERVER_UNAVAILABLE)
                    setSlowFlight();
            }
            else {
                setFastFlight();
                opCount[0]++;
                Benchmark.txnCount.incrementAndGet();
                // add the lcid to the next queue
                outQueue.add(p);
            }
        }
    }

    class InsertCopyCallback implements ProcedureCallback {

        CountDownLatch latch;
        final long lcid;

        InsertCopyCallback(CountDownLatch latch, long lcid) {
            this.latch = latch;
            this.lcid = lcid;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            currentRowCount.incrementAndGet();
            latch.countDown();
            byte status = clientResponse.getStatus();
            if (status != ClientResponse.SUCCESS && !TxnId2Utils.isServerUnavailableStatus(status)) {
                // log what happened
                hardStop("LoadTableLoader gracefully failed to copy from table " + m_tableName
                        + " and this shoudn't happen. Exiting.", clientResponse);
            }
            if (status == ClientResponse.RESPONSE_UNKNOWN
                    || status == ClientResponse.CONNECTION_LOST
                    || status == ClientResponse.CONNECTION_TIMEOUT)
                cpyUnkQueue.add(lcid);
            else if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.warn("LoadTableLoader ungracefully failed to copy from table " + m_tableName + " lcid " + lcid);
                log.warn(((ClientResponseImpl) clientResponse).toJSONString());
                cpyQueue.add(lcid);
                if (status == ClientResponse.SERVER_UNAVAILABLE)
                    setSlowFlight();
            }
            else {
                cpDelQueue.add(lcid);
                copyTxnCount++;
                setFastFlight();
                Benchmark.txnCount.incrementAndGet();
            }
        }
    }

    class DeleteCallback implements ProcedureCallback {

        final CountDownLatch latch;
        final int expected_delete;
        final long lcid;
        final BlockingQueue<Long> wrkQueue;
        final BlockingQueue<Long> unkQueue;
        final BlockingQueue<Long> cpyUnkQueue;
        final BlockingQueue<Long> delUnkQueue;
        final byte source;

        DeleteCallback(CountDownLatch latch, long lcid, BlockingQueue<Long> wrkQueue, BlockingQueue<Long> unkQueue,
                       BlockingQueue<Long> cpyUnkQueue, BlockingQueue<Long> delUnkQueue, int expected_delete,
                       byte source) {
            this.latch = latch;
            this.lcid = lcid;
            this.wrkQueue = wrkQueue;
            this.unkQueue = unkQueue;
            this.cpyUnkQueue = cpyUnkQueue;
            this.delUnkQueue = delUnkQueue;
            this.expected_delete = expected_delete;
            this.source = source;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            latch.countDown();
            byte status = clientResponse.getStatus();
            if (status != ClientResponse.SUCCESS && !TxnId2Utils.isServerUnavailableStatus(status)) {
                // log what happened
                hardStop("LoadTableLoader gracefully failed to delete from table " + m_tableName
                        + " and this shoudn't happen. " + source + " Exiting.", clientResponse);
            }
            if (status == ClientResponse.RESPONSE_UNKNOWN
                    || status == ClientResponse.CONNECTION_LOST
                    || status == ClientResponse.CONNECTION_TIMEOUT)
                delUnkQueue.add(lcid);
            else if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.warn("LoadTableLoader ungracefully failed to delete from table " + m_tableName
                            + " lcid " + lcid + " source " + source);
                log.warn(((ClientResponseImpl) clientResponse).toJSONString());
                wrkQueue.add(lcid);
                if (status == ClientResponse.SERVER_UNAVAILABLE)
                    setSlowFlight();
            }
            else {
                deleteTxnCount++;
                setFastFlight();
                Benchmark.txnCount.incrementAndGet();
                if (expected_delete > 0) {
                    /* when we delete, if the row cid is on an unknown list we don't know if it was inserted/copied on not
                       with any certainty, so we can't check its presence or absence and assert a test failure at delete time,
                       but we try to delete anyway to clean up the tables.
                     */
                    long cnt = clientResponse.getResults()[0].asScalarLong();
                    int mask = Integer.MAX_VALUE;
                    if (source == 2) {
                        // response is: bit 30 - delete from cpload(p|mp)
                        //              bit 31 - delete from load(p|mp)
                        if (unkQueue != null) {
                            if (unkQueue.contains(lcid)) {
                                mask &= 3 - 1;
                                unkQueue.remove(lcid);
                            }
                        }
                        if (cpyUnkQueue != null) {
                            if (cpyUnkQueue.contains(lcid)) {
                                mask &= 3 - 2;
                                cpyUnkQueue.remove(lcid);
                            }
                        }
                    }
                    if ((cnt & mask) != (expected_delete & mask)) {
                        hardStop("LoadTableLoader ungracefully failed to delete lcid " + lcid + " from "
                                + m_tableName + " count=" + cnt + " Expected: " + expected_delete + " mask " + mask
                                + " source " + source);
                    }
                }
            }
        }
    }

    //Local task thread doing copy and delete under the loadtable tables.
    class CopyAndDeleteDataTask extends Thread {

        final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
        int m_copyDeleteDoneCount = 0;
        //proc name for copy
        final String m_cpprocName;
        //proc name for 2 delete
        final String m_delprocName;
        final Random r2;
        final Random gr;

        void shutdown() {
            m_shouldContinue.set(false);
        }

        public CopyAndDeleteDataTask() {
            setName("CopyAndDeleteDataTask-" + m_tableName);
            setDaemon(true);
            m_cpprocName = (m_isMP ? "CopyLoadPartitionedMP" : "CopyLoadPartitionedSP");
            m_delprocName = (m_isMP ? "DeleteLoadPartitionedMP" : "DeleteLoadPartitionedSP");
            r2 = new Random();
            gr = new Random();
        }

        @Override
        public void run() {
            try {
                log.info("Starting Copy Delete Task for table: " + m_tableName);
                while (m_shouldContinue.get()) {
                    if (cpyQueue.size() + cpDelQueue.size() == 0) {
                        Thread.sleep(1000);
                        continue;
                    }
                    List<Long> workList = new ArrayList<Long>();
                    cpyQueue.drainTo(workList, 10);
                    if (workList.size() > 0) {
                        //log.info("from copyqueue to copy: " + workList.toString());
                        //log.debug("WorkList Size: " + workList.size());
                        CountDownLatch clatch = new CountDownLatch(workList.size());
                        boolean success;
                        VoltTable vtable = new VoltTable(m_colInfo);
                        for (Long lcid : workList) {
                            ArrayList<Object> row = nextRow(gr, lcid);
                            vtable.clearRowData();
                            vtable.addRow(row.toArray(new Object[row.size()]));
                            try {
                                /* copy proc can use select then insert (0) or insert into select from (1)
                                   the random variable determines which one is used.
                                 */
                                success = client.callProcedure(new InsertCopyCallback(clatch, lcid), m_cpprocName, lcid, r2.nextInt(2), vtable);
                                if (!success) {
                                    hardStop("Failed to copy upsert for: " + lcid);
                                }
                            } catch (NoConnectionsException e) {
                                cpyQueue.add(lcid);
                                setSlowFlight();
                            } catch (Exception e) {
                                // on exception, log and end the thread, but don't kill the process
                                hardStop("CopyAndDeleteDataTask Copy failed a procedure call for table " + m_tableName
                                        + " and the thread will now stop.", e);
                            }
                            if (m_slowFlight)
                                Thread.sleep(slowDownDelayMs);
                        }
                        clatch.await();
                        // nb. these could be separate threads
                        workList.clear();
                    }

                    cpDelQueue.drainTo(workList, 10);
                    if (workList.size() > 0) {
                        //log.info("from copydeleteq to delete: " + workList.toString());
                        CountDownLatch dlatch = new CountDownLatch(workList.size());
                        boolean success;
                        VoltTable vtable = new VoltTable(m_colInfo);
                        for (Long lcid : workList) {
                            ArrayList<Object> row = nextRow(gr, lcid);
                            vtable.clearRowData();
                            vtable.addRow(row.toArray(new Object[row.size()]));
                            try {
                                success = client.callProcedure(new DeleteCallback(dlatch, lcid, cpDelQueue, unkQueue,
                                        cpyUnkQueue, delUnkQueue, 3, (byte) 2), m_delprocName, lcid, vtable);
                                if (!success) {
                                    hardStop("Failed to delete (copy) for: " + lcid);
                                }
                            } catch (NoConnectionsException e) {
                                cpDelQueue.add(lcid);
                                setSlowFlight();
                            } catch (Exception e) {
                                // on exception, log and end the thread, but don't kill the process
                                hardStop("CopyAndDeleteDataTask Delete failed a procedure call for table " + m_tableName
                                        + " and the thread will now stop.", e);
                            }
                            if (m_slowFlight)
                                Thread.sleep(slowDownDelayMs);
                        }
                        dlatch.await();
                        m_copyDeleteDoneCount += workList.size();
                        workList.clear();
                    }
                }
            }
            catch (Exception e) {
                hardStop(e);
            }
            log.info("CopyAndDeleteTask row count: " + m_copyDeleteDoneCount);
        }
    }

    private ArrayList<Object> nextRow(Random random, long seq) {
        ArrayList<Object> newRow = new ArrayList<Object>();
        random.setSeed(seq);
        newRow.add(seq);
        newRow.add(random.nextInt());
        newRow.add(random.nextInt());
        return newRow;
    }

    @Override
    public void run() {
        // ratio of upsert for @Load*Table
        final float upsertratio = 0.50F;
        // ratio of upsert to an existing table for @Load*Table
        final float upserthitratio = 0.20F;

        CopyAndDeleteDataTask cdtask = new CopyAndDeleteDataTask();
        cdtask.start();
        long p;
        List<Long> cidList = new ArrayList<Long>(batchSize);
        try {
            // pick up where we left off
            ClientResponse cr = TxnId2Utils.doAdHoc(client, "select nvl(max(cid)+1,0) from " + m_tableName + ";");
            p = cr.getResults()[0].asScalarLong();

            while (m_shouldContinue.get()) {
                //1 in 3 gets copied and then deleted after leaving some data
                byte shouldCopy = (byte) (m_random.nextInt(3) == 0 ? 1 : 0);
                byte upsertMode = (byte) (m_random.nextFloat() < upsertratio ? 1: 0);
                byte upsertHitMode = (byte) ((upsertMode != 0) && (m_random.nextFloat() < upserthitratio) ? 1: 0);

                CountDownLatch latch = new CountDownLatch(batchSize);
                final BlockingQueue<Long> lcpDelQueue = new LinkedBlockingQueue<Long>();
                cidList.clear();

                // try to insert/upsert batchSize random rows
                for (int i = 0; i < batchSize; i++) {
                    m_table.clearRowData();
                    m_permits.acquire();
                    //Increment p so that we always get new key.
                    p++;
                    ArrayList<Object> nextrow = nextRow(g_random, p);
                    m_table.addRow(nextrow.toArray(new Object[nextrow.size()]));
                    cidList.add(p);
                    BlockingQueue<Long> wrkQueue;
                    if (shouldCopy != 0) {
                        wrkQueue = lcpDelQueue;
                    } else {
                        wrkQueue = onlyDelQueue;
                    }
                    boolean success;
                    try {
                        if (!m_isMP) {
                            Object rpartitionParam
                                    = VoltType.valueToBytes(m_table.fetchRow(0).get(m_partitionedColumnIndex, VoltType.BIGINT));
                            if (upsertHitMode != 0) {// for test upsert an existing row, insert it and then upsert same row again.
                                // only insert
                                success = client.callProcedure(new InsertCallback(latch, p, shouldCopy, wrkQueue, unkQueue, loadTxnCount, (byte)1), m_procName, rpartitionParam, m_tableName, (byte) 0, m_table);
                            } else {
                                // insert or upsert
                                success = client.callProcedure(new InsertCallback(latch, p, shouldCopy, wrkQueue, unkQueue, loadTxnCount, (byte)2), m_procName, rpartitionParam, m_tableName, upsertMode, m_table);
                            }
                        } else {
                            if (upsertHitMode != 0) {
                                // only insert
                                success = client.callProcedure(new InsertCallback(latch, p, shouldCopy, wrkQueue, unkQueue, loadTxnCount, (byte)3), m_procName, m_tableName, (byte) 0, m_table);
                            } else {
                                // insert or upsert
                                success = client.callProcedure(new InsertCallback(latch, p, shouldCopy, wrkQueue, unkQueue, loadTxnCount, (byte)4), m_procName, m_tableName, upsertMode, m_table);
                            }
                        }
                        if (!success) {
                            hardStop("Failed to insert upsert for: " + p);
                        }
                        if (m_slowFlight)
                            Thread.sleep(slowDownDelayMs);
                    }
                    catch (NoConnectionsException e) {
                        //drop this lcid on the floor, we'll just move on
                        setSlowFlight();
                    }
                    catch (Exception e) {
                        hardStop(e);
                    }
                }

                log.debug("Waiting for all inserts for @Load* done.");
                //Wait for all @Load{SP|MP}Done
                latch.await();
                log.debug("Done Waiting for all inserts for @Load* done.");
                //log.info("unknown status for these: " + unkQueue.toString());

                // try to upsert if want the collision
                if (upsertHitMode != 0) {
                    CountDownLatch upserHitLatch = new CountDownLatch(batchSize * upsertHitMode);
                    BlockingQueue<Long> cpywrkQueue = new LinkedBlockingQueue<Long>();
                    BlockingQueue<Long> cpyunkQueue = new LinkedBlockingQueue<Long>();
                    for (int i = 0; i < batchSize; i++) {
                        m_table.clearRowData();
                        m_permits.acquire();
                        ArrayList<Object> nextrow = nextRow(g_random, cidList.get(i));
                        m_table.addRow(nextrow.toArray(new Object[nextrow.size()]));
                        boolean success;
                        try {
                            if (!m_isMP) {
                                Object rpartitionParam = VoltType.valueToBytes(m_table.fetchRow(0).get(m_partitionedColumnIndex, VoltType.BIGINT));
                                // upsert only
                                success = client.callProcedure(new InsertCallback(upserHitLatch, p, shouldCopy, cpywrkQueue,
                                        cpyunkQueue, upsertTxnCount, (byte)5), m_procName, rpartitionParam, m_tableName, (byte) 1, m_table);
                            } else {
                                // upsert only
                                success = client.callProcedure(new InsertCallback(upserHitLatch, p, shouldCopy, cpywrkQueue,
                                        cpyunkQueue, upsertTxnCount, (byte)6), m_procName, m_tableName, (byte) 1, m_table);
                            }
                            if (!success) {
                                hardStop("Failed to invoke upsert for: " + cidList.get(i));
                            }
                            if (m_slowFlight)
                                Thread.sleep(slowDownDelayMs);
                        }
                        catch (NoConnectionsException e) {
                            //drop this lcid on the floor, we'll just move on
                            setSlowFlight();
                        }
                        catch (Exception e) {
                            hardStop(e);
                        }
                    }
                    log.debug("Waiting for all upsert for @Load* done.");
                    //Wait for all additional upsert @Load{SP|MP}Done
                    upserHitLatch.await();
                    log.debug("Done Waiting for all upsert for @Load* done.");
                }

                //log.info("to copy: " + lcpDelQueue.toString());
                cpyQueue.addAll(lcpDelQueue);

                try {
                    long nextRowCount = TxnId2Utils.getRowCount(client, m_tableName);
                    long nextCpRowCount = TxnId2Utils.getRowCount(client, "cp"+m_tableName);
                    // report counts of successful txns
                    log.info("LoadTableLoader rowcounts " + nextRowCount + "/"+ nextCpRowCount
                            + " Insert/Upsert txs: " + loadTxnCount[0]
                            + " UpsertHit txs: " + upsertTxnCount[0]
                            + " Copy txs: " + copyTxnCount
                            + " Delete txn: " + deleteTxnCount);
                } catch (Exception e) {
                    hardStop("getrowcount exception", e);
                }

                if (onlyDelQueue.size() > 0 && m_shouldContinue.get()) {
                    List<Long> workList = new ArrayList<Long>();
                    onlyDelQueue.drainTo(workList);
                    //log.info("from deleteonly to delete: " + workList.toString());
                    CountDownLatch odlatch = new CountDownLatch(workList.size());
                    VoltTable vtable = new VoltTable(m_colInfo);
                    for (Long lcid : workList) {
                        ArrayList<Object> row = nextRow(g_random, lcid);
                        vtable.clearRowData();
                        vtable.addRow(row.toArray(new Object[row.size()]));
                        try {
                            boolean success;
                            success = client.callProcedure(new DeleteCallback(odlatch, lcid, onlyDelQueue, unkQueue,
                                    null, delUnkQueue, 1, (byte)1), m_onlydelprocName,
                                     lcid, vtable);
                            if (!success) {
                                hardStop("Failed to invoke delete for: " + lcid);
                            }
                            if (m_slowFlight)
                                Thread.sleep(slowDownDelayMs);
                        }
                        catch (NoConnectionsException e) {
                            //requeue for next time
                            onlyDelQueue.add(lcid);
                            setSlowFlight();
                        }
                        catch (Exception e) {
                            hardStop(e);
                        }
                    }
                    odlatch.await();
                }

                if (unkQueue.size() > 0 && m_shouldContinue.get()) {
                    List<Long> workList = new ArrayList<Long>();
                    unkQueue.drainTo(workList);
                    //log.info("from unknownqueue to delete: " + workList.toString());
                    CountDownLatch odlatch = new CountDownLatch(workList.size());
                    VoltTable vtable = new VoltTable(m_colInfo);
                    for (Long lcid : workList) {
                        ArrayList<Object> row = nextRow(g_random, lcid);
                        vtable.clearRowData();
                        vtable.addRow(row.toArray(new Object[row.size()]));
                        try {
                            boolean success;
                            success = client.callProcedure(new DeleteCallback(odlatch, lcid, unkQueue, null,
                                    null, unkQueue, -1, (byte)3), m_onlydelprocName,
                                     lcid, vtable);
                            if (!success) {
                                hardStop("Failed to invoke delete for: " + lcid);
                            }
                            if (m_slowFlight)
                                Thread.sleep(slowDownDelayMs);
                        }
                        catch (NoConnectionsException e) {
                            //requeue for next time
                            unkQueue.add(lcid);
                            setSlowFlight();
                        }
                        catch (Exception e) {
                            hardStop(e);
                        }
                    }
                    odlatch.await();
                }
            }
        }
        catch (Exception e) {
            // on exception, log and end the thread, but don't kill the process
            if (e instanceof ProcCallException)
                log.error(((ProcCallException)e).getClientResponse().toString());
            hardStop("LoadTableLoader failed a procedure call for table " + m_tableName
                    + " and the thread will now stop.", e);
        } finally {
            cdtask.shutdown();
            try {
                cdtask.join();
            } catch (InterruptedException ex) {
                log.error("CopyDelete Task was stopped.", ex);
            }
        }
    }
}
