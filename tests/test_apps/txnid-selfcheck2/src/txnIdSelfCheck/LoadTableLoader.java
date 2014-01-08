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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterConverter;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class LoadTableLoader extends Thread {

    static VoltLogger log = new VoltLogger("HOST");

    final Client client;
    final long targetCount;
    final String m_tableName;
    final int batchSize;
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final boolean m_isMP;
    final Semaphore m_permits;

    //Types of columns
    private List<VoltType> m_typeList = new ArrayList<VoltType>();
    //Column information
    private VoltTable.ColumnInfo m_colInfo[];
    //Column types
    final private Map<Integer, VoltType> m_columnTypes;
    //Column Names
    final private Map<Integer, String> m_colNames;
    //Zero based index of the partitioned column
    private int m_partitionedColumnIndex = -1;
    //Partitioned column type
    private VoltType m_partitionColumnType = VoltType.NULL;
    //Number of columns
    private int m_columnCnt = 0;
    //proc name
    final String m_procName;
    //Table that keeps building.
    final VoltTable m_table;
    final Random m_random = new Random();


    LoadTableLoader(Client client, String tableName, int targetCount, int batchSize, Semaphore permits)
            throws IOException, ProcCallException {
        setName("LoadTableLoader-" + tableName);
        setDaemon(true);

        this.client = client;
        this.m_tableName = tableName;
        this.targetCount = targetCount;
        this.batchSize = batchSize;
        m_permits = permits;

        VoltTable procInfo = client.callProcedure("@SystemCatalog",
                "COLUMNS").getResults()[0];
        m_columnTypes = new TreeMap<Integer, VoltType>();
        m_colNames = new TreeMap<Integer, String>();
        while (procInfo.advanceRow()) {
            String table = procInfo.getString("TABLE_NAME");
            if (tableName.equalsIgnoreCase(table)) {
                VoltType vtype = VoltType.typeFromString(procInfo.getString("TYPE_NAME"));
                int idx = (int) procInfo.getLong("ORDINAL_POSITION") - 1;
                m_columnTypes.put(idx, vtype);
                m_colNames.put(idx, procInfo.getString("COLUMN_NAME"));
                String remarks = procInfo.getString("REMARKS");
                if (remarks != null && remarks.equalsIgnoreCase("PARTITION_COLUMN")) {
                    m_partitionColumnType = vtype;
                    m_partitionedColumnIndex = idx;
                    log.debug("Table " + tableName + " Partition Column Name is: "
                            + procInfo.getString("COLUMN_NAME"));
                    log.debug("Table " + tableName + " Partition Column Type is: " + vtype.toString());
                }
            }
        }

        if (m_columnTypes.isEmpty()) {
            //csvloader will exit.
            log.error("Table " + m_tableName + " Not found");
            throw new RuntimeException("Table Not found: " + m_tableName);
        }
        m_columnCnt = m_columnTypes.size();
        //Build column info so we can build VoltTable
        m_colInfo = new VoltTable.ColumnInfo[m_columnTypes.size()];
        for (int i = 0; i < m_columnTypes.size(); i++) {
            VoltType type = m_columnTypes.get(i);
            String cname = m_colNames.get(i);
            VoltTable.ColumnInfo ci = new VoltTable.ColumnInfo(cname, type);
            m_colInfo[i] = ci;
        }
        m_typeList = new ArrayList<VoltType>(m_columnTypes.values());
        m_isMP = (m_partitionedColumnIndex == -1 ? true : false);
        m_procName = (m_isMP ? "@LoadMultipartitionTable" : "@LoadSinglepartitionTable");
        m_table = new VoltTable(m_colInfo);

        System.out.println("Table " + m_tableName + " Is : " + (m_isMP ? "MP" : "SP"));
        // make this run more than other threads
        setPriority(getPriority() + 1);
    }

    long getRowCount() throws NoConnectionsException, IOException, ProcCallException {
        VoltTable t = client.callProcedure("@AdHoc", "select count(*) from " + m_tableName + ";").getResults()[0];
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
                log.error("LoadTableLoader gracefully failed to insert into table " + m_tableName + " and this shoudn't happen. Exiting.");
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
                // stop the world
                System.exit(-1);
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.error("LoadTableLoader ungracefully failed to insert into table " + m_tableName);
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
                // stop the loader
                m_shouldContinue.set(false);
            }
            latch.countDown();
        }
    }

    class InsertCopyCallback implements ProcedureCallback {

        CountDownLatch latch;

        InsertCopyCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            byte status = clientResponse.getStatus();
            if (status == ClientResponse.GRACEFUL_FAILURE) {
                // log what happened
                log.error("LoadTableLoader gracefully failed to copy from table " + m_tableName + " and this shoudn't happen. Exiting.");
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
                // stop the world
                System.exit(-1);
            }
            if (status != ClientResponse.SUCCESS) {
                // log what happened
                log.error("LoadTableLoader ungracefully failed to copy from table " + m_tableName);
                log.error(((ClientResponseImpl) clientResponse).toJSONString());
                // stop the loader
                m_shouldContinue.set(false);
            }
            latch.countDown();
        }
    }

    /**
     * Add rows data to VoltTable given fields values.
     *
     * @param table
     * @param fields
     * @return
     */
    private boolean addRowToVoltTableFromLine(VoltTable table, String fields[])
            throws Exception {

        if (fields == null || fields.length <= 0) {
            return false;
        }
        Object row_args[] = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final VoltType type = m_columnTypes.get(i);
            row_args[i] = ParameterConverter.tryToMakeCompatible(type.classFromType(), fields[i]);
        }
        table.addRow(row_args);
        return true;
    }

    @Override
    public void run() {

        try {
            long currentRowCount = getRowCount();
            ArrayList<Long> cpList = new ArrayList<Long>();
            while ((currentRowCount < targetCount) && (m_shouldContinue.get())) {
                //1 in 5 gets copied
                byte shouldCopy = (byte) (m_random.nextInt(5) == 0 ? 1 : 0);
                CountDownLatch latch = new CountDownLatch(batchSize);
                // try to insert batchSize random rows
                String fields[] = new String[3];
                for (int i = 0; i < batchSize; i++) {
                    m_table.clearRowData();
                    m_permits.acquire();
                    long p = Math.abs(r.nextLong());
                    fields[0] = Long.toString(p);
                    fields[1] = Long.toString(p);
                    fields[2] = Long.toString(Calendar.getInstance().getTimeInMillis());
                    addRowToVoltTableFromLine(m_table, fields);
                    if (shouldCopy != 0) {
                        cpList.add(p);
                    }
                    if (!m_isMP) {
                        Object rpartitionParam
                                = TheHashinator.valueToBytes(m_table.fetchRow(0).get(
                                                m_partitionedColumnIndex, m_partitionColumnType));
                        client.callProcedure(new InsertCallback(latch), m_procName, rpartitionParam, m_tableName, m_table);
                    } else {
                        client.callProcedure(new InsertCallback(latch), m_procName, m_tableName, m_table);
                    }
                }
                latch.await(10, TimeUnit.SECONDS);
                long nextRowCount = getRowCount();
                // if no progress, throttle a bit
                if (nextRowCount == currentRowCount) {
                    Thread.sleep(1000);
                }
                if (!m_isMP) {
                    CountDownLatch clatch = new CountDownLatch(cpList.size());
                    for (Long lcid : cpList) {
                        client.callProcedure(new InsertCopyCallback(clatch), "CopyLoadPartitionedSP", lcid);
                    }
                    clatch.await(10, TimeUnit.SECONDS);
                    cpList.clear();
                } else {
                    CountDownLatch clatch = new CountDownLatch(cpList.size());
                    for (Long lcid : cpList) {
                        client.callProcedure(new InsertCopyCallback(clatch), "CopyLoadPartitionedMP", lcid);
                    }
                    clatch.await(10, TimeUnit.SECONDS);
                    cpList.clear();
                }
                currentRowCount = nextRowCount;
            }

        }
        catch (Exception e) {
            // on exception, log and end the thread, but don't kill the process
            log.error("LoadTableLoader failed a procedure call for table " + m_tableName
                    + " and the thread will now stop.", e);
        }
    }

}
