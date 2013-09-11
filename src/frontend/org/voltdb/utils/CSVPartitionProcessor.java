/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.voltcore.logging.VoltLogger;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import static org.voltdb.utils.CSVFileReader.synchronizeErrorInfo;

class CSVPartitionProcessor implements Runnable {

    static public CSVLoader.CSVConfig config;
    Client csvClient;
    BlockingQueue<CSVLineWithMetaData> lineq;
    int m_columnCnt = 0;
    int m_partitionedColumnIndex;
    CSVLineWithMetaData endOfData;
    int m_partitionId;
    static String insertProcedure = "";
    String m_tableName;
    String m_processorName;
    static Map<Integer, VoltType> columnTypes;
    static VoltTable.ColumnInfo colInfo[];
    static boolean isMP = false;
    AtomicLong m_partitionProcessedCount = new AtomicLong(0);
    AtomicLong m_partitionAcknowledgedCount = new AtomicLong(0);
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
    static CountDownLatch processor_cdl;
    boolean m_errored = false;
    static int reportEveryNRows = 10000;

    //Callback for single row procedure invoke called for rows in failed batch.
    public static final class PartitionSingleExecuteProcedureCallback implements ProcedureCallback {

        CSVPartitionProcessor m_processor;
        final CSVLineWithMetaData m_csvLine;

        public PartitionSingleExecuteProcedureCallback(CSVLineWithMetaData csvLine, CSVPartitionProcessor processor) {
            m_processor = processor;
            m_csvLine = csvLine;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                m_log.error(response.getStatusString());
                String[] info = {m_csvLine.rawLineData.toString(), response.getException().getMessage()};
                if (CSVFileReader.synchronizeErrorInfo(m_csvLine.lineNumber, info)) {
                    m_processor.m_errored = true;
                }
                return;
            }
            long currentCount = m_processor.m_partitionAcknowledgedCount.incrementAndGet();

            if (currentCount % reportEveryNRows == 0) {
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }
    //Queue of batch entries where some rows failed.
    private BlockingQueue<CSVLineWithMetaData> failedQueue = new LinkedBlockingQueue<CSVLineWithMetaData>();
    private class FailedBatchProcessor extends Thread {

        private final CSVPartitionProcessor m_processor;
        private final String m_procName;
        private final String m_tableName;
        private final Object m_partitionParam;

        public FailedBatchProcessor(CSVPartitionProcessor pp, String procName, String tableName, Object partitionParam) {
            m_processor = pp;
            m_procName = procName;
            m_tableName = tableName;
            m_partitionParam = partitionParam;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    CSVLineWithMetaData lineList;
                    lineList = failedQueue.take();
                    if (lineList == endOfData || m_processor.m_errored) {
                        m_log.info("Shutting down failure processor. " + failedQueue.size());
                        break;
                    }
                    try {
                        VoltTable table = new VoltTable(colInfo);
                        VoltTableUtil.addRowToVoltTableFromLine(table, lineList.line, columnTypes);
                        PartitionSingleExecuteProcedureCallback cbmt = new PartitionSingleExecuteProcedureCallback(lineList, m_processor);
                        if (!CSVPartitionProcessor.isMP) {
                            csvClient.callProcedure(cbmt, m_procName, m_partitionParam, m_tableName, table);
                        } else {
                            csvClient.callProcedure(cbmt, m_procName, m_tableName, table);
                        }
                        m_partitionProcessedCount.addAndGet(table.getRowCount());
                    } catch (IOException ioex) {
                        m_log.warn("Failure Processor failed, failures will not be processed: " + ioex);
                    }
                } catch (InterruptedException ex) {
                    m_log.info("Stopped failure processor.");
                    break;
                }
            }
        }
    }

    // Callback for batch invoke when table has more than 1 entries. The callback on failure feeds failedQueue for
    // one row at a time processing.
    public static final class PartitionProcedureCallback implements ProcedureCallback {

        private static int lastMultiple = 0;
        protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
        protected CSVPartitionProcessor m_processor;
        final private List<CSVLineWithMetaData> m_batchList;
        private final BlockingQueue<CSVLineWithMetaData> failedQueue;

        public PartitionProcedureCallback(List<CSVLineWithMetaData> batchList, CSVPartitionProcessor pp, BlockingQueue<CSVLineWithMetaData> fq) {
            m_processor = pp;
            m_batchList = new ArrayList(batchList);
            failedQueue = fq;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                // Batch failed queue it for individual processing and find out which actually m_errored.
                m_processor.m_partitionProcessedCount.addAndGet(-1 * m_batchList.size());
                failedQueue.addAll(m_batchList);
                return;
            }
            long executed = response.getResults()[0].asScalarLong();
            long currentCount = m_processor.m_partitionAcknowledgedCount.addAndGet(executed);
            int newMultiple = (int) currentCount / reportEveryNRows;
            if (newMultiple != lastMultiple) {
                lastMultiple = newMultiple;
                m_log.info(m_processor.m_processorName + " Inserted " + currentCount + " rows");
            }
        }
    }

    // while there are rows and endOfData not seen batch and call procedure for insert.
    private void process(VoltTable table, String procName, Object partitionParam) {
        List<CSVLineWithMetaData> batchList = new ArrayList<CSVLineWithMetaData>();
        while (true) {
            if (m_errored) {
                //Let file reader know not to read any more. All Partition Processors will quit processing.
                CSVFileReader.errored = true;
                return;
            }
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            lineq.drainTo(mlineList);
            for (CSVLineWithMetaData lineList : mlineList) {
                if (lineList == endOfData) {
                    //Process anything that we didnt process. User supplied will not build anything in table
                    //so for -p options this will just skip and return.
                    if (table.getRowCount() > 0) {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this, failedQueue);
                        try {
                            if (!isMP) {
                                csvClient.callProcedure(cbmt, procName, partitionParam, m_tableName, table);
                            } else {
                                csvClient.callProcedure(cbmt, procName, m_tableName, table);
                            }
                            m_partitionProcessedCount.addAndGet(table.getRowCount());
                        } catch (IOException ex) {
                            String[] info = {lineList.rawLineData.toString(), ex.toString()};
                            m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        }
                    }
                    return;
                }
                //Build table or just call one proc at a time.
                if (!config.useSuppliedProcedure) {
                    try {
                        if (VoltTableUtil.addRowToVoltTableFromLine(table, lineList.line, columnTypes)) {
                            batchList.add(lineList);
                        } else {
                            String[] info = {lineList.rawLineData.toString(), "Missing or Invalid Data in Row."};
                            m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                            continue;
                        }
                    } catch (Exception ex) {
                        //Failed to add row....things like larger than supported row size
                        String[] info = {lineList.rawLineData.toString(), ex.toString()};
                        m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        continue;
                    }
                    //If our batch is complete submit it.
                    if (table.getRowCount() >= config.batch) {
                        try {
                            PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this, failedQueue);
                            if (!isMP) {
                                csvClient.callProcedure(cbmt, procName, partitionParam, m_tableName, table);
                            } else {
                                csvClient.callProcedure(cbmt, procName, m_tableName, table);
                            }
                            m_partitionProcessedCount.addAndGet(table.getRowCount());
                            //Clear table data as we start building new table with new rows.
                            table.clearRowData();
                        } catch (IOException ex) {
                            table.clearRowData();
                            String[] info = {lineList.rawLineData.toString(), ex.toString()};
                            m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                            return;
                        }
                        batchList = new ArrayList<CSVLineWithMetaData>();
                    }
                } else {
                    // submit a traditional/legacy insert
                    try {
                        PartitionSingleExecuteProcedureCallback cbmt = new PartitionSingleExecuteProcedureCallback(lineList, this);
                        csvClient.callProcedure(cbmt, procName, (Object[]) lineList.line);
                        m_partitionProcessedCount.incrementAndGet();
                    } catch (IOException ex) {
                        String[] info = {lineList.rawLineData.toString(), ex.toString()};
                        m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public void run() {

        //Pick the procedure.
        VoltTable table = new VoltTable(colInfo);
        String procName = (isMP ? "@LoadMultipartitionTable" : "@LoadSinglepartitionTable");
        if (config.useSuppliedProcedure) {
            procName = insertProcedure;
        }

        //If SP get partition param from Hashinator.
        Object partitionParam = null;
        if (!isMP) {
            partitionParam = TheHashinator.valueToBytes(m_partitionId);
        }

        //Launch failureProcessor
        FailedBatchProcessor failureProcessor = new FailedBatchProcessor(this, procName, m_tableName, partitionParam);
        failureProcessor.start();

        //Process the Partition queue.
        process(table, procName, partitionParam);
        lineq.clear();

        //Let partition processor drain and put any failures on failure processing.
        try {
            csvClient.drain();
            failedQueue.put(endOfData);
            failureProcessor.join();
            //Drain again for failure callbacks to finish.
            csvClient.drain();
        } catch (NoConnectionsException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        } catch (InterruptedException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        }

        CSVPartitionProcessor.processor_cdl.countDown();
        m_log.info("Done Processing partition: " + m_partitionId + " Processed: " + m_partitionProcessedCount);
    }
}
