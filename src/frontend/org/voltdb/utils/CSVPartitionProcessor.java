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
import java.text.NumberFormat;
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

/**
 * Process partition specific data. If the table is not partitioned only one instance of this processor will be used
 *
 */
class CSVPartitionProcessor implements Runnable {

    static public CSVLoader.CSVConfig m_config;
    final Client m_csvClient;
    //Queue for processing for this partition.
    final BlockingQueue<CSVLineWithMetaData> m_partitionQueue;
    final int m_partitionColumnIndex;
    final CSVLineWithMetaData m_endOfData;
    //Partition for which this processor thread is processing.
    final int m_partitionId;
    static String m_insertProcedure = "";
    static String m_tableName;
    final String m_processorName;
    static Map<Integer, VoltType> m_columnTypes;
    static VoltTable.ColumnInfo m_colInfo[];
    static boolean m_isMP = false;
    //Processed count indicates how many inser sent to server.
    final AtomicLong m_partitionProcessedCount = new AtomicLong(0);
    //Incremented after insert is acknowledged by server.
    static AtomicLong m_partitionAcknowledgedCount = new AtomicLong(0);
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
    static CountDownLatch m_processor_cdl;
    boolean m_errored = false;
    static int m_reportEveryNRows = 10000;

    public CSVPartitionProcessor(Client client, int partitionId,
            int partitionColumnIndex, BlockingQueue<CSVLineWithMetaData> partitionQueue, CSVLineWithMetaData eod) {
        m_csvClient = client;
        m_partitionId = partitionId;
        m_partitionQueue = partitionQueue;
        m_partitionColumnIndex = partitionColumnIndex;
        m_endOfData = eod;
        m_processorName = "PartitionProcessor-" + partitionId;
    }

    //Callback for single row procedure invoke called for rows in failed batch.
    public static class PartitionSingleExecuteProcedureCallback implements ProcedureCallback {

        CSVPartitionProcessor m_processor;
        final CSVLineWithMetaData m_csvLine;

        public PartitionSingleExecuteProcedureCallback(CSVLineWithMetaData csvLine, CSVPartitionProcessor processor) {
            m_processor = processor;
            m_csvLine = csvLine;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                String[] info = {m_csvLine.rawLine.toString(), response.getStatusString()};
                if (CSVFileReader.synchronizeErrorInfo(m_csvLine.lineNumber, info)) {
                    m_processor.m_errored = true;
                    return;
                }
                m_log.error(response.getStatusString());
                return;
            }
            long currentCount = CSVPartitionProcessor.m_partitionAcknowledgedCount.incrementAndGet();

            if (currentCount % m_reportEveryNRows == 0) {
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }
    //Queue of batch entries where some rows failed.
    private BlockingQueue<CSVLineWithMetaData> failedQueue = null;
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
            NumberFormat nf = NumberFormat.getInstance();
            while (true) {
                try {
                    CSVLineWithMetaData lineList;
                    lineList = failedQueue.take();
                    //If we see m_endOfData or processor has indicated to be in error stop further error processing.
                    //we must have reached maxerrors or end of processing.
                    if (lineList == m_endOfData || m_processor.m_errored) {
                        m_log.info("Shutting down failure processor for  " + m_processor.m_processorName);
                        break;
                    }
                    try {
                        VoltTable table = new VoltTable(m_colInfo);
                        //No need to check error here if a correctedLine has come here it was previously successful.
                        try {
                            VoltTableUtil.addRowToVoltTableFromLine(table, lineList.correctedLine, m_columnTypes, nf);
                        } catch (Exception ex) {
                            continue;
                        }

                        PartitionSingleExecuteProcedureCallback cbmt = new PartitionSingleExecuteProcedureCallback(lineList, m_processor);
                        if (!CSVPartitionProcessor.m_isMP) {
                            m_csvClient.callProcedure(cbmt, m_procName, m_partitionParam, m_tableName, table);
                        } else {
                            m_csvClient.callProcedure(cbmt, m_procName, m_tableName, table);
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
    public static class PartitionProcedureCallback implements ProcedureCallback {

        static int lastMultiple = 0;
        protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
        protected CSVPartitionProcessor m_processor;
        final private List<CSVLineWithMetaData> m_batchList;
        private final BlockingQueue<CSVLineWithMetaData> failedQueue;

        public PartitionProcedureCallback(List<CSVLineWithMetaData> batchList,
                CSVPartitionProcessor pp, BlockingQueue<CSVLineWithMetaData> fq) {
            m_processor = pp;
            m_batchList = new ArrayList(batchList);
            failedQueue = fq;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                // Batch failed queue it for individual processing and find out which actually m_errored.
                m_processor.m_partitionProcessedCount.addAndGet(-1 * m_batchList.size());
                if (!m_processor.m_errored) {
                    //If we have not reached the limit continue pushing to failure processor.
                    failedQueue.addAll(m_batchList);
                }
                return;
            }
            long executed = response.getResults()[0].asScalarLong();
            long currentCount = CSVPartitionProcessor.m_partitionAcknowledgedCount.addAndGet(executed);
            int newMultiple = (int) currentCount / m_reportEveryNRows;
            if (newMultiple != lastMultiple) {
                lastMultiple = newMultiple;
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }

    // while there are rows and m_endOfData not seen batch and call procedure for insert.
    private void processLoadTable(VoltTable table, String procName, Object partitionParam) {
        List<CSVLineWithMetaData> batchList = new ArrayList<CSVLineWithMetaData>();
        NumberFormat nf = NumberFormat.getInstance();

        while (true) {
            if (m_errored) {
                //Let file reader know not to read any more. All Partition Processors will quit processing.
                CSVFileReader.m_errored = true;
                return;
            }
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            m_partitionQueue.drainTo(mlineList, m_config.batch);
            for (CSVLineWithMetaData lineList : mlineList) {
                if (lineList == m_endOfData) {
                    //Process anything that we didnt process yet.
                    if (table.getRowCount() > 0) {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this, failedQueue);
                        try {
                            if (!m_isMP) {
                                m_csvClient.callProcedure(cbmt, procName, partitionParam, m_tableName, table);
                            } else {
                                m_csvClient.callProcedure(cbmt, procName, m_tableName, table);
                            }
                            m_partitionProcessedCount.addAndGet(table.getRowCount());
                        } catch (IOException ex) {
                            String[] info = {lineList.rawLine.toString(), ex.toString()};
                            m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        }
                    }
                    return;
                }
                //Build table or just call one proc at a time.
                try {
                    if (VoltTableUtil.addRowToVoltTableFromLine(table, lineList.correctedLine, m_columnTypes, nf)) {
                        batchList.add(lineList);
                    } else {
                        String[] info = {lineList.rawLine.toString(), "Missing or Invalid Data in Row."};
                        m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        continue;
                    }
                } catch (Exception ex) {
                    //Failed to add row....things like larger than supported row size
                    String[] info = {lineList.rawLine.toString(), ex.toString()};
                    m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                    continue;
                }
                //If our batch is complete submit it.
                if (table.getRowCount() >= m_config.batch) {
                    try {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this, failedQueue);
                        if (!m_isMP) {
                            m_csvClient.callProcedure(cbmt, procName, partitionParam, m_tableName, table);
                        } else {
                            m_csvClient.callProcedure(cbmt, procName, m_tableName, table);
                        }
                        m_partitionProcessedCount.addAndGet(table.getRowCount());
                        //Clear table data as we start building new table with new rows.
                        table.clearRowData();
                    } catch (IOException ex) {
                        table.clearRowData();
                        String[] info = {lineList.rawLine.toString(), ex.toString()};
                        m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        return;
                    }
                    batchList = new ArrayList<CSVLineWithMetaData>();
                }
            }
        }
    }

    // while there are rows and m_endOfData not seen batch and call procedure supplied by user.
    private void processUserSuppliedProcedure(String procName) {
        while (true) {
            if (m_errored) {
                //Let file reader know not to read any more. All Partition Processors will quit processing.
                CSVFileReader.m_errored = true;
                return;
            }
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            m_partitionQueue.drainTo(mlineList, m_config.batch);
            for (CSVLineWithMetaData lineList : mlineList) {
                if (lineList == m_endOfData) {
                    return;
                }
                // call supplied procedure.
                try {
                    PartitionSingleExecuteProcedureCallback cbmt = new PartitionSingleExecuteProcedureCallback(lineList, this);
                    m_csvClient.callProcedure(cbmt, procName, (Object[]) lineList.correctedLine);
                    m_partitionProcessedCount.incrementAndGet();
                } catch (IOException ex) {
                    String[] info = {lineList.rawLine.toString(), ex.toString()};
                    m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                    return;
                }
            }
        }
    }

    @Override
    public void run() {

        FailedBatchProcessor failureProcessor = null;
        //Process the Partition queue.
        if (m_config.useSuppliedProcedure) {
            processUserSuppliedProcedure(m_insertProcedure);
        } else {
            //If SP get partition param from Hashinator.
            Object partitionParam = null;
            if (!m_isMP) {
                partitionParam = TheHashinator.valueToBytes(m_partitionId);
            }

            VoltTable table = new VoltTable(m_colInfo);
            String procName = (m_isMP ? "@LoadMultipartitionTable" : "@LoadSinglepartitionTable");

            //Launch failureProcessor
            failedQueue = new LinkedBlockingQueue<CSVLineWithMetaData>();
            failureProcessor = new FailedBatchProcessor(this, procName, m_tableName, partitionParam);
            failureProcessor.start();

            processLoadTable(table, procName, partitionParam);
        }
        m_partitionQueue.clear();

        //Let partition processor drain and put any failures on failure processing.
        try {
            m_csvClient.drain();
            if (failureProcessor != null) {
                failedQueue.put(m_endOfData);
                failureProcessor.join();
                //Drain again for failure callbacks to finish.
                m_csvClient.drain();
            }
        } catch (NoConnectionsException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        } catch (InterruptedException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        }

        CSVPartitionProcessor.m_processor_cdl.countDown();
        m_log.info("Done Processing partition: " + m_partitionId + " Processed: " + m_partitionProcessedCount);
    }
}
