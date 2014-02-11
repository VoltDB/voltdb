/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ParameterConverter;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import static org.voltdb.utils.CSVFileReader.synchronizeErrorInfo;

/**
 * Process partition specific data. If the table is not partitioned only one instance of this processor will be used
 *
 */
class CSVPartitionProcessor implements Runnable {

    static public CSVLoader.CSVConfig m_config;
    final Client m_csvClient;
    //Queue for processing for this partition
    final BlockingQueue<CSVLineWithMetaData> m_partitionQueue;
    final CSVLineWithMetaData m_endOfData;
    //Partition for which this processor thread is processing.
    final long m_partitionId;
    //This is just so we can identity thread name and log information.
    final String m_processorName;
    //Processed count indicates how many insert sent to server.
    final AtomicLong m_partitionProcessedCount = new AtomicLong(0);
    //Incremented after insert is acknowledged by server.
    static AtomicLong m_partitionAcknowledgedCount = new AtomicLong(0);
    protected static final VoltLogger m_log = new VoltLogger("CSVLOADER");
    static CountDownLatch m_processor_cdl;
    boolean m_errored = false;
    static int m_reportEveryNRows = 10000;
    //Following fields are detected by initializeProcessorInformation
    //Procedure supplied in -p mode
    static String m_insertProcedure = "";
    //Table name to insert into.
    static String m_tableName;
    //Types of columns
    static List<VoltType> m_typeList = new ArrayList<VoltType>();
    //Column information
    static VoltTable.ColumnInfo m_colInfo[];
    //Column types
    static Map<Integer, VoltType> m_columnTypes;
    //Column Names
    static Map<Integer, String> m_colNames;
    //Zero based index of the partitioned column
    static int m_partitionedColumnIndex = -1;
    //Partitioned column type
    static VoltType m_partitionColumnType = VoltType.NULL;
    //Number of columns
    static int m_columnCnt = 0;
    //Is this a MP transaction
    static boolean m_isMP = false;
    //Number of processors default to 1 when its a MP procedure or table.
    static int m_numProcessors = 1;

    //Queue of batch entries where some rows failed.
    private BlockingQueue<CSVLineWithMetaData> m_failedQueue = null;
    //Counter that goes down when acknowledged so that drain is not needed.
    private AtomicLong m_batchWaitCount = new AtomicLong(0);

    public CSVPartitionProcessor(Client client, long partitionId,
            int partitionColumnIndex, BlockingQueue<CSVLineWithMetaData> partitionQueue, CSVLineWithMetaData eod) {
        m_csvClient = client;
        m_partitionId = partitionId;
        m_partitionQueue = partitionQueue;
        m_endOfData = eod;
        m_processorName = "PartitionProcessor-" + partitionId;
    }

    //Check if the procedure you are using with -p option is multi-part.
    private static boolean isProcedureMp(Client csvClient)
            throws IOException, org.voltdb.client.ProcCallException {
        boolean procedure_is_mp = false;
        VoltTable procInfo = csvClient.callProcedure("@SystemCatalog",
                "PROCEDURES").getResults()[0];
        while (procInfo.advanceRow()) {
            if (m_insertProcedure.matches(procInfo.getString("PROCEDURE_NAME"))) {
                String remarks = procInfo.getString("REMARKS");
                if (remarks.contains("\"singlePartition\":false")) {
                    procedure_is_mp = true;
                }
                break;
            }
        }
        return procedure_is_mp;
    }

    // Based on method of loading do sanity check and setup info for loading. if this method returns false
    // you have not enough information to continue and we must exit.
    public static boolean initializeProcessorInformation(CSVLoader.CSVConfig config, Client csvClient)
            throws IOException, ProcCallException, InterruptedException {
        VoltTable procInfo;
        m_config = config;
        if (m_config.useSuppliedProcedure) {
            // -p mode where user specified a procedure name could be standard CRUD or written from scratch.
            boolean isProcExist = false;
            m_insertProcedure = m_config.procedure;
            procInfo = csvClient.callProcedure("@SystemCatalog",
                    "PROCEDURECOLUMNS").getResults()[0];
            while (procInfo.advanceRow()) {
                if (m_insertProcedure.matches((String) procInfo.get(
                        "PROCEDURE_NAME", VoltType.STRING))) {
                    m_columnCnt++;
                    isProcExist = true;
                    String typeStr = (String) procInfo.get("TYPE_NAME", VoltType.STRING);
                    m_typeList.add(VoltType.typeFromString(typeStr));
                }
            }
            if (isProcExist == false) {
                //csvloader will exit
                m_log.error("No matching insert procedure available");
                return false;
            }
            m_isMP = isProcedureMp(csvClient);
        } else {
            //Table mode get table details and then build partition and column information.
            m_tableName = m_config.table;
            procInfo = csvClient.callProcedure("@SystemCatalog",
                    "COLUMNS").getResults()[0];
            m_columnTypes = new TreeMap<Integer, VoltType>();
            m_colNames = new TreeMap<Integer, String>();
            while (procInfo.advanceRow()) {
                String table = procInfo.getString("TABLE_NAME");
                if (m_config.table.equalsIgnoreCase(table)) {
                    VoltType vtype = VoltType.typeFromString(procInfo.getString("TYPE_NAME"));
                    int idx = (int) procInfo.getLong("ORDINAL_POSITION") - 1;
                    m_columnTypes.put(idx, vtype);
                    m_colNames.put(idx, procInfo.getString("COLUMN_NAME"));
                    String remarks = procInfo.getString("REMARKS");
                    if (remarks != null && remarks.equalsIgnoreCase("PARTITION_COLUMN")) {
                        m_partitionColumnType = vtype;
                        m_partitionedColumnIndex = idx;
                        m_log.debug("Table " + m_config.table + " Partition Column Name is: "
                                + procInfo.getString("COLUMN_NAME"));
                        m_log.debug("Table " + m_config.table + " Partition Column Type is: " + vtype.toString());
                    }
                }
            }

            if (m_columnTypes.isEmpty()) {
                //csvloader will exit.
                m_log.error("Table " + m_config.table + " Not found");
                return false;
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
            int sitesPerHost = 1;
            int kfactor = 0;
            int hostcount = 1;
            procInfo = csvClient.callProcedure("@SystemInformation",
                    "deployment").getResults()[0];
            while (procInfo.advanceRow()) {
                String prop = procInfo.getString("PROPERTY");
                if (prop != null && prop.equalsIgnoreCase("sitesperhost")) {
                    sitesPerHost = Integer.parseInt(procInfo.getString("VALUE"));
                }
                if (prop != null && prop.equalsIgnoreCase("hostcount")) {
                    hostcount = Integer.parseInt(procInfo.getString("VALUE"));
                }
                if (prop != null && prop.equalsIgnoreCase("kfactor")) {
                    kfactor = Integer.parseInt(procInfo.getString("VALUE"));
                }
            }
            m_isMP = (m_partitionedColumnIndex == -1 ? true : false);
            if (!m_isMP) {
                m_numProcessors = (hostcount * sitesPerHost) / (kfactor + 1);
                m_log.debug("Number of Partitions: " + m_numProcessors);
                m_log.info("CSV loader will attempt to load rows in batches of size: " + m_config.batch);
            }
        }

        //Only print warning with -p case for table case we use sys procs.
        if (m_isMP && m_config.useSuppliedProcedure) {
            m_log.warn("Using a multi-partitioned procedure to load data will be slow. "
                    + "If loading a partitioned table, use a single-partitioned procedure "
                    + "for best performance.");
        }
        //Create the CDL based on number of processors we are going to run.
        CSVPartitionProcessor.m_processor_cdl = new CountDownLatch(m_numProcessors);
        return true;
    }

    //Callback for single row procedure invoke called for rows in failed batch.
    public static class PartitionSingleExecuteProcedureCallback implements ProcedureCallback {

        CSVPartitionProcessor m_processor;
        final CSVLineWithMetaData m_csvLine;

        public PartitionSingleExecuteProcedureCallback(CSVLineWithMetaData csvLine, CSVPartitionProcessor processor) {
            m_processor = processor;
            m_csvLine = csvLine;
        }

        //one insert at a time callback
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            byte status = response.getStatus();
            if (status != ClientResponse.SUCCESS) {
                if (status != ClientResponse.USER_ABORT && status != ClientResponse.GRACEFUL_FAILURE) {
                    System.out.println("Fatal Response from server for: " + response.getStatusString()
                            + " for: " + m_csvLine.rawLine.toString());
                    System.exit(1);
                }
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

    //This is the thread which processes one insert at a time if the batch fails
    private class FailedBatchProcessor extends Thread {

        private final CSVPartitionProcessor m_processor;
        private final String m_procName;

        public FailedBatchProcessor(CSVPartitionProcessor pp, String procName) {
            m_processor = pp;
            m_procName = procName;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    CSVLineWithMetaData lineList;
                    lineList = m_failedQueue.take();
                    //If we see m_endOfData or processor has indicated to be in error stop further error processing.
                    //we must have reached maxerrors or end of processing.
                    if (lineList == m_endOfData || m_processor.m_errored) {
                        m_log.debug("Shutting down failure processor for  " + m_processor.m_processorName);
                        break;
                    }
                    try {
                        VoltTable table = new VoltTable(m_colInfo);
                        //No need to check error here if a correctedLine has come here it was previously successful.
                        try {
                            addRowToVoltTableFromLine(table, lineList.correctedLine);
                        } catch (Exception ex) {
                            continue;
                        }

                        PartitionSingleExecuteProcedureCallback cbmt =
                                new PartitionSingleExecuteProcedureCallback(lineList, m_processor);
                        submitWorkToServer(table, m_procName, cbmt, true);
                    } catch (IOException ioex) {
                        //Put this processor in error so we exit
                        m_log.warn("Fallback to single row inserts failed, failures will not be processed: " + ioex);
                        m_failedQueue.clear();
                        m_failedQueue = null;
                        m_processor.m_errored = true;
                        break;
                    }
                } catch (InterruptedException ex) {
                    //Put this processor in error so we exit
                    m_log.debug("Stopped failure processor.");
                    m_failedQueue.clear();
                    m_failedQueue = null;
                    m_processor.m_errored = true;
                    break;
                }
            }
        }
    }
    //This is to keep track of when to report how many rows inserted, shared by all processors.
    static AtomicLong lastMultiple = new AtomicLong(0);
    // Callback for batch invoke when table has more than 1 entries. The callback on failure feeds m_failedQueue for
    // one row at a time processing.
    public class PartitionProcedureCallback implements ProcedureCallback {

        protected CSVPartitionProcessor m_processor;
        final private List<CSVLineWithMetaData> m_batchList;

        public PartitionProcedureCallback(List<CSVLineWithMetaData> batchList,
                CSVPartitionProcessor pp) {
            m_processor = pp;
            m_batchList = new ArrayList(batchList);
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            byte status = response.getStatus();
            if (status != ClientResponse.SUCCESS) {
                if (status != ClientResponse.USER_ABORT && status != ClientResponse.GRACEFUL_FAILURE) {
                    System.out.println("Fatal Response from server for batch. "
                            + "Please check health of the server. Status: "
                            + response.getStatusString());
                    System.exit(1);
                }
                // Batch failed queue it for individual processing and find out which actually m_errored.
                m_log.info("Unable to insert rows in a batch.  Attempting to insert them one-by-one.");
                m_log.info("Note: this will result in reduced insertion performance.");
                m_log.debug("Batch Failed Will be processed by Failure Processor: " + response.getStatusString());
                m_processor.m_partitionProcessedCount.addAndGet(-1 * m_batchList.size());
                if (!m_processor.m_errored) {
                    //If we have not reached the limit continue pushing to failure processor only if
                    //failure processor is available.
                    if (m_failedQueue != null) {
                        m_failedQueue.addAll(m_batchList);
                    }
                }
                m_batchWaitCount.decrementAndGet();
                return;
            }
            //Successful invocations.
            m_batchWaitCount.decrementAndGet();
            long executed = response.getResults()[0].asScalarLong();
            long currentCount = CSVPartitionProcessor.m_partitionAcknowledgedCount.addAndGet(executed);
            int newMultiple = (int) currentCount / m_reportEveryNRows;
            if (newMultiple != lastMultiple.get()) {
                lastMultiple.set(newMultiple);
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }

    /**
     * Submit work to server if this fails for any reason bail.
     * In case submitting from failure processing dont do batch counting.
     *
     * @param table That contains rows to be sent in case of table load
     * @param procName procedure name we are using.
     * @param cbmt callback.
     * @param fromFailure Indicates if submitting work from regular or failure processor.
     * @throws IOException
     */
    private void submitWorkToServer(VoltTable table, String procName,
            ProcedureCallback cbmt, boolean fromFailure) throws IOException {
        boolean success;
        if (!CSVPartitionProcessor.m_isMP) {
            //If transaction is restarted because of wrong partition client will retry
            Object rpartitionParam =
                    TheHashinator.valueToBytes(table.fetchRow(0).get(
                    CSVPartitionProcessor.m_partitionedColumnIndex, m_partitionColumnType));
            success = m_csvClient.callProcedure(cbmt, procName, rpartitionParam, m_tableName, table);
        } else {
            success = m_csvClient.callProcedure(cbmt, procName, m_tableName, table);
        }
        if (success) {
            m_partitionProcessedCount.addAndGet(table.getRowCount());
            if (!fromFailure) {
                m_batchWaitCount.incrementAndGet();
            }
        } else {
            //We failed to send work to cluster lets exit.
            m_log.fatal("Failed to send CSV insert to VoltDB cluster.");
            System.exit(1);
        }
    }

    // while there are rows and m_endOfData not seen batch and call procedure for insert.
    private void processLoadTable(VoltTable table, String procName) throws InterruptedException {
        List<CSVLineWithMetaData> batchList = new ArrayList<CSVLineWithMetaData>();
        while (true) {
            if (m_errored) {
                //Let file reader know not to read any more. All Partition Processors will quit processing.
                CSVFileReader.m_errored = true;
                return;
            }
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            mlineList.add(m_partitionQueue.take());
            m_partitionQueue.drainTo(mlineList, m_config.batch-1);
            for (CSVLineWithMetaData lineList : mlineList) {
                if (lineList == m_endOfData) {
                    //Process anything that we didn't process yet.
                    if (table.getRowCount() > 0) {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this);
                        try {
                            submitWorkToServer(table, procName, cbmt, false);
                        } catch (IOException ex) {
                            String[] info = {lineList.rawLine.toString(), ex.toString()};
                            m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        }
                    }
                    return;
                }
                //Build table or just call one proc at a time.
                try {
                    if (addRowToVoltTableFromLine(table, lineList.correctedLine)) {
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
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this);
                        submitWorkToServer(table, procName, cbmt, false);
                        //Clear table data as we start building new table with new rows.
                        table.clearRowData();
                        batchList = new ArrayList<CSVLineWithMetaData>();
                    } catch (IOException ex) {
                        //We lost network.
                        table.clearRowData();
                        String[] info = {lineList.rawLine.toString(), ex.toString()};
                        m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        CSVFileReader.m_errored = true;
                        return;
                    }
                }
            }
        }
    }

    // while there are rows and m_endOfData not seen batch and call procedure supplied by user.
    // When -p option is used only 1 processor is created.
    private void processUserSuppliedProcedure(String procName) throws InterruptedException {
        while (true) {
            if (m_errored) {
                //Let file reader know not to read any more. All Partition Processors will quit processing.
                CSVFileReader.m_errored = true;
                return;
            }
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            mlineList.add(m_partitionQueue.take());
            m_partitionQueue.drainTo(mlineList, m_config.batch);
            //Go over lines we collected and submit one insert at a time as this is with -p option.
            for (CSVLineWithMetaData lineList : mlineList) {
                if (lineList == m_endOfData) {
                    return;
                }
                // call supplied procedure.
                try {
                    PartitionSingleExecuteProcedureCallback cbmt =
                            new PartitionSingleExecuteProcedureCallback(lineList, this);
                    if (m_csvClient.callProcedure(cbmt, procName, (Object[]) lineList.correctedLine)) {
                        m_partitionProcessedCount.incrementAndGet();
                    } else {
                        m_log.fatal("Failed to send CSV insert to VoltDB cluster.");
                        System.exit(1);
                    }
                } catch (IOException ex) {
                    String[] info = {lineList.rawLine.toString(), ex.toString()};
                    m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                    return;
                }
            }
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

        FailedBatchProcessor failureProcessor = null;
        try {
            //Process the Partition queue.
            if (m_config.useSuppliedProcedure) {
                processUserSuppliedProcedure(m_insertProcedure);
            } else {

                VoltTable table = new VoltTable(m_colInfo);
                String procName = (m_isMP ? "@LoadMultipartitionTable" : "@LoadSinglepartitionTable");

                //Launch failureProcessor
                m_failedQueue = new LinkedBlockingQueue<CSVLineWithMetaData>();
                failureProcessor = new FailedBatchProcessor(this, procName);
                failureProcessor.start();

                processLoadTable(table, procName);
            }
            m_partitionQueue.clear();
            //Make sure all callbacks have been called and any failures are posted on failure queue before we
            //we put end of queue marker on failure processor.
            m_log.debug("Waiting for all batches acknowledged.");
            while (m_batchWaitCount.get() > 0) {
                Thread.sleep(100);
            }
            m_log.debug("All batches acknowledged.");
            //Let partition processor drain on failure processing.
            if (failureProcessor != null) {
                if (m_failedQueue != null) {
                    m_failedQueue.put(m_endOfData);
                }
                failureProcessor.join();
            }
        } catch (Exception ex) {
            //Let the CSV reader know that dont read any more lines we are exiting.
            CSVFileReader.m_errored = true;
            m_log.error("Failed to process partitioned data: " + ex);
        } finally {
            CSVPartitionProcessor.m_processor_cdl.countDown();
            m_log.debug("Done Processing partition: " + m_partitionId + " Processed: " + m_partitionProcessedCount);
        }
    }
}
