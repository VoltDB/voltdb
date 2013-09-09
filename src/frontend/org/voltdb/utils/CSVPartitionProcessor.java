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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
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

    static public CSVLoaderMT.CSVConfig config;
    public Client csvClient;
    public BlockingQueue<CSVLineWithMetaData> lineq;
    public boolean done = false;
    int columnCnt = 0;
    int partitionedColumnIndex;
    String parColumnName;
    CSVLineWithMetaData dummy;
    int partitionId;
    public static String insertProcedure = "";
    public String tableName;
    public String name;
    public static Map<Integer, VoltType> columnTypes;
    public static VoltTable.ColumnInfo colInfo[];
    public static boolean isMP = false;
    long partitionProcessedCount = 0;
    AtomicLong partitionAcknowledgedCount = new AtomicLong(0);
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
    static Map<Long, String[]> errorInfo = new HashMap<Long, String[]>();
    static CountDownLatch pcount;
    boolean errored = false;

    public static boolean synchronizeErrorInfo(long errLineNum, String[] info) {
        errorInfo.put(errLineNum, info);
        if (errorInfo.size() >= config.maxerrors) {
            m_log.error("The number of Failure row data exceeds "
                    + config.maxerrors);
            return true;
        }
        return false;
    }

    public static final class PartitionProcedureCallback implements ProcedureCallback {

        private static int lastMultiple = 0;
        protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
        protected CSVPartitionProcessor m_processor;
        public List<CSVLineWithMetaData> m_batchList;

        public PartitionProcedureCallback(List<CSVLineWithMetaData> batchList, CSVPartitionProcessor pp) {
            m_processor = pp;
            m_batchList = batchList;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            int reportEveryNRows = 10000;
            if (response.getStatus() != ClientResponse.SUCCESS) {
                m_log.error(response.getStatusString());
                CSVLineWithMetaData lineData = m_batchList.get((int) 0);
                String[] info = {lineData.rawLineData.toString(), response.getStatusString()};
                if (synchronizeErrorInfo(lineData.lineNumber, info)) {
                    m_processor.errored = true;
                }
                return;
            }
            long executed = response.getResults()[0].asScalarLong();
            long currentCount = m_processor.partitionAcknowledgedCount.addAndGet(executed);
            int newMultiple = (int) currentCount / reportEveryNRows;
            if (newMultiple != lastMultiple) {
                lastMultiple = newMultiple;
                m_log.info(m_processor.name + " Inserted " + currentCount + " rows");
            }
        }
    }

    @Override
    public void run() {

        VoltTable table = new VoltTable(colInfo);
        String procName = (isMP ? "@LoadMultipartitionTable" : "@LoadSinglepartitionTable");
        if (config.useSuppliedProcedure) {
            procName = insertProcedure;
        }

        Object partitionParam = null;
        if (!isMP) {
            partitionParam = TheHashinator.valueToBytes(partitionId);
        }
        while (true) {
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            List<CSVLineWithMetaData> batchList = new ArrayList<CSVLineWithMetaData>();
            lineq.drainTo(mlineList);
            boolean end = false;
            for (CSVLineWithMetaData lineList : mlineList) {
                if (errored) {
                    end = true;
                    break;
                }
                if (lineList == dummy) {
                    if (config.useSuppliedProcedure) {
                        end = true;
                        break;
                    }
                    //Process anything that we didnt process.
                    if (table.getRowCount() > 0) {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this);
                        try {
                            if (!isMP) {
                                csvClient.callProcedure(cbmt, procName, partitionParam, tableName, table);
                            } else {
                                csvClient.callProcedure(cbmt, procName, tableName, table);
                            }
                            partitionProcessedCount += table.getRowCount();
                        } catch (IOException ex) {
                            String[] info = {lineList.rawLineData.toString(), ex.toString()};
                            errored = synchronizeErrorInfo(lineList.lineNumber, info);
                            break;
                        }
                    }
                    end = true;
                    break;
                }
                if (!config.useSuppliedProcedure) {
                    try {
                        VoltTableUtil.toVoltTableFromLine(table, lineList.line, columnTypes);
                        batchList.add(lineList);
                    } catch (Exception ex) {
                        //Failed to add row....
                        String[] info = {lineList.rawLineData.toString(), ex.toString()};
                        errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        continue;
                    }
                    if (table.getRowCount() > config.batch) {
                        try {
                            batchList.add(lineList);
                            PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this);
                            if (!isMP) {
                                csvClient.callProcedure(cbmt, procName, partitionParam, tableName, table);
                            } else {
                                csvClient.callProcedure(cbmt, procName, tableName, table);
                            }
                            partitionProcessedCount += table.getRowCount();
                            table.clearRowData();
                            batchList.clear();
                            batchList = new ArrayList<CSVLineWithMetaData>();
                        } catch (IOException ex) {
                            table.clearRowData();
                            String[] info = {lineList.rawLineData.toString(), ex.toString()};
                            errored = synchronizeErrorInfo(lineList.lineNumber, info);
                            break;
                        }
                    }
                } else {
                    try {
                        batchList.add(lineList);
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this);
                        csvClient.callProcedure(cbmt, procName, (Object[]) lineList.line);
                        partitionProcessedCount++;
                        batchList = new ArrayList<CSVLineWithMetaData>();
                    } catch (IOException ex) {
                        String[] info = {lineList.rawLineData.toString(), ex.toString()};
                        errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        break;
                    }
                }
            }
            if (end) {
                break;
            }
        }
        try {
            csvClient.drain();
        } catch (NoConnectionsException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        } catch (InterruptedException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        }
        CSVPartitionProcessor.pcount.countDown();
        m_log.info("Done Processing partition: " + partitionId + " Processed: " + partitionProcessedCount);
    }
}
