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
import java.util.logging.Level;
import java.util.logging.Logger;
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
    public static ArrayList<VoltType> columnTypes;
    public static VoltTable.ColumnInfo colInfo[];
    public static boolean isMP = false;
    public static boolean isLoadTable = false;
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

        private int m_batchCount;
        private final long m_lineNum = 0;
        private final String[] m_rowdata;
        private static int lastMultiple = 0;
        protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
        protected CSVPartitionProcessor pprocessor;

        public PartitionProcedureCallback(int batchCount, String[] rowData, CSVPartitionProcessor pp) {
            m_batchCount = batchCount;
            m_rowdata = rowData;
            pprocessor = pp;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            int reportEveryNRows = 10000;
            if (response.getStatus() != ClientResponse.SUCCESS) {
                m_log.error(response.getStatusString());
                String[] info = {m_rowdata.toString(), response.getStatusString()};
                if (synchronizeErrorInfo(m_lineNum, info)) {
                    pprocessor.errored = true;
                }
                return;
            }
            long currentCount = pprocessor.partitionAcknowledgedCount.addAndGet(m_batchCount);
            int newMultiple = (int) currentCount / reportEveryNRows;
            if (newMultiple != lastMultiple) {
                lastMultiple = newMultiple;
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }

    @Override
    public void run() {

        Client lcsvClient = csvClient;
        VoltTable table = new VoltTable(colInfo);
        String procName = (isMP ? "@LoadMultipartitionTable" : (isLoadTable ? "@LoadPartitionData" : "@LoadSinglepartitionTable"));
        if (config.ping) {
            procName = "@Ping";
        } else if (config.useSuppliedProcedure) {
            procName = insertProcedure;
        }

        m_log.info("Using Procedure: " + procName);
        Object partitionParam;
        if (isLoadTable) {
            partitionParam = partitionId;
        } else {
            partitionParam = TheHashinator.valueToBytes(partitionId);
        }
        String lastLine[] = null;
        while (true) {
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
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
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(table.getRowCount(), lastLine, this);
                        try {
                            if (!isMP) {
                                lcsvClient.callProcedure(cbmt, procName, partitionParam, tableName, table);
                            } else {
                                lcsvClient.callProcedure(cbmt, procName, tableName, table);
                            }
                            partitionProcessedCount += table.getRowCount();
                        } catch (IOException ex) {
                            errored = synchronizeErrorInfo(lineList.lineNumber, lineList.line);
                            break;
                        }
                    }
                    end = true;
                    break;
                }
                lastLine = lineList.line;
                if (!config.useSuppliedProcedure) {
                    try {
                        VoltTableUtil.toVoltTableFromLine(table, lineList.line, columnTypes);
                    } catch (Exception ex) {
                        //Failed to add row....
                        errored = synchronizeErrorInfo(lineList.lineNumber, lineList.line);
                        continue;
                    }
                    if (table.getRowCount() > config.batch) {
                        try {
                            PartitionProcedureCallback cbmt = new PartitionProcedureCallback(table.getRowCount(), lineList.line, this);
                            if (!isMP) {
                                lcsvClient.callProcedure(cbmt, procName, partitionParam, tableName, table);
                            } else {
                                lcsvClient.callProcedure(cbmt, procName, tableName, table);
                            }
                            partitionProcessedCount += table.getRowCount();
                            table.clearRowData();
                        } catch (IOException ex) {
                            table.clearRowData();
                            errored = synchronizeErrorInfo(lineList.lineNumber, lineList.line);
                            break;
                        }
                    }
                } else {
                    try {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(1, lineList.line, this);
                        csvClient.callProcedure(cbmt, procName, (Object[]) lineList.line);
                        partitionProcessedCount++;
                    } catch (IOException ex) {
                        errored = synchronizeErrorInfo(lineList.lineNumber, lineList.line);
                        break;
                    }
                }
            }
            if (end) {
                break;
            }
        }
        try {
            lcsvClient.drain();
        } catch (NoConnectionsException ex) {
            Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        CSVPartitionProcessor.pcount.countDown();
        System.out.println("Done Processing partition: " + partitionId + " Processed: " + partitionProcessedCount);
    }
}
