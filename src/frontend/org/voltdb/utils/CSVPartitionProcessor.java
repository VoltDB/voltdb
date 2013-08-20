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
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;

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
    int batchmax = 200;
    public static String insertProcedure = "";
    public CSVFileReader rdr;
    public String tableName;
    public static ArrayList<VoltType> columnTypes;
    public static ArrayList<String> colNames;
    public static VoltTable.ColumnInfo colInfo[];
    public static boolean isMP = false;
    public static boolean isLoadTable = false;
    long partitionProcessedCount = 0;

    @Override
    public void run() {
        VoltTable table = new VoltTable(colInfo);
        //@LoadSinglepartitionTable
        String procName = (isMP ? "@LoadMultipartitionTable" : (isLoadTable ? "@LoadPartitionData" : "@LoadSinglepartitionTable"));
        if (config.ping) {
            procName = "@Ping";
        } else if (config.legacy) {
            procName = insertProcedure;
            table = null; // We are using legacy insert method.
        }

        System.out.println("Using Procedure: " + procName);

        while (true) {
            CSVLineWithMetaData lineList = null;
            try {
                lineList = lineq.take();
            } catch (InterruptedException ex) {
                Logger.getLogger(CSVLoaderMT.class.getName()).log(Level.SEVERE, null, ex);
            }
            if (lineList == dummy) {
                if (config.legacy) {
                    System.out.println("Done Processing partition: " + partitionId + " Processed: " + partitionProcessedCount);
                    break;
                }
                //Process anything that we didnt process.
                if (table.getRowCount() > 0) {
                    CSVLoaderMT.MyMTCallback cbmt = new CSVLoaderMT.MyMTCallback(table.getRowCount(), lineList.line);
                    try {
                        if (!isMP) {
                            Object param1;
                            if (isLoadTable) {
                                param1 = partitionId;
                            } else {
                                param1 = TheHashinator.valueToBytes(partitionId);
                            }
                            csvClient.callProcedure(cbmt, procName, param1, tableName, table);
                        } else {
                            csvClient.callProcedure(cbmt, procName, tableName, table);
                        }
                        CSVFileReader.inCount.addAndGet(table.getRowCount());
                        partitionProcessedCount += table.getRowCount();
                    } catch (IOException ex) {
                        Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                System.out.println("Done Processing partition: " + partitionId + " Processed: " + partitionProcessedCount);
                break;
            }
            if (lineList == null) {
                continue;
            }
            if (!config.legacy) {
                VoltTableUtil.toVoltTableFromLine(table, lineList.line, colNames, columnTypes);
                if (table.getRowCount() > batchmax) {
                    try {
                        CSVLoaderMT.MyMTCallback cbmt = new CSVLoaderMT.MyMTCallback(table.getRowCount(), lineList.line);
                        if (!isMP) {
                            Object param1;
                            if (isLoadTable) {
                                param1 = partitionId;
                            } else {
                                param1 = TheHashinator.valueToBytes(partitionId);
                            }
                            csvClient.callProcedure(cbmt, procName, param1, tableName, table);
                        } else {
                            csvClient.callProcedure(cbmt, procName, tableName, table);
                        }
                        CSVFileReader.inCount.addAndGet(table.getRowCount());
                        partitionProcessedCount += table.getRowCount();
                        table.clearRowData();
                    } catch (IOException ex) {
                        Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else {
                try {
                    CSVLoaderMT.MyCallback cbmt = new CSVLoaderMT.MyCallback(0, config, lineList.line);
                    csvClient.callProcedure(cbmt, procName, (Object[]) lineList.line);
                    CSVFileReader.inCount.incrementAndGet();
                } catch (IOException ex) {
                    Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

    }
}
