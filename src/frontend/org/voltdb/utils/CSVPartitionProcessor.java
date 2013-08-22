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
import org.voltcore.logging.VoltLogger;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.NoConnectionsException;

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
    public CSVFileReader rdr;
    public String tableName;
    public static ArrayList<VoltType> columnTypes;
    public static VoltTable.ColumnInfo colInfo[];
    public static boolean isMP = false;
    public static boolean isLoadTable = false;
    long partitionProcessedCount = 0;
    long processBatchEveryMilliseconds = 100;
    protected final VoltLogger m_log = new VoltLogger("CONSOLE");


    @Override
    public void run() {

        Client lcsvClient = csvClient;
        if (config.ppc) {
            System.out.println("Using per partition client connection.");
            String[] serverlist = config.servers.split(",");

            ClientConfig c_config = new ClientConfig(config.user, config.password);
            c_config.setProcedureCallTimeout(0); // Set procedure all to infinite
            try {
                lcsvClient = CSVLoaderMT.getClient(c_config, serverlist, config.port);
            } catch (Exception e) {
                m_log.error("Error to connect to the servers:"
                        + config.servers);
            }
            assert (lcsvClient != null);
        }

        VoltTable table = new VoltTable(colInfo);
        String procName = (isMP ? "@LoadMultipartitionTable" : (isLoadTable ? "@LoadPartitionData" : "@LoadSinglepartitionTable"));
        if (config.ping) {
            procName = "@Ping";
        } else if (config.legacy) {
            procName = insertProcedure;
            table = null; // We are using legacy insert method.
        }

        System.out.println("Using Procedure: " + procName);
        long batch_start = System.currentTimeMillis();
        long batch_process_time = batch_start + processBatchEveryMilliseconds;
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
                            lcsvClient.callProcedure(cbmt, procName, param1, tableName, table);
                        } else {
                            lcsvClient.callProcedure(cbmt, procName, tableName, table);
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
                VoltTableUtil.toVoltTableFromLine(table, lineList.line, columnTypes);
                if (table.getRowCount() > config.batch) {
                    try {
                        CSVLoaderMT.MyMTCallback cbmt = new CSVLoaderMT.MyMTCallback(table.getRowCount(), lineList.line);
                        if (!isMP) {
                            Object param1;
                            if (isLoadTable) {
                                param1 = partitionId;
                            } else {
                                param1 = TheHashinator.valueToBytes(partitionId);
                            }
                            lcsvClient.callProcedure(cbmt, procName, param1, tableName, table);
                        } else {
                            lcsvClient.callProcedure(cbmt, procName, tableName, table);
                        }
                        CSVFileReader.inCount.addAndGet(table.getRowCount());
                        partitionProcessedCount += table.getRowCount();
                        table.clearRowData();
                    } catch (IOException ex) {
                        Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    batch_start = System.currentTimeMillis();;
                    batch_process_time = batch_start + processBatchEveryMilliseconds;
                    //System.out.println("Batch Processing Time: " + (batch_end - batch_start));
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
        if (config.ppc) {
            try {
                lcsvClient.drain();
                lcsvClient.close();
            } catch (NoConnectionsException ex) {
                Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
