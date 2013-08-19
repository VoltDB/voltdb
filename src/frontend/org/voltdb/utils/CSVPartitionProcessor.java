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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;

class CSVPartitionProcessor implements Runnable {

    public Client csvClient;
    public BlockingQueue<CSVLineWithMetaData> lineq;
    public boolean done = false;
    long parsingTimeSt = System.nanoTime();
    long parsingTimeEnd = System.nanoTime();
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

    @Override
    public void run() {
        VoltTable table = new VoltTable(colInfo);
        while (true) {
            CSVLineWithMetaData lineList = null;
            try {
                lineList = lineq.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                Logger.getLogger(CSVLoaderMT.class.getName()).log(Level.SEVERE, null, ex);
            }
            if (lineList == dummy) {
                System.out.println("Done Processing partition: " + partitionId);
                //Process anything that we didnt process.
                if (table.getRowCount() > 0) {
                    CSVLoaderMT.MyMTCallback cbmt = new CSVLoaderMT.MyMTCallback(table.getRowCount());
                    try {
                        csvClient.callProcedure(cbmt, "@LoadPartitionData", partitionId, tableName, table);
                        CSVFileReader.inCount.addAndGet(table.getRowCount());
                    } catch (IOException ex) {
                        Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                break;
            }
            if (lineList == null) {
                continue;
            }
            VoltTableUtil.toVoltTableFromLine(table, lineList.line, colNames, columnTypes);
            if (table.getRowCount() > batchmax) {
                try {
                    CSVLoaderMT.MyMTCallback cbmt = new CSVLoaderMT.MyMTCallback(table.getRowCount());
                    csvClient.callProcedure(cbmt, "@LoadPartitionData", partitionId, tableName, table);
                    CSVFileReader.inCount.addAndGet(table.getRowCount());
                    table = new VoltTable(colInfo);
                } catch (IOException ex) {
                    Logger.getLogger(CSVPartitionProcessor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

    }
}
