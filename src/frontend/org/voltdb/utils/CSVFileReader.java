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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.ICsvListReader;
import org.voltdb.TheHashinator;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import static org.voltdb.utils.CSVLoaderMT.m_log;

class CSVFileReader implements Runnable {

    public CSVLoaderMT.CSVConfig config;
    public ICsvListReader listReader;
    Client csvClient;
    public Map<Integer, BlockingQueue<CSVLineWithMetaData>> lineq;
    public boolean done = false;
    long parsingTimeSt = System.nanoTime();
    long parsingTimeEnd = System.nanoTime();
    int columnCnt = 0;
    int partitionedColumnIndex;
    String parColumnName;
    CSVLineWithMetaData dummy = new CSVLineWithMetaData();
    int pcnt = 0;
    int batchmax = 200;
    int cur_sz = 0;
    List<Thread> spawned = null;
    public static AtomicLong inCount = new AtomicLong(0);
    public static AtomicLong outCount = new AtomicLong(0);
    public static AtomicLong totalLineCount = new AtomicLong(0);
    public static AtomicLong totalRowCount = new AtomicLong(0);
    public String tableName;

    @Override
    public void run() {
        List<String> lineList = new ArrayList<String>();
        long ecnt = 0;
        while ((config.limitrows-- > 0)) {
            try {
                //Initial setting of totalLineCount
                if (listReader.getLineNumber() == 0) {
                    totalLineCount.set(config.skip);
                } else {
                    totalLineCount.set(listReader.getLineNumber());
                }

                long st = System.nanoTime();
                lineList = listReader.read();
                long end = System.nanoTime();
                parsingTimeEnd += (end - st);
                if (lineList == null) {
                    if (totalLineCount.get() > listReader.getLineNumber()) {
                        totalLineCount.set(listReader.getLineNumber());
                    }
                    break;
                }

                String lineCheckResult;
                String[] correctedLine = lineList.toArray(new String[0]);

                if ((lineCheckResult = CSVLoaderMT.checkparams_trimspace(correctedLine,
                        columnCnt)) != null) {
                    String[] info = {lineList.toString(), lineCheckResult};
                    CSVLoaderMT.synchronizeErrorInfoForFuture(totalLineCount.get() + 1, info);
                    if (++ecnt > config.maxerrors) {
                        break;
                    }
                    totalRowCount.getAndIncrement();
                    continue;
                }

                CSVLineWithMetaData lineData = new CSVLineWithMetaData();
                lineData.parColumnName = parColumnName;
                lineData.line = correctedLine;
                int partitionId = 0;
                if (!config.check) {
                    if (!CSVPartitionProcessor.isMP) {
                        partitionId = TheHashinator.getPartitionForParameter(VoltType.INTEGER.getValue(), (Object) lineData.line[partitionedColumnIndex - 1]);
                    }
                    if (lineq.get(partitionId) == null) {
                        ArrayBlockingQueue<CSVLineWithMetaData> q = new ArrayBlockingQueue<CSVLineWithMetaData>(2000);
                        q.put(lineData);
                        lineq.put(partitionId, q);
                        CSVPartitionProcessor pp = new CSVPartitionProcessor();
                        pp.csvClient = csvClient;
                        pp.partitionId = partitionId;
                        pp.tableName = tableName;
                        pp.columnCnt = columnCnt;
                        pp.lineq = q;
                        pp.rdr = this;
                        pp.dummy = dummy;
                        Thread th = new Thread(pp);
                        th.setName("PartitionProcessor-" + partitionId);
                        th.setDaemon(true);
                        th.start();
                        spawned.add(th);
                    } else {
                        BlockingQueue<CSVLineWithMetaData> q = lineq.get(partitionId);
                        q.put(lineData);
                    }
                }
                totalRowCount.getAndIncrement();
            } catch (SuperCsvException e) {
                //Catch rows that can not be read by superCSV listReader. E.g. items without quotes when strictquotes is enabled.
                e.printStackTrace();
                totalRowCount.getAndIncrement();
                String[] info = {e.getMessage(), ""};
                try {
                    CSVLoaderMT.synchronizeErrorInfo(totalLineCount.get() + 1, info);
                } catch (IOException ex) {
                } catch (InterruptedException ex) {
                }
                break;
            } catch (Throwable ex) {
                //Catch rows that can not be read by superCSV listReader. E.g. items without quotes when strictquotes is enabled.
                ex.printStackTrace();
                totalRowCount.getAndIncrement();
                String[] info = {ex.getMessage(), ""};
                try {
                    CSVLoaderMT.synchronizeErrorInfo(totalLineCount.get() + 1, info);
                } catch (IOException ex1) {
                } catch (InterruptedException ex2) {
                }
                break;
            }
        }
        try {
            listReader.close();
        } catch (IOException ex) {
            m_log.error("Error cloging Reader: " + ex);
        }
        done = true;
        for (BlockingQueue<CSVLineWithMetaData> q : lineq.values()) {
            try {
                q.put(dummy);
            } catch (InterruptedException ex) {
                Logger.getLogger(CSVLoaderMT.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        m_log.info("Rows Queued by Reader: " + totalRowCount.get());
    }
}
