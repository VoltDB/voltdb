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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.ICsvListReader;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import static org.voltdb.utils.CSVLoader.m_log;

/**
 * This is a single thread reader which feeds the lines after validating syntax to correct Partition Processors.
 *
 */
class CSVFileReader implements Runnable {

    static AtomicLong totalRowCount = new AtomicLong(0);
    static AtomicLong totalLineCount = new AtomicLong(0);
    static CSVLoader.CSVConfig config;
    static int columnCnt;
    static ICsvListReader listReader;
    static Client csvClient;
    static Map<Integer, BlockingQueue<CSVLineWithMetaData>> processorQueues;
    static int partitionedColumnIndex;
    static VoltType partitionColumnType;
    static CSVLineWithMetaData endOfData;
    static int batchmax = 200;
    static String tableName;
    static CountDownLatch processor_cdl;
    static boolean errored = false;
    long m_parsingTimeSt = System.nanoTime();
    long m_parsingTimeEnd = System.nanoTime();
    private static Map<VoltType, String> blankValues = new EnumMap<VoltType, String>(VoltType.class);
    static {
        blankValues.put(VoltType.NUMERIC, "0");
        blankValues.put(VoltType.TINYINT, "0");
        blankValues.put(VoltType.SMALLINT, "0");
        blankValues.put(VoltType.INTEGER, "0");
        blankValues.put(VoltType.BIGINT, "0");
        blankValues.put(VoltType.FLOAT, "0.0");
        blankValues.put(VoltType.TIMESTAMP, "0");
        blankValues.put(VoltType.STRING, "");
        blankValues.put(VoltType.DECIMAL, "0");
        blankValues.put(VoltType.VARBINARY, "");
    }
    static List<VoltType> typeList = new ArrayList<VoltType>();
    static Map<Long, String[]> errorInfo = new TreeMap<Long, String[]>();

    @Override
    public void run() {
        while ((config.limitrows-- > 0)) {
            if (errored) {
                break;
            }
            try {
                //Initial setting of totalLineCount
                if (listReader.getLineNumber() == 0) {
                    totalLineCount.set(config.skip);
                } else {
                    totalLineCount.set(listReader.getLineNumber());
                }
                long st = System.nanoTime();
                List<String> lineList = listReader.read();
                long end = System.nanoTime();
                m_parsingTimeEnd += (end - st);
                if (lineList == null) {
                    if (totalLineCount.get() > listReader.getLineNumber()) {
                        totalLineCount.set(listReader.getLineNumber());
                    }
                    break;
                }
                totalRowCount.incrementAndGet();

                String lineCheckResult;
                String[] correctedLine = lineList.toArray(new String[0]);

                if ((lineCheckResult = checkparams_trimspace(correctedLine,
                        columnCnt)) != null) {
                    String[] info = {lineList.toString(), lineCheckResult};
                    if (synchronizeErrorInfo(totalLineCount.get() + 1, info)) {
                        errored = true;
                    }
                    continue;
                }

                CSVLineWithMetaData lineData = new CSVLineWithMetaData(correctedLine, lineList, listReader.getLineNumber());
                int partitionId = 0;
                //Find partiton to send this line to and put on correct partition processor queue.
                if (!CSVPartitionProcessor.isMP) {
                    partitionId = TheHashinator.getPartitionForParameter(partitionColumnType.getValue(),
                            (Object) lineData.correctedLine[partitionedColumnIndex]);
                }
                BlockingQueue<CSVLineWithMetaData> q = processorQueues.get(partitionId);
                q.put(lineData);
            } catch (SuperCsvException e) {
                //Catch rows that can not be read by superCSV listReader. E.g. items without quotes when strictquotes is enabled.
                e.printStackTrace();
                String[] info = {e.getMessage(), ""};
                if (synchronizeErrorInfo(totalLineCount.get() + 1, info)) {
                    break;
                }
            } catch (IOException ioex) {
                ioex.printStackTrace();
                break;
            } catch (InterruptedException ex) {
                m_log.error("Failed to add lines for partition processor queues: " + ex);
            }
        }

        //Did we hit the maxerror ceiling?
        if (errorInfo.size() >= config.maxerrors) {
            m_log.info("The number of Failure row data exceeds "
                    + config.maxerrors);
        }

        //Close the reader and push endOfData lines to indicate Partition Processor to wind down.
        try {
            listReader.close();
        } catch (IOException ex) {
            m_log.error("Error cloging Reader: " + ex);
        } finally {
            for (BlockingQueue<CSVLineWithMetaData> q : processorQueues.values()) {
                if (errored) {
                    q.clear();
                }
                try {
                    q.put(endOfData);
                } catch (InterruptedException ex) {
                    m_log.error("Failed to add endOfData for Partition Processor. " + ex);
                }
            }
            m_log.info("Rows Queued by Reader: " + totalRowCount.get());
        }
        try {
            m_log.info("Waiting for partition processors to finish.");
            processor_cdl.await();
            m_log.info("Partition Processors Done.");
        } catch (InterruptedException ex) {
            ;
        }
    }

    /**
     * Add errors to be reported.
     *
     * @param errLineNum
     * @param info
     * @return true if we have reached limit....false to continue processing and reporting.
     */
    public static boolean synchronizeErrorInfo(long errLineNum, String[] info) {
        synchronized (errorInfo) {
            //Dont collect more than we want to report.
            if (errorInfo.size() >= config.maxerrors) {
                return true;
            }
            if (!errorInfo.containsKey(errLineNum)) {
                errorInfo.put(errLineNum, info);
            }
            return false;
        }
    }

    private String checkparams_trimspace(String[] slot, int columnCnt) {
        if (slot.length != columnCnt) {
            return "Error: Incorrect number of columns. " + slot.length
                    + " found, " + columnCnt + " expected.";
        }
        for (int i = 0; i < slot.length; i++) {
            //supercsv read "" to null
            if (slot[i] == null) {
                if (config.blank.equalsIgnoreCase("error")) {
                    return "Error: blank item";
                } else if (config.blank.equalsIgnoreCase("empty")) {
                    slot[i] = blankValues.get(typeList.get(i));
                }
                //else config.blank == null which is already the case
            } // trim white space in this correctedLine. SuperCSV preserves all the whitespace by default
            else {
                if (config.nowhitespace
                        && (slot[i].charAt(0) == ' ' || slot[i].charAt(slot[i].length() - 1) == ' ')) {
                    return "Error: White Space Detected in nowhitespace mode.";
                } else {
                    slot[i] = ((String) slot[i]).trim();
                }
                // treat NULL, \N and "\N" as actual null value
                if (slot[i].equals("NULL")
                        || slot[i].equals(VoltTable.CSV_NULL)
                        || slot[i].equals(VoltTable.QUOTED_CSV_NULL)) {
                    slot[i] = null;
                }
            }
        }
        return null;
    }
}
