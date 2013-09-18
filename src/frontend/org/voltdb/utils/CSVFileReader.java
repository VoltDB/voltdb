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
 *
 * This is a single thread reader which feeds the lines after validating syntax
 * to correct Partition Processors. In caseof MP table or user supplied procedure
 * we use just one processor.
 *
 */
class CSVFileReader implements Runnable {

    static AtomicLong m_totalRowCount = new AtomicLong(0);
    static AtomicLong m_totalLineCount = new AtomicLong(0);
    static CSVLoader.CSVConfig m_config;
    //Columns in table we are inserting.
    static int m_columnCnt;
    static ICsvListReader m_listReader;
    static Client m_csvClient;
    //Map of partition to Queues to put CSVLineWithMetaData
    static Map<Integer, BlockingQueue<CSVLineWithMetaData>> processorQueues;
    // zero based index of partition column in table.
    static int m_partitionedColumnIndex;
    //Type detected for the partition column.
    static VoltType m_partitionColumnType;
    // This is the last thing put on Queue so that processors will detect the end.
    static CSVLineWithMetaData m_endOfData;
    //Count down latch for each processor.
    static CountDownLatch m_processor_cdl;
    static boolean m_errored = false;
    long m_parsingTime = 0;
    private static Map<VoltType, String> m_blankValues = new EnumMap<VoltType, String>(VoltType.class);
    static {
        m_blankValues.put(VoltType.NUMERIC, "0");
        m_blankValues.put(VoltType.TINYINT, "0");
        m_blankValues.put(VoltType.SMALLINT, "0");
        m_blankValues.put(VoltType.INTEGER, "0");
        m_blankValues.put(VoltType.BIGINT, "0");
        m_blankValues.put(VoltType.FLOAT, "0.0");
        m_blankValues.put(VoltType.TIMESTAMP, null);
        m_blankValues.put(VoltType.STRING, "");
        m_blankValues.put(VoltType.DECIMAL, "0");
        m_blankValues.put(VoltType.VARBINARY, "");
    }
    //Types of table we are loading data into.
    static List<VoltType> m_typeList = new ArrayList<VoltType>();
    //Errors we keep track only upto maxerrors
    static Map<Long, String[]> m_errorInfo = new TreeMap<Long, String[]>();

    @Override
    public void run() {
        List<String> lineList = null;
        while ((m_config.limitrows-- > 0)) {
            if (m_errored) {
                break;
            }
            try {
                //Initial setting of m_totalLineCount
                if (m_listReader.getLineNumber() == 0) {
                    m_totalLineCount.set(m_config.skip);
                } else {
                    m_totalLineCount.set(m_listReader.getLineNumber());
                }
                long st = System.nanoTime();
                lineList = m_listReader.read();
                long end = System.nanoTime();
                m_parsingTime += (end - st);
                if (lineList == null) {
                    if (m_totalLineCount.get() > m_listReader.getLineNumber()) {
                        m_totalLineCount.set(m_listReader.getLineNumber());
                    }
                    break;
                }
                m_totalRowCount.incrementAndGet();

                String[] correctedLine = lineList.toArray(new String[0]);

                String lineCheckResult;
                if ((lineCheckResult = checkparams_trimspace(correctedLine,
                        m_columnCnt)) != null) {
                    String[] info = {lineList.toString(), lineCheckResult};
                    if (synchronizeErrorInfo(m_totalLineCount.get() + 1, info)) {
                        m_errored = true;
                    }
                    continue;
                }

                CSVLineWithMetaData lineData = new CSVLineWithMetaData(correctedLine, lineList,
                        m_listReader.getLineNumber());
                int partitionId = 0;
                //Find partiton to send this line to and put on correct partition processor queue.
                //If Parser got error and we have reached limit this loop will exit and no more elements
                //will be pushed to queue. Queues will break out as well after seeing endOfData
                if (!CSVPartitionProcessor.m_isMP && !m_config.useSuppliedProcedure) {
                    partitionId = TheHashinator.getPartitionForParameter(m_partitionColumnType.getValue(),
                            (Object) lineData.correctedLine[m_partitionedColumnIndex]);
                }
                BlockingQueue<CSVLineWithMetaData> q = processorQueues.get(partitionId);
                q.offer(lineData);
            } catch (SuperCsvException e) {
                //Catch rows that can not be read by superCSV m_listReader.
                // E.g. items without quotes when strictquotes is enabled.
                e.printStackTrace();
                String[] info = {e.getMessage(), ""};
                if (synchronizeErrorInfo(m_totalLineCount.get() + 1, info)) {
                    break;
                }
            } catch (IOException ioex) {
                ioex.printStackTrace();
                break;
            }
        }

        //Did we hit the maxerror ceiling?
        if (m_errorInfo.size() >= m_config.maxerrors) {
            m_log.info("The number of Failure row data exceeds "
                    + m_config.maxerrors);
        }

        //Close the reader and push m_endOfData lines to indicate Partition Processor to wind down.
        try {
            m_listReader.close();
        } catch (IOException ex) {
            m_log.error("Error cloging Reader: " + ex);
        } finally {
            for (BlockingQueue<CSVLineWithMetaData> q : processorQueues.values()) {
                try {
                    q.put(m_endOfData);
                } catch (InterruptedException ex) {
                    m_log.error("Failed to add endOfData for Partition Processor. " + ex);
                }
            }
            m_log.info("Rows Queued by Reader: " + m_totalRowCount.get());
        }
        try {
            m_log.info("Waiting for partition processors to finish.");
            m_processor_cdl.await();
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
        synchronized (m_errorInfo) {
            //Dont collect more than we want to report.
            if (m_errorInfo.size() >= m_config.maxerrors) {
                return true;
            }
            if (!m_errorInfo.containsKey(errLineNum)) {
                m_errorInfo.put(errLineNum, info);
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
                if (m_config.blank.equalsIgnoreCase("error")) {
                    return "Error: blank item";
                } else if (m_config.blank.equalsIgnoreCase("empty")) {
                    slot[i] = m_blankValues.get(m_typeList.get(i));
                }
                //else m_config.blank == null which is already the case
            } // trim white space in this correctedLine. SuperCSV preserves all the whitespace by default
            else {
                if (m_config.nowhitespace
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
