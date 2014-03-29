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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.ICsvListReader;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.common.Constants;

/**
 *
 * This is a single thread reader which feeds the lines after validating syntax
 * to correct Partition Processors. In case of MP table or user supplied procedure
 * we use just one processor.
 *
 */
class CSVFileReader implements Runnable {

    static AtomicLong m_totalRowCount = new AtomicLong(0);
    static AtomicLong m_totalLineCount = new AtomicLong(0);
    static CSVLoader.CSVConfig m_config;
    static Client m_csvClient;
    static ICsvListReader m_listReader;
    static boolean m_errored = false;
    long m_parsingTime = 0;
    private static final Map<VoltType, String> m_blankValues = new EnumMap<VoltType, String>(VoltType.class);
    private static final VoltLogger m_log = new VoltLogger("CSVLOADER");
    private static String m_insertProcedure = "";
    static int m_columnCnt;
    //Types of columns
    static List<VoltType> m_typeList = new ArrayList<VoltType>();
    final AtomicLong m_processedCount = new AtomicLong(0);
    final AtomicLong m_acknowledgedCount = new AtomicLong(0);
    final int m_reportEveryNRows = 10000;

    static {
        m_blankValues.put(VoltType.TINYINT, "0");
        m_blankValues.put(VoltType.SMALLINT, "0");
        m_blankValues.put(VoltType.INTEGER, "0");
        m_blankValues.put(VoltType.BIGINT, "0");
        m_blankValues.put(VoltType.FLOAT, "0.0");
        m_blankValues.put(VoltType.TIMESTAMP, null);
        m_blankValues.put(VoltType.STRING, "");
        m_blankValues.put(VoltType.DECIMAL, "0.0");
        m_blankValues.put(VoltType.VARBINARY, "");
    }
    //Errors we keep track only upto maxerrors
    final static Map<Long, String[]> m_errorInfo = new TreeMap<Long, String[]>();

    public static boolean initializeReader(CSVLoader.CSVConfig config, Client csvClient, ICsvListReader reader)
            throws IOException, ProcCallException, InterruptedException {
        VoltTable procInfo;
        m_config = config;
        m_csvClient = csvClient;
        m_listReader = reader;

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
        return true;
    }

    //Callback for single row procedure invoke called for rows in failed batch.
    public class PartitionSingleExecuteProcedureCallback implements ProcedureCallback {
        final CSVLineWithMetaData m_csvLine;

        public PartitionSingleExecuteProcedureCallback(CSVLineWithMetaData csvLine) {
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
                if (synchronizeErrorInfo(m_csvLine.lineNumber, info)) {
                    m_errored = true;
                    return;
                }
                m_log.error(response.getStatusString());
                return;
            }
            long currentCount = m_acknowledgedCount.incrementAndGet();

            if (currentCount % m_reportEveryNRows == 0) {
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }

    @Override
    public void run() {
        List<String> lineList;
        ClientImpl clientImpl = (ClientImpl) m_csvClient;
        int sleptTimes = 0;
        while (!clientImpl.isHashinatorInitialized() && sleptTimes < 120) {
            try {
                Thread.sleep(500);
                sleptTimes++;
            } catch (InterruptedException ex) {
                ;
            }
        }
        m_log.debug("Client Initialization Done.");

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
                int currLineNumber = m_listReader.getLineNumber();

                String[] correctedLine = lineList.toArray(new String[lineList.size()]);

                String lineCheckResult;
                if ((lineCheckResult = checkparams_trimspace(correctedLine, m_columnCnt)) != null) {
                    String[] info = {lineList.toString(), lineCheckResult};
                    if (synchronizeErrorInfo(m_totalLineCount.get() + 1, info)) {
                        m_errored = true;
                        break;
                    }
                    continue;
                }

                CSVLineWithMetaData lineData = new CSVLineWithMetaData(correctedLine, lineList,
                        currLineNumber);

                try {
                    PartitionSingleExecuteProcedureCallback cbmt =
                            new PartitionSingleExecuteProcedureCallback(lineData);
                    if (m_csvClient.callProcedure(cbmt, m_insertProcedure, (Object[]) correctedLine)) {
                        m_processedCount.incrementAndGet();
                    } else {
                        m_log.fatal("Failed to send CSV insert to VoltDB cluster.");
                        System.exit(1);
                    }
                } catch (IOException ex) {
                    String[] info = {lineList.toString(), ex.toString()};
                    m_errored = synchronizeErrorInfo(currLineNumber, info);
                    return;
                }
            } catch (SuperCsvException e) {
                //Catch rows that can not be read by superCSV m_listReader.
                // e.g. items without quotes when strictquotes is enabled.
                m_log.error("Failed to process CSV line: " + e);
                String[] info = {e.getMessage(), ""};
                if (synchronizeErrorInfo(m_totalLineCount.get() + 1, info)) {
                    break;
                }
            } catch (IOException ex) {
                m_log.error("Failed to read CSV line from file: " + ex);
                break;
            }
        }

        //Did we hit the maxerror ceiling?
        if (m_errorInfo.size() >= m_config.maxerrors) {
            m_log.warn("The number of failed rows exceeds the configured maximum failed rows: "
                    + m_config.maxerrors);
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
                    slot[i] = slot[i].trim();
                }
                // treat NULL, \N and "\N" as actual null value
                if (slot[i].equals("NULL")
                        || slot[i].equals(Constants.CSV_NULL)
                        || slot[i].equals(Constants.QUOTED_CSV_NULL)) {
                    slot[i] = null;
                }
            }
        }
        return null;
    }
}
