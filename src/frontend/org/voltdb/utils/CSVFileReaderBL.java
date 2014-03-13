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
import java.util.EnumMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.ICsvListReader;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;
import org.voltdb.common.Constants;

/**
 *
 * This is a single thread reader which feeds the lines after validating syntax
 * to VoltBulkLoader.
 *
 */
class CSVFileReaderBL implements Runnable {

    static AtomicLong m_totalRowCount = new AtomicLong(0);
    static AtomicLong m_totalLineCount = new AtomicLong(0);
    static CSVLoader.CSVConfig m_config = null;
    static Client m_csvClient = null;
    static ICsvListReader m_listReader = null;
    static boolean m_errored = false;
    long m_parsingTime = 0;
    private static final Map<VoltType, String> m_blankStrings = new EnumMap<VoltType, String>(VoltType.class);
    private static final Map<VoltType, Object> m_blankValues = new EnumMap<VoltType, Object>(VoltType.class);
    private static final VoltLogger m_log = new VoltLogger("CSVLOADER");
    VoltBulkLoader bulkLoader = null;
    VoltType[] m_columnTypes;
    int m_columnCount;
    final AtomicLong m_failedInsertCount = new AtomicLong(0);

    static {
        m_blankStrings.put(VoltType.TINYINT, "0");
        m_blankStrings.put(VoltType.SMALLINT, "0");
        m_blankStrings.put(VoltType.INTEGER, "0");
        m_blankStrings.put(VoltType.BIGINT, "0");
        m_blankStrings.put(VoltType.FLOAT, "0.0");
        m_blankStrings.put(VoltType.TIMESTAMP, null);
        m_blankStrings.put(VoltType.STRING, "");
        m_blankStrings.put(VoltType.DECIMAL, "0.0");
        m_blankStrings.put(VoltType.VARBINARY, "");

        m_blankValues.put(VoltType.TINYINT, VoltType.TINYINT.getNullValue());
        m_blankValues.put(VoltType.SMALLINT, VoltType.SMALLINT.getNullValue());
        m_blankValues.put(VoltType.INTEGER, VoltType.INTEGER.getNullValue());
        m_blankValues.put(VoltType.BIGINT, VoltType.BIGINT.getNullValue());
        m_blankValues.put(VoltType.FLOAT, VoltType.FLOAT.getNullValue());
        m_blankValues.put(VoltType.TIMESTAMP, VoltType.TIMESTAMP.getNullValue());
        m_blankValues.put(VoltType.STRING, VoltType.STRING.getNullValue());
        m_blankValues.put(VoltType.DECIMAL, VoltType.DECIMAL.getNullValue());
        m_blankValues.put(VoltType.VARBINARY, VoltType.VARBINARY.getNullValue());
    }

    //Errors we keep track only upto maxerrors
    final static Map<Long, String[]> m_errorInfo = new TreeMap<Long, String[]>();

    public static void initializeReader(CSVLoader.CSVConfig config, Client csvClient, ICsvListReader reader) {
        m_config = config;
        m_csvClient = csvClient;
        m_listReader = reader;
    }

    public class csvFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            m_failedInsertCount.incrementAndGet();
            CSVLineWithMetaData lineData = (CSVLineWithMetaData) rowHandle;
            String[] info = {lineData.rawLine.toString(), response.getStatusString()};
            synchronizeErrorInfo(lineData.lineNumber, info);
        }
    }

    @Override
    public void run() {
        List<String> lineList;
        ClientImpl clientImpl = (ClientImpl) m_csvClient;

        csvFailureCallback errorCB = new csvFailureCallback();
        try {
            bulkLoader = clientImpl.getNewBulkLoader(m_config.table, m_config.batch, errorCB);
        }
        catch (Exception e) {
            m_log.info("CSV Loader failed: " + e.getMessage());
            return;
        }
        m_log.debug("Client Initialization Done.");

        m_columnTypes = bulkLoader.getColumnTypes();
        m_columnCount = m_columnTypes.length;

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

                if (lineList.size() == 0) {
                    continue;
                }
                Object row_args[] = new Object[lineList.size()];
                String lineCheckResult;
                if ((lineCheckResult = checkparams_trimspace(lineList, row_args)) != null) {
                    String[] info = {lineList.toString(), lineCheckResult};
                    if (synchronizeErrorInfo(m_totalLineCount.get() + 1, info)) {
                        m_errored = true;
                        break;
                    }
                    continue;
                }

                CSVLineWithMetaData lineData = new CSVLineWithMetaData(null, lineList,
                        m_listReader.getLineNumber());
                bulkLoader.insertRow(lineData, row_args);
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
            } catch (InterruptedException e) {
                m_log.error("CSVLoader interrupted: " + e);
                break;
            }
        }

        //Did we hit the maxerror ceiling?
        if (m_errorInfo.size() >= m_config.maxerrors) {
            m_log.warn("The number of failed rows exceeds the configured maximum failed rows: "
                    + m_config.maxerrors);
        }

        //Now wait for processors to see endOfData and count down. After that drain to finish all callbacks
        try {
            m_log.debug("Waiting for VoltBulkLoader to finish.");
            bulkLoader.close();
            m_log.debug("VoltBulkLoader Done.");
        } catch (InterruptedException ex) {
            m_log.warn("Stopped processing because of connection error. "
                    + "A report will be generated with what we processed so far. Error: " + ex);
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

    private String checkparams_trimspace(List<String> lineList, Object row_args[]) {
        if (lineList.size() != m_columnCount) {
            return "Error: Incorrect number of columns. " + lineList.size()
                    + " found, " + m_columnCount + " expected.";
        }
        ListIterator<String> it = lineList.listIterator();
        for (int i = 0; i<lineList.size(); i++) {
            String field = it.next();
            //supercsv read "" to null
            if (field == null) {
                if (m_config.blank.equalsIgnoreCase("error")) {
                    return "Error: blank item";
                } else if (m_config.blank.equalsIgnoreCase("empty")) {
                    field = m_blankStrings.get(m_columnTypes[i]);
                    row_args[i] = m_blankStrings.get(m_columnTypes[i]);
                }
                //else m_config.blank == null which is already the case
            } // trim white space in this correctedLine. SuperCSV preserves all the whitespace by default
            else {
                if (m_config.nowhitespace
                        && (field.charAt(0) == ' ' || field.charAt(field.length() - 1) == ' ')) {
                    return "Error: White Space Detected in nowhitespace mode.";
                } else {
                    field = field.trim();
                }
                // treat NULL, \N and "\N" as actual null value
                if (field.equals("NULL")
                        || field.equals(Constants.CSV_NULL)
                        || field.equals(Constants.QUOTED_CSV_NULL)) {
                    field = null;
                    row_args[i] = m_blankValues.get(m_columnTypes[i]);
                }
                else {
                    try {
                        row_args[i] = field;
                    }
                    catch (VoltTypeException e) {
                        return e.getMessage();
                    }
                }
            }
        }
        return null;
    }
}
