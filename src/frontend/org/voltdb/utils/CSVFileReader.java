/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.ICsvListReader;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.common.Constants;

/**
 *
 * This is a single thread reader which feeds the lines after validating syntax
 * to CSVDataLoader.
 *
 */
class CSVFileReader implements Runnable {
    private static final String COLUMN_COUNT_ERROR =
            "Incorrect number of columns. %d found, %d expected. Please check the table schema " +
            "and the line content";
    private static final String BLANK_ERROR =
            "A blank value is detected in column %d while \"--blank error\" is used. " +
            "To proceed, either fill in the blank column or use \"--blank {null|empty}\".";
    private static final String WHITESPACE_ERROR =
            "Whitespace detected in column %d while --nowhitespace is used. " +
            "To proceed, either remove the whitespaces from the column or remove --nowhitespace.";

    static AtomicLong m_totalRowCount = new AtomicLong(0);
    static AtomicLong m_totalLineCount = new AtomicLong(0);
    static CSVLoader.CSVConfig m_config = null;
    static Client m_csvClient = null;
    static ICsvListReader m_listReader = null;
    long m_parsingTime = 0;
    private static final Map<VoltType, String> m_blankStrings = new EnumMap<VoltType, String>(VoltType.class);
    private static final VoltLogger m_log = new VoltLogger("CSVLOADER");
    private final CSVDataLoader m_loader;
    private final BulkLoaderErrorHandler m_errHandler;
    private final VoltType[] m_columnTypes;
    private final int m_columnCount;

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
    }

    public static void initializeReader(CSVLoader.CSVConfig config, Client csvClient, ICsvListReader reader) {
        m_config = config;
        m_csvClient = csvClient;
        m_listReader = reader;
    }

    public CSVFileReader(CSVDataLoader loader, BulkLoaderErrorHandler errorHandler)    {
        m_loader = loader;
        m_errHandler = errorHandler;
        m_columnTypes = m_loader.getColumnTypes();
        m_columnCount = m_columnTypes.length;
    }

    @Override
    public void run() {
        List<String> lineList;

        while ((m_config.limitrows-- > 0)) {
            if (m_errHandler.hasReachedErrorLimit()) {
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

                if (lineList.isEmpty()) {
                    continue;
                }

                String[] lineValues = lineList.toArray(new String[0]);
                String lineCheckResult;
                if ((lineCheckResult = checkparams_trimspace(lineValues)) != null) {
                    final RowWithMetaData metaData
                            = new RowWithMetaData(m_listReader.getUntokenizedRow(),
                                    m_totalLineCount.get() + 1);
                    if (m_errHandler.handleError(metaData, null, lineCheckResult)) {
                        break;
                    }
                    continue;
                }

                RowWithMetaData lineData
                        = new RowWithMetaData(m_listReader.getUntokenizedRow(),
                                m_listReader.getLineNumber());
                m_loader.insertRow(lineData, lineValues);
            } catch (SuperCsvException e) {
                //Catch rows that can not be read by superCSV m_listReader.
                // e.g. items without quotes when strictquotes is enabled.
                final RowWithMetaData metaData
                        = new RowWithMetaData(m_listReader.getUntokenizedRow(),
                                m_totalLineCount.get() + 1);
                if (m_errHandler.handleError(metaData, null, e.getMessage())) {
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

        //Now wait for processors to see endOfData and count down. After that drain to finish all callbacks
        try {
            m_log.debug("Waiting for CSVDataLoader to finish.");
            m_loader.close();
            m_log.debug("CSVDataLoader Done.");
        } catch (Exception ex) {
            m_log.warn("Stopped processing because of connection error. "
                    + "A report will be generated with what we processed so far. Error: " + ex);
        }
    }

    private String checkparams_trimspace(String[] lineValues) {
        if (lineValues.length != m_columnCount) {
            return String.format(COLUMN_COUNT_ERROR, lineValues.length, m_columnCount);
        }

        for (int i = 0; i<lineValues.length; i++) {
            //supercsv read "" to null
            if (lineValues[i] == null) {
                if (m_config.blank.equalsIgnoreCase("error")) {
                    return String.format(BLANK_ERROR, i + 1);
                } else if (m_config.blank.equalsIgnoreCase("empty")) {
                    lineValues[i] = m_blankStrings.get(m_columnTypes[i]);
                }
                //else m_config.blank == null which is already the case
            } // trim white space in this correctedLine. SuperCSV preserves all the whitespace by default
            else {
                if (m_config.nowhitespace
                        && (lineValues[i].charAt(0) == ' ' || lineValues[i].charAt(lineValues[i].length() - 1) == ' ')) {
                    return String.format(WHITESPACE_ERROR, i + 1);
                } else {
                    lineValues[i] = lineValues[i].trim();
                }

                if(!m_config.customNullString.isEmpty()){
                    if(lineValues[i].equals(m_config.customNullString)){
                        lineValues[i] = null;
                    }
                }
                // treat NULL, \N and "\N" as actual null value
                else if (lineValues[i].equals("NULL")
                        || lineValues[i].equals(Constants.CSV_NULL)
                        || lineValues[i].equals(Constants.QUOTED_CSV_NULL)) {
                    lineValues[i] = null;
                }
            }
        }
        return null;
    }
}
