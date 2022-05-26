/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.ICsvListReader;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;

import com.google_voltpatches.common.collect.BiMap;
import com.google_voltpatches.common.collect.HashBiMap;

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
    private static final String HEADER_COUNT_ERROR =
            "Incorrect number of columns. %d found, %d expected. Please check the csv file header " +
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
    static ICsvListReader m_listReader = null;
    long m_parsingTime = 0;
    private static final Map<VoltType, String> m_blankStrings = new EnumMap<VoltType, String>(VoltType.class);
    private static final VoltLogger m_log = new VoltLogger("CSVLOADER");
    private final CSVDataLoader m_loader;
    private final BulkLoaderErrorHandler m_errHandler;
    private final VoltType[] m_columnTypes;
    private final int m_columnCount;
    private int headerlen;
    private Integer[] order;

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

    public static void initializeReader(CSVLoader.CSVConfig config, ICsvListReader reader) {
        m_config = config;
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
        //if header option is true, check whether csv first line is valid
        if (m_config.header) {
            if (!checkHeader()) {
                m_log.error("In the CSV file " + m_config.file + ", the header "+ m_listReader.getUntokenizedRow() +" does not match "
                        + "an existing column in the table " + m_config.table + ".");
                System.exit(-1);
            }
        }

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
                String[] reorderValues = new String[m_columnCount];
                if ((lineCheckResult = checkparams_trimspace_reorder(lineValues, reorderValues)) != null) {
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
                m_loader.insertRow(lineData, reorderValues);
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

    private boolean checkHeader() {
        try {
            String[] firstline = m_listReader.getHeader(false);
            Set<String> firstset = new HashSet<String>();
            BiMap<Integer, String> colNames = HashBiMap.create(m_loader.getColumnNames());
            headerlen = firstline.length;
            // remove duplicate.
            for (String name : firstline) {
                if (name != null) {
                    firstset.add(name.toUpperCase());
                } else {
                    return false;
                }
            }
            // whether column num matches.
            if (headerlen < m_columnCount) {
                return false;
            } else {
                // whether column name has according table column.
                int matchColCount = 0;
                for (String name : firstset) {
                    if (colNames.containsValue(name.trim())) {
                        matchColCount++;
                    }
                }
                if (matchColCount != m_columnCount) {
                    return false;
                }
            }
            // get the mapping from file column num to table column num.
            order = new Integer[headerlen];
            for (int fileCol = 0; fileCol < headerlen; fileCol++) {
                String name = firstline[fileCol];
                Integer tableCol = colNames.inverse().get(name.trim().toUpperCase());
                order[fileCol] = tableCol;
            }
        } catch (IOException ex) {
            m_log.error("Failed to read CSV line from file: " + ex);
        }
        return true;
    }

    private String checkparams_trimspace_reorder(String[] lineValues, String[] reorderValues) {
        if (lineValues.length != m_columnCount && !m_config.header) {
            return String.format(COLUMN_COUNT_ERROR, lineValues.length, m_columnCount);
        }

        if (lineValues.length != headerlen && m_config.header) {
            return String.format(HEADER_COUNT_ERROR, lineValues.length, headerlen);
        }

        for (int fileCol = 0; fileCol<lineValues.length; fileCol++) {
            int i = fileCol;
            if (m_config.header) {
                if (order[fileCol] != null) {
                    i = order[fileCol];
                } else {
                    continue;
                }
            }
            reorderValues[i] = lineValues[fileCol];
            //supercsv read "" to null
            if (reorderValues[i] == null) {
                if (m_config.blank.equalsIgnoreCase("error")) {
                    return String.format(BLANK_ERROR, i + 1);
                } else if (m_config.blank.equalsIgnoreCase("empty")) {
                    reorderValues[i] = m_blankStrings.get(m_columnTypes[i]);
                }
                //else m_config.blank == null which is already the case
            } // trim white space in this correctedLine. SuperCSV preserves all the whitespace by default
            else {
                if (m_config.nowhitespace
                        && (reorderValues[i].charAt(0) == ' ' || reorderValues[i].charAt(reorderValues[i].length() - 1) == ' ')) {
                    return String.format(WHITESPACE_ERROR, i + 1);
                } else {
                    reorderValues[i] = reorderValues[i].trim();
                }

                if(!m_config.customNullString.isEmpty()){
                    if(lineValues[i].equals(m_config.customNullString)){
                        reorderValues[i] = null;
                    }
                }
                // treat NULL, \N and "\N" as actual null value
                else if (reorderValues[i].equals("NULL")
                        || reorderValues[i].equals(Constants.CSV_NULL)
                        || reorderValues[i].equals(Constants.QUOTED_CSV_NULL)) {
                    reorderValues[i] = null;
                }
            }
        }
        return null;
    }
}
