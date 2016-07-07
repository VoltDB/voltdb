/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.CLIConfig.Option;
import org.voltdb.common.Constants;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;
import au.com.bytecode.opencsv_voltpatches.CSVWriter;


/*
 * Utility methods for work with VoltTables.
 */
public class VoltTableUtil {

    /*
     * Ugly hack to allow SnapshotConverter which
     * shares this code with the server to specify it's own time zone.
     * You wouldn't want to convert to anything other then GMT if you want to get the data back into
     * Volt using the CSV loader because that relies on the server to coerce the date string
     * and the server only supports GMT.
     */
    public static TimeZone tz = VoltDB.VOLT_TIMEZONE;

    // VoltTable status code to indicate null dependency table. Joining SPI replies to fragment
    // task messages with this.
    public static byte NULL_DEPENDENCY_STATUS = -1;
    
    private static VoltTableUtilConfig config = null;
    private static VoltTable m_vt;

    private static final ThreadLocal<SimpleDateFormat> m_sdf = new ThreadLocal<SimpleDateFormat>() {
        @Override
        public SimpleDateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat(
                    Constants.ODBC_DATE_FORMAT_STRING);
            sdf.setTimeZone(tz);
            return sdf;
        }
    };

    public static void toCSVWriter(CSVWriter csv, VoltTable vt, List<VoltType> columnTypes) throws IOException {
        final SimpleDateFormat sdf = m_sdf.get();
        String[] fields = new String[vt.getColumnCount()];
        while (vt.advanceRow()) {
            for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                final VoltType type = columnTypes.get(ii);
                if (type == VoltType.BIGINT
                        || type == VoltType.INTEGER
                        || type == VoltType.SMALLINT
                        || type == VoltType.TINYINT) {
                    final long value = vt.getLong(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = Long.toString(value);
                    }
                } else if (type == VoltType.FLOAT) {
                    final double value = vt.getDouble(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = Double.toString(value);
                    }
                } else if (type == VoltType.DECIMAL) {
                    final BigDecimal bd = vt.getDecimalAsBigDecimal(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = bd.toString();
                    }
                } else if (type == VoltType.STRING) {
                    final String str = vt.getString(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = str;
                    }
                } else if (type == VoltType.TIMESTAMP) {
                    final TimestampType timestamp = vt.getTimestampAsTimestamp(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    } else {
                        fields[ii] = sdf.format(timestamp.asApproximateJavaDate());
                        fields[ii] += String.format("%03d", timestamp.getUSec());
                    }
                } else if (type == VoltType.VARBINARY) {
                   byte bytes[] = vt.getVarbinary(ii);
                   if (vt.wasNull()) {
                       fields[ii] = Constants.CSV_NULL;
                   } else {
                       fields[ii] = Encoder.hexEncode(bytes);
                   }
                }
                else if (type == VoltType.GEOGRAPHY_POINT) {
                    final GeographyPointValue pt = vt.getGeographyPointValue(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    }
                    else {
                        fields[ii] = pt.toString();
                    }
                }
                else if (type == VoltType.GEOGRAPHY) {
                    final GeographyValue gv = vt.getGeographyValue(ii);
                    if (vt.wasNull()) {
                        fields[ii] = Constants.CSV_NULL;
                    }
                    else {
                        fields[ii] = gv.toString();
                    }
                }
            }
            csv.writeNext(fields);
        }
        csv.flush();
    }

    public static Pair<Integer,byte[]>  toCSV(
            VoltTable vt,
            char delimiter,
            char fullDelimiters[],
            int lastNumCharacters) throws IOException {
        ArrayList<VoltType> types = new ArrayList<VoltType>(vt.getColumnCount());
        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
            types.add(vt.getColumnType(ii));
        }
        return toCSV(vt, types, delimiter, fullDelimiters, lastNumCharacters);
    }

    /*
     * Returns the number of characters generated and the csv data
     * in UTF-8 encoding.
     */
    public static Pair<Integer,byte[]> toCSV(
            VoltTable vt,
            ArrayList<VoltType> columns,
            char delimiter,
            char fullDelimiters[],
            int lastNumCharacters) throws IOException {
        StringWriter sw = new StringWriter((int)(lastNumCharacters * 1.2));
        CSVWriter writer;
        if (fullDelimiters != null) {
            writer = new CSVWriter(sw,
                    fullDelimiters[0], fullDelimiters[1], fullDelimiters[2], String.valueOf(fullDelimiters[3]));
        }
        else if (delimiter == ',')
            // CSV
            writer = new CSVWriter(sw, delimiter);
        else {
            // TSV
            writer = CSVWriter.getStrictTSVWriter(sw);
        }
        toCSVWriter(writer, vt, columns);
        String csvString = sw.toString();
        return Pair.of(csvString.length(), csvString.getBytes(com.google_voltpatches.common.base.Charsets.UTF_8));
    }

    /**
     * Utility to aggregate a list of tables sharing a schema. Common for
     * sysprocs to do this, to aggregate results.
     */
    public static VoltTable unionTables(Collection<VoltTable> operands) {
        VoltTable result = null;

        // Locate the first non-null table to get the schema
        for (VoltTable vt : operands) {
            if (vt != null) {
                VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
                for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                    columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii),
                            vt.getColumnType(ii));
                }
                result = new VoltTable(columns);
                result.setStatusCode(vt.getStatusCode());
                break;
            }
        }

        if (result != null) {
            for (VoltTable vt : operands) {
                if (vt != null) {
                    vt.resetRowPosition();
                    while (vt.advanceRow()) {
                        result.add(vt);
                    }
                }
            }

            result.resetRowPosition();
        }

        return result;
    }

    /**
     * Extract a table's schema.
     * @param vt  input table with source schema
     * @return  schema as column info array
     */
    public static VoltTable.ColumnInfo[] extractTableSchema(VoltTable vt)
    {
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
            columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii),
                    vt.getColumnType(ii));
        }
        return columns;
    }
    
    private static SQLCommandOutputFormatter m_outputFormatter = new SQLCommandOutputFormatterDefault();
    
    public static void printVoltTableFromFile(String fileName) throws IOException {
    	VoltTable vt = getTableFromFile(fileName);
    	m_outputFormatter.printTable(System.out, vt, true);
    }
    
    /**
     * Load a volt table from a file
     * @param fileTable table containing a the name of a volt table file
     * @return a volt table 
     */
    public static VoltTable getTableFromFileTable(VoltTable fileTable) {
        String fileName = fileTable.fetchRow(0).getString("filenames");
        return getTableFromFile(fileName);
    }

    /**
     * 
     * @param fileName name of a volt table file
     * @return a volt table loaded with the file's table
     */
    public static VoltTable getTableFromFile(String fileName) {
        byte[] bytes = null;
        try {
            bytes = Files.readAllBytes(Paths.get(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (bytes == null) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        VoltTable vt = new VoltTable();
        vt.initFromBuffer(buf);
        return vt;
    }
    
    /**
     * Configuration options.
     */
    public static class VoltTableUtilConfig extends CLIConfig {

        @Option(shortOpt = "f", desc = "location of Volt Table input file")
        String file = "";
        
        /**
         * Usage
         */
        @Override
        public void printUsage() {
            System.out
                    .println("Usage: volttableutil [args]");
            super.printUsage();
        }
    }
    
    /**
     * volttableutil main.
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     *
     */
    public static void main(String[] args) throws IOException,
            InterruptedException {
    	final VoltTableUtilConfig cfg = new VoltTableUtilConfig();
        cfg.parse(VoltTableUtil.class.getName(), args);
        
        printVoltTableFromFile(cfg.file);
    }
}
