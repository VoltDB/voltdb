/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

/*
 * Utility methods for work with VoltTables.
 */
public class VoltTableUtil {

    // String used to indicate NULL value in the output CSV file
    private static final String CSV_NULL = "\\N";

    private static final ThreadLocal<SimpleDateFormat> m_sdf = new ThreadLocal<SimpleDateFormat>() {
        @Override
        public SimpleDateFormat initialValue() {
            return new SimpleDateFormat(
                    "yyyy.MM.dd HH:mm:ss:SSS:");
        }
    };

    public static void toCSVWriter(CSVWriter csv, VoltTable vt, ArrayList<VoltType> columnTypes) throws IOException {
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
                        fields[ii] = CSV_NULL;
                    } else {
                        fields[ii] = Long.toString(value);
                    }
                } else if (type == VoltType.FLOAT) {
                    final double value = vt.getDouble(ii);
                    if (vt.wasNull()) {
                        fields[ii] = CSV_NULL;
                    } else {
                        fields[ii] = Double.toString(value);
                    }
                } else if (type == VoltType.DECIMAL) {
                    final BigDecimal bd = vt
                            .getDecimalAsBigDecimal(ii);
                    if (vt.wasNull()) {
                        fields[ii] = CSV_NULL;
                    } else {
                        fields[ii] = bd.toString();
                    }
                } else if (type == VoltType.STRING) {
                    final String str = vt.getString(ii);
                    if (vt.wasNull()) {
                        fields[ii] = CSV_NULL;
                    } else {
                        fields[ii] = str;
                    }
                } else if (type == VoltType.TIMESTAMP) {
                    final TimestampType timestamp = vt
                            .getTimestampAsTimestamp(ii);
                    if (vt.wasNull()) {
                        fields[ii] = CSV_NULL;
                    } else {
                        fields[ii] = sdf.format(timestamp
                                .asApproximateJavaDate());
                        fields[ii] += String.valueOf(timestamp
                                .getUSec());
                    }
                } else if (type == VoltType.VARBINARY) {
                   byte bytes[] = vt.getVarbinary(ii);
                   if (vt.wasNull()) {
                       fields[ii] = CSV_NULL;
                   } else {
                       fields[ii] = Encoder.hexEncode(bytes);
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
        return Pair.of(csvString.length(), csvString.getBytes(com.google.common.base.Charsets.UTF_8));
    }
}
