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

package org.voltdb.exportclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

/**
 *
 * @author akhanzode
 */
public class ExportRow {

    public final List<String> names;
    public final Object[] values;
    public final List<VoltType> types;
    public final List<Integer> lengths;
    public final int partitionColIndex;
    public final Object partitionValue;
    //Partition id is here for convenience.
    public final int partitionId;
    public final String tableName;
    public final long generation;
    public static final int INTERNAL_FIELD_COUNT = 6;
    public static final int EXPORT_TIMESTAMP_COLUMN = 1;
    public static final int SEQUENCE_NUMBER_COLUMN = 2;
    public static final int INTERNAL_OPERATION_COLUMN = 5;
    public enum ROW_OPERATION { INVALID, INSERT, DELETE, UPDATE_OLD, UPDATE_NEW, MIGRATE }
    public static final String KEY_SUFFIX = "_key";

    public ExportRow(String tableName, List<String> columnNames, List<VoltType> t, List<Integer> l,
            Object[] vals, Object pval, int partitionColIndex, int pid, long generation) {
        this.tableName = tableName;
        values = vals;
        partitionValue = pval;
        partitionId = pid;
        names = columnNames;
        types = t;
        lengths = l;
        this.generation = generation;
        this.partitionColIndex = partitionColIndex;
    }


    public String getPartitionColumnName() {
        if (partitionColIndex >= INTERNAL_FIELD_COUNT) {
             return names.get(partitionColIndex);
         }
        return "";
    }

    public ROW_OPERATION getOperation() {
        return ROW_OPERATION.values()[(byte)values[INTERNAL_OPERATION_COLUMN]];
    }

    public String toSchemaString() {
        Iterator<String> itNames = this.names.iterator();
        Iterator<VoltType> itTypes = this.types.iterator();
        Iterator<Integer> itSizes = this.lengths.iterator();

        StringBuilder sb = new StringBuilder("[");
        while(itNames.hasNext()) {
            sb.append(itNames.next())
              .append(":")
              .append(itTypes.next())
              .append(":")
              .append(itSizes.next())
              .append((itNames.hasNext()) ? ", " : "]");
        }
        return sb.toString();
    }

    public Long getTimestamp() {
        return (Long)values[EXPORT_TIMESTAMP_COLUMN];
    }

    public Long getSequenceNumber() {
        return (Long)values[SEQUENCE_NUMBER_COLUMN];
    }

    // Temporary: only print schema, values omitted
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.tableName)
                .append(":")
                .append(this.partitionId)
                .append(":")
                .append(this.generation)
                .append(" - ")
                .append(toSchemaString());
        return sb.toString();
    }

    // Note: used to decode schemas in encoded rows produced by {@code ExportEncoder.encodeRow}
    public static ExportRow decodeBufferSchema(ByteBuffer bb, int schemaSize, int partitionId, long generation) {
        String tableName = ExportRow.decodeString(bb);
        List<String> colNames = new ArrayList<>();
        List<Integer> colLengths = new ArrayList<>();
        List<VoltType> colTypes = new ArrayList<>();
        while (bb.position() < schemaSize) {
            colNames.add(ExportRow.decodeString(bb));
            colTypes.add(VoltType.get(bb.get()));
            colLengths.add(bb.getInt());
        }
        return new ExportRow(tableName, colNames, colTypes, colLengths,
                new Object[] {}, null, -1, partitionId, generation);
    }

    /**
     * Decode a byte array of row data into ExportRow
     *
     * @param previous previous row for schema purposes.
     * @param partition partition of this data
     * @param startTS start time for this export data source
     * @param rowData row data to decode.
     * @return ExportRow row data with metadata
     * @throws IOException
     */
    public static ExportRow decodeRow(ExportRow previous, int partition, ByteBuffer bb) throws IOException {
        assert (previous != null);
        if (previous == null) {
            throw new IOException("Export block with no schema found without prior block with schema.");
        }

        final int partitionColIndex = bb.getInt();
        final int columnCount = bb.getInt();
        assert(columnCount <= DDLCompiler.MAX_COLUMNS);
        if (columnCount != previous.types.size()) {
            throw new IOException(
                    String.format("Read %d columns from row but expected %d columns: %s", columnCount,
                            previous.types.size(), previous));
        }

        boolean[] is_null = extractNullFlags(bb, columnCount);

        final long generation = previous.generation;
        final String tableName = previous.tableName;
        final List<String> colNames = previous.names;
        final List<VoltType> colTypes = previous.types;
        final List<Integer> colLengths = previous.lengths;

        Object[] retval = new Object[colTypes.size()];
        Object pval = null;
        for (int i = 0; i < colTypes.size(); ++i) {
            if (is_null[i]) {
                retval[i] = null;
            } else {
                retval[i] = decodeNextColumn(bb, colTypes.get(i));
            }
            if (i == partitionColIndex) {
                pval = retval[i];
            }
        }

        return new ExportRow(tableName, colNames, colTypes, colLengths, retval, (pval == null ? partition : pval), partitionColIndex, partition, generation);
    }

    /**
     * Decode a byte array of row data into ExportRow
     *
     * @param rowData
     * @return ExportRow
     * @throws IOException
     */
    public static ExportRow decodeRow(ExportRow previous, int partition, byte[] rowData) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(rowData);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        return decodeRow(previous, partition, bb);
    }

    //Based on your skipinternal value return index of first field.
    public static int getFirstField(boolean skipinternal) {
        return skipinternal ? INTERNAL_FIELD_COUNT : 0;
    }

    public static boolean writeRow(Object row[], CSVWriter writer, boolean skipinternal,
            ExportDecoderBase.BinaryEncoding binaryEncoding, SimpleDateFormat dateFormatter, List<VoltType> types) {

        int firstfield = getFirstField(skipinternal);
        try {
            String[] fields = new String[types.size() - firstfield];
            for (int i = firstfield; i < types.size(); i++) {
                if (row[i] == null) {
                    fields[i - firstfield] = "NULL";
                } else if (types.get(i) == VoltType.VARBINARY && binaryEncoding != null) {
                    if (binaryEncoding == ExportDecoderBase.BinaryEncoding.HEX) {
                        fields[i - firstfield] = Encoder.hexEncode((byte[]) row[i]);
                    } else {
                        fields[i - firstfield] = Encoder.base64Encode((byte[]) row[i]);
                    }
                } else if (types.get(i) == VoltType.STRING) {
                    fields[i - firstfield] = (String) row[i];
                } else if (types.get(i) == VoltType.TIMESTAMP && dateFormatter != null) {
                    TimestampType timestamp = (TimestampType) row[i];
                    fields[i - firstfield] = dateFormatter.format(timestamp.asApproximateJavaDate());
                } else {
                    fields[i - firstfield] = row[i].toString();
                }
            }
            writer.writeNext(fields);
        } catch (Exception x) {
            x.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean[] extractNullFlags(ByteBuffer bb, int columnCount) {
        // compute the number of bytes necessary to hold one bit per schema column
        int null_array_length = ((columnCount + 7) & -8) >> 3;
        byte[] null_array = new byte[null_array_length];
        for (int i = 0; i < null_array_length; i++) {
            null_array[i] = bb.get();
        }

        boolean[] retval = new boolean[columnCount];

        // The null flags were written with this mapping to column index:
        // given an array of octets, the index into the array for the flag is
        // column index / 8, and the bit in that byte for the flag is
        // 0x80 >> (column index % 8).
        for (int i = 0; i < columnCount; i++) {
            int index = i >> 3;
            int bit = i % 8;
            byte mask = (byte) (0x80 >>> bit);
            byte flag = (byte) (null_array[index] & mask);
            retval[i] = (flag != 0);
        }
        return retval;
    }

    // This does not decode an arbitrary column because fds keeps getting
    // consumed.
    // Rather, it decodes the next non-null column in the FastDeserializer
    private static Object decodeNextColumn(ByteBuffer bb, VoltType columnType)
            throws IOException {
        try {
            return columnType.decodeValue(bb);
        } catch (UnsupportedOperationException e) {
            throw new IOException("Invalid column type: " + columnType, e);
        }
    }

    /**
     * Read a string according to the Export encoding specification
     *
     * @param bb
     * @throws IOException
     */
    static public String decodeString(final ByteBuffer bb) {
        return (String) VoltType.STRING.decodeValue(bb);
    }
}
