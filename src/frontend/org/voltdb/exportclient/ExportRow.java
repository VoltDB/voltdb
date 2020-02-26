/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.exportclient.decode.RowDecoder;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Preconditions;

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
    public static final int INTERNAL_OPERATION_COLUMN = 5;
    public enum ROW_OPERATION { INVALID, INSERT, DELETE, UPDATE_OLD, UPDATE_NEW, MIGRATE }

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

    /**
     * Extract a key from the {@link ExportRow} instance.
     * <p>
     * Creates a special {@link ExportRow} instance that contains a shallow copy
     * of the columns that form the key of this row. The list of key column names
     * is passed-in and the returned row contains these columns in the same order
     * as the provided list (which may be different from the table order).
     * <p>
     * In order to remain compatible with {@link RowDecoder} operation, the internal
     * columns are also copied ahead of the key columns, but the data won't be used.
     * <p>
     * If the list of key column names is empty, the returned key is the partition column,
     * and if there is no partition column, a fake column is created, carrying the partitionId.
     * <p>
     * The life-cycle of the returned {@link ExportRow} instance is intended to be short,
     * mainly to be used to feed decoders extending {@link RowDecoder}, in order to convert
     * the key to a target format e.g. CSV or AVRO.
     * <p>
     * The tableName generated for the key {@link ExportRow} is the source's tableName,
     * with a "-key" suffix added: this is necessary to distinguish it from the values
     * in whatever target namespace the row will be decoded into.
     *
     * @param keyColumnNames list of key column names in the desired order for the key
     * @return the {@link ExportRow} instance carrying the data for the key columns
     */
    public ExportRow extractKey(List<String> keyColumnNames) {
        String keyName = tableName + "_key";
        List<String> colNames = new ArrayList<>();
        List<Integer> colLengths = new ArrayList<>();
        List<VoltType> colTypes = new ArrayList<>();

        // Determine key column count and allocate array of values
        boolean hasCols = keyColumnNames != null && !keyColumnNames.isEmpty();
        int nCols =  hasCols ? keyColumnNames.size() : 1;
        nCols += INTERNAL_FIELD_COUNT;
        Object[] colValues = new Object[nCols];

        // Always copy the internal fields for {@link RowDecoder} compatibility
        for (int i = 0; i < INTERNAL_FIELD_COUNT; i++) {
            colNames.add(names.get(i));
            colLengths.add(lengths.get(i));
            colTypes.add(types.get(i));
            colValues[i] = values[i];
        }

        // Build key data
        if (hasCols) {
            // Use the listed key columns, in the order specified
            Map<String, Integer> colMap = new HashMap<>();
            for (int i = 0; i < names.size(); i++) {
                String colName = names.get(i);
                if (keyColumnNames.contains(colName)) {
                    assert !colMap.containsKey(colName) : colName + ": duplicate key column name";
                    colMap.put(colName, i);
                }
            }
            for (int i = 0; i < keyColumnNames.size(); i++) {
                Integer iCol = colMap.get(keyColumnNames.get(i));
                assert iCol != null : "Unable to find key column " + keyColumnNames.get(i);

                colNames.add(names.get(iCol));
                colLengths.add(lengths.get(iCol));
                colTypes.add(types.get(iCol));
                colValues[INTERNAL_FIELD_COUNT + i] = values[iCol];
            }
        }
        else if (partitionValue != null) {
            // Use the partition column value
            colNames.add(names.get(partitionColIndex));
            colLengths.add(lengths.get(partitionColIndex));
            colTypes.add(types.get(partitionColIndex));
            colValues[INTERNAL_FIELD_COUNT] = partitionValue;
        }
        else {
            // Use a fake column with the partition
            colNames.add("partitionId");
            colLengths.add(VoltType.INTEGER.getLengthInBytesForFixedTypes());
            colTypes.add(VoltType.INTEGER);
            colValues[INTERNAL_FIELD_COUNT] = new Integer(partitionId);
        }
        return new ExportRow(keyName, colNames, colTypes, colLengths, colValues, null, -1, partitionId, generation);
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
    public static ExportRow decodeBufferSchema(ByteBuffer bb, int schemaSize,
            int partitionId, long generation) throws IOException {
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
    public static ExportRow decodeRow(ExportRow previous, int partition, long startTS, ByteBuffer bb) throws IOException {
        assert (previous != null);
        if (previous == null) {
            throw new IOException("Export block with no schema found without prior block with schema.");
        }

        final int partitionColIndex = bb.getInt();
        final int columnCount = bb.getInt();
        assert(columnCount <= DDLCompiler.MAX_COLUMNS);
        if (columnCount != previous.names.size()) {
            throw new IOException(
                    String.format("Read %d columns from row but expected %d columns: %s", columnCount,
                            previous.names.size(), previous));
        }

        boolean[] is_null = extractNullFlags(bb, columnCount);

        final long generation = previous.generation;
        final String tableName = previous.tableName;
        final List<String> colNames = previous.names;
        final List<VoltType> colTypes = previous.types;
        final List<Integer> colLengths = previous.lengths;

        Object[] retval = new Object[colNames.size()];
        Object pval = null;
        for (int i = 0; i < colNames.size(); ++i) {
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
    public static ExportRow decodeRow(ExportRow previous, int partition, long startTS, byte[] rowData) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(rowData);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        return decodeRow(previous, partition, startTS, bb);
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

    @Deprecated
    public static boolean[] extractNullFlags(FastDeserializer fds, int columnCount) throws IOException {
        Preconditions.checkArgument(
                fds.buffer().order() == ByteOrder.LITTLE_ENDIAN,
                "incorret byte order in the deserializer underlying buffer"
                );
        return extractNullFlags(fds.buffer(), columnCount);
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
        Object retval = null;
        switch (columnType) {
        case TINYINT:
            retval = decodeTinyInt(bb);
            break;
        case SMALLINT:
            retval = decodeSmallInt(bb);
            break;
        case INTEGER:
            retval = decodeInteger(bb);
            break;
        case BIGINT:
            retval = decodeBigInt(bb);
            break;
        case FLOAT:
            retval = decodeFloat(bb);
            break;
        case TIMESTAMP:
            retval = decodeTimestamp(bb);
            break;
        case STRING:
            retval = decodeString(bb);
            break;
        case VARBINARY:
            retval = decodeVarbinary(bb);
            break;
        case DECIMAL:
            retval = decodeDecimal(bb);
            break;
        case GEOGRAPHY_POINT:
            retval = decodeGeographyPoint(bb);
            break;
        case GEOGRAPHY:
            retval = decodeGeography(bb);
            break;
        default:
            throw new IOException("Invalid column type: " + columnType);
        }

        return retval;
    }

    /**
     * Read a decimal according to the Four Dot Four encoding specification.
     *
     * @param bb
     *            ByteBuffer containing Export stream data
     * @return decoded BigDecimal value
     */
    static public BigDecimal decodeDecimal(final ByteBuffer bb) {
        final int scale = bb.get();
        final int precisionBytes = bb.get();
        final byte[] bytes = new byte[precisionBytes];
        bb.get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    /**
     * Read a string according to the Export encoding specification
     *
     * @param bb
     * @throws IOException
     */
    static public String decodeString(final ByteBuffer bb) {
        final int strlength = bb.getInt();
        final int position = bb.position();
        String decoded = null;
        if (bb.hasArray()) {
            decoded = new String(bb.array(), bb.arrayOffset() + position, strlength, Charsets.UTF_8);
        } else {
            // Must be a direct buffer
            byte[] dst = new byte[strlength];
            bb.get(dst, 0, strlength);
            decoded = new String(dst, Charsets.UTF_8);
        }
        bb.position(position + strlength);
        return decoded;
    }

    /**
     * Read a varbinary according to the Export encoding specification
     *
     * @param bb
     * @throws IOException
     */
    static public Object decodeVarbinary(final ByteBuffer bb) {
        final int length = bb.getInt();
        final byte[] data = new byte[length];
        bb.get(data);
        return data;
    }

    /**
     * Read a timestamp according to the Export encoding specification.
     *
     * @param bb
     * @throws IOException
     */
    static public TimestampType decodeTimestamp(final ByteBuffer bb) {
        final Long val = bb.getLong();
        return new TimestampType(val);
    }

    /**
     * Read a float according to the Export encoding specification
     *
     * @param bb
     * @throws IOException
     */
    static public double decodeFloat(final ByteBuffer bb) {
        return bb.getDouble();
    }

    /**
     * Read a bigint according to the Export encoding specification.
     *
     * @param bb
     * @throws IOException
     */
    static public long decodeBigInt(final ByteBuffer bb) {
        return bb.getLong();
    }

    /**
     * Read an integer according to the Four Dot Four Export encoding specification.
     *
     * @param bb
     * @throws IOException
     */
    static public int decodeInteger(final ByteBuffer bb) {
        return bb.getInt();
    }

    /**
     * Read a small int according to the Four Dot Four Export encoding specification.
     *
     * @param bb
     * @throws IOException
     */
    static public short decodeSmallInt(final ByteBuffer bb) {
        return bb.getShort();
    }

    /**
     * Read a tiny int according to the Four Dot Four Export encoding specification.
     *
     * @param bb
     * @throws IOException
     */
    static public byte decodeTinyInt(final ByteBuffer bb) {
        return bb.get();
    }

    /**
     * Read a point according to the Four Dot Four Export encoding specification.
     *
     * @param bb
     * @throws IOException
     */
    static public GeographyPointValue decodeGeographyPoint(final ByteBuffer bb) {
        return GeographyPointValue.unflattenFromBuffer(bb);
    }

    /**
     * Read a geography according to the Four Dot Four Export encoding specification.
     *
     * @param bb
     * @throws IOException
     */
    static public GeographyValue decodeGeography(final ByteBuffer bb) {
        final int strLength = bb.getInt();
        final int startPosition = bb.position();
        GeographyValue gv = GeographyValue.unflattenFromBuffer(bb);
        assert(bb.position() - startPosition == strLength);
        return gv;
    }
}
