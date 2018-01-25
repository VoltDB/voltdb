/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltType;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.AdvertisedDataSource.ExportFormat;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * Provide the basic functionality of decoding tuples from our export wire
 * protocol into arrays of POJOs.
 *
 */
public abstract class ExportDecoderBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    public static final int INTERNAL_FIELD_COUNT = 6;
    public static final int PARTITION_ID_INDEX = 3;

    public static class RestartBlockException extends Exception {
        private static final long serialVersionUID = 1L;
        public final boolean requestBackoff;
        public RestartBlockException(boolean requestBackoff) {
            this.requestBackoff = requestBackoff;
        }
        public RestartBlockException(String msg, Throwable cause, boolean requestBackoff) {
            super(msg,cause);
            this.requestBackoff = requestBackoff;
        }
        public RestartBlockException(String msg, boolean requestBackoff) {
            super(msg);
            this.requestBackoff = requestBackoff;
        }
    }

    public static enum BinaryEncoding {
        BASE64,
        HEX
    }

    protected AdvertisedDataSource m_source;
    // This is available as a convenience, could go away.
    protected final ArrayList<VoltType> m_tableSchema;
    private int m_partitionColumnIndex = PARTITION_ID_INDEX;
    private final ExportFormat m_exportFormat;

    public ExportDecoderBase(AdvertisedDataSource source) {
        m_source = source;
        m_tableSchema = source.columnTypes;
        m_exportFormat = source.exportFormat;
        setPartitionColumnName(source.getPartitionColumnName());
    }

    /**
     * Process a row of octets from the Export stream. Overridden by subclasses
     * to provide whatever specific processing is desired by this ELClient
     *
     * @param rowSize
     *            the length of the row (in octets)
     * @param rowData
     *            a byte array containing the row data
     * @return whether or not the row processing was successful
     */
    abstract public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException;

    abstract public void sourceNoLongerAdvertised(AdvertisedDataSource source);

    /**
     * Finalize operation upon block completion - provides a means for a
     * specific decoder to flush data to disk - virtual method
     */
    public void onBlockCompletion() throws RestartBlockException {
    }

    /**
     * Notify that a new block of data is going to be processed now
     */
    public void onBlockStart() throws RestartBlockException {

    }

    public ListeningExecutorService getExecutor() {
        return CoreUtils.LISTENINGSAMETHREADEXECUTOR;
    }

    //Used for override of column for partitioning.
    public final void setPartitionColumnName(String partitionColumnName) {
        if (partitionColumnName == null || partitionColumnName.trim().isEmpty()) {
            return;
        }
        int idx = -1;
        for (String name : m_source.columnNames) {
            if (name.equalsIgnoreCase(partitionColumnName)) {
                idx = m_source.columnNames.indexOf(name);
                break;
            }
        }
        if (idx == -1) {
            m_partitionColumnIndex = PARTITION_ID_INDEX;
            m_logger.error("Export configuration error: specified " + m_source.tableName + "." + partitionColumnName
                    + " does not exist. A default partition or routing key will be used.");
        } else {
            m_partitionColumnIndex = idx;
        }
    }

    public static class ExportRowData {
        public final Object[] values;
        public final Object partitionValue;
        //Partition id is here for convenience.
        public final int partitionId;

        public ExportRowData(Object[] vals, Object pval, int pid) {
            values = vals;
            partitionValue = pval;
            partitionId = pid;
        }
    }

    /**
     * Decode a byte array of row data into ExportRowData
     *
     * @param rowData
     * @return ExportRowData
     * @throws IOException
     */
    protected ExportRowData decodeRow(byte[] rowData) throws IOException {
        switch(m_exportFormat) {
        case FOURDOTFOUR: return decodeTuple(rowData);
        case ORIGINAL: return decodeTupleLegacy(rowData);
        default: throw new IOException("Unknown export format: " + m_exportFormat.name());
        }
    }

    private ExportRowData decodeTuple(byte[] rowData) throws IOException {
        Preconditions.checkState(
                m_exportFormat == ExportFormat.FOURDOTFOUR,
                "decoder may be called on curently formatted row data"
                );
        ByteBuffer bb = ByteBuffer.wrap(rowData);
        bb.order(ByteOrder.LITTLE_ENDIAN);

        Object[] retval = new Object[m_tableSchema.size()];
        Object pval = null;

        boolean[] is_null = extractNullFlags(bb);
        for (int i = 0; i < m_tableSchema.size(); ++i) {
            if (is_null[i]) {
                retval[i] = null;
            } else {
                retval[i] = decodeNextColumn(bb, m_tableSchema.get(i));
            }
            if (i == m_partitionColumnIndex) {
                pval = retval[i];
            }
        }
        return new ExportRowData(retval, pval, m_source.partitionId);
    }

    private ExportRowData decodeTupleLegacy(byte[] rowData) throws IOException {
        Preconditions.checkState(
                m_exportFormat == ExportFormat.ORIGINAL,
                "leagacy decoder may be called on legacy row data"
                );

        FastDeserializer fds = new FastDeserializer(rowData, ByteOrder.LITTLE_ENDIAN);

        Object[] retval = new Object[m_tableSchema.size()];
        boolean[] is_null = extractNullFlags(fds);
        Object pval = null;

        for (int i = 0; i < m_tableSchema.size(); i++) {
            if (is_null[i]) {
                retval[i] = null;
            } else {
                retval[i] = decodeNextColumnLegacy(fds, m_tableSchema.get(i));
            }
            if (i == m_partitionColumnIndex) {
                pval = retval[i];
            }
        }

        return new ExportRowData(retval, pval, m_source.partitionId);
    }

    //Based on your skipinternal value return index of first field.
    public int getFirstField(boolean skipinternal) {
        return skipinternal ? INTERNAL_FIELD_COUNT : 0;
    }

    public boolean writeRow(Object row[], CSVWriter writer, boolean skipinternal,
            BinaryEncoding binaryEncoding, SimpleDateFormat dateFormatter) {

        int firstfield = getFirstField(skipinternal);
        try {
            String[] fields = new String[m_tableSchema.size() - firstfield];
            for (int i = firstfield; i < m_tableSchema.size(); i++) {
                if (row[i] == null) {
                    fields[i - firstfield] = "NULL";
                } else if (m_tableSchema.get(i) == VoltType.VARBINARY && binaryEncoding != null) {
                    if (binaryEncoding == BinaryEncoding.HEX) {
                        fields[i - firstfield] = Encoder.hexEncode((byte[]) row[i]);
                    } else {
                        fields[i - firstfield] = Encoder.base64Encode((byte[]) row[i]);
                    }
                } else if (m_tableSchema.get(i) == VoltType.STRING) {
                    fields[i - firstfield] = (String) row[i];
                } else if (m_tableSchema.get(i) == VoltType.TIMESTAMP && dateFormatter != null) {
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
    boolean[] extractNullFlags(FastDeserializer fds) throws IOException {
        Preconditions.checkArgument(
                fds.buffer().order() == ByteOrder.LITTLE_ENDIAN,
                "incorret byte order in the deserializer underlying buffer"
                );
        return extractNullFlags(fds.buffer());
    }

    boolean[] extractNullFlags(ByteBuffer bb) {
        // compute the number of bytes necessary to hold one bit per
        // schema column
        int null_array_length = ((m_tableSchema.size() + 7) & -8) >> 3;
        byte[] null_array = new byte[null_array_length];
        for (int i = 0; i < null_array_length; i++) {
            null_array[i] = bb.get();
        }

        boolean[] retval = new boolean[m_tableSchema.size()];

        // The null flags were written with this mapping to column index:
        // given an array of octets, the index into the array for the flag is
        // column index / 8, and the bit in that byte for the flag is
        // 0x80 >> (column index % 8).
        for (int i = 0; i < m_tableSchema.size(); i++) {
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
    Object decodeNextColumn(ByteBuffer bb, VoltType columnType)
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

    // This does not decode an arbitrary column because fds keeps getting
    // consumed.
    // Rather, it decodes the next non-null column in the FastDeserializer
    @Deprecated
    Object decodeNextColumnLegacy(FastDeserializer fds, VoltType columnType)
            throws IOException {
        Object retval = null;
        switch (columnType) {
        case TINYINT:
            retval = decodeTinyIntLegacy(fds);
            break;
        case SMALLINT:
            retval = decodeSmallIntLegacy(fds);
            break;
        case INTEGER:
            retval = decodeIntegerLegacy(fds);
            break;
        case BIGINT:
            retval = decodeBigInt(fds);
            break;
        case FLOAT:
            retval = decodeFloat(fds);
            break;
        case TIMESTAMP:
            retval = decodeTimestamp(fds);
            break;
        case STRING:
            retval = decodeString(fds);
            break;
        case VARBINARY:
            retval = decodeVarbinary(fds);
            break;
        case DECIMAL:
            retval = decodeDecimalLegacy(fds);
            break;
        default:
            // Note: we can come here for the case of GEOGRAPHY or GEOGRAPHY_POINT data.
            // These types were added in VoltDB 6.0 aren't supported for export format
            // version older than 4.4.
            throw new IOException("Invalid column type: " + columnType);
        }
        ;
        return retval;
    }

    /**
     * Read a decimal according to the Export encoding specification.
     *
     * @param fds
     *            Fastdeserializer containing Export stream data
     * @return decoded BigDecimal value
     * @throws IOException
     */
    @Deprecated
    static public BigDecimal decodeDecimalLegacy(final FastDeserializer fds)
            throws IOException {
        final int strlength = fds.readInt();
        final byte[] strdata = new byte[strlength];
        fds.readFully(strdata);
        final String str = new String(strdata);
        BigDecimal bd = null;
        try {
            bd = new BigDecimal(str);
        } catch (Exception e) {
            System.out.println("error creating decimal from string(" + str
                    + ")");
            e.printStackTrace();
        }
        return bd;
    }

    /**
     * Read a decimal according to the Four Dot Four encoding specification.
     *
     * @param fds
     *            Fastdeserializer containing Export stream data
     * @return decoded BigDecimal value
     * @throws IOException
     */
    @Deprecated
    static public BigDecimal decodeDecimal(final FastDeserializer fds)
            throws IOException {
        final int scale = fds.readByte();
        final int precisionBytes = fds.readByte();
        final byte[] bytes = new byte[precisionBytes];
        fds.readFully(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
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
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public String decodeString(final FastDeserializer fds)
            throws IOException {
        final int strlength = fds.readInt();
        final byte[] strdata = new byte[strlength];
        fds.readFully(strdata);
        return new String(strdata);
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
        String decoded = new String(bb.array(), bb.arrayOffset() + position, strlength, Charsets.UTF_8);
        bb.position(position + strlength);
        return decoded;
    }

    /**
     * Read a varbinary according to the Export encoding specification
     *
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public Object decodeVarbinary(final FastDeserializer fds)
            throws IOException {
        final int length = fds.readInt();
        final byte[] data = new byte[length];
        fds.readFully(data);
        return data;
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
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public TimestampType decodeTimestamp(final FastDeserializer fds)
            throws IOException {
        final Long val = fds.readLong();
        return new TimestampType(val);
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
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public double decodeFloat(final FastDeserializer fds)
            throws IOException {
        return fds.readDouble();
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
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public long decodeBigInt(final FastDeserializer fds)
            throws IOException {
        return fds.readLong();
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
     * Read an integer according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public int decodeIntegerLegacy(final FastDeserializer fds)
            throws IOException {
        return (int) fds.readLong();
    }

    /**
     * Read an integer according to the Four Dot Four Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public int decodeInteger(final FastDeserializer fds)
            throws IOException {
        return fds.readInt();
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
     * Read a small int according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public short decodeSmallIntLegacy(final FastDeserializer fds)
            throws IOException {
        return (short) fds.readLong();
    }

    /**
     * Read a small int according to the Four Dot Four Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public short decodeSmallInt(final FastDeserializer fds)
            throws IOException {
        return fds.readShort();
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
     * Read a tiny int according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public byte decodeTinyIntLegacy(final FastDeserializer fds)
            throws IOException {
        return (byte) fds.readLong();
    }

    /**
     * Read a tiny int according to the Four Dot Four Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    @Deprecated
    static public byte decodeTinyInt(final FastDeserializer fds)
            throws IOException {
        return fds.readByte();
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
