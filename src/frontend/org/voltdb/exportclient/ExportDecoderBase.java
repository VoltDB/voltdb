/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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


import static org.voltdb.exportclient.ExportRow.getFirstField;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltType;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;


/**
 * Provide the basic functionality of decoding tuples from our export wire
 * protocol into arrays of POJOs.
 *
 */
public abstract class ExportDecoderBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    public static final int INTERNAL_FIELD_COUNT = ExportRow.INTERNAL_FIELD_COUNT;
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
    protected final int m_partition;
    protected final long m_startTS;
    //If true we have detected an old style connector.
    private boolean m_legacy = false;
    //Only used for legacy connector which picks up schema from ADS
    protected final ArrayList<VoltType> m_tableSchema = new ArrayList<>();
    //Only used for legacy connector which picks up schema from ADS
    private int m_partitionColumnIndex = PARTITION_ID_INDEX;
    //Only used for legacy connector which picks up schema from ADS
    protected final AdvertisedDataSource m_source;
    //Only used for legacy connector which picks up schema from ADS
    final ExportRow m_legacyRow;

    //Used by new style connector to pickup schema information from previous record.
    ExportRow m_rowSchema;
    public ExportDecoderBase(AdvertisedDataSource ads) {
        m_source = ads;
        m_startTS = System.currentTimeMillis();
        m_partition = ads.partitionId;

        m_tableSchema.addAll(ads.columnTypes);
        setPartitionColumnName(m_source.getPartitionColumnName());
        m_legacyRow = new ExportRow(ads.tableName, ads.columnNames, m_tableSchema, ads.columnLengths, null, null, m_partitionColumnIndex, ads.partitionId, ads.m_generation);
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
        ExportRow row = ExportRow.decodeRow(m_legacyRow, getPartition(), m_startTS, rowData);
        return new ExportRowData(row.values, row.partitionValue, row.partitionId);
    }

    //This is for legacy connector.
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

    //Used for override of column for partitioning. This is for legacy connector only.
    public final int setPartitionColumnName(String partitionColumnName) {
        if (partitionColumnName == null || partitionColumnName.trim().isEmpty()) {
            return PARTITION_ID_INDEX;
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
        return m_partitionColumnIndex;
    }

    /**
     * Process a row of octets from the Export stream. Overridden by subclasses
     * to provide whatever specific processing is desired by this ELClient
     *
     * @param row Decoded Export Data
     * @return whether or not the row processing was successful
     * @throws org.voltdb.exportclient.ExportDecoderBase.RestartBlockException
     */
    public boolean processRow(ExportRow row) throws RestartBlockException {
        throw new UnsupportedOperationException("processRow must be implemented.");
    }

    public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
        throw new UnsupportedOperationException("processRow must be implemented.");
    }

    abstract public void sourceNoLongerAdvertised(AdvertisedDataSource source);

    /**
     * Finalize operation upon block completion - provides a means for a
     * specific decoder to flush data to disk - virtual method
     * @param row The last row for the block
     * @throws org.voltdb.exportclient.ExportDecoderBase.RestartBlockException
     */
    public void onBlockCompletion(ExportRow row) throws RestartBlockException {
    }

    /**
     * Notify that a new block of data is going to be processed now
     * @param row first row of the block.
     * @throws org.voltdb.exportclient.ExportDecoderBase.RestartBlockException
     */
    public void onBlockStart(ExportRow row) throws RestartBlockException {

    }

    /**
     * Finalize operation upon block completion - provides a means for a
     * specific decoder to flush data to disk - virtual method
     * @throws org.voltdb.exportclient.ExportDecoderBase.RestartBlockException
     */
    public void onBlockCompletion() throws RestartBlockException {
    }

    /**
     * Notify that a new block of data is going to be processed now
     * @throws org.voltdb.exportclient.ExportDecoderBase.RestartBlockException
     */
    public void onBlockStart() throws RestartBlockException {

    }

    public ListeningExecutorService getExecutor() {
        return CoreUtils.LISTENINGSAMETHREADEXECUTOR;
    }

    public int getPartition() {
        return m_partition;
    }

    public void setLegacy(boolean legacy) {
        m_legacy = legacy;
    }

    public boolean isLegacy() {
        return m_legacy;
    }

    public void setExportRowSchema(ExportRow row) {
        //We do keep the values in the schema row but they are not used.
        m_rowSchema = row;
    }

    public ExportRow getExportRowSchema() {
        //We do keep the values of previous row but they should not be relied upon only schema information is used.
        return m_rowSchema;
    }
}
