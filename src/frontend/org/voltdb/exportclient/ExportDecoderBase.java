/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.exportclient;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.util.ArrayList;

import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.TimestampType;

/**
 * Provide the basic functionality of decoding tuples from our export wire
 * protocol into arrays of POJOs.
 *
 */
public abstract class ExportDecoderBase {
    protected AdvertisedDataSource m_source;
    // This is available as a convenience, could go away.
    protected ArrayList<VoltType> m_tableSchema;

    public ExportDecoderBase(AdvertisedDataSource source) {
        m_source = source;
        m_tableSchema = source.columnTypes;
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
    abstract public boolean processRow(int rowSize, byte[] rowData);

    abstract public void sourceNoLongerAdvertised(AdvertisedDataSource source);

    /**
     * Finalize operation upon block completion - provides a means for a
     * specific decoder to flush data to disk - virtual method
     */
    public void onBlockCompletion() {
    }

    /**
     * Notify that a new block of data is going to be processed now
     */
    public void onBlockStart() {

    }

    /**
     * Called when the protocol handler received no data in response to a poll.
     * Default behavior is to do nothing, but can be overridden if the decoder
     * cares about this case.
     */
    public void noDataReceived(long ackOffset) {
    }

    /**
     * Decode a byte array of row data into an array of Objects corresponding to
     * the schema in the AdvertisedDataSource used to construct this object
     *
     * @throws IOException
     */
    protected Object[] decodeRow(byte[] rowData) throws IOException {
        FastDeserializer fds = new FastDeserializer(rowData,
                ByteOrder.LITTLE_ENDIAN);
        Object[] retval = new Object[m_tableSchema.size()];
        boolean[] is_null = extractNullFlags(fds);
        for (int i = 0; i < m_tableSchema.size(); i++) {
            if (is_null[i]) {
                retval[i] = null;
            } else {
                retval[i] = decodeNextColumn(fds, m_tableSchema.get(i));
            }
        }
        return retval;
    }

    boolean[] extractNullFlags(FastDeserializer fds) throws IOException {
        // compute the number of bytes necessary to hold one bit per
        // schema column
        int null_array_length = ((m_tableSchema.size() + 7) & -8) >> 3;
        byte[] null_array = new byte[null_array_length];
        for (int i = 0; i < null_array_length; i++) {
            null_array[i] = fds.readByte();
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
    Object decodeNextColumn(FastDeserializer fds, VoltType columnType)
            throws IOException {
        Object retval = null;
        switch (columnType) {
        case TINYINT:
            retval = decodeTinyInt(fds);
            break;
        case SMALLINT:
            retval = decodeSmallInt(fds);
            break;
        case INTEGER:
            retval = decodeInteger(fds);
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
            retval = decodeDecimal(fds);
            break;
        default:
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
    static public BigDecimal decodeDecimal(final FastDeserializer fds)
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
     * Read a string according to the Export encoding specification
     *
     * @param fds
     * @throws IOException
     */
    static public String decodeString(final FastDeserializer fds)
            throws IOException {
        final int strlength = fds.readInt();
        final byte[] strdata = new byte[strlength];
        fds.readFully(strdata);
        return new String(strdata);
    }

    /**
     * Read a varbinary according to the Export encoding specification
     *
     * @param fds
     * @throws IOException
     */
    static public Object decodeVarbinary(final FastDeserializer fds)
            throws IOException {
        final int length = fds.readInt();
        final byte[] data = new byte[length];
        fds.readFully(data);
        return data;
    }

    /**
     * Read a timestamp according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    static public TimestampType decodeTimestamp(final FastDeserializer fds)
            throws IOException {
        final Long val = fds.readLong();
        return new TimestampType(val);
    }

    /**
     * Read a float according to the Export encoding specification
     *
     * @param fds
     * @throws IOException
     */
    static public double decodeFloat(final FastDeserializer fds)
            throws IOException {
        return fds.readDouble();
    }

    /**
     * Read a bigint according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    static public long decodeBigInt(final FastDeserializer fds)
            throws IOException {
        return fds.readLong();
    }

    /**
     * Read an integer according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    static public int decodeInteger(final FastDeserializer fds)
            throws IOException {
        return (int) fds.readLong();
    }

    /**
     * Read a small int according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    static public short decodeSmallInt(final FastDeserializer fds)
            throws IOException {
        return (short) fds.readLong();
    }

    /**
     * Read a tiny int according to the Export encoding specification.
     *
     * @param fds
     * @throws IOException
     */
    static public byte decodeTinyInt(final FastDeserializer fds)
            throws IOException {
        return (byte) fds.readLong();
    }
}
