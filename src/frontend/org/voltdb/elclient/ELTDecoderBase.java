/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.elclient;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.voltdb.VoltType;
import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.TimestampType;

public abstract class ELTDecoderBase
{
    protected AdvertisedDataSource m_source;
    // This is available as a convenience, could go away.
    protected ArrayList<VoltType> m_tableSchema;

    public ELTDecoderBase(AdvertisedDataSource source)
    {
        m_source = source;
        m_tableSchema = source.columnTypes();
    }

    /**
     * Process a row of octets from the ELT stream.  Overridden by
     * subclasses to provide whatever specific processing is desired by
     * this ELClient
     * @param rowSize the length of the row (in octets)
     * @param rowData a byte array containing the row data
     * @return whether or not the row processing was successful
     */
    abstract public boolean processRow(int rowSize, byte[] rowData);

    /**
     * Called when the protocol handler received no data in response to
     * a poll.  Default behavior is to do nothing, but can be overridden
     * if the decoder cares about this case.
     */
    public void noDataReceived(long ackOffset)
    {
    }

    /**
     * Read a decimal according to the ELT encoding specification.
     * @param fds Fastdeserializer containing ELT stream data
     * @return decoded BigDecimal value
     * @throws IOException
     */
    static public BigDecimal decodeDecimal(final FastDeserializer fds) throws IOException
    {
        final int strlength = fds.readInt();
        final byte[] strdata = new byte[strlength];
        fds.readFully(strdata);
        final String str = new String(strdata);
        BigDecimal bd = null;
        try
        {
            bd = new BigDecimal(str);
        } catch (Exception e) {
            System.out.println("error creating decimal from string(" + str + ")");
            e.printStackTrace();
        }
        return bd;
    }

    /**
     * Read a string according to the ELT encoding specification
     * @param fds
     * @return
     * @throws IOException
     */
    static public String decodeString(final FastDeserializer fds) throws IOException
    {
        final int strlength = fds.readInt();
        final byte[] strdata = new byte[strlength];
        fds.readFully(strdata);
        return new String(strdata);
    }

    /**
     * Read a timestamp according to the ELT encoding specification.
     * @param fds
     * @return
     * @throws IOException
     */
    static public TimestampType decodeTimestamp(final FastDeserializer fds) throws IOException
    {
        final Long val = fds.readLong();
        return new TimestampType(val);
    }

    /**
     * Read a float according to the ELT encoding specification
     * @param fds
     * @return
     * @throws IOException
     */
    static public double decodeFloat(final FastDeserializer fds) throws IOException
    {
        return fds.readDouble();
    }

    /**
     * Read a bigint according to the ELT encoding specification.
     * @param fds
     * @return
     * @throws IOException
     */
    static public long decodeBigInt(final FastDeserializer fds) throws IOException
    {
        return fds.readLong();
    }

    /**
     * Read an integer according to the ELT encoding specification.
     * @param fds
     * @return
     * @throws IOException
     */
    static public int decodeInteger(final FastDeserializer fds) throws IOException
    {
        return (int)fds.readLong();
    }

    /**
     * Read a small int according to the ELT encoding specification.
     * @param fds
     * @return
     * @throws IOException
     */
    static public short decodeSmallInt(final FastDeserializer fds) throws IOException
    {
        return (short)fds.readLong();
    }

    /**
     * Read a tiny int according to the ELT encoding specification.
     * @param fds
     * @return
     * @throws IOException
     */
    static public byte decodeTinyInt(final FastDeserializer fds) throws IOException
    {
        return (byte)fds.readLong();
    }
}
