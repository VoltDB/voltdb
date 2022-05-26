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

package org.voltdb.sysprocs.saverestore;


import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.voltcore.utils.InstanceId;

/**
 * Encapsulates knowledge about the storage format of the hashinator config snapshot file.
 */
public class HashinatorSnapshotData
{
    /// Config data in serializeable/compressible format.
    public byte[] m_serData;

    /// Version number/transaction ID.
    public Long m_version;

    // Buffer positions.
    private static final int OFFSET_CRC = 0;
    private static final int OFFSET_INSTID_COORD = 8;
    private static final int OFFSET_INSTID_TIMESTAMP = 12;
    private static final int OFFSET_VERSION = 20;
    private static final int OFFSET_DATA = 28;

    /**
     * Constructor with data for save.
     * @param serData config data in compressed serializeable format
     * @param version version number/transaction ID
     */
    public HashinatorSnapshotData(byte[] serData, long version)
    {
        m_serData = serData;
        m_version = version;
    }

    /**
     * Default constructor for restore.
     */
    public HashinatorSnapshotData() {
        m_serData = null;
        m_version = null;
    }

    /**
     * Save to output buffer, including header and config data.
     * @return  byte buffer ready to write to a file.
     * @throws I/O exception on failure
     */
    public ByteBuffer saveToBuffer(InstanceId instId)
            throws IOException
    {
        if (instId == null) {
            throw new IOException("Null instance ID.");
        }
        if (m_serData == null) {
            throw new IOException("Uninitialized hashinator snapshot data.");
        }

        // Assume config data is the last field.
        ByteBuffer buf = ByteBuffer.allocate(m_serData.length + OFFSET_DATA);

        // Make sure the CRC starts at zero since those bytes figure into the CRC calculation.
        buf.putLong(OFFSET_CRC, 0);
        buf.putInt(OFFSET_INSTID_COORD, instId.getCoord());
        buf.putLong(OFFSET_INSTID_TIMESTAMP, instId.getTimestamp());
        buf.putLong(OFFSET_VERSION, m_version);
        buf.position(OFFSET_DATA);
        buf.put(m_serData);

        // Finalize the CRC based on the entire buffer and reset the current position.
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(buf.array());
        buf.putLong(OFFSET_CRC, crc.getValue());
        buf.rewind();
        return buf;
    }

    /**
     * Restore and check hashinator config data.
     * @param buf input buffer
     * @return instance ID read from buffer
     * @throws I/O exception on failure
     */
    public InstanceId restoreFromBuffer(ByteBuffer buf)
            throws IOException
    {
        buf.rewind();

        // Assumes config data is the last field.
        int dataSize = buf.remaining() - OFFSET_DATA;
        if (dataSize <= 0) {
            throw new IOException("Hashinator snapshot data is too small.");
        }

        // Get the CRC, zero out its buffer field, and compare to calculated CRC.
        long crcHeader = buf.getLong(OFFSET_CRC);
        buf.putLong(OFFSET_CRC, 0);
        final PureJavaCrc32 crcBuffer = new PureJavaCrc32();
        assert(buf.hasArray());
        crcBuffer.update(buf.array());
        if (crcHeader != crcBuffer.getValue()) {
            throw new IOException("Hashinator snapshot data CRC mismatch.");
        }

        // Slurp the data.
        int coord = buf.getInt(OFFSET_INSTID_COORD);
        long timestamp = buf.getLong(OFFSET_INSTID_TIMESTAMP);
        InstanceId instId = new InstanceId(coord, timestamp);
        m_version = buf.getLong(OFFSET_VERSION);
        m_serData = new byte[dataSize];
        buf.position(OFFSET_DATA);
        buf.get(m_serData);

        return instId;
    }

    /**
     * Restore and check hashinator config data from a file.
     * @param file hashinator config file
     * @param return the buffer with the raw data
     * @throws IOException
     */
    public void restoreFromFile(File file) throws IOException
    {
        byte[] rawData = new byte[(int) file.length()];
        ByteBuffer bufData = null;
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(file);
            dis = new DataInputStream(fis);
            dis.readFully(rawData);
            bufData = ByteBuffer.wrap(rawData);
            restoreFromBuffer(bufData);
        }
        finally {
            if (dis != null) {
                dis.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
    }
}
