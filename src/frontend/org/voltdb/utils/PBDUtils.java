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

package org.voltdb.utils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import org.voltcore.utils.DeferredSerialization;
import org.voltdb.HybridCrc32;

public class PBDUtils {
    public static int writeDeferredSerialization(ByteBuffer mbuf, DeferredSerialization ds) throws IOException
    {
        int written = 0;
        try {
            mbuf.position(mbuf.position() + PBDSegment.ENTRY_HEADER_BYTES);
            final int objStartPosition = mbuf.position();
            ds.serialize(mbuf);
            written = mbuf.position() - objStartPosition;
        } finally {
            ds.cancel();
        }
        return written;
    }

    public static void writeBuffer(FileChannel fc, ByteBuffer buf, int startPos) throws IOException
    {
        int pos = startPos;
        while (buf.hasRemaining()) {
            pos += fc.write(buf, pos);
        }
    }

    public static void readBufferFully(FileChannel fc, ByteBuffer buf, int startPos) throws IOException
    {
        int pos = startPos;
        while (buf.hasRemaining()) {
            int read = fc.read(buf, pos);
            if (read == -1) {
                throw new EOFException();
            }
            pos += read;
        }
        buf.flip();
    }

    public static void writeEntryHeader(HybridCrc32 crc, ByteBuffer headerBuf,
            ByteBuffer destBuf, int size, int flag) {
        crc.update(size);
        crc.update(flag);
        crc.update(destBuf);
        headerBuf.order(ByteOrder.LITTLE_ENDIAN);
        headerBuf.clear();
        headerBuf.putLong(crc.getValue());
        headerBuf.putInt(size);
        headerBuf.putInt(flag);
        headerBuf.order(ByteOrder.BIG_ENDIAN);
    }
}
