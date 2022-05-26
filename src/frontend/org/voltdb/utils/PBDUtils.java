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

package org.voltdb.utils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

public class PBDUtils {
    public static class ConfigurationException extends Exception {
        private static final long serialVersionUID = 1L;
        ConfigurationException() { super(); }
        ConfigurationException(String s) { super(s); }
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

    public static int calculateEntryCrc(CRC32 crc, ByteBuffer destBuf, int entryId, char flags) {
        crc.reset();
        crc.update(destBuf.remaining());
        crc.update(entryId);
        crc.update(flags);
        crc.update(destBuf);
        // the checksum here is really an unsigned int, store integer to save 4 bytes
        return (int) crc.getValue();
    }

    public static void writeEntryHeader(CRC32 crc, ByteBuffer headerBuf,
            ByteBuffer destBuf, int entryId, char flag) {
        int length = destBuf.remaining();
        headerBuf.putInt(calculateEntryCrc(crc, destBuf, entryId, flag));
        headerBuf.putInt(length);
        headerBuf.putInt(entryId);
        headerBuf.putChar(flag);
    }
}
