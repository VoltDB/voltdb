/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltcore.utils.DeferredSerialization;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class PBDUtils {
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
}
