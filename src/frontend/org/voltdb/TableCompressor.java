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

package org.voltdb;

import java.io.IOException;
import java.util.concurrent.Future;

import org.voltdb.utils.CompressionService;

/**
 * Helper class to compress a table's internal buffer.
 *
 * This code was removed from core VoltTable so VoltTables could be used
 * without depending on the CompressionService.
 *
 */
public class TableCompressor {

    public static byte[] getCompressedTableBytes(VoltTable t) throws IOException {
        final int startPosition = t.m_buffer.position();
        try {
            t.m_buffer.position(0);
            if (t.m_buffer.isDirect()) {
                return CompressionService.compressBuffer(t.m_buffer);
            } else {
                assert(t.m_buffer.hasArray());
                return CompressionService.compressBytes(
                        t.m_buffer.array(),
                        t.m_buffer.arrayOffset() + t.m_buffer.position(),
                        t.m_buffer.limit());
            }
        } finally {
            t.m_buffer.position(startPosition);
        }
    }

    public static Future<byte[]> getCompressedTableBytesAsync(VoltTable t) throws IOException {
        final int startPosition = t.m_buffer.position();
        try {
            t.m_buffer.position(0);
            if (t.m_buffer.isDirect()) {
                return CompressionService.compressBufferAsync(t.m_buffer.duplicate());
            } else {
                assert(t.m_buffer.hasArray());
                return CompressionService.compressBytesAsync(
                        t.m_buffer.array(),
                        t.m_buffer.arrayOffset() + t.m_buffer.position(),
                        t.m_buffer.limit());
            }
        } finally {
            t.m_buffer.position(startPosition);
        }
    }
}
