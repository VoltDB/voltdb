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

import java.nio.ByteBuffer;

/**
 * A ByteBuffer allocator that caches the buffer and reuse it in the future. If the cached buffer
 * is smaller than the requested size, it will replace the cached buffer with a new buffer that
 * fits the size.
 *
 * Note: this class is not thread-safe.
 */
public class CachedByteBufferAllocator {
    private ByteBuffer m_buffer = null;

    public ByteBuffer allocate(int size) {
        if (m_buffer == null || m_buffer.capacity() < size) {
            m_buffer = ByteBuffer.allocate(size);
        }
        m_buffer.clear();
        m_buffer.limit(size);
        return m_buffer;
    }
}
