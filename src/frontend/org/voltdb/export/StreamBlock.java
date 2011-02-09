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
package org.voltdb.export;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;

public class StreamBlock {

    StreamBlock(BBContainer cont, long uso, boolean isPersisted) {
        m_buffer = cont;
        m_uso = uso;
        m_totalUso = m_buffer.b.capacity();
        m_isPersisted = isPersisted;
    }

    void deleteContent() {
        m_buffer.discard();
        m_buffer = null;
    }

    long uso() {
        return m_uso;
    }

    /**
     * Returns the USO of the first unreleased octet in this block
     */
    long unreleasedUso()
    {
        return m_uso + m_releaseOffset;
    }

    /**
     * Returns the total amount of data in the USO stream
     * @return
     */
    long totalUso() {
        return m_totalUso;
    }

    /**
     * Returns the size of the unreleased data in this block.
     * -4 due to the length prefix that isn't part of the USO
     */
    long unreleasedSize()
    {
        return totalUso() - m_releaseOffset;
    }

    // The USO for octets up to which are being released
    void releaseUso(long releaseUso)
    {
        assert(releaseUso >= m_uso);
        m_releaseOffset = releaseUso - m_uso;
        assert(m_releaseOffset <= totalUso());
    }

    boolean isPersisted() {
        return m_isPersisted;
    }

    private final long m_uso;
    private final long m_totalUso;
    private BBContainer m_buffer;
    private long m_releaseOffset;

    /*
     * True if this block is still backed by a file and false
     * if the buffer is only stored in memory. No guarantees about fsync though
     */
    private final boolean m_isPersisted;

    ByteBuffer unreleasedBuffer() {
        ByteBuffer responseBuffer = ByteBuffer.allocate((int)(4 + unreleasedSize()));
        responseBuffer.order(ByteOrder.LITTLE_ENDIAN);
        responseBuffer.putInt((int)unreleasedSize());
        responseBuffer.order(ByteOrder.BIG_ENDIAN);
        m_buffer.b.position((int)m_releaseOffset);
        responseBuffer.put(m_buffer.b);
        responseBuffer.flip();
        return responseBuffer;
    }

    BBContainer[] asBufferChain() {
        ByteBuffer usoBuffer = ByteBuffer.allocate(8);
        usoBuffer.putLong(uso()).flip();
        BBContainer cont = DBBPool.wrapBB(usoBuffer);
        return new BBContainer[] { cont, m_buffer };
    }
}
