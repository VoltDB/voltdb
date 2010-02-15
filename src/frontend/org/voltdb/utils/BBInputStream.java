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

package org.voltdb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

import org.voltdb.utils.DBBPool.BBContainer;

/**
 * InputStream implementation that can present data stored in ByteBuffers
 */
public class BBInputStream extends InputStream {

    /**
     * ByteBuffer containers to be used as a source of data for this input stream.
     */
    private final ArrayDeque<BBContainer> containers = new ArrayDeque<BBContainer>();

    /**
     * Container for the ByteBuffer that is being used as a source of bytes.
     */
    private BBContainer cContainer;

    /**
     * Current ByteBuffer being used as a source of data.
     */
    private ByteBuffer cBuffer;

    /**
     * Set to true when no more buffers are going to be offered.
     */
    private boolean m_eof = false;

    /**
     * Number of bytes available for reading without blocking
     */
    private int m_available = 0;

    @Override
    public int available() throws IOException {
        return m_available;
    }

    @Override
    public int read() throws IOException {
        if (cBuffer == null) {
            getNextBuffer(true);
            if (cBuffer == null) {
                assert m_eof;
                return -1;
            }
        }
        int retval = cBuffer.get();
        if (!cBuffer.hasRemaining()) {
            cBuffer = null;
            cContainer.discard();
            cContainer = null;
        }
        m_available--;
        return retval;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        // total bytes written to b[], the return value.
        int consumed = 0;

        while (consumed < len) {
            // no current buffer? get next or block on eof.
            if (cBuffer == null) {
                getNextBuffer(true);
                if (cBuffer == null) {
                    assert m_eof;
                    assert(m_available == 0);
                    // partial reads return bytes read, otherwise eof.
                    return (consumed > 0) ? consumed : -1;
                }
            }

            assert (cBuffer != null);
            final int maxread = java.lang.Math.min(cBuffer.remaining(), (len-consumed));
            cBuffer.get(b, (off + consumed), maxread);
            consumed += maxread;
            m_available -= maxread;
            assert (m_available > -1);

            // cleanup completed buffers
            if (!cBuffer.hasRemaining()) {
                cBuffer = null;
                cContainer.discard();
                cContainer = null;
            }
        }
        return consumed;
    }

    /**
     * Get the next buffer container from the containers Deque. If there are no containers in the Deque
     * and block is true then the call will block until a buffer is available or EOF is received.
     * @param block
     */
    private void getNextBuffer(boolean block) {
        synchronized(containers) {
            if (containers.isEmpty()) {
                if (block == false) {
                    return;
                }

                while(containers.isEmpty() && !m_eof) {
                    try {
                        containers.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                assert !containers.isEmpty() || m_eof;
                if (containers.isEmpty()) {
                    return;
                }
            }
            cContainer = containers.poll();
            cBuffer = cContainer.b;
            assert cBuffer.hasRemaining();
        }
    }

    /**
     * Offer a ByteBuffer as a source of data for this InputStream
     * @param c ByteBuffer container to be used as a source of data
     */
    public void offer(BBContainer c) {
        assert c.b != null && c.b.hasRemaining();
        synchronized (containers) {
            containers.offer(c);
            m_available += c.b.remaining();
            containers.notifyAll();
        }
    }

    /**
     * Indicate that there are no more ByteBuffers coming and that the stream should return -1
     * after all the data has been read.
     */
    public void EOF() {
        assert m_eof == false;
        synchronized (containers) {
            m_eof = true;
            containers.notifyAll();
        }
    }

    public void close() {
        cBuffer = null;
        cContainer = null;
        for (BBContainer c : containers) {
            c.discard();
        }
        containers.clear();
    }
}
