/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltcore.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;

import org.voltcore.utils.DBBPool.BBContainer;

/**
Provides a non-blocking stream-like interface on top of the Java NIO ReadableByteChannel. It calls
the underlying read() method only when needed.
*/
public class NIOReadStream {

    /** @returns the number of bytes available to be read. */
    public int dataAvailable() {
        return m_totalAvailable;
    }

    int getInt() {
        // TODO: Optimize?
        byte[] intbytes = new byte[4];
        getBytes(intbytes);
        int output = 0;
        for (int i = 0; i < intbytes.length; ++i) {
            output <<= 8;
            output |= (intbytes[i]) & 0xff;
        }
        return output;
    }

    /**
     * Move all bytes in current read buffers to output array, free read buffers
     * back to thread local memory pool.
     * @param output
     */
    void getBytes(byte[] output) {
        if (m_totalAvailable < output.length) {
            throw new IllegalStateException("Requested " + output.length + " bytes; only have "
                    + m_totalAvailable + " bytes; call tryRead() first");
        }

        int bytesCopied = 0;
        while (bytesCopied < output.length) {
            BBContainer firstC = m_readBBContainers.peekFirst();
            if (firstC == null) {
                // Steal the write buffer
                m_poolBBContainer.b().flip();
                m_readBBContainers.add(m_poolBBContainer);
                firstC = m_poolBBContainer;
                m_poolBBContainer = null;
            }
            ByteBuffer first = firstC.b();
            assert first.remaining() > 0;

            // Copy bytes from first into output
            int bytesRemaining = first.remaining();
            int bytesToCopy = output.length - bytesCopied;
            if (bytesToCopy > bytesRemaining) bytesToCopy = bytesRemaining;
            first.get(output, bytesCopied, bytesToCopy);
            bytesCopied += bytesToCopy;
            m_totalAvailable -= bytesToCopy;

            if (first.remaining() == 0) {
                // read an entire block: move it to the empty buffers list
                m_readBBContainers.poll();
                firstC.discard();
            }
        }
    }

    /**
     * Read at most maxBytes from the network. Will read until the network would
     * block, the stream is closed or the maximum bytes to read is reached.
     * @param maxBytes
     * @return -1 if closed otherwise total buffered bytes. In all cases,
     * data may be buffered in the stream - even when the channel is closed.
     */
    final int read(ReadableByteChannel channel, int maxBytes, NetworkDBBPool pool) throws IOException {
        int bytesRead = 0;
        int lastRead = 1;
        try {
            while (bytesRead < maxBytes && lastRead > 0) {
                ByteBuffer poolBuffer = null;
                if (m_poolBBContainer == null) {
                    m_poolBBContainer = pool.acquire();
                    poolBuffer = m_poolBBContainer.b();
                    poolBuffer.clear();
                } else {
                    poolBuffer = m_poolBBContainer.b();
                }

                lastRead = channel.read(poolBuffer);

                // EOF, no data read
                if (lastRead < 0 && bytesRead == 0) {
                    if (poolBuffer.position() == 0) {
                        m_poolBBContainer.discard();
                        m_poolBBContainer = null;
                    }
                    return -1;
                }

                //Data read
                if (lastRead > 0) {
                    bytesRead += lastRead;
                    if (!poolBuffer.hasRemaining()) {
                        poolBuffer.flip();
                        m_readBBContainers.add(m_poolBBContainer);
                        m_poolBBContainer = null;
                    } else {
                        break;
                    }
                } else if (poolBuffer.position() == 0) {
                    m_poolBBContainer.discard();
                    m_poolBBContainer = null;
                }
            }
        } finally {
            if (bytesRead > 0) {
                m_bytesRead += bytesRead;
                m_totalAvailable += bytesRead;
            }
        }

        return bytesRead;
    }

    void shutdown() {
        for (BBContainer c : m_readBBContainers) {
            c.discard();
        }
        if (m_poolBBContainer != null) {
            m_poolBBContainer.discard();
        }
        m_readBBContainers.clear();
        m_poolBBContainer = null;
    }

    private final ArrayDeque<BBContainer> m_readBBContainers = new ArrayDeque<BBContainer>();
    private BBContainer m_poolBBContainer = null;
    private int m_totalAvailable = 0;
    private long m_bytesRead = 0;
    private long m_lastBytesRead = 0;

    long getBytesRead(boolean interval) {
        if (interval) {
            final long bytesRead = m_bytesRead;
            final long bytesReadThisTime = bytesRead - m_lastBytesRead;
            m_lastBytesRead = bytesRead;
            return bytesReadThisTime;
        } else {
            return m_bytesRead;
        }
    }
}
