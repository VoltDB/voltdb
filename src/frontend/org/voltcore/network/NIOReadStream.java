/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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
import java.util.BitSet;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.voltcore.utils.DBBPool.BBContainer;

import com.google_voltpatches.common.collect.ImmutableList;

/**
Provides a non-blocking stream-like interface on top of the Java NIO ReadableByteChannel. It calls
the underlying read() method only when needed.
*/
public class NIOReadStream {

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

    Slice getSlice(final int size) {
        if (size < 0) {
            throw new IllegalArgumentException("negative slice size: " + size);
        }
        if (m_totalAvailable < size) {
            throw new IllegalStateException("Requested " + size + " bytes; only have "
                    + m_totalAvailable + " bytes; call tryRead() first");
        }

        ImmutableList.Builder<ContainerSlice> slices = ImmutableList.builder();
        int bytesSliced = 0;
        while (bytesSliced < size) {
            BBContainer firstC = m_readBBContainers.peekFirst();
            if (firstC == null) {
                // Steal the write buffer
                m_poolBBContainer.b().flip();
                m_readBBContainers.add(m_poolBBContainer);
                firstC = m_poolBBContainer;
                m_poolBBContainer = null;
            }
            ByteBuffer first = firstC.b();
            assert first.remaining() > 0 : "no remaining bytes to read";

            int bytesRemaining = first.remaining();
            int bytesToCopy = size - bytesSliced;

            if (bytesToCopy > bytesRemaining) {
                bytesToCopy = bytesRemaining;
            }
            slices.add(new ContainerSlice(firstC, bytesToCopy));
            first.position(first.position() + bytesToCopy);

            bytesSliced += bytesToCopy;
            m_totalAvailable -= bytesToCopy;
            if (first.remaining() == 0) {
                m_readBBContainers.poll();
            }
        }
        return new Slice(slices.build());
    }

    void peekBytes(byte [] output) {
        if (m_totalAvailable < output.length) {
            throw new IllegalStateException("Requested " + output.length + " bytes; only have "
                    + m_totalAvailable + " bytes; call tryRead() first");
        }
        int bytesPeeked = 0;
        Iterator<BBContainer> cntnritr = m_readBBContainers.iterator();
        while (bytesPeeked < output.length) {
            final ByteBuffer slice;
            if (!cntnritr.hasNext()) {
                slice = m_poolBBContainer.b().duplicate();
                slice.flip();
            } else {
                slice = cntnritr.next().b().slice();
            }
            // Copy bytes from first into output
            int bytesRemaining = slice.remaining();
            int bytesToCopy = output.length - bytesPeeked;
            if (bytesToCopy > bytesRemaining) bytesToCopy = bytesRemaining;
            slice.get(output, bytesPeeked, bytesToCopy);
            bytesPeeked += bytesToCopy;
        }
    }

    int read(ReadableByteChannel channel, int maxBytes, NetworkDBBPool pool) throws IOException {
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

    private final Deque<BBContainer> m_readBBContainers = new ArrayDeque<BBContainer>();
    private BBContainer m_poolBBContainer = null;
    protected int m_totalAvailable = 0;
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

    /**
     * Component class to {@link Slice} that encompasses a
     * {@link BBContainer}
     */
    private static final class ContainerSlice {
        private final BBContainer bbc;
        private final ByteBuffer bb;
        private final boolean discard;
        private final int size;
        private ContainerSlice(BBContainer bbc, final int size) {
            discard = size == bbc.b().remaining();
            ByteBuffer slice = bbc.b().slice();
            slice.limit(size);
            bb = slice;
            this.bbc = bbc;
            this.size = size;
        }
    }

    /**
     * A means to defer {@link BBContainer#discard()}. When
     * the data is read from {@link NIOReadStream#m_readBBContainers} it may
     * span one or more {@link BBContainer}. This class collects them, and
     * uses a {@link io.netty.buffer.CompositeByteBuf} to map
     * them for easy read access
     */
    public static final class Slice {
        private final List<ContainerSlice> m_slices;
        public final io.netty.buffer.ByteBuf bb;
        private final BitSet m_discarded;

        private Slice(List<ContainerSlice> slices) {
            ByteBuffer [] bbs = new ByteBuffer[slices.size()];
            m_discarded = new BitSet(slices.size());
            for (int i = 0; i < slices.size(); ++i) {
                bbs[i] = slices.get(i).bb;
            }
            bb = io.netty.buffer.Unpooled.wrappedBuffer(bbs);
            m_slices = slices;
        }

        public Slice markConsumed() {
            if (bb.isReadable()) {
                bb.readerIndex(bb.writerIndex());
            }
            return this;
        }

        public int discard() {
            int discarded = 0;
            int size = 0;
            for (int i = 0; i < m_slices.size(); ++i) {
                ContainerSlice slc = m_slices.get(i);
                size += slc.size;
                if (m_discarded.get(i)) continue;
                if (slc.discard && bb.readerIndex() >= size) {
                    slc.bbc.discard();
                    m_discarded.set(i);
                    discarded += 1;
                }
            }
            return discarded;
        }
    }
}
