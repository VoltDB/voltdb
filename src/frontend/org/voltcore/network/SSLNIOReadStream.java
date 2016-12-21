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

import org.voltcore.utils.DBBPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

class SSLNIOReadStream extends NIOReadStream {

    int getBytes(ByteBuffer output) {
        int totalBytesCopied = 0;
        DBBPool.BBContainer poolCont = null;
        while (m_totalAvailable > 0 && output.hasRemaining()) {
            if (poolCont == null) {
                poolCont = m_readBBContainers.peekFirst();
                if (poolCont == null) {
                    return totalBytesCopied;
                }
            }
            if (poolCont.b().remaining() > output.remaining()) {
                int bytesToCopy = output.remaining();
                int oldLimit = poolCont.b().limit();
                poolCont.b().limit(poolCont.b().position() + bytesToCopy);
                output.put(poolCont.b());
                poolCont.b().limit(oldLimit);
                totalBytesCopied += bytesToCopy;
                m_totalAvailable -= bytesToCopy;
            } else {
                int bytesToCopy = poolCont.b().remaining();
                output.put(poolCont.b());
                poolCont = m_readBBContainers.pollFirst();
                poolCont.discard();
                poolCont = null;
                totalBytesCopied += bytesToCopy;
                m_totalAvailable -= bytesToCopy;
            }
        }
        return totalBytesCopied;
    }

    int read(ReadableByteChannel channel, int maxBytes, NetworkDBBPool pool) throws IOException {
        int bytesRead = 0;
        int lastRead = 1;
        DBBPool.BBContainer poolCont = null;
        try {
            while (bytesRead < maxBytes && lastRead > 0) {
                if (poolCont == null) {
                    poolCont = pool.acquire();
                    poolCont.b().clear();
                }

                lastRead = channel.read(poolCont.b());

                // EOF, no data read
                if (lastRead < 0 && bytesRead == 0) {
                    return -1;
                }

                //Data read
                if (lastRead > 0) {
                    bytesRead += lastRead;
                    if (!poolCont.b().hasRemaining()) {
                        poolCont.b().flip();
                        m_readBBContainers.addLast(poolCont);
                        poolCont = null;
                    }
                }
            }
        } finally {
            if (poolCont != null) {
                if (poolCont.b().position() > 0) {
                    poolCont.b().flip();
                    m_readBBContainers.addLast(poolCont);
                } else {
                    poolCont.discard();
                }
            }
            if (bytesRead > 0) {
                m_totalAvailable += bytesRead;
            }
        }

        return bytesRead;
    }

}
