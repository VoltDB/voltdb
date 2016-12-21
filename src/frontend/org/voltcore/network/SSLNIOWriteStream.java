/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltcore.network;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;

public class SSLNIOWriteStream extends NIOWriteStream {

    public SSLNIOWriteStream(VoltPort port, Runnable offBackPressureCallback, Runnable onBackPressureCallback, QueueMonitor monitor) {
        super(port, offBackPressureCallback, onBackPressureCallback, monitor);
    }

    DBBPool.BBContainer getWriteBuffer() {
        return m_queuedBuffers.poll();
    }

    @Override
    int serializeQueuedWrites(final NetworkDBBPool pool) throws IOException {
        int processedWrites = 0;
        final Deque<DeferredSerialization> oldlist = getQueuedWrites();
        if (oldlist.isEmpty()) return 0;

        DeferredSerialization ds = null;
        int bytesQueued = 0;
        DBBPool.BBContainer outCont = pool.acquire();
        outCont.b().clear();
        while ((ds = oldlist.poll()) != null) {
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) continue;
            processedWrites++;
            if (outCont.b().remaining() >= serializedSize) {
                // Fast path, serialize to direct buffer creating no garbage
                final int oldLimit = outCont.b().limit();
                outCont.b().limit(outCont.b().position() + serializedSize);
                final ByteBuffer slice = outCont.b().slice();
                ds.serialize(slice);
                checkSloppySerialization(slice, ds);
                slice.position(0);
                bytesQueued += slice.remaining();
                outCont.b().position(outCont.b().limit());
                outCont.b().limit(oldLimit);
            } else {
                // Slow path serialize to heap, and then put in buffers
                ByteBuffer buf = ByteBuffer.allocate(serializedSize);
                ds.serialize(buf);
                checkSloppySerialization(buf, ds);
                buf.position(0);
                bytesQueued += buf.remaining();
                // Copy data allocated in heap buffer to direct buffer
                while (buf.hasRemaining()) {
                    if (!outCont.b().hasRemaining()) {
                        m_queuedBuffers.offer(outCont);
                        outCont = pool.acquire();
                        outCont.b().clear();
                    }
                    if (outCont.b().remaining() >= buf.remaining()) {
                        outCont.b().put(buf);
                    } else {
                        final int oldLimit = buf.limit();
                        buf.limit(buf.position() + outCont.b().remaining());
                        outCont.b().put(buf);
                        buf.limit(oldLimit);
                    }
                }
            }
        }

        if (outCont != null) {
            if (outCont.b().position() > 0) {
                m_queuedBuffers.offer(outCont);
            } else {
                outCont.discard();
            }
        }
        updateQueued(bytesQueued, true);
        return processedWrites;
    }

    public void updateWriteState(int bytesWritten) {
        //We might fail after writing few bytes. make sure the ones that are written accounted for.
        //Not sure if we need to do any backpressure magic as client is dead and so no backpressure on this may be needed.
        if (m_queuedBuffers.isEmpty() && m_hadBackPressure && m_queuedWrites.size() <= m_maxQueuedWritesBeforeBackpressure) {
            backpressureEnded();
        }
        //Same here I dont know if we do need to do this housekeeping??
        if (!isEmpty()) {
            if (bytesWritten > 0) {
                m_lastPendingWriteTime = EstTime.currentTimeMillis();
            }
        } else {
            m_lastPendingWriteTime = -1;
        }
        if (bytesWritten > 0) {
            m_bytesWritten += bytesWritten;
        }
    }

}
