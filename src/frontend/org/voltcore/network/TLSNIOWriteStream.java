/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;

import io.netty_voltpatches.buffer.CompositeByteBuf;
import io.netty_voltpatches.buffer.Unpooled;

public class TLSNIOWriteStream extends VoltNIOWriteStream {

    private final ConcurrentLinkedDeque<EncryptFrame> m_encrypted = new ConcurrentLinkedDeque<>();

    private final CompositeByteBuf m_outbuf;
    private int m_queuedBytes = 0;
    private final TLSEncryptionAdapter m_tlsEncryptAdapter;

    public TLSNIOWriteStream(Connection connection, Runnable offBackPressureCallback,
            Runnable onBackPressureCallback, QueueMonitor monitor,
            SSLEngine engine, CipherExecutor cipherExecutor) {
        super(connection, offBackPressureCallback, onBackPressureCallback, monitor);
        m_outbuf = Unpooled.compositeBuffer();
        m_tlsEncryptAdapter = new TLSEncryptionAdapter(connection, engine, cipherExecutor, m_encrypted);
    }

    @Override
    int serializeQueuedWrites(NetworkDBBPool pool) throws IOException {
        m_tlsEncryptAdapter.checkForGatewayExceptions();

        final int frameMax = Math.min(CipherExecutor.FRAME_SIZE, m_tlsEncryptAdapter.applicationBufferSize());
        final Deque<DeferredSerialization> oldlist = getQueuedWrites();
        if (oldlist.isEmpty()) return 0;

        Pair<Integer, Integer> processedWrites = m_tlsEncryptAdapter.encryptBuffers(oldlist, frameMax);

        updateQueued(processedWrites.getSecond(), true);
        return processedWrites.getFirst();
    }

    public void waitForPendingEncrypts() throws IOException {
        m_tlsEncryptAdapter.waitForPendingEncrypts();
    }

    @Override
    public void updateQueued(int queued, boolean noBackpressureSignal) {
        super.updateQueued(queued, noBackpressureSignal);
        m_queuedBytes += queued;
    }

    private final List<EncryptFrame> m_partial = new ArrayList<>();
    private volatile int m_partialSize = 0;

    static final class EncryptLedger {
        final int delta;
        final int bytes;

        EncryptLedger(int aDelta, int aBytes) {
            delta = aDelta;
            bytes = aBytes;
        }
    }
    /**
     * Gather all the frames that comprise a whole Volt Message
     */
    private EncryptLedger addFramesForCompleteMessage() {
        boolean added = false;
        EncryptFrame frame = null;
        int bytes = 0;
        int delta = 0;

        while (!added && (frame = m_encrypted.poll()) != null) {
            if (!frame.isLast()) {
                synchronized(m_partial) {
                    m_partial.add(frame);
                    ++m_partialSize;
                }
                continue;
            }

            final int partialSize = m_partialSize;
            if (partialSize > 0) {
                assert frame.chunks == partialSize + 1
                        : "partial frame buildup has wrong number of preceding pieces";

                synchronized(m_partial) {
                    for (EncryptFrame frm: m_partial) {
                        m_outbuf.addComponent(true, frm.frame);
                        bytes += frm.frame.readableBytes();
                        delta += frm.delta;
                    }
                    m_partial.clear();
                    m_partialSize = 0;
                }
            }
            m_outbuf.addComponent(true, frame.frame);
            bytes += frame.frame.readableBytes();
            delta += frame.delta;

            m_messagesInOutBuf += frame.msgs;
            added = true;
        }
        return added ? new EncryptLedger(delta, bytes) : null;
    }

    @Override
    synchronized public boolean isEmpty() {
        return m_queuedWrites.isEmpty()
            && m_tlsEncryptAdapter.isEmpty()
            && m_encrypted.isEmpty()
            && m_partialSize == 0
            && !m_outbuf.isReadable();
    }

    private int m_messagesInOutBuf = 0;

    @Override
    int drainTo(final GatheringByteChannel channel) throws IOException {
        int written = 0;
        int delta = 0;
        try {
            long rc = 0;
            do {
                m_tlsEncryptAdapter.checkForGatewayExceptions();
                EncryptLedger queued = null;
                // add to output buffer frames that contain whole messages
                while ((queued=addFramesForCompleteMessage()) != null) {
                    delta += queued.delta;
                }

                rc = m_outbuf.readBytes(channel, m_outbuf.readableBytes());
                m_outbuf.discardReadComponents();
                written += rc;

                if (m_outbuf.isReadable()) {
                    if (!m_hadBackPressure) {
                        backpressureStarted();
                    }
                } else if (rc > 0) {
                    m_messagesWritten += m_messagesInOutBuf;
                    m_messagesInOutBuf = 0;
                }

            } while (rc > 0);
        } finally {
            if (    m_outbuf.numComponents() <= 1
                 && m_hadBackPressure
                 && m_queuedWrites.size() <= m_maxQueuedWritesBeforeBackpressure
            ) {
                backpressureEnded();
            }
            if (written > 0 && !isEmpty()) {
                m_lastPendingWriteTime = EstTime.currentTimeMillis();
            } else {
                m_lastPendingWriteTime = -1L;
            }
            if (written > 0) {
                updateQueued(delta-written, false);
                m_bytesWritten += written;
            } else if (delta > 0) {
                updateQueued(delta, false);
            }
        }
        return written;
    }

    String dumpState() {
        return new StringBuilder(256).append("TLSNIOWriteStream[")
                .append("isEmpty()=").append(isEmpty())
                .append(", encrypted.isEmpty()=").append(m_encrypted.isEmpty())
                .append(", encryptionAdapter=").append(m_tlsEncryptAdapter.dumpState())
                .append(", outbuf.readableBytes()=").append(m_outbuf.readableBytes())
                .append("]").toString();
    }

    @Override
    public synchronized int getOutstandingMessageCount() {
        return m_encrypted.size()
             + m_queuedWrites.size()
             + m_partialSize
             + m_outbuf.numComponents();
    }

    @Override
    synchronized void shutdown() {
        m_isShutdown = true;
        DeferredSerialization ds = null;
        while ((ds = m_queuedWrites.poll()) != null) {
            ds.cancel();
        }

        m_tlsEncryptAdapter.shutdown();

        EncryptFrame frame = null;
        while ((frame = m_encrypted.poll()) != null) {
            frame.frame.release();
        }

        for (EncryptFrame ef: m_partial) {
            ef.frame.release();
        }
        m_partial.clear();

        m_outbuf.release();

        // we have to use ledger because we have no idea how much encrypt delta
        // corresponds to what is left in the output buffer
        final int unqueue = -m_queuedBytes;
        updateQueued(unqueue, false);
    }
}
