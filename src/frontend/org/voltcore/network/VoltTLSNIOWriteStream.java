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

package org.voltcore.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.Deque;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;

public class VoltTLSNIOWriteStream extends VoltNIOWriteStream {

    private int m_queuedBytes = 0;
    private final TLSEncryptionAdapter m_tlsEncryptAdapter;

    public VoltTLSNIOWriteStream(Connection connection, Runnable offBackPressureCallback,
            Runnable onBackPressureCallback, QueueMonitor monitor,
            SSLEngine engine, CipherExecutor cipherExecutor) {
        super(connection, offBackPressureCallback, onBackPressureCallback, monitor);
        m_tlsEncryptAdapter = new TLSEncryptionAdapter(connection, engine, cipherExecutor);
    }

    @Override
    int serializeQueuedWrites(NetworkDBBPool pool) throws IOException {
        m_tlsEncryptAdapter.checkForGatewayExceptions();

        final int frameMax = Math.min(CipherExecutor.FRAME_SIZE, m_tlsEncryptAdapter.applicationBufferSize());
        final Deque<DeferredSerialization> oldlist = getQueuedWrites();
        if (oldlist.isEmpty()) {
            return 0;
        }

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

    @Override
    synchronized public boolean isEmpty() {
        return m_queuedWrites.isEmpty()
            && m_tlsEncryptAdapter.isEmpty();
    }

    @Override
    int drainTo(final GatheringByteChannel channel) throws IOException {
        int totalWritten = 0;
        int delta = 0;
        try {
            TLSEncryptionAdapter.EncryptLedger ledger = null;
            do {
                ledger = m_tlsEncryptAdapter.drainEncryptedMessages(channel);
                delta += ledger.encryptedBytesDelta;
                totalWritten += ledger.bytesWritten;
                m_messagesWritten += ledger.messagesWritten;
                if (m_tlsEncryptAdapter.hasOutstandingData()) {
                    if (!m_hadBackPressure) {
                        backpressureStarted();
                    }
                }
            } while (ledger.bytesWritten > 0);
        } finally {
            if (!m_tlsEncryptAdapter.hasOutstandingData()
                 && m_hadBackPressure
                 && m_queuedWrites.size() <= m_maxQueuedWritesBeforeBackpressure
            ) {
                backpressureEnded();
            }
            if (totalWritten > 0 && !isEmpty()) {
                m_lastPendingWriteTime = EstTime.currentTimeMillis();
            } else {
                m_lastPendingWriteTime = -1L;
            }
            if (totalWritten > 0) {
                updateQueued(delta-totalWritten, false);
                m_bytesWritten += totalWritten;
            } else if (delta > 0) {
                updateQueued(delta, false);
            }
        }
        return totalWritten;
    }

    String dumpState() {
        return new StringBuilder(256).append("TLSNIOWriteStream[")
                .append("isEmpty()=").append(isEmpty())
                .append(", encryptionAdapter=").append(m_tlsEncryptAdapter.dumpState())
                .append("]").toString();
    }

    @Override
    public synchronized int getOutstandingMessageCount() {
        return m_tlsEncryptAdapter.getOutstandingMessageCount() + m_queuedWrites.size();
    }

    @Override
    synchronized void shutdown() {
        m_isShutdown = true;
        DeferredSerialization ds = null;
        while ((ds = m_queuedWrites.poll()) != null) {
            ds.cancel();
        }

        m_tlsEncryptAdapter.shutdown();

        // we have to use ledger because we have no idea how much encrypt delta
        // corresponds to what is left in the output buffer
        final int unqueue = -m_queuedBytes;
        updateQueued(unqueue, false);
    }
}
