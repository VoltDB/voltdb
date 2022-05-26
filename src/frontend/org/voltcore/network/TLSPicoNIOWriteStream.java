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
import org.voltcore.utils.Pair;

/**
 * PicoNIOWriteStream with TLS enabled.
 * Sends write jobs to encryption adapter and drains them as the encrypted buffers become available from the adapter.
 */
public class TLSPicoNIOWriteStream extends PicoNIOWriteStream {

    private final TLSEncryptionAdapter m_tlsEncryptionAdapter;

    public TLSPicoNIOWriteStream(Connection connection, SSLEngine engine, CipherExecutor cipherExecutor)
    {
        m_tlsEncryptionAdapter = new TLSEncryptionAdapter(connection, engine, cipherExecutor);
    }

    /**
     * Sends the queued writes in this stream to the encryption adapter for encryption.
     */
    @Override
    int serializeQueuedWrites(NetworkDBBPool pool) throws IOException {
        m_tlsEncryptionAdapter.checkForGatewayExceptions();

        final int frameMax = Math.min(CipherExecutor.FRAME_SIZE, m_tlsEncryptionAdapter.applicationBufferSize());
        final Deque<DeferredSerialization> oldlist = getQueuedWrites();
        if (oldlist.isEmpty()) return 0;

        Pair<Integer, Integer> processedWrites = m_tlsEncryptionAdapter.encryptBuffers(oldlist, frameMax);

        updateQueued(processedWrites.getSecond(), true);
        return processedWrites.getFirst();
    }

    /**
     * Uses the encryption adapter to drain any encrypted buffers that are available.
     */
    @Override
    int drainTo(final GatheringByteChannel channel) throws IOException {
        int totalWritten = 0;
        try {
            TLSEncryptionAdapter.EncryptLedger ledger = null;
            do {
                ledger = m_tlsEncryptionAdapter.drainEncryptedMessages(channel);
                totalWritten += ledger.bytesWritten;
                m_messagesWritten += ledger.messagesWritten;
            } while (ledger.bytesWritten > 0);
        } finally {
            if (totalWritten > 0) {
                m_bytesWritten += totalWritten;
            }
        }
        return totalWritten;
    }

    @Override
    synchronized void shutdown() {
        super.shutdown();
        m_tlsEncryptionAdapter.shutdown();
    }
}
