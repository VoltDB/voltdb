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

import org.voltcore.utils.ssl.SSLBufferEncrypter;
import org.voltdb.common.Constants;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SSLNIOWriteStream extends NIOWriteStream {

    private final SSLBufferEncrypter m_sslBufferEncrypter;
    private ByteBuffer m_encryptedWriteBuffer;

    public SSLNIOWriteStream(VoltPort port, Runnable offBackPressureCallback, Runnable onBackPressureCallback, QueueMonitor monitor, SSLEngine sslEngine) {
        super(port, offBackPressureCallback, onBackPressureCallback, monitor);
        m_sslBufferEncrypter = new SSLBufferEncrypter(sslEngine);
    }

    @Override
    synchronized public boolean isEmpty()
    {
        return super.isEmpty() && m_encryptedWriteBuffer == null && m_queuedWrites.isEmpty();
    }

    protected ByteBuffer getBufferToWrite() throws IOException {
        if (m_encryptedWriteBuffer != null) {
            return m_encryptedWriteBuffer;
        }
        ByteBuffer writeBuffer;
        if ((writeBuffer = super.getBufferToWrite()) == null) {
            return null;
        }
        ByteBuffer buffToEncrypt;
        if (writeBuffer.remaining() <= Constants.SSL_CHUNK_SIZE) {
            buffToEncrypt = writeBuffer.slice();
            writeBuffer.position(writeBuffer.limit());
        } else {
            int oldLimit = writeBuffer.limit();
            writeBuffer.limit(writeBuffer.position() + Constants.SSL_CHUNK_SIZE);
            buffToEncrypt = writeBuffer.slice();
            writeBuffer.position(writeBuffer.limit());
            writeBuffer.limit(oldLimit);
        }
        m_encryptedWriteBuffer = m_sslBufferEncrypter.encryptBuffer(buffToEncrypt);
        return m_encryptedWriteBuffer;
    }

    protected int discardBuffer() {
        m_encryptedWriteBuffer = null;
        if (!m_currentWriteBuffer.b().hasRemaining()) {
            return super.discardBuffer();
        }
        return 0;
    }

}
