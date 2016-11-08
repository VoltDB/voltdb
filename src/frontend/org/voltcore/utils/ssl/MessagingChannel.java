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

package org.voltcore.utils.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import static org.voltdb.common.Constants.SSL_CHUNK_SIZE;

/**
 * Wraps a SocketChannel, knows how to read and write VoltDB
 * protocol messages.  Does SSL encryption and decryption of messages
 * if SSL is enabled.
 */
public class MessagingChannel {

    private final SocketChannel m_socketChannel;
    private final SSLEngine m_sslEngine;
    private ByteBuffer m_encBuffer;
    private final ByteBuffer m_readBuffer;
    private final SSLBufferDecrypter m_sslBufferDescrypter;

    public MessagingChannel(SocketChannel m_socketChannel, SSLEngine sslEngine) {
        this.m_socketChannel = m_socketChannel;
        this.m_sslEngine = sslEngine;
        if (sslEngine != null) {
            int packetBufferSize = m_sslEngine.getSession().getPacketBufferSize();
            // wrapMessage will overflow until the encryption buffer is this size.
            this.m_encBuffer = ByteBuffer.allocate(packetBufferSize);
            this.m_readBuffer = ByteBuffer.allocate(16 * 1024);
            this.m_sslBufferDescrypter = new SSLBufferDecrypter(sslEngine);
        } else {
            this.m_readBuffer = null;
            this.m_sslBufferDescrypter = null;
        }
    }

    public ByteBuffer readMessage() throws IOException {
        return m_sslEngine == null ? readClear() : readEncrypted();
    }

    private ByteBuffer readClear() throws IOException {
        int read;
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        for (int i = 0; i < 4 && lengthBuffer.hasRemaining(); i++) {
            read = m_socketChannel.read(lengthBuffer);
            if (read == -1) {
                throw new IOException("Failed to read message length");
            }
        }
        lengthBuffer.flip();
        int len = lengthBuffer.getInt();
        ByteBuffer message = ByteBuffer.allocate(len);
        while (message.hasRemaining()) {
            read = m_socketChannel.read(message);
            if (read == -1) {
                throw new IOException("Failed to read message");
            }
        }
        message.flip();
        return message;
    }

    private ByteBuffer readEncrypted() throws IOException {
        int read;
        read = m_socketChannel.read(m_readBuffer);
        if (read == -1) {
            throw new IOException("Failed to read message");
        }
        // there will always be a length, need to have more than four bytes to have a message
        if (read > 4) {
            m_readBuffer.flip();
            List<ByteBuffer> messages = m_sslBufferDescrypter.decryptBuffer(m_readBuffer);
            if (messages != null) {
                if (messages.size() > 1) {
                    throw new IOException("Unexpectedly read more than one message");
                }
                return messages.get(0);
            }
        }
        return null;
    }

    public int writeMessage(ByteBuffer message) throws IOException {
        message = m_sslEngine == null ? message : wrapMessage(message);
        int bytesWritten = 0;
        for (int i = 0; i < 4 && message.hasRemaining(); i++) {
            bytesWritten += m_socketChannel.write(message);
        }
        if (message.hasRemaining()) {
            throw new IOException("Unable to write message");
        }
        return bytesWritten;
    }

    private ByteBuffer wrapMessage(ByteBuffer src) throws IOException {
        m_encBuffer.clear();
        while (true) {
            SSLEngineResult result = m_sslEngine.wrap(src, m_encBuffer);
            switch (result.getStatus()) {
                case OK:
                    m_encBuffer.flip();
                    return m_encBuffer;
                case BUFFER_OVERFLOW:
                    m_encBuffer = ByteBuffer.allocate(m_encBuffer.capacity() << 1);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new IOException("Underflow on ssl wrapMessage of buffer.");
                case CLOSED:
                    throw new IOException("SSL engine is closed on ssl wrapMessage of buffer.");
                default:
                    throw new IOException("Unexpected SSLEngineResult.Status");
            }
        }
    }
}
