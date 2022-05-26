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

package org.voltcore.utils.ssl;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

import org.voltcore.network.VoltPort;

/**
 * Wraps a SocketChannel, knows how to read and write VoltDB
 * protocol messages.  Does SSL encryption and decryption of messages
 * if SSL is enabled.
 */
public class MessagingChannel {

    public static MessagingChannel get(SocketChannel socketChannel, SSLEngine sslEngine) {
        if (sslEngine == null) {
            return new MessagingChannel(socketChannel);
        }
        return new TLSMessagingChannel(socketChannel, sslEngine);
    }

    protected final SocketChannel m_socketChannel;

    MessagingChannel(SocketChannel socketChannel) {
        this.m_socketChannel = socketChannel;
    }

    public SocketChannel getSocketChannel() {
        return m_socketChannel;
    }

    public ByteBuffer readBytes(int numBytes) throws IOException {
        ByteBuffer message = ByteBuffer.allocate(numBytes);
        while (message.hasRemaining()) {
            int read = m_socketChannel.read(message);
            if (read == -1) {
                throw new IOException("Failed to read message");
            }
        }
        assert message.position() == numBytes : "Bytes read is at an unexpected position. " + numBytes + "!=" + message.position();
        message.flip();
        return message;
    }

    public ByteBuffer readMessage() throws IOException {
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
        if (len <= 0) {
            throw new IOException("Packet size is invalid");
        }
        if (len > VoltPort.MAX_MESSAGE_LENGTH) {
            throw new IOException("Packet exceeds maximum allowed size");
        }
        return readBytes(len);
    }

    public int writeMessage(ByteBuffer message) throws IOException {
        int bytesWritten = 0;
        for (int i = 0; i < 4 && message.hasRemaining(); i++) {
            bytesWritten += m_socketChannel.write(message);
        }
        if (message.hasRemaining()) {
            throw new IOException("Unable to write message");
        }
        return bytesWritten;
    }

    public void cleanUp() {}
}
