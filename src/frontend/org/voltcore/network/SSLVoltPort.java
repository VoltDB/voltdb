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

import org.voltcore.utils.ssl.SSLBufferDecrypter;
import org.voltdb.common.Constants;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class SSLVoltPort extends VoltPort {

    private final SSLEngine m_sslEngine;
    private final SSLBufferDecrypter m_sslBufferDecrypter;
    private final ByteBuffer m_sslSrcBuffer;
    private final ByteBuffer m_messageHeader;
    private final byte[] m_twoBytes = new byte[2];

    public SSLVoltPort(VoltNetwork network, InputHandler handler, InetSocketAddress remoteAddress, NetworkDBBPool pool, SSLEngine sslEngine) {
        super(network, handler, remoteAddress, pool);
        this.m_sslEngine = sslEngine;
        m_sslBufferDecrypter = new SSLBufferDecrypter(sslEngine);
        m_sslSrcBuffer = ByteBuffer.allocateDirect(Constants.SSL_CHUNK_SIZE + 128);
        m_messageHeader = ByteBuffer.allocate(5);
    }

    protected void setKey (SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        m_readStream = new NIOReadStream();
        m_writeStream = new SSLNIOWriteStream (
                this,
                m_handler.offBackPressure(),
                m_handler.onBackPressure(),
                m_handler.writestreamMonitor(),
                m_sslEngine);
        m_interestOps = key.interestOps();
    }

    protected void processNextMessage() throws InputHandler.BadMessageLength, IOException {
        while (true) {
            int nextMessageLength = m_handler.getNextMessageLength();
            if (nextMessageLength == 0) {
                if (!m_handler.retrieveNextMessageHeader(readStream(), m_messageHeader)) {
                    break;
                }
                m_messageHeader.flip();
                m_sslSrcBuffer.clear();
                m_sslSrcBuffer.put(m_messageHeader);
                nextMessageLength = getMessageLengthFromHeader(m_messageHeader);
                m_handler.checkMessageLength(nextMessageLength);
                m_handler.setNextMessageLength(nextMessageLength);
                m_sslSrcBuffer.limit(m_sslSrcBuffer.position() + nextMessageLength);
            }
            m_messageHeader.clear();
            if (!m_handler.retrieveNextMessage(readStream(), m_sslSrcBuffer)) {
                break;
            }
            m_handler.setNextMessageLength(0);
            m_sslBufferDecrypter.decrypt(m_sslSrcBuffer);
            m_sslSrcBuffer.clear();
            ByteBuffer message;
            while ((message = m_sslBufferDecrypter.message()) != null) {
                m_handler.handleMessage(message, this);
                m_messagesRead++;
            }
        }
    }

    protected int getMessageLengthFromHeader(ByteBuffer header) {
        header.position(3);
        header.get(m_twoBytes);
        return ((m_twoBytes[0] & 0xff) << 8) | (m_twoBytes[1] & 0xff);
    }
}
