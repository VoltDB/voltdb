package org.voltcore.utils.ssl;

import org.voltcore.utils.DBBPool;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 *
 */
public class SSLMessagingChannel extends MessagingChannel {

    private final SSLEngine m_sslEngine;
    private final DBBPool.BBContainer m_encCont;
    private final ByteBuffer m_encBuffer;
    private final DBBPool.BBContainer m_dstCont;
    private final ByteBuffer m_dstBuffer;
    private final SSLBufferDecrypter m_sslBufferDescrypter;
    private final SSLMessageParser m_sslMessageParser;

    SSLMessagingChannel(SocketChannel socketChannel, SSLEngine sslEngine) {
        super(socketChannel);
        this.m_sslEngine = sslEngine;
        int packetBufferSize = m_sslEngine.getSession().getPacketBufferSize();
        this.m_encCont = DBBPool.allocateDirectAndPool(packetBufferSize);
        this.m_encBuffer = m_encCont.b();
        m_encBuffer.clear();
        int applicationBufferSize = m_sslEngine.getSession().getApplicationBufferSize();
        this.m_dstCont = DBBPool.allocateDirectAndPool(applicationBufferSize);
        this.m_dstBuffer = m_dstCont.b();
        m_dstBuffer.clear();
        this.m_sslBufferDescrypter = new SSLBufferDecrypter(sslEngine);
        this.m_sslMessageParser = new SSLMessageParser();
    }

    public ByteBuffer readMessage() throws IOException {
        ByteBuffer frameHeader ;

        m_encBuffer.clear();
        int read;
        read = m_socketChannel.read(m_encBuffer);
        if (read == -1) {
            throw new IOException("Failed to read message");
        }
        m_encBuffer.flip();
        // there will always be a length, need to have more than four bytes to have a message
        if (read > 4) {
            m_sslBufferDescrypter.unwrap(m_encBuffer, m_dstBuffer);
            if (m_dstBuffer.hasRemaining()) {
                return m_sslMessageParser.message(m_dstBuffer);
            }
        }
        return null;
    }

    public int writeMessage(ByteBuffer message) throws IOException {
        ByteBuffer wrapped = wrapMessage(message);
        return super.writeMessage(wrapped);
    }

    public void shutdown() {
        m_encCont.discard();
        m_dstCont.discard();
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
                    throw new IOException("Unexpected overflow on ssl wrapMessage of buffer.");
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
