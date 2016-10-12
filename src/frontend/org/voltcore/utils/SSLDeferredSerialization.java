package org.voltcore.utils;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SSLDeferredSerialization implements DeferredSerialization {

    private final SSLEngine m_sslEngine;
    private final ByteBuffer m_src;

    public SSLDeferredSerialization(SSLEngine sslEngine, ByteBuffer src) {
        this.m_sslEngine = sslEngine;
        this.m_src = src;
    }

    @Override
    public ByteBuffer serialize(ByteBuffer dst) throws IOException {
        int initialDstPosition = dst.position();
        dst.position(dst.position() + 4);
        ByteBuffer encryptInto = dst.slice();
        ByteBuffer allocated = encrypt(m_src, encryptInto);
        if (allocated == null) {
            // encrypt used dst, put the length on dst.
            int amountEncrypted = encryptInto.position();
            dst.position(initialDstPosition);
            dst.putInt(amountEncrypted);
            dst.position(dst.position() + amountEncrypted);
        } else {
            // reset the position on dst
            dst.position(initialDstPosition);
            // encrypt used allocated, put the length on allocated.
            int newPosition = allocated.position();
            allocated.position(0);
            dst.putInt(newPosition - 4);
            dst.position(newPosition);
        }
        return allocated;
    }

    @Override
    public void cancel() {

    }

    @Override
    public int getSerializedSize() throws IOException {
        throw new UnsupportedOperationException("getSerializedSize is not supported by SSLDeferredSerialization");
    }

    private ByteBuffer encrypt(ByteBuffer src, ByteBuffer dst) throws IOException {
        SSLEngineResult result = m_sslEngine.wrap(src, dst);
        switch (result.getStatus()) {
            case OK:
                return null;
            case BUFFER_OVERFLOW:
                return encryptWithAllocated(src);
            case BUFFER_UNDERFLOW:
                throw new IOException("Underflow on ssl wrap of buffer.");
            case CLOSED:
                throw new IOException("SSL engine is closed on ssl wrap of buffer.");
            default:
                throw new IOException("Unexpected SSLEngineResult.Status");
        }
    }

    private ByteBuffer encryptWithAllocated(ByteBuffer src) throws IOException {
        ByteBuffer allocated = ByteBuffer.allocate((int) (src.capacity() * 1.2));
        while (true) {
            allocated.position(4);
            SSLEngineResult result = m_sslEngine.wrap(src, allocated.slice());
            switch (result.getStatus()) {
                case OK:
                    return allocated;
                case BUFFER_OVERFLOW:
                    allocated = ByteBuffer.allocate(allocated.capacity() * 2);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new IOException("Underflow on ssl wrap of buffer.");
                case CLOSED:
                    throw new IOException("SSL engine is closed on ssl wrap of buffer.");
                default:
                    throw new IOException("Unexpected SSLEngineResult.Status");
            }

        }
    }
}
