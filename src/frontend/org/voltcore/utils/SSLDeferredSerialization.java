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

        ByteBuffer encryptInto = ByteBuffer.allocate(m_src.capacity() * 5);
        ByteBuffer allocated = encrypt(m_src, encryptInto);
        if (allocated != null) {
            int encryptedSize = allocated.remaining();
            ByteBuffer full = ByteBuffer.allocate(encryptedSize + 4);
            full.putInt(encryptedSize);
            full.put(allocated);
            full.flip();
            return full;
        } else {
            int encryptedSize = encryptInto.remaining();
            ByteBuffer full = ByteBuffer.allocate(encryptedSize + 4);
            full.putInt(encryptedSize);
            full.put(encryptInto);
            full.flip();
            return full;
        }

//        int initialDstPosition = dst.position();
//        dst.position(dst.position() + 4);
//        ByteBuffer encryptInto = dst.slice();
//        ByteBuffer allocated = encrypt(m_src, encryptInto);
//        if (allocated == null) {
//            // encrypt used dst, put the length on dst.
//            int amountEncrypted = encryptInto.position();
//            dst.position(initialDstPosition);
//            dst.putInt(amountEncrypted);
//            dst.position(dst.position() + amountEncrypted);
//        } else {
//            // reset the position on dst
//            dst.position(initialDstPosition);
//            // encrypt used allocated, put the length on allocated.
//            int newPosition = allocated.position();
//            allocated.position(0);
//            dst.putInt(newPosition - 4);
//            dst.position(newPosition);
//        }
//        return allocated;
    }

    @Override
    public void cancel() {

    }

    @Override
    public int getSerializedSize() throws IOException {
        throw new UnsupportedOperationException("getSerializedSize is not supported by SSLDeferredSerialization");
    }

    private ByteBuffer encrypt(ByteBuffer src, ByteBuffer dst) throws IOException {
        if (dst == null) {
            ByteBuffer allocated = ByteBuffer.allocate((int) (src.capacity() * 1.2));
            return encryptWithAllocated(src, allocated);
        }
        SSLEngineResult result = m_sslEngine.wrap(src, dst);
        switch (result.getStatus()) {
            case OK:
                // added this
                dst.flip();



                return null;
            case BUFFER_OVERFLOW:
                ByteBuffer allocated = ByteBuffer.allocate((int) (dst.capacity() * 1.2));
                return encryptWithAllocated(src, allocated);
            case BUFFER_UNDERFLOW:
                throw new IOException("Underflow on ssl wrap of buffer.");
            case CLOSED:
                throw new IOException("SSL engine is closed on ssl wrap of buffer.");
            default:
                throw new IOException("Unexpected SSLEngineResult.Status");
        }
    }

    private ByteBuffer encryptWithAllocated(ByteBuffer src, ByteBuffer dst) throws IOException {
        ByteBuffer allocated = dst;
        while (true) {

            // TODO: also changed this:
//            allocated.position(4);
//            ByteBuffer encryptInto = allocated.slice();
//            SSLEngineResult result = m_sslEngine.wrap(src, encryptInto);
            SSLEngineResult result = m_sslEngine.wrap(src, allocated);


            switch (result.getStatus()) {
                case OK:
                    // allocated.position(allocated.position() + 4);
                    // added flip
                    allocated.flip();
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
