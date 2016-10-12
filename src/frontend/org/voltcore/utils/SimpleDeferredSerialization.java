package org.voltcore.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SimpleDeferredSerialization implements DeferredSerialization {

    private final ByteBuffer m_message;

    public SimpleDeferredSerialization(ByteBuffer m_message) {
        this.m_message = m_message;
    }

    @Override
    public ByteBuffer serialize(ByteBuffer buf) throws IOException {
        m_message.position(0);
        if (buf.remaining() > m_message.remaining()) {
            buf.put(m_message);
            return null;
        } else {
            ByteBuffer allocated = ByteBuffer.allocate(m_message.remaining());
            allocated.put(m_message);
            return allocated;
        }
    }

    @Override
    public void cancel() {
    }

    @Override
    public int getSerializedSize() throws IOException {
        throw new UnsupportedOperationException();
    }
}
