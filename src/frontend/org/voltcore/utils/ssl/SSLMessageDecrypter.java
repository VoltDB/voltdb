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
import java.util.ArrayList;
import java.util.List;

import static org.voltdb.common.Constants.SSL_CHUNK_SIZE;

/**
 * Doesn't support messages bigger then SSL_CHUNK_SIZE
 */
public class SSLMessageDecrypter {

    private final SSLEngine m_sslEngine;

    public SSLMessageDecrypter(SSLEngine m_sslEngine) {
        this.m_sslEngine = m_sslEngine;
    }

    public ByteBuffer[] decryptMessage(ByteBuffer buffer) throws IOException {
        List<ByteBuffer> messages = new ArrayList<>();
        ByteBuffer dst = ByteBuffer.allocate(buffer.remaining());
        unwrapMessage(buffer, dst);

        dst.flip();
        while (dst.hasRemaining()) {
            int length = dst.getInt();
            if (length > dst.remaining()) {
                throw new IOException("no partial messages yet");
            }
            int oldLimit = dst.limit();
            dst.limit(dst.position() + length);
            messages.add(buffer.slice());
            dst.position(dst.limit());
            dst.limit(oldLimit);
        }
        return messages.toArray(new ByteBuffer[messages.size()]);
    }

    private void unwrapMessage(ByteBuffer message, ByteBuffer dst) throws IOException {
        while (true) {
            SSLEngineResult result = m_sslEngine.unwrap(message, dst);
            switch (result.getStatus()) {
                case OK:
                    return;
                case BUFFER_OVERFLOW:
                    dst = ByteBuffer.allocate(dst.capacity() * 2);
                    break;  // try again
                case BUFFER_UNDERFLOW:
                    throw new SSLException("SSL engine should never underflow on ssl unwrapMessage of buffer.");
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrapMessage of buffer.");
            }
        }
    }
}
