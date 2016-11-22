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

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

public class SSLBufferDecrypter {

    private final SSLEngine m_sslEngine;

    public SSLBufferDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
    }

    public ByteBuffer unwrap(ByteBuffer srcBuffer, ByteBuffer dstBuffer) throws IOException {
        // save initial state of dst buffer in case of underflow.
        int initialDstPos = dstBuffer.position();
        while (true) {
            SSLEngineResult result = m_sslEngine.unwrap(srcBuffer, dstBuffer.slice());
            switch (result.getStatus()) {
                case OK:
                    // in m_dstBuffer, newly decrtyped data is between pos and lim
                    if (result.bytesProduced() > 0) {
                        dstBuffer.limit(dstBuffer.position() + result.bytesProduced());
                        return dstBuffer;
                        }
                    else {
                        continue;
                    }
                case BUFFER_OVERFLOW:
                    // the dst buffer holds partial volt messages, so its state needs to
                    // be retained on overflow.
                    ByteBuffer tmp = ByteBuffer.allocateDirect(dstBuffer.capacity() << 1);
                    dstBuffer.position(0);
                    tmp.put(dstBuffer);
                    tmp.position(initialDstPos);
                    dstBuffer = tmp;
                    break;
                case BUFFER_UNDERFLOW:
                    // on underflow, want to read again.  There are unprocessed bytes up to limit.
                    // reset the buffers to their state prior to the underflow.
                    srcBuffer.position(srcBuffer.limit());
                    srcBuffer.limit(srcBuffer.capacity());
                    dstBuffer.position(initialDstPos);
                    dstBuffer.limit(initialDstPos);
                    return dstBuffer;
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }
}
