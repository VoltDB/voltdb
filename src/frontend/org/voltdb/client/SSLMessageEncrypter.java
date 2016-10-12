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

package org.voltdb.client;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SSLMessageEncrypter {

    public static ByteBuffer encrypt(SSLEngine sslEngine, ByteBuffer src, ByteBuffer dst) throws IOException {
        SSLEngineResult result = sslEngine.wrap(src, dst);
        switch (result.getStatus()) {
            case OK:
                dst.flip();
                return null;
            case BUFFER_OVERFLOW:
                return encryptWithAllocated(sslEngine, src);
            case BUFFER_UNDERFLOW:
                throw new IOException("Underflow on ssl wrap of buffer.");
            case CLOSED:
                throw new IOException("SSL engine is closed on ssl wrap of buffer.");
            default:
                throw new IOException("Unexpected SSLEngineResult.Status");
        }
    }

    private static ByteBuffer encryptWithAllocated(SSLEngine sslEngine, ByteBuffer src) throws IOException {
        ByteBuffer allocated = ByteBuffer.allocate((int) (src.capacity() * 1.2));
        while (true) {
            SSLEngineResult result = sslEngine.wrap(src, allocated);
            switch (result.getStatus()) {
                case OK:
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
