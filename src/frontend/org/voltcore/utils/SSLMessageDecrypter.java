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

package org.voltcore.utils;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.voltdb.common.Constants.SSL_CHUNK_SIZE;

public class SSLMessageDecrypter {

    private final SSLEngine sslEngine;
    private ByteBuffer decryptionBuffer;
    private boolean needsChunk;  // true when a message is partially processed
    private int fullMessageSize;
    private ByteBuffer fullMessage;

    public SSLMessageDecrypter(SSLEngine sslEngine) {
        this.sslEngine = sslEngine;
        this.decryptionBuffer = ByteBuffer.allocate((int) (SSL_CHUNK_SIZE * 1.2));
        this.needsChunk = false;
    }

    public void initialize(ByteBuffer initialChunk) throws IOException {
        decryptionBuffer.clear();
        while (true) {
            SSLEngineResult result;
            synchronized (sslEngine) {
                result = sslEngine.unwrap(initialChunk, decryptionBuffer);
            }
            switch (result.getStatus()) {
                case OK:
                    decryptionBuffer.flip();
                    fullMessageSize = decryptionBuffer.getInt();
                    fullMessage = ByteBuffer.allocate(fullMessageSize);
                    fullMessage.put(decryptionBuffer);
                    if (fullMessage.position() == fullMessageSize) {
                        fullMessage.flip();
                        needsChunk = false;
                    } else {
                        needsChunk = true;
                    }
                    return;
                case BUFFER_OVERFLOW:
                    decryptionBuffer = ByteBuffer.allocate(decryptionBuffer.capacity() * 2);
                    break;  // try again
                case BUFFER_UNDERFLOW:
                    throw new SSLException("SSL engine should never underflow on ssl unwrap of buffer.");
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }

    public void addChunk(ByteBuffer chunk) throws IOException {
        decryptionBuffer.clear();
        while (true) {
            SSLEngineResult result;
            synchronized (sslEngine) {
                result = sslEngine.unwrap(chunk, decryptionBuffer);
            }
            switch (result.getStatus()) {
                case OK:
                    decryptionBuffer.flip();
                    fullMessage.put(decryptionBuffer);
                    if (fullMessage.position() == fullMessageSize) {
                        fullMessage.flip();
                        needsChunk = false;
                    }
                    return;
                case BUFFER_OVERFLOW:
                    decryptionBuffer = ByteBuffer.allocate(decryptionBuffer.capacity() * 2);
                    break;  // try again
                case BUFFER_UNDERFLOW:
                    throw new SSLException("SSL engine should never underflow on ssl unwrap of buffer.");
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }

    public boolean needsChunk() {
        return needsChunk;
    }

    public ByteBuffer getMessage() throws IOException {
        if (needsChunk) {
            throw new IOException("message not fully decrypted");
        }
        return fullMessage;
    }
}
