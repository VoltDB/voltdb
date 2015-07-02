/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.types;

import java.io.IOException;
import java.io.InputStream;

import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.java.JavaSystem;

/**
 * This class is used as an InputStream to retrieve data from a Blob.
 * mark() and reset() are not supported.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class BlobInputStream extends InputStream {

    final BlobData                blob;
    final long                    availableLength;
    long                          bufferOffset;
    long                          currentPosition;
    byte[]                        buffer;
    boolean                       isClosed;
    int                           streamBlockSize;
    public final SessionInterface session;

    public BlobInputStream(SessionInterface session, BlobData blob,
                           long offset, long length) {

        final long blobLength = blob.length(session);

        this.session         = session;
        this.blob            = blob;
        this.availableLength = offset + Math.min(length, blobLength - offset);
        this.currentPosition = offset;
        this.streamBlockSize = session.getStreamBlockSize();
    }

    public int read() throws IOException {

        checkClosed();

        if (currentPosition >= availableLength) {
            return -1;
        }

        if (buffer == null
                || currentPosition >= bufferOffset + buffer.length) {
            try {
                checkClosed();
                readIntoBuffer();
            } catch (Exception e) {
                throw JavaSystem.toIOException(e);
            }
        }

        int val = buffer[(int) (currentPosition - bufferOffset)] & 0xff;

        currentPosition++;

        return val;
    }

    public long skip(long n) throws IOException {

        checkClosed();

        if (n <= 0) {
            return 0;
        }

        if (currentPosition + n > availableLength) {
            n = availableLength - currentPosition;
        }

        currentPosition += n;

        return n;
    }

    public int available() {

        long avail = availableLength - currentPosition;

        if (avail > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int) avail;
    }

    public void close() throws IOException {
        isClosed = true;
    }

    private void checkClosed() throws IOException {

        if (isClosed || blob.isClosed()) {
            throw new IOException(Error.getMessage(ErrorCode.X_0F503));
        }
    }

    private void readIntoBuffer() {

        long readLength = availableLength - currentPosition;

        if (readLength <= 0) {
            return;
        }

        if (readLength > streamBlockSize) {
            readLength = streamBlockSize;
        }

        buffer = blob.getBytes(session, currentPosition, (int) readLength);
        bufferOffset = currentPosition;
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}
