/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb_voltpatches.jdbc;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;

/**
 * This class is used as an InputStream to retrieve data from a Blob.
 * mark() and reset() are not supported.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class ClobInputStream extends Reader {

    final JDBCClobClient clob;
    final long           availableLength;
    long                 bufferOffset;
    long                 currentPosition;
    char[]               buffer;
    boolean              isClosed;
    int                  streamBlockSize;

    public ClobInputStream(JDBCClobClient clob, long offset, long length,
                           int blockSize) throws SQLException {

        if (!JDBCClobClient.isInLimits(clob.length(), offset, length)) {
            throw Util.outOfRangeArgument();
        }

        this.clob            = clob;
        this.availableLength = offset + length;
        this.currentPosition = offset;
        this.streamBlockSize = blockSize;
    }

    public int read() throws IOException {

        checkClosed();

        if (currentPosition >= availableLength) {
            return -1;
        }

        if (buffer == null
                || currentPosition >= bufferOffset + buffer.length) {
            try {
                readIntoBuffer();
            } catch (SQLException e) {
                throw new IOException(e.getMessage());
            }
        }

        int val = buffer[(int) (currentPosition - bufferOffset)] & 0xff;

        currentPosition++;

        return val;
    }

    public int read(char[] cbuf, int off, int len) throws IOException {

        checkClosed();

        if (currentPosition + len >= availableLength) {
            return -1;
        }

        for (int i = off; i < len; i++) {
            cbuf[i] = (char) read();
        }

        return len;
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

    public int available() throws IOException {
        return (int) (bufferOffset + buffer.length - currentPosition);
    }

    public void close() throws IOException {
        isClosed = true;
    }

    private void checkClosed() throws IOException {

        if (isClosed || clob.isClosed()) {
            throw new IOException(
                Error.getMessage(ErrorCode.X_0F503));
        }
    }

    private void readIntoBuffer() throws SQLException {

        long readLength = availableLength - currentPosition;

        if (readLength <= 0) {}

        if (readLength > streamBlockSize) {
            readLength = streamBlockSize;
        }

        buffer       = clob.getChars(currentPosition, (int) readLength);
        bufferOffset = currentPosition;
    }
}
