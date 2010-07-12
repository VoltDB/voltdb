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


package org.hsqldb_voltpatches.persist;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.lib.HsqlByteArrayInputStream;

/**
 * This class is a random access wrapper around a DataInputStream object and
 * enables access to cached tables when a database is included in a jar.
 *
 * A proof-of-concept prototype was first contributed by winfriedthom@users.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version  1.8.0
 * @since  1.8.0
 */
final class ScaledRAFileInJar implements ScaledRAInterface {

    DataInputStream          file;
    final String             fileName;
    long                     fileLength;
    boolean                  bufferDirty = true;
    byte[]                   buffer      = new byte[4096];
    HsqlByteArrayInputStream ba = new HsqlByteArrayInputStream(buffer);
    long                     bufferOffset;

    //
    long seekPosition;
    long realPosition;

    ScaledRAFileInJar(String name) throws FileNotFoundException, IOException {

        fileName = name;

        resetStream();
        file.skip(DataFileCache.LONG_FREE_POS_POS);

        fileLength = file.readLong();

        resetStream();
    }

    public long length() throws IOException {
        return fileLength;
    }

    /**
     * Some JVM's do not allow seek beyond end of file, so zeros are written
     * first in that case. Reported by bohgammer@users in Open Disucssion
     * Forum.
     */
    public void seek(long position) throws IOException {
        seekPosition = position;
    }

    public long getFilePointer() throws IOException {
        return seekPosition;
    }

    private void readIntoBuffer() throws IOException {

        long filePos = seekPosition;

        bufferDirty = false;

        long subOffset  = filePos % buffer.length;
        long readLength = fileLength - (filePos - subOffset);

        if (readLength <= 0) {
            throw new IOException("read beyond end of file");
        }

        if (readLength > buffer.length) {
            readLength = buffer.length;
        }

        fileSeek(filePos - subOffset);
        file.readFully(buffer, 0, (int) readLength);

        bufferOffset = filePos - subOffset;
        realPosition = bufferOffset + readLength;
    }

    public int read() throws IOException {

        if (seekPosition >= fileLength) {
            return -1;
        }

        if (bufferDirty || seekPosition < bufferOffset
                || seekPosition >= bufferOffset + buffer.length) {
            readIntoBuffer();
        }

        ba.reset();
        ba.skip(seekPosition - bufferOffset);

        int val = ba.read();

        seekPosition++;

        return val;
    }

    public long readLong() throws IOException {

        long hi = readInt();
        long lo = readInt();

        return (hi << 32) + (lo & 0xffffffffL);
    }

    public int readInt() throws IOException {

        if (bufferDirty || seekPosition < bufferOffset
                || seekPosition >= bufferOffset + buffer.length) {
            readIntoBuffer();
        }

        ba.reset();
        ba.skip(seekPosition - bufferOffset);

        int val = ba.readInt();

        seekPosition += 4;

        return val;
    }

    public void read(byte[] b, int offset, int length) throws IOException {

        if (bufferDirty || seekPosition < bufferOffset
                || seekPosition >= bufferOffset + buffer.length) {
            readIntoBuffer();
        }

        ba.reset();
        ba.skip(seekPosition - bufferOffset);

        int bytesRead = ba.read(b, offset, length);

        seekPosition += bytesRead;

        if (bytesRead < length) {
            if (seekPosition != realPosition) {
                fileSeek(seekPosition);
            }

            file.readFully(b, offset + bytesRead, length - bytesRead);

            seekPosition += (length - bytesRead);
            realPosition = seekPosition;
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {}

    public void writeInt(int i) throws IOException {}

    public void writeLong(long i) throws IOException {}

    public void close() throws IOException {
        file.close();
    }

    public boolean isReadOnly() {
        return true;
    }

    public boolean wasNio() {
        return false;
    }

    private void resetStream() throws IOException {

        if (file != null) {
            file.close();
        }

        InputStream fis = getClass().getResourceAsStream(fileName);

        file = new DataInputStream(fis);
    }

    private void fileSeek(long position) throws IOException {

        long skipPosition = realPosition;

        if (position < skipPosition) {
            resetStream();

            skipPosition = 0;
        }

        while (position > skipPosition) {
            skipPosition += file.skip(position - skipPosition);
        }
    }

    public boolean canAccess(int length) {
        return false;
    }

    public boolean canSeek(long position) {
        return false;
    }

    public Database getDatabase() {
        return null;
    }
}
