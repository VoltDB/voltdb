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


package org.hsqldb_voltpatches.lib;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class is a replacement for both java.io.ByteArrayInputStream
 * (without synchronization) and java.io.DataInputStream
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
public class HsqlByteArrayInputStream extends InputStream
implements DataInput {

    protected byte[] buf;
    protected int    pos;
    protected int    mark = 0;
    protected int    count;

    public HsqlByteArrayInputStream(byte[] buf) {

        this.buf   = buf;
        this.pos   = 0;
        this.count = buf.length;
    }

    public HsqlByteArrayInputStream(byte[] buf, int offset, int length) {

        this.buf   = buf;
        this.pos   = offset;
        this.count = Math.min(offset + length, buf.length);
        this.mark  = offset;
    }

    // methods that implement java.io.DataInput
    public final void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public final void readFully(byte[] b, int off,
                                int len) throws IOException {

        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int count = read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException();
            }

            n += count;
        }
    }

    public final boolean readBoolean() throws IOException {

        int ch = read();

        if (ch < 0) {
            throw new EOFException();
        }

        return (ch != 0);
    }

    public final byte readByte() throws IOException {

        int ch = read();

        if (ch < 0) {
            throw new EOFException();
        }

        return (byte) ch;
    }

    public final int readUnsignedByte() throws IOException {

        int ch = read();

        if (ch < 0) {
            throw new EOFException();
        }

        return ch;
    }

    public short readShort() throws IOException {

        if (count - pos < 2) {
            pos = count;

            throw new EOFException();
        }

        int ch1 = buf[pos++] & 0xff;
        int ch2 = buf[pos++] & 0xff;

        return (short) ((ch1 << 8) + (ch2));
    }

    public final int readUnsignedShort() throws IOException {

        int ch1 = read();
        int ch2 = read();

        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }

        return (ch1 << 8) + (ch2);
    }

    public final char readChar() throws IOException {

        int ch1 = read();
        int ch2 = read();

        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }

        return (char) ((ch1 << 8) + (ch2));
    }

    public int readInt() throws IOException {

        if (count - pos < 4) {
            pos = count;

            throw new EOFException();
        }

        int ch1 = buf[pos++] & 0xff;
        int ch2 = buf[pos++] & 0xff;
        int ch3 = buf[pos++] & 0xff;
        int ch4 = buf[pos++] & 0xff;

        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
    }

    public long readLong() throws IOException {
        return (((long) readInt()) << 32)
               + (((long) readInt()) & 0xffffffffL);
    }

    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public int skipBytes(int n) throws IOException {
        return (int) skip(n);
    }

    public String readLine() throws IOException {

        /** @todo: this will probably be useful */
        throw new java.lang.RuntimeException("not implemented.");
    }

    public String readUTF() throws IOException {

        int bytecount = readUnsignedShort();

        if (pos + bytecount >= count) {
            throw new EOFException();
        }

        String result = StringConverter.readUTF(buf, pos, bytecount);

        pos += bytecount;

        return result;
    }

// methods that extend java.io.InputStream
    public int read() {
        return (pos < count) ? (buf[pos++] & 0xff)
                             : -1;
    }

    public int read(byte[] b, int off, int len) {

        if (pos >= count) {
            return -1;
        }

        if (pos + len > count) {
            len = count - pos;
        }

        if (len <= 0) {
            return 0;
        }

        System.arraycopy(buf, pos, b, off, len);

        pos += len;

        return len;
    }

    public long skip(long n) {

        if (pos + n > count) {
            n = count - pos;
        }

        if (n < 0) {
            return 0;
        }

        pos += n;

        return n;
    }

    public int available() {
        return count - pos;
    }

    public boolean markSupported() {
        return true;
    }

    public void mark(int readAheadLimit) {
        mark = pos;
    }

    public void reset() {
        pos = mark;
    }

    public void close() throws IOException {}
}
