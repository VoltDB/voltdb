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
import java.io.Reader;

/**
 * This class is an part implementation of DataInput. It wraps a Reader object.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ReaderDataInput implements DataInput {

    protected Reader reader;
    protected int    pos;
    int              lastChar = -1;

    public ReaderDataInput(Reader reader) {
        this.reader = reader;
        this.pos    = 0;
    }

    // methods that implement java.io.DataInput
    public final void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public final void readFully(byte[] bytes, int off,
                                int len) throws IOException {

        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int b = read();

            if (b < 0) {
                throw new EOFException();
            }

            bytes[off + n++] = (byte) b;
        }
    }

    public final boolean readBoolean() throws IOException {

        int b = read();

        if (b < 0) {
            throw new EOFException();
        }

        return (b != 0);
    }

    public final byte readByte() throws IOException {

        int b = read();

        if (b < 0) {
            throw new EOFException();
        }

        return (byte) b;
    }

    public final int readUnsignedByte() throws IOException {

        int b = read();

        if (b < 0) {
            throw new EOFException();
        }

        return b;
    }

    public short readShort() throws IOException {

        int b1 = read();

        if (b1 < 0) {
            throw new EOFException();
        }

        int b2 = read();

        if (b2 < 0) {
            throw new EOFException();
        }

        return (short) ((b1 << 8) | b2);
    }

    public final int readUnsignedShort() throws IOException {

        int b1 = read();
        int b2 = read();

        if ((b1 | b2) < 0) {
            throw new EOFException();
        }

        return ((b1 << 8) + (b2));
    }

    public final char readChar() throws IOException {

        int b1 = read();
        int b2 = read();

        if ((b1 | b2) < 0) {
            throw new EOFException();
        }

        return (char) ((b1 << 8) + (b2));
    }

    public int readInt() throws IOException {
        throw new java.lang.RuntimeException("not implemented.");
    }

    public long readLong() throws IOException {
        throw new java.lang.RuntimeException("not implemented.");
    }

    public final float readFloat() throws IOException {
        throw new java.lang.RuntimeException("not implemented.");
    }

    public final double readDouble() throws IOException {
        throw new java.lang.RuntimeException("not implemented.");
    }

    public int skipBytes(int n) throws IOException {
        throw new java.lang.RuntimeException("not implemented.");
    }

    public String readLine() throws IOException {
        throw new java.lang.RuntimeException("not implemented.");
    }

    public String readUTF() throws IOException {
        throw new java.lang.RuntimeException("not implemented.");
    }

    public int read() throws IOException {

        if (lastChar >= 0) {
            int val = lastChar & 0xff;

            lastChar = -1;

            pos++;
            return val;
        }

        lastChar = reader.read();

        if (lastChar < 0) {
            return lastChar;
        }

        pos++;
        return lastChar >> 8;
    }
}
