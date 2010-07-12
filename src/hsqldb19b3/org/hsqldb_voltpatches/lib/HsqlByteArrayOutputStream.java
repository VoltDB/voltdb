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

import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.io.UnsupportedEncodingException;

/**
 * This class is a replacement for both java.io.ByteArrayOuputStream
 * (without synchronization) and java.io.DataOutputStream
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class HsqlByteArrayOutputStream extends java.io.OutputStream
implements DataOutput {

    protected byte[] buffer;
    protected int    count;

    public HsqlByteArrayOutputStream() {
        this(128);
    }

    public HsqlByteArrayOutputStream(int size) {

        if (size < 128) {
            size = 128;
        }

        buffer = new byte[size];
    }

    public HsqlByteArrayOutputStream(byte[] buffer) {
        this.buffer = buffer;
    }

    /**
     * Constructor from an InputStream limits size to the length argument.
     * Throws if the actual length of the InputStream is smaller than
     * length value.
     */
    public HsqlByteArrayOutputStream(InputStream input,
                                     int length) throws IOException {

        buffer = new byte[length];

        for (int left = length; left > 0; ) {
            int read = input.read(buffer, count, left);

            if (read == -1) {
                if (left > 0) {
                    input.close();

                    throw new EOFException();
                }

                break;
            }

            left  -= read;
            count += read;
        }
    }

    // methods that implement dataOutput
    public void writeShort(int v) {

        ensureRoom(2);

        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) v;
    }

    public void writeInt(int v) {

        if (count + 4 > buffer.length) {
            ensureRoom(4);
        }

        buffer[count++] = (byte) (v >>> 24);
        buffer[count++] = (byte) (v >>> 16);
        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) v;
    }

    public void writeLong(long v) {
        writeInt((int) (v >>> 32));
        writeInt((int) v);
    }

    public final void writeBytes(String s) {

        int len = s.length();

        ensureRoom(len);

        for (int i = 0; i < len; i++) {
            buffer[count++] = (byte) s.charAt(i);
        }
    }

    public final void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }

    public final void writeDouble(double v) {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBoolean(boolean v) {

        ensureRoom(1);

        buffer[count++] = (byte) (v ? 1
                                    : 0);
    }

    public void writeByte(int v) {

        ensureRoom(1);

        buffer[count++] = (byte) (v);
    }

    public void writeChar(int v) {

        ensureRoom(2);

        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) v;
    }

    public void writeChars(String s) {

        int len = s.length();

        ensureRoom(len * 2);

        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);

            buffer[count++] = (byte) (v >>> 8);
            buffer[count++] = (byte) v;
        }
    }

    public void writeUTF(String str) throws IOException {

        int len = str.length();

        if (len > 0xffff) {
            throw new UTFDataFormatException();
        }

        ensureRoom(len * 3 + 2);

        //
        int initpos = count;

        count += 2;

        StringConverter.stringToUTFBytes(str, this);

        int bytecount = count - initpos - 2;

        if (bytecount > 0xffff) {
            count = initpos;

            throw new UTFDataFormatException();
        }

        buffer[initpos++] = (byte) (bytecount >>> 8);
        buffer[initpos]   = (byte) bytecount;
    }

    /**
     * does nothing
     */
    public void flush() throws java.io.IOException {}

    // methods that extend java.io.OutputStream
    public void write(int b) {

        ensureRoom(1);

        buffer[count++] = (byte) b;
    }

    public void writeNoCheck(int b) {
        buffer[count++] = (byte) b;
    }

    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) {

        ensureRoom(len);
        System.arraycopy(b, off, buffer, count, len);

        count += len;
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(buffer, 0, count);
    }

    public void reset() {
        count = 0;
    }

    public byte[] toByteArray() {

        byte[] newbuf = new byte[count];

        System.arraycopy(buffer, 0, newbuf, 0, count);

        return newbuf;
    }

    public int size() {
        return count;
    }

    public void setPosition(int newPos) {

        if (newPos > buffer.length) {
            throw new ArrayIndexOutOfBoundsException();
        }

        count = newPos;
    }

    public String toString() {
        return new String(buffer, 0, count);
    }

    public String toString(String enc) throws UnsupportedEncodingException {
        return new String(buffer, 0, count, enc);
    }

    public void close() throws IOException {}

    // additional public methods not in similar java.util classes
    public void write(char[] c, int off, int len) {

        ensureRoom(len * 2);

        for (int i = off; i < len; i++) {
            int v = c[i];

            buffer[count++] = (byte) (v >>> 8);
            buffer[count++] = (byte) v;
        }
    }

    public void fill(int b, int len) {

        ensureRoom(len);

        for (int i = 0; i < len; i++) {
            buffer[count++] = (byte) b;
        }
    }

    public byte[] getBuffer() {
        return this.buffer;
    }

    public void setBuffer(byte[] buffer) {
        count       = 0;
        this.buffer = buffer;
    }

    public void ensureRoom(int extra) {

        int newcount = count + extra;
        int newsize  = buffer.length;

        if (newcount > newsize) {
            while (newcount > newsize) {
                newsize *= 2;
            }

            byte[] newbuf = new byte[newsize];

            System.arraycopy(buffer, 0, newbuf, 0, count);

            buffer = newbuf;
        }
    }

    public void reset(int newSize) {

        count = 0;

        if (newSize > buffer.length) {
            buffer = new byte[newSize];
        }
    }

    public void reset(byte[] buffer) {
        count       = 0;
        this.buffer = buffer;
    }
}
