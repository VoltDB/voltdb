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


package org.hsqldb_voltpatches.lib;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UTFDataFormatException;

/**
 * A wrapper for OutputStream
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class DataOutputStream extends java.io.BufferedOutputStream
implements DataOutput {

    byte[] tempBuffer = new byte[8];

    public DataOutputStream(OutputStream stream) {
        super(stream, 8);
    }

    public final void writeByte(int v) throws IOException {
        write(v);
    }

    public final void writeInt(int v) throws IOException {

        int count = 0;

        tempBuffer[count++] = (byte) (v >>> 24);
        tempBuffer[count++] = (byte) (v >>> 16);
        tempBuffer[count++] = (byte) (v >>> 8);
        tempBuffer[count++] = (byte) v;

        write(tempBuffer, 0, count);
    }

    public final void writeLong(long v) throws IOException {
        writeInt((int) (v >>> 32));
        writeInt((int) v);
    }

    public void writeChar(int v) throws IOException {

        int count = 0;

        tempBuffer[count++] = (byte) (v >>> 8);
        tempBuffer[count++] = (byte) v;

        write(tempBuffer, 0, count);
    }

    public void writeChars(String s) throws IOException {

        int len = s.length();

        for (int i = 0; i < len; i++) {
            int v     = s.charAt(i);
            int count = 0;

            tempBuffer[count++] = (byte) (v >>> 8);
            tempBuffer[count++] = (byte) v;

            write(tempBuffer, 0, count);
        }
    }

    public void writeChars(char[] c) throws IOException {
        writeChars(c, c.length);
    }

    public void writeChars(char[] c, int length) throws IOException {

        for (int i = 0; i < length; i++) {
            int v     = c[i];
            int count = 0;

            tempBuffer[count++] = (byte) (v >>> 8);
            tempBuffer[count++] = (byte) v;

            write(tempBuffer, 0, count);
        }
    }

    public long write(Reader reader, long length) throws IOException {

        InputStream inputStream = new ReaderInputStream(reader);

        return write(inputStream, length * 2) / 2;
    }

    public long write(InputStream inputStream,
                      long length) throws IOException {

        byte[] data       = new byte[1024];
        long   totalCount = 0;

        while (true) {
            long count = length - totalCount;

            if (count > data.length) {
                count = data.length;
            }

            count = inputStream.read(data, 0, (int) count);

            if (count < 1) {
                break;
            }

            write(data, 0, (int) count);

            totalCount += count;
        }

        return totalCount;
    }

    public void writeBoolean(boolean v) throws IOException {

        int val = v ? 1
                    : 0;

        write(val);
    }

    public void writeShort(int v) throws IOException {

        int count = 0;

        tempBuffer[count++] = (byte) (v >> 8);
        tempBuffer[count++] = (byte) v;

        write(tempBuffer, 0, count);
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s) throws IOException {

        int length = s.length();

        for (int i = 0; i < length; i++) {
            out.write((byte) s.charAt(i));
        }
    }

    public void writeUTF(String str) throws IOException {

        int len = str.length();

        if (len > 0xffff) {
            throw new UTFDataFormatException();
        }

        int bytecount = StringConverter.getUTFSize(str);

        if (bytecount > 0xffff) {
            throw new UTFDataFormatException();
        }

        //
        writeChar(bytecount);

        HsqlByteArrayOutputStream bao =
            new HsqlByteArrayOutputStream(bytecount);

        StringConverter.stringToUTFBytes(str, bao);
        this.write(bao.getBuffer(), 0, bao.size());
    }
}
