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

import java.io.IOException;
import java.io.Reader;
import java.io.EOFException;

/**
 * A writer for char strings.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class CharArrayWriter {

    protected char[] buffer;
    protected int    count;

    public CharArrayWriter(char[] buffer) {
        this.buffer = buffer;
    }

    public CharArrayWriter(Reader reader, int length) throws IOException {

        buffer = new char[length];

        for (int left = length; left > 0; ) {
            int read = reader.read(buffer, count, left);

            if (read == -1) {
                if (left > 0) {
                    reader.close();

                    throw new EOFException();
                }

                break;
            }

            left  -= read;
            count += read;
        }
    }

    public void write(int c) {

        if (count == buffer.length) {
            ensureSize(count + 1);
        }

        buffer[count++] = (char) c;
    }

    void ensureSize(int size) {

        if (size <= buffer.length) {
            return;
        }

        int newSize = buffer.length;

        while (newSize < size) {
            newSize *= 2;
        }

        char[] newBuffer = new char[newSize];

        System.arraycopy(buffer, 0, newBuffer, 0, count);

        buffer = newBuffer;
    }

    public void write(String str, int off, int len) {

        ensureSize(count + len);
        str.getChars(off, off + len, buffer, count);

        count += len;
    }

    public void reset() {
        count = 0;
    }

    public void reset(char[] buffer) {
        count       = 0;
        this.buffer = buffer;
    }

    public char[] toCharArray() {

        char[] newBuffer = new char[count];

        System.arraycopy(buffer, 0, newBuffer, 0, count);

        return (char[]) newBuffer;
    }

    public int size() {
        return count;
    }

    /**
     * Converts input data to a string.
     * @return the string.
     */
    public String toString() {
        return new String(buffer, 0, count);
    }
}
