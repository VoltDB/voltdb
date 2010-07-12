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
import java.io.InputStream;

// fredt@users - 1.9.0 corrected read(byte[], int, int)

/**
 * Counts down from a specified value the number of bytes actually read
 * from the wrapped InputStream. <p>
 *
 * Returns minus one (-1) early from readXXX methods if the count
 * down reaches zero (0) before the end of the wrapped InputStream
 * is encountered. <p>
 *
 * This class is especially useful when a fixed number of bytes is to be read
 * from an InputStream that is in turn to be used as the source for an
 * {@link java.io.InputStreamReader InputStreamReader}.
 *
 * @author boucherb@users
 * @version 1.9.0
 * @since 1.9.0
 */
public final class CountdownInputStream extends InputStream {

    private long        count;
    private InputStream input;

    public CountdownInputStream(final InputStream is) {
        this.input = is;
    }

    public int read() throws IOException {

        if (this.count <= 0) {
            return -1;
        }

        final int b = this.input.read();

        if (b >= 0) {
            this.count--;
        }

        return b;
    }

    public int read(final byte[] buf) throws IOException {

        if (this.count <= 0) {
            return -1;
        }

        int len = buf.length;

        if (len > this.count) {
            len = (int) this.count;
        }

        final int r = this.input.read(buf, 0, len);

        if (r > 0) {
            this.count -= r;
        }

        return r;
    }

    public int read(final byte[] buf, final int off,
                    int len) throws IOException {

        if (this.count <= 0) {
            return -1;
        }

        if (len > this.count) {
            len = (int) this.count;
        }

        final int r = this.input.read(buf, off, len);

        if (r > 0) {
            this.count -= r;
        }

        return r;
    }

    public void close() throws IOException {
        this.input.close();
    }

    public long getCount() {
        return this.count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
