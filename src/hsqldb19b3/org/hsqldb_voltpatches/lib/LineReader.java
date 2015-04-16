/* Copyright (c) 2001-2014, The HSQL Development Group
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
import java.nio.charset.Charset;

/**
 * A converter for InputStream to return String objects using the given
 * charset for conversion. The readLine() method returns the next string
 * excluding carriage-return and line-feed characters. No other character is
 * excluded. A carriage-return followed by a line-feed is treated as a single
 * end-of-line marker. Otherwise, each carriage-return or line-feed is treated
 * as an end-of_line marker.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 2.0.1
 */
public class LineReader {

    boolean                   finished = false;
    boolean                   wasCR    = false;
    boolean                   wasEOL   = false;
    HsqlByteArrayOutputStream baOS     = new HsqlByteArrayOutputStream(1024);

    //
    final InputStream stream;
    final Charset     charset;
    final String      charsetName;

    public LineReader(InputStream stream, String charsetName) {

        this.stream      = stream;
        this.charsetName = charsetName;
        this.charset     = Charset.forName(charsetName);
    }

    public String readLine() throws IOException {

        if (finished) {
            return null;
        }

        while (true) {
            int c = stream.read();

            if (c == -1) {
                finished = true;

                if (baOS.size() == 0) {
                    return null;
                }

                break;
            }

            switch (c) {

                case '\r' : {
                    wasCR = true;

                    break;
                }
                case '\n' : {
                    if (wasCR) {
                        wasCR = false;

                        continue;
                    } else {
                        break;
                    }
                }
                default : {
                    baOS.write(c);

                    wasCR = false;

                    continue;
                }
            }

            break;
        }

        // can use charset with JDK 1.6
        String string = new String(baOS.getBuffer(), 0, baOS.size(),
                                   charsetName);

        baOS.reset();

        return string;
    }

    public void close() throws IOException {
        stream.close();
    }
}
