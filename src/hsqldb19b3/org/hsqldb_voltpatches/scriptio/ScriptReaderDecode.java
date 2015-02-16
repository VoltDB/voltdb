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


package org.hsqldb_voltpatches.scriptio;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPInputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.LineReader;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.persist.Crypto;
import org.hsqldb_voltpatches.rowio.RowInputTextLog;

/**
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class ScriptReaderDecode extends ScriptReaderText {

    DataInputStream dataInput;
    Crypto          crypto;
    byte[]          buffer = new byte[256];

    public ScriptReaderDecode(Database db, String fileName, Crypto crypto,
                              boolean forLog) throws IOException {
        this(db, db.logger.getFileAccess().openInputStreamElement(fileName),
             crypto, forLog);
    }

    public ScriptReaderDecode(Database db, InputStream inputStream,
                              Crypto crypto,
                              boolean forLog) throws IOException {

        super(db);

        this.crypto = crypto;
        rowIn       = new RowInputTextLog();

        if (forLog) {
            dataInput =
                new DataInputStream(new BufferedInputStream(inputStream));
        } else {
            InputStream stream =
                crypto.getInputStream(new BufferedInputStream(inputStream));

            stream       = new GZIPInputStream(stream);
            dataStreamIn = new LineReader(stream, ScriptWriterText.ISO_8859_1);
        }
    }

    public boolean readLoggedStatement(Session session) {

        if (dataInput == null) {
            return super.readLoggedStatement(session);
        }

        int count;

        try {
            count = dataInput.readInt();

            if (count * 2 > buffer.length) {
                buffer = new byte[count * 2];
            }

            dataInput.readFully(buffer, 0, count);
        } catch (Throwable t) {
            return false;
        }

        count = crypto.decode(buffer, 0, count, buffer, 0);

        String s;

        try {
            s = new String(buffer, 0, count, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR, null);
        }

        lineCount++;

//        System.out.println(lineCount);
        statement = StringConverter.unicodeStringToString(s);

        if (statement == null) {
            return false;
        }

        processStatement(session);

        return true;
    }

    public void close() {

        try {
            if (dataStreamIn != null) {
                dataStreamIn.close();
            }

            if (dataInput != null) {
                dataInput.close();
            }
        } catch (Exception e) {}
    }
}
