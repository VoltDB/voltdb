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


package org.hsqldb_voltpatches.scriptio;

import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.FileAccess;

/**
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @since 1.7.2
 * @version 1.7.2
 */
class ScriptWriterZipped extends ScriptWriterBinary {

    private static final int bufferSize = 1 << 15;

    ScriptWriterZipped(Database db, String file, boolean includeCached,
                       boolean newFile) {
        super(db, file, includeCached, newFile);
    }

    /**
     * Override the underlying method with no operation.
     */
    public void sync() {}

    protected void openFile() {

        try {
            FileAccess           fa  = database.getFileAccess();
            java.io.OutputStream fos = fa.openOutputStreamElement(outFile);

            outDescriptor = fa.getFileSync(fos);
            fileStreamOut = new DeflaterOutputStream(fos,
                    new Deflater(Deflater.DEFAULT_COMPRESSION), bufferSize);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                e.toString(), outFile
            });
        }
    }

    /**
     * This may not really be necessary, unless we add implementations where
     * non-compressed data is added to the end of the copressed part.
     */
    protected void finishStream() throws IOException {
        ((DeflaterOutputStream) fileStreamOut).finish();
        fileStreamOut.flush();
    }
}
