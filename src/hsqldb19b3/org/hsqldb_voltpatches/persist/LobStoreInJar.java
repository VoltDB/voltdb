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


package org.hsqldb_voltpatches.persist;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class LobStoreInJar implements LobStore {

    final int       lobBlockSize;
    Database        database;
    DataInputStream dataInput;
    final String    fileName;

    //
    long realPosition;

    public LobStoreInJar(Database database, int lobBlockSize) {

        this.lobBlockSize = lobBlockSize;
        this.database     = database;

        try {
            fileName = database.getPath() + ".lobs";
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    public byte[] getBlockBytes(int blockAddress, int blockCount) {

        try {
            long   address   = (long) blockAddress * lobBlockSize;
            int    count     = blockCount * lobBlockSize;
            byte[] dataBytes = new byte[count];

            fileSeek(address);
            dataInput.readFully(dataBytes, 0, count);

            realPosition = address + count;

            return dataBytes;
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    public void setBlockBytes(byte[] dataBytes, int blockAddress,
                              int blockCount) {}

    public void setBlockBytes(byte[] dataBytes, long position, int offset,
                              int length) {}

    public int getBlockSize() {
        return lobBlockSize;
    }

    public long getLength() {
        return 0;
    }

    public void setLength(long length) {}

    public void close() {

        try {
            if (dataInput != null) {
                dataInput.close();
            }
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    public void synch() {}

    private void resetStream() throws IOException {

        if (dataInput != null) {
            dataInput.close();
        }

        InputStream fis = null;

        try {
            fis = getClass().getResourceAsStream(fileName);

            if (fis == null) {
                ClassLoader cl =
                    Thread.currentThread().getContextClassLoader();

                if (cl != null) {
                    fis = cl.getResourceAsStream(fileName);
                }
            }
        } catch (Throwable t) {

            //
        } finally {
            if (fis == null) {
                throw new FileNotFoundException(fileName);
            }
        }

        dataInput    = new DataInputStream(fis);
        realPosition = 0;
    }

    private void fileSeek(long position) throws IOException {

        if (dataInput == null) {
            resetStream();
        }

        long skipPosition = realPosition;

        if (position < skipPosition) {
            resetStream();

            skipPosition = 0;
        }

        while (position > skipPosition) {
            skipPosition += dataInput.skip(position - skipPosition);
        }

        realPosition = position;
    }
}
