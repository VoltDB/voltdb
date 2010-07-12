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


package org.hsqldb_voltpatches.persist;

import java.io.IOException;

import org.hsqldb_voltpatches.Database;

/**
 * Mixe NIO / non-NIO version of ScaledRAFile.
 * This class is used only for storing a CACHED
 * TABLE .data file and cannot be used for TEXT TABLE source files.
 *
 * Due to various issues with java.nio classes, this class will use a mapped
 * channel of fixed size. After reaching this size, the file and channel are
 * closed and a new one opened, up to the maximum size.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version  1.8.0.5
 * @since 1.7.2
 */
public final class ScaledRAFileHybrid implements ScaledRAInterface {

    final Database    database;
    final String      fileName;
    final boolean     isReadOnly;
    final boolean     wasNio;
    long              maxLength;
    ScaledRAInterface store;

    public ScaledRAFileHybrid(Database database, String name,
                              boolean readOnly) throws IOException {

        this.database   = database;
        this.fileName   = name;
        this.isReadOnly = readOnly;

        newStore(0);

        this.wasNio = store.wasNio();
    }

    public long length() throws IOException {
        return store.length();
    }

    public void seek(long position) throws IOException {
        checkSeek(position);
        store.seek(position);
    }

    public long getFilePointer() throws IOException {
        return store.getFilePointer();
    }

    public int read() throws IOException {

        checkLength(1);

        return store.read();
    }

    public void read(byte[] b, int offset, int length) throws IOException {
        checkLength(length);
        store.read(b, offset, length);
    }

    public void write(byte[] b, int offset, int length) throws IOException {
        checkLength(length);
        store.write(b, offset, length);
    }

    public int readInt() throws IOException {

        checkLength(4);

        return store.readInt();
    }

    public void writeInt(int i) throws IOException {
        checkLength(4);
        store.writeInt(i);
    }

    public long readLong() throws IOException {

        checkLength(8);

        return store.readLong();
    }

    public void writeLong(long i) throws IOException {
        checkLength(8);
        store.writeLong(i);
    }

    public void close() throws IOException {
        store.close();
    }

    public boolean isReadOnly() {
        return store.isReadOnly();
    }

    public boolean wasNio() {
        return wasNio;
    }

    public boolean canAccess(int length) {
        return true;
    }

    public boolean canSeek(long position) {
        return true;
    }

    public Database getDatabase() {
        return null;
    }

    private void checkLength(int length) throws IOException {

        if (store.canAccess(length)) {
            return;
        }

        newStore(store.getFilePointer() + length);
    }

    private void checkSeek(long position) throws IOException {

        if (store.canSeek(position)) {
            return;
        }

        newStore(position);
    }

    void newStore(long requiredPosition) throws IOException {

        long currentPosition = 0;

        if (store != null) {
            currentPosition = store.getFilePointer();

            store.close();
        }

        if (requiredPosition <= ScaledRAFile.MAX_NIO_LENGTH) {
            try {
                store = new ScaledRAFileNIO(database, fileName, isReadOnly,
                                            (int) requiredPosition);

                store.seek(currentPosition);

                return;
            } catch (Throwable e) {}
        }

        store = new ScaledRAFile(database, fileName, isReadOnly);

        store.seek(currentPosition);
    }
}
