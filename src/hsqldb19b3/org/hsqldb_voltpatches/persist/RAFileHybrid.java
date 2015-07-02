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
 * @version 2.3.0
 * @since 1.7.2
 */
public final class RAFileHybrid implements RandomAccessInterface {

    final Database        database;
    final String          fileName;
    final boolean         isReadOnly;
    boolean               preNio;
    boolean               isNio;
    long initialMaxLength = RAFileNIO.largeBufferSize / 2;
    RandomAccessInterface store;

    public RAFileHybrid(Database database, String name,
                              boolean readOnly) throws IOException {

        this.database   = database;
        this.fileName   = name;
        this.isReadOnly = readOnly;

        long         fileLength;
        java.io.File fi = new java.io.File(name);

        fileLength = fi.length();

        newStore(fileLength);
    }

    public long length() throws IOException {
        return store.length();
    }

    public void seek(long position) throws IOException {
        store.seek(position);
    }

    public long getFilePointer() throws IOException {
        return store.getFilePointer();
    }

    public int read() throws IOException {
        return store.read();
    }

    public void read(byte[] b, int offset, int length) throws IOException {
        store.read(b, offset, length);
    }

    public void write(byte[] b, int offset, int length) throws IOException {
        store.write(b, offset, length);
    }

    public int readInt() throws IOException {
        return store.readInt();
    }

    public void writeInt(int i) throws IOException {
        store.writeInt(i);
    }

    public long readLong() throws IOException {
        return store.readLong();
    }

    public void writeLong(long i) throws IOException {
        store.writeLong(i);
    }

    public void close() throws IOException {
        store.close();
    }

    public boolean isReadOnly() {
        return store.isReadOnly();
    }

    public boolean ensureLength(long newLength) {

        if (newLength <= initialMaxLength) {
            return store.ensureLength(newLength);
        } else if (preNio) {
            try {
                newStore(newLength);
            } catch (IOException e) {}
        }

        if (store.ensureLength(newLength)) {
            return true;
        } else if (isNio) {
            try {
                newStore(newLength);
            } catch (IOException e) {}
        }

        return store.ensureLength(newLength);
    }

    public boolean setLength(long newLength) {
        return store.setLength(newLength);
    }

    public Database getDatabase() {
        return null;
    }

    public void synch() {
        store.synch();
    }

    void newStore(long requiredPosition) throws IOException {

        long currentPosition = 0;

        if (store == null) {
            preNio = requiredPosition <= database.logger.propNioMaxSize;
        } else {
            currentPosition = store.getFilePointer();

            store.synch();
            store.close();
        }

        if (preNio && initialMaxLength <= requiredPosition) {
            try {
                store = new RAFileNIO(database, fileName, isReadOnly,
                                            requiredPosition,
                                            database.logger.propNioMaxSize);

                store.seek(currentPosition);

                preNio = false;
                isNio  = true;

                return;
            } catch (Throwable e) {
                preNio = false;

                // log event
            }
        }

        isNio = false;
        store = new RAFile(database, fileName, isReadOnly, true, false);

        store.seek(currentPosition);
    }
}
