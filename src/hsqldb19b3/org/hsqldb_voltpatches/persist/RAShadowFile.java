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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.lib.HsqlByteArrayOutputStream;
import org.hsqldb_voltpatches.lib.InputStreamInterface;
import org.hsqldb_voltpatches.lib.java.JavaSystem;
import org.hsqldb_voltpatches.map.BitMap;

/*
 * Wrapper for random access file for incremental backup of the .data file.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class RAShadowFile {

    final Database              database;
    final String                pathName;
    final RandomAccessInterface source;
    RandomAccessInterface       dest;
    final int                   pageSize;
    final long                  maxSize;
    final BitMap                bitMap;
    boolean                     zeroPageSet;
    long                        savedLength;
    long                        synchLength;
    byte[]                      buffer;
    HsqlByteArrayOutputStream   byteArrayOutputStream;

    RAShadowFile(Database database, RandomAccessInterface source,
                 String pathName, long maxSize, int pageSize) {

        this.database = database;
        this.pathName = pathName;
        this.source   = source;
        this.pageSize = pageSize;
        this.maxSize  = maxSize;

        int bitSize = (int) (maxSize / pageSize);

        if (maxSize % pageSize != 0) {
            bitSize++;
        }

        bitMap                = new BitMap(bitSize, false);
        buffer                = new byte[pageSize + 12];
        byteArrayOutputStream = new HsqlByteArrayOutputStream(buffer);
    }

    void copy(long fileOffset, int size) throws IOException {

        // always copy the first page
        if (!zeroPageSet) {
            copy(0);
            bitMap.set(0);

            zeroPageSet = true;
        }

        if (fileOffset >= maxSize) {
            return;
        }

        long endOffset = fileOffset + size;

        if (endOffset > maxSize) {
            endOffset = maxSize;
        }

        int startPageOffset = (int) (fileOffset / pageSize);
        int endPageOffset   = (int) (endOffset / pageSize);

        if (endOffset % pageSize == 0) {
            endPageOffset--;
        }

        for (; startPageOffset <= endPageOffset; startPageOffset++) {
            copy(startPageOffset);
        }
    }

    private void copy(int pageOffset) throws IOException {

        if (bitMap.set(pageOffset) == 1) {
            return;
        }

        long position = (long) pageOffset * pageSize;
        int  readSize = pageSize;

        if (maxSize - position < pageSize) {
            readSize = (int) (maxSize - position);
        }

        if (dest == null) {
            open();
        }

        long writePos = dest.length();

        try {
            byteArrayOutputStream.reset();

            if (readSize < pageSize) {
                byteArrayOutputStream.fill(0, buffer.length);
                byteArrayOutputStream.reset();
            }

            byteArrayOutputStream.writeInt(pageSize);
            byteArrayOutputStream.writeLong(position);
            source.seek(position);
            source.read(buffer, 12, readSize);
            dest.seek(writePos);
            dest.write(buffer, 0, buffer.length);

            savedLength = writePos + buffer.length;
        } catch (Throwable t) {
            bitMap.unset(pageOffset);
            dest.seek(0);
            dest.setLength(writePos);
            close();
            database.logger.logSevereEvent("shadow backup failure pos "
                                           + position + " " + readSize, t);

            throw JavaSystem.toIOException(t);
        }
    }

    private void open() throws IOException {

        if (database.logger.isStoredFileAccess()) {
            dest = RAFile.newScaledRAFile(database, pathName, false,
                                          RAFile.DATA_FILE_STORED);
        } else {
            dest = new RAFileSimple(database, pathName, "rws");
        }
    }

    /**
     * Called externally after a series of copy() calls.
     * Called internally after a restore or when error in writing
     */
    void close() throws IOException {

        if (dest != null) {
            dest.synch();
            dest.close();

            dest = null;
        }
    }

    public void synch() {

        if (dest != null) {
            synchLength = savedLength;

            dest.synch();
        }
    }

    public long getSavedLength() {
        return savedLength;
    }

    public InputStreamInterface getInputStream() {
        return new InputStreamShadow();
    }

    private static RandomAccessInterface getStorage(Database database,
            String pathName, String openMode) throws IOException {

        if (database.logger.isStoredFileAccess()) {
            return RAFile.newScaledRAFile(database, pathName,
                                          openMode.equals("r"),
                                          RAFile.DATA_FILE_STORED);
        } else {
            return new RAFileSimple(database, pathName, openMode);
        }
    }

    /** todo - take account of incomplete addition of block due to lack of disk */

    // buggy database files had size == position == 0 at the end
    public static void restoreFile(Database database, String sourceName,
                                   String destName) throws IOException {

        RandomAccessInterface source = getStorage(database, sourceName, "r");
        RandomAccessInterface dest   = getStorage(database, destName, "rw");

        while (source.getFilePointer() != source.length()) {
            int    size     = source.readInt();
            long   position = source.readLong();
            byte[] buffer   = new byte[size];

            source.read(buffer, 0, buffer.length);
            dest.seek(position);
            dest.write(buffer, 0, buffer.length);
        }

        source.close();
        dest.synch();
        dest.close();
    }

    class InputStreamShadow implements InputStreamInterface {

        FileInputStream is;
        long            limitSize   = 0;
        long            fetchedSize = 0;
        boolean         initialised = false;

        public int read() throws IOException {

            if (!initialised) {
                initialise();
            }

            if (fetchedSize == limitSize) {
                return -1;
            }

            int byteread = is.read();

            if (byteread < 0) {
                throw new IOException("backup file not complete "
                                      + fetchedSize + " " + limitSize);
            }

            fetchedSize++;

            return byteread;
        }

        public int read(byte bytes[]) throws IOException {
            return read(bytes, 0, bytes.length);
        }

        public int read(byte bytes[], int offset,
                        int length) throws IOException {

            if (!initialised) {
                initialise();
            }

            if (fetchedSize == limitSize) {
                return -1;
            }

            if (limitSize >= 0 && limitSize - fetchedSize < length) {
                length = (int) (limitSize - fetchedSize);
            }

            int count = is.read(bytes, offset, length);

            if (count < 0) {
                throw new IOException("backup file not complete "
                                      + fetchedSize + " " + limitSize);
            }

            fetchedSize += count;

            return count;
        }

        public long skip(long count) throws IOException {
            return 0;
        }

        public int available() throws IOException {
            return 0;
        }

        public void close() throws IOException {

            if (is != null) {
                is.close();
            }
        }

        public void setSizeLimit(long count) {
            limitSize = count;
        }

        public long getSizeLimit() {

            if (!initialised) {
                initialise();
            }

            return limitSize;
        }

        private void initialise() {

            limitSize = synchLength;

            database.logger.logDetailEvent("shadow file size for backup: "
                                           + limitSize);

            if (limitSize > 0) {
                try {
                    is = new FileInputStream(pathName);
                } catch (FileNotFoundException e) {}
            }

            initialised = true;
        }
    }
}
