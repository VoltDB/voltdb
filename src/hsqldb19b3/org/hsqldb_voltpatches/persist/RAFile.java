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

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlByteArrayInputStream;
import org.hsqldb_voltpatches.lib.HsqlByteArrayOutputStream;
import org.hsqldb_voltpatches.lib.Storage;

/**
 * This class is a wrapper for a random access file such as that used for
 * CACHED table storage.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since  1.7.2
 */
final class RAFile implements RandomAccessInterface {

    static final int DATA_FILE_RAF    = 0;
    static final int DATA_FILE_NIO    = 1;
    static final int DATA_FILE_JAR    = 2;
    static final int DATA_FILE_STORED = 3;
    static final int DATA_FILE_SINGLE = 4;
    static final int DATA_FILE_TEXT   = 5;

    //
    static final int  bufferScale = 12;
    static final int  bufferSize  = 1 << bufferScale;
    static final long bufferMask  = 0xffffffffffffffffl << bufferScale;

    //
    final Database                  database;
    final RandomAccessFile          file;
    final FileDescriptor            fileDescriptor;
    private final boolean           readOnly;
    final String                    fileName;
    final byte[]                    buffer;
    final HsqlByteArrayInputStream  ba;
    final byte[]                    valueBuffer;
    final HsqlByteArrayOutputStream vbao;
    final HsqlByteArrayInputStream  vbai;
    long                            bufferOffset;
    long                            fileLength;
    final boolean                   extendLength;

    //
    long seekPosition;
    int  cacheHit;

    /**
     * seekPosition is the position in seek() calls or after reading or writing
     * realPosition is the file position
     */
    static RandomAccessInterface newScaledRAFile(Database database,
            String name, boolean readonly,
            int type) throws FileNotFoundException, IOException {

        if (type == DATA_FILE_STORED) {
            try {
                String cname = database.getURLProperties().getProperty(
                    HsqlDatabaseProperties.url_storage_class_name);
                String skey = database.getURLProperties().getProperty(
                    HsqlDatabaseProperties.url_storage_key);
                Class storageClass;

                try {
                    ClassLoader classLoader =
                        Thread.currentThread().getContextClassLoader();

                    storageClass = classLoader.loadClass(cname);
                } catch (ClassNotFoundException e) {
                    storageClass = Class.forName(cname);
                }

                Constructor constructor =
                    storageClass.getConstructor(new Class[] {
                    String.class, Boolean.class, Object.class
                });
                Object accessor = constructor.newInstance(new Object[] {
                    name, Boolean.valueOf(readonly), skey
                });

                if (accessor instanceof RandomAccessInterface) {
                    return (RandomAccessInterface) accessor;
                } else if (accessor instanceof org.hsqldb_voltpatches.lib.Storage) {
                    return new RAStorageWrapper((Storage) accessor);
                } else {
                    throw new IOException();
                }
            } catch (ClassNotFoundException e) {
                throw new IOException();
            } catch (NoSuchMethodException e) {
                throw new IOException();
            } catch (InstantiationException e) {
                throw new IOException();
            } catch (IllegalAccessException e) {
                throw new IOException();
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw new IOException();
            }
        }

        if (type == DATA_FILE_JAR) {
            return new RAFileInJar(name);
        } else if (type == DATA_FILE_TEXT) {
            RAFile ra = new RAFile(database, name, readonly, false, true);

            return ra;
        } else if (type == DATA_FILE_RAF) {
            return new RAFile(database, name, readonly, true, false);
        } else {
            java.io.File fi     = new java.io.File(name);
            long         length = fi.length();

            if (length > database.logger.propNioMaxSize) {
                return new RAFile(database, name, readonly, true, false);
            }

            try {
                Class.forName("java.nio.MappedByteBuffer");

                return new RAFileHybrid(database, name, readonly);
            } catch (Exception e) {
                return new RAFile(database, name, readonly, true, false);
            }
        }
    }

    RAFile(Database database, String name, boolean readonly,
            boolean extendLengthToBlock,
            boolean commitOnChange) throws FileNotFoundException, IOException {

        this.database     = database;
        this.fileName     = name;
        this.readOnly     = readonly;
        this.extendLength = extendLengthToBlock;

        String accessMode = readonly ? "r"
                                     : commitOnChange ? "rws"
                                                      : "rw";

        this.file      = new RandomAccessFile(name, accessMode);
        buffer         = new byte[bufferSize];
        ba             = new HsqlByteArrayInputStream(buffer);
        valueBuffer    = new byte[8];
        vbao           = new HsqlByteArrayOutputStream(valueBuffer);
        vbai           = new HsqlByteArrayInputStream(valueBuffer);
        fileDescriptor = file.getFD();
        fileLength     = length();

        readIntoBuffer();
    }

    public long length() throws IOException {
        return file.length();
    }

    public void seek(long position) throws IOException {

        if (readOnly && fileLength < position) {
            throw new IOException("read beyond end of file");
        }

        seekPosition = position;
    }

    public long getFilePointer() throws IOException {
        return seekPosition;
    }

    private void readIntoBuffer() throws IOException {

        long filePos    = seekPosition & bufferMask;
        long readLength = fileLength - filePos;

        if (readLength > buffer.length) {
            readLength = buffer.length;
        }

        if (readLength < 0) {
            throw new IOException("read beyond end of file");
        }

        try {
            file.seek(filePos);
            file.readFully(buffer, 0, (int) readLength);

            bufferOffset = filePos;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent(" " + filePos + " " + readLength,
                                            e);

            throw e;
        }
    }

    public int read() throws IOException {

        try {
            if (seekPosition >= fileLength) {
                return -1;
            }

            if (seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }

            int val = buffer[(int) (seekPosition - bufferOffset)] & 0xff;

            seekPosition++;

            return val;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("read failed", e);

            throw e;
        }
    }

    public long readLong() throws IOException {

        vbai.reset();
        read(valueBuffer, 0, 8);

        return vbai.readLong();
    }

    public int readInt() throws IOException {

        vbai.reset();
        read(valueBuffer, 0, 4);

        return vbai.readInt();
    }

    public void read(byte[] b, int offset, int length) throws IOException {

        try {
            if (seekPosition + length > fileLength) {
                throw new EOFException();
            }

            if (length > buffer.length
                    && (seekPosition < bufferOffset
                        || seekPosition >= bufferOffset + buffer.length)) {
                file.seek(seekPosition);
                file.readFully(b, offset, length);

                seekPosition += length;

                return;
            }

            if (seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }

            ba.reset();

            if (seekPosition - bufferOffset
                    != ba.skip(seekPosition - bufferOffset)) {
                throw new EOFException();
            }

            int bytesRead = ba.read(b, offset, length);

            seekPosition += bytesRead;

            if (bytesRead < length) {
                file.seek(seekPosition);
                file.readFully(b, offset + bytesRead, length - bytesRead);

                seekPosition += (length - bytesRead);
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to read a byte array", e);

            throw e;
        }
    }

    public void write(byte[] b, int off, int length) throws IOException {

        try {
            file.seek(seekPosition);

            if (seekPosition < bufferOffset + buffer.length
                    && seekPosition + length > bufferOffset) {
                writeToBuffer(b, off, length);
            }

            file.write(b, off, length);

            seekPosition += length;

            if (!extendLength && fileLength < seekPosition) {
                fileLength = seekPosition;
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to write a byte array", e);

            throw e;
        }
    }

    public void writeInt(int i) throws IOException {

        vbao.reset();
        vbao.writeInt(i);
        write(valueBuffer, 0, 4);
    }

    public void writeLong(long i) throws IOException {

        vbao.reset();
        vbao.writeLong(i);
        write(valueBuffer, 0, 8);
    }

    public void close() throws IOException {
        file.close();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean ensureLength(long newLength) {

        if (newLength <= fileLength) {
            return true;
        }

        try {
            extendLength(newLength);
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    public boolean setLength(long newLength) {

        try {
            file.setLength(newLength);
            file.seek(0);

            fileLength   = file.length();
            seekPosition = 0;

            readIntoBuffer();

            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    public void synch() {

        try {
            fileDescriptor.sync();
        } catch (Throwable t) {
            try {
                fileDescriptor.sync();
            } catch (Throwable tt) {
                database.logger.logSevereEvent("RA file sync error ", tt);

                throw Error.error(t, ErrorCode.FILE_IO_ERROR, null);
            }
        }
    }

    private int writeToBuffer(byte[] b, int off, int len) throws IOException {
        return ArrayUtil.copyBytes(seekPosition - off, b, off, len,
                                   bufferOffset, buffer, buffer.length);
    }

    private long getExtendLength(long position) {

        if (!extendLength) {
            return position;
        }

        int scaleUp;

        if (position < 256 * 1024) {
            scaleUp = 2;
        } else if (position < 1024 * 1024) {
            scaleUp = 6;
        } else if (position < 32 * 1024 * 1024) {
            scaleUp = 8;
        } else {
            scaleUp = 12;
        }

        position = getBinaryNormalisedCeiling(position, bufferScale + scaleUp);

        return position;
    }

    /**
     * Some old JVM's do not allow seek beyond end of file, so zeros must be written
     * first in that case. Reported by bohgammer@users in Open Disucssion
     * Forum.
     */
    private void extendLength(long position) throws IOException {

        long newSize = getExtendLength(position);

        if (newSize > fileLength) {
            try {
                file.seek(newSize - 1);
                file.write(0);

                fileLength = newSize;
            } catch (IOException e) {
                database.logger.logWarningEvent("data file enlarge failed ",
                                                e);

                throw e;
            }
        }
    }

    private void resetPointer() {

        try {
            seekPosition = 0;
            fileLength   = length();

            readIntoBuffer();
        } catch (Throwable e) {}
    }

    /**
     * uses 2**scale form and returns a multipe of this that is larger or equal to value
     */
    static long getBinaryNormalisedCeiling(long value, int scale) {

        long mask    = 0xffffffffffffffffl << scale;
        long newSize = value & mask;

        if (newSize != value) {
            newSize += 1 << scale;
        }

        return newSize;
    }
}
