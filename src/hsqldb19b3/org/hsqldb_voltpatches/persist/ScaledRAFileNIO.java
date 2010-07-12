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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.lib.SimpleLog;

/**
 * New NIO version of ScaledRAFile. This class is used only for storing a CACHED
 * TABLE .data file and cannot be used for TEXT TABLE source files.
 *
 * Due to various issues with java.nio classes, this class will use a mapped
 * channel of fixed size. After reaching this size, the file and channel are
 * closed.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version  1.8.0.5
 * @since 1.8.0.5
 */
final class ScaledRAFileNIO implements ScaledRAInterface {

    private final boolean       readOnly;
    private final long          bufferLength;
    private RandomAccessFile    file;
    private MappedByteBuffer    buffer;
    private FileChannel         channel;
    private boolean             bufferModified;
    private SimpleLog           appLog;
    private final static String JVM_ERROR = "JVM threw unsupported Exception";

    ScaledRAFileNIO(Database database, String name, boolean readOnly,
                    int bufferLength) throws Throwable {

        long fileLength;

        if (bufferLength < 1 << 18) {
            bufferLength = 1 << 18;
        }

        try {
            file = new RandomAccessFile(name, readOnly ? "r"
                                                       : "rw");
        } catch (Throwable e) {
            throw e;
        }

        try {
            fileLength = file.length();
        } catch (Throwable e) {
            file.close();

            throw e;
        }

        if (fileLength > ScaledRAFile.MAX_NIO_LENGTH) {
            file.close();

            throw new IOException("length exceeds nio limit");
        }

        if (bufferLength < fileLength) {
            bufferLength = (int) fileLength;
        }

        bufferLength = newNIOBufferSize(bufferLength);

        if (readOnly) {
            bufferLength = (int) fileLength;
        }

        if (fileLength < bufferLength) {
            try {
                file.seek(bufferLength - 1);
                file.writeByte(0);
                file.getFD().sync();
                file.close();

                file = new RandomAccessFile(name, readOnly ? "r"
                                                           : "rw");
            } catch (Throwable e) {
                file.close();

                throw e;
            }
        }

        this.appLog       = database.logger.appLog;
        this.readOnly     = readOnly;
        this.bufferLength = bufferLength;
        this.channel      = file.getChannel();

        try {
            buffer = channel.map(readOnly ? FileChannel.MapMode.READ_ONLY
                                          : FileChannel.MapMode.READ_WRITE, 0,
                                          bufferLength);

            Error.printSystemOut("NIO file instance created. mode: "
                                 + readOnly);

            if (!readOnly) {
                long tempSize = bufferLength - fileLength;

                if (tempSize > 1 << 18) {
                    tempSize = 1 << 18;
                }

                byte[] temp = new byte[(int) tempSize];

                try {
                    long pos = fileLength;

                    for (; pos < bufferLength - tempSize; pos += tempSize) {
                        buffer.position((int) pos);
                        buffer.put(temp, 0, temp.length);
                    }

                    buffer.position((int) pos);
                    buffer.put(temp, 0, (int) (bufferLength - pos));
                    buffer.force();
                } catch (Throwable t) {
                    appLog.logContext(t, JVM_ERROR + " " + "length: "
                                      + bufferLength);
                }

                buffer.position(0);
            }
        } catch (Throwable e) {
            Error.printSystemOut("NIO constructor failed:  " + bufferLength);

            buffer  = null;
            channel = null;

            file.close();
            System.gc();

            throw e;
        }
    }

    public long length() throws IOException {

        try {
            return file.length();
        } catch (IOException e) {
            appLog.logContext(e, "nio");

            throw e;
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public void seek(long newPos) throws IOException {

        try {
            buffer.position((int) newPos);
        } catch (IllegalArgumentException e) {
            appLog.logContext(e, "nio");

            throw new IOException(e.toString());
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public long getFilePointer() throws IOException {

        try {
            return buffer.position();
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public int read() throws IOException {

        try {
            return buffer.get();
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public void read(byte[] b, int offset, int length) throws IOException {

        try {
            buffer.get(b, offset, length);
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public int readInt() throws IOException {

        try {
            return buffer.getInt();
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public long readLong() throws IOException {

        try {
            return buffer.getLong();
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public void write(byte[] b, int offset, int len) throws IOException {

        try {
            bufferModified = true;

            buffer.put(b, offset, len);
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public void writeInt(int i) throws IOException {

        try {
            bufferModified = true;

            buffer.putInt(i);
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public void writeLong(long i) throws IOException {

        try {
            bufferModified = true;

            buffer.putLong(i);
        } catch (Throwable e) {
            appLog.logContext(e, JVM_ERROR);

            throw new IOException(e.toString());
        }
    }

    public void close() throws IOException {

        try {
            Error.printSystemOut("NIO close() start - fileLength = "
                                 + bufferLength);

            if (buffer != null && bufferModified) {
                try {
                    buffer.force();
                } catch (Throwable t) {
                    try {
                        buffer.force();
                    } catch (Throwable t1) {
                        appLog.logContext(t, JVM_ERROR + " " + "length: "
                                          + bufferLength);
                    }
                }
            }

            buffer  = null;
            channel = null;

            file.close();
            System.gc();
        } catch (Throwable e) {
            appLog.logContext(e, "length: " + bufferLength);

            throw new IOException(e.toString());
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean wasNio() {
        return true;
    }

    public boolean canAccess(int length) {
        return buffer.position() + length <= bufferLength;
    }

    public boolean canSeek(long position) {
        return position <= bufferLength;
    }

    public Database getDatabase() {
        return null;
    }

    static int newNIOBufferSize(int newSize) {

        int bufSize = 0;

        for (int scale = 20; scale < 30; scale++) {
            bufSize = 1 << scale;

            if (bufSize >= newSize) {
                break;
            }
        }

        return bufSize;
    }
}
