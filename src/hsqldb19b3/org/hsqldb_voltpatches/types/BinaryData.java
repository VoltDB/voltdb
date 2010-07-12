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


package org.hsqldb_voltpatches.types;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.lib.ArrayUtil;

/**
 * Implementation of BlobData for memory binary data.<p>
 * A Binary object instance always wraps a non-null byte[] object.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class BinaryData implements BlobData {

    public final static BinaryData zeroLengthBinary =
        new BinaryData(new byte[0], false);
    long             id;
    protected byte[] data;
    private boolean  isBits;
    private long     bitLength;

    /**
     * This constructor is used inside the engine when an already serialized
     * byte[] is read from a file (.log, .script, .data or text table source).
     * In this case clone is false.
     *
     * When a byte[] is submitted as a parameter of PreparedStatement then
     * clone is true.
     */
    public BinaryData(byte[] data, boolean clone) {

        if (clone) {
            data = (byte[]) ArrayUtil.duplicateArray(data);
        }

        this.data      = data;
        this.bitLength = data.length * 8;
    }

    public BinaryData(SessionInterface session, BlobData b1, BlobData b2) {

        long length = (b1.length(session) + b2.length(session));

        if (length > Integer.MAX_VALUE) {
            throw Error.error(ErrorCode.X_22001);
        }

        data = new byte[(int) length];

        System.arraycopy(b1.getBytes(), 0, data, 0, (int) b1.length(session));
        System.arraycopy(b2.getBytes(), 0, data, (int) b1.length(session),
                         (int) b2.length(session));

        this.bitLength = (b1.length(session) + b2.length(session)) * 8;
    }

    public BinaryData(byte[] data, long bitLength) {

        this.data      = data;
        this.bitLength = bitLength;
        this.isBits    = true;
    }

    public BinaryData(long length, DataInput stream) {

        data      = new byte[(int) length];
        bitLength = data.length * 8;

        try {
            stream.readFully(data);
        } catch (IOException e) {
            throw Error.error(ErrorCode.GENERAL_IO_ERROR, e);
        }
    }

    public byte[] getBytes() {
        return data;
    }

    public long length(SessionInterface session) {
        return data.length;
    }

    public long bitLength(SessionInterface session) {
        return bitLength;
    }

    public boolean isBits() {
        return isBits;
    }

    public byte[] getBytes(SessionInterface session, long pos, int length) {

        if (!isInLimits(data.length, pos, length)) {
            throw new IndexOutOfBoundsException();
        }

        byte[] bytes = new byte[length];

        System.arraycopy(data, (int) pos, bytes, 0, length);

        return bytes;
    }

    public BlobData getBlob(SessionInterface session, long pos, long length) {
        throw Error.runtimeError(ErrorCode.U_S0500, "BinaryData");
    }

    public InputStream getBinaryStream(SessionInterface session) {
        return new BlobInputStream(session, this, 0L, length(session));
    }

    public InputStream getBinaryStream(SessionInterface session, long pos,
                                       long length) {

        if (!isInLimits(data.length, pos, length)) {
            throw new IndexOutOfBoundsException();
        }

        return new BlobInputStream(session, this, pos, length(session));
    }

    public int setBytes(SessionInterface session, long pos, byte[] bytes,
                        int offset, int length) {

        if (!isInLimits(data.length, pos, 0)) {
            throw new IndexOutOfBoundsException();
        }

        if (!isInLimits(data.length, pos, length)) {
            data = (byte[]) ArrayUtil.resizeArray(data, (int) pos + length);
        }

        System.arraycopy(bytes, offset, data, (int) pos, length);

        bitLength = data.length * 8;

        return length;
    }

    public int setBytes(SessionInterface session, long pos, byte[] bytes) {

        setBytes(session, pos, bytes, 0, bytes.length);

        return bytes.length;
    }

    public long setBinaryStream(SessionInterface session, long pos,
                                InputStream in) {
        return 0;
    }

    public OutputStream setBinaryStream(SessionInterface session, long pos) {
        return null;
    }

    public void truncate(SessionInterface session, long len) {

        if (data.length > len) {
            data      = (byte[]) ArrayUtil.resizeArray(data, (int) len);
            bitLength = data.length * 8;
        }
    }

    public BlobData duplicate(SessionInterface session) {
        return new BinaryData(data, true);
    }

    public long position(SessionInterface session, byte[] pattern,
                         long start) {

        if (pattern.length > data.length) {
            return -1;
        }

        if (start >= data.length) {
            return -1;
        }

        return ArrayUtil.find(data, (int) start, data.length, pattern);
    }

    public long position(SessionInterface session, BlobData pattern,
                         long start) {

        if (pattern.length(session) > data.length) {
            return -1;
        }

        byte[] bytes = pattern.getBytes();

        return position(session, bytes, start);
    }

    public long nonZeroLength(SessionInterface session) {

        // temp
        return data.length;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getStreamBlockSize() {
        return 512 * 1024;
    }

    public boolean isClosed() {
        return false;
    }

    public void free() {}

    public void setSession(SessionInterface session) {}

    //---
    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }

    public boolean isBinary() {
        return true;
    }
}
