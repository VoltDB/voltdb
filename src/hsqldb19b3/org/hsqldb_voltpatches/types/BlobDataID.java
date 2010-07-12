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

import java.io.InputStream;
import java.io.OutputStream;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultLob;

public class BlobDataID implements BlobData {

    long id;

    public BlobDataID(long id) {
        this.id = id;
    }

    public BlobData duplicate(SessionInterface session) {
        return null;
    }

    public void free() {}

    public InputStream getBinaryStream(SessionInterface session) {
        return null;
    }

    public InputStream getBinaryStream(SessionInterface session, long pos,
                                       long length) {
        return null;
    }

    public byte[] getBytes() {
        return null;
    }

    public byte[] getBytes(SessionInterface session, long pos, int length) {

        ResultLob resultOut = ResultLob.newLobGetBytesRequest(id, pos, length);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw Error.error(resultIn);
        }

        return ((ResultLob) resultIn).getByteArray();
    }

    public BlobData getBlob(SessionInterface session, long pos, long length) {

        ResultLob resultOut = ResultLob.newLobGetRequest(id, pos, length);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw Error.error(resultIn);
        }

        long lobID = ((ResultLob) resultIn).getLobID();

        return new BlobDataID(lobID);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getStreamBlockSize() {
        return 0;
    }

    public boolean isClosed() {
        return false;
    }

    public long length(SessionInterface session) {

        ResultLob resultOut = ResultLob.newLobGetLengthRequest(id);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        return ((ResultLob) resultIn).getBlockLength();
    }

    public long bitLength(SessionInterface session) {
        return 0;
    }

    public boolean isBits() {
        return false;
    }

    public long position(SessionInterface session, BlobData pattern,
                         long start) {
        return 0L;
    }

    public long position(SessionInterface session, byte[] pattern,
                         long start) {

        ResultLob resultOut = ResultLob.newLobGetBytePatternPositionRequest(id,
            pattern, start);
        ResultLob resultIn = (ResultLob) session.execute(resultOut);

        return resultIn.getOffset();
    }

    public long nonZeroLength(SessionInterface session) {
        return 0;
    }

    public OutputStream setBinaryStream(SessionInterface session, long pos) {
        return null;
    }

    public int setBytes(SessionInterface session, long pos, byte[] bytes,
                        int offset, int len) {

        ResultLob resultOut = ResultLob.newLobSetBytesRequest(id, pos, bytes);
        Result    resultIn  = (ResultLob) session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        return bytes.length;
    }

    public int setBytes(SessionInterface session, long pos, byte[] bytes) {
        return 0;
    }

    public long setBinaryStream(SessionInterface session, long pos,
                                InputStream in) {
        return 0;
    }

    public void setSession(SessionInterface session) {}

    public void truncate(SessionInterface session, long len) {

        ResultLob resultOut = ResultLob.newLobTruncateRequest(id, len);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }
    }

    public boolean isBinary() {
        return true;
    }
}
