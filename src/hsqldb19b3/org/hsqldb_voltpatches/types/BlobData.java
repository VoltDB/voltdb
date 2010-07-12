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

import org.hsqldb_voltpatches.SessionInterface;

/**
 * Interface for Binary Large Object implementations.<p>
 *
 * All positions are 0 based
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public interface BlobData extends LobData {

    byte[] getBytes();

    byte[] getBytes(SessionInterface session, long pos, int length);

    BlobData getBlob(SessionInterface session, long pos, long length);

    InputStream getBinaryStream(SessionInterface session);

    InputStream getBinaryStream(SessionInterface session, long pos,
                                long length);

    long length(SessionInterface session);

    long bitLength(SessionInterface session);

    boolean isBits();

    int setBytes(SessionInterface session, long pos, byte[] bytes, int offset,
                 int len);

    int setBytes(SessionInterface session, long pos, byte[] bytes);

    public long setBinaryStream(SessionInterface session, long pos,
                                InputStream in);

    OutputStream setBinaryStream(SessionInterface session, long pos);

    void truncate(SessionInterface session, long len);

    BlobData duplicate(SessionInterface session);

    long position(SessionInterface session, byte[] pattern, long start);

    long position(SessionInterface session, BlobData pattern, long start);

    long nonZeroLength(SessionInterface session);

    long getId();

    void setId(long id);

    void free();

    boolean isClosed();

    void setSession(SessionInterface session);

    int getStreamBlockSize();
}
