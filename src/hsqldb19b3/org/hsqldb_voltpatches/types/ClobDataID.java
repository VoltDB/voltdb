/* Copyright (c) 2001-2014, The HSQL Development Group
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

import java.io.Reader;

import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultLob;

/**
 * Locator for CLOB.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.6
 * @since 1.9.0
 */
public class ClobDataID implements ClobData {

    long id;
    long length = -1;

    public ClobDataID(long id) {
        this.id = id;
    }

    public char[] getChars(SessionInterface session, long position,
                           int length) {

        ResultLob resultOut = ResultLob.newLobGetCharsRequest(id, position,
            length);
        Result resultIn = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        return ((ResultLob) resultIn).getCharArray();
    }

    public long length(SessionInterface session) {

        if (length > -1) {
            return length;
        }

        ResultLob resultOut = ResultLob.newLobGetLengthRequest(id);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        length = ((ResultLob) resultIn).getBlockLength();

        return length;
    }

    public String getSubString(SessionInterface session, long pos,
                               int length) {

        char[] chars = getChars(session, pos, length);

        return new String(chars);
    }

    public ClobData duplicate(SessionInterface session) {

        ResultLob resultOut = ResultLob.newLobDuplicateRequest(id);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        long lobID = ((ResultLob) resultIn).getLobID();

        return new ClobDataID(lobID);
    }

    public ClobData getClob(SessionInterface session, long position,
                            long length) {

        ResultLob resultOut = ResultLob.newLobGetRequest(id, position, length);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        long lobID = ((ResultLob) resultIn).getLobID();

        return new ClobDataID(lobID);
    }

    public void truncate(SessionInterface session, long len) {

        ResultLob resultOut = ResultLob.newLobTruncateRequest(id, len);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        this.length = ((ResultLob) resultIn).getBlockLength();
    }

    public Reader getCharacterStream(SessionInterface session) {

        long length = length(session);

        return new ClobInputStream(session, this, 0, length);
    }

    public void setCharacterStream(SessionInterface session, long pos,
                                   Reader in) {}

    public void setString(SessionInterface session, long pos, String str) {

        ResultLob resultOut = ResultLob.newLobSetCharsRequest(id, pos,
            str.toCharArray());
        Result resultIn = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        this.length = ((ResultLob) resultIn).getBlockLength();
    }

    public void setClob(SessionInterface session, long pos, ClobData clob,
                        long offset, long len) {}

    public void setChars(SessionInterface session, long pos, char[] chars,
                         int offset, int len) {

        if (offset != 0 || len != chars.length) {
            if (!isInLimits(chars.length, offset, len)) {
                throw Error.error(ErrorCode.X_22001);
            }

            if (offset != 0 || len != chars.length) {
                char[] newChars = new char[len];

                System.arraycopy(chars, offset, newChars, 0, len);

                chars = newChars;
            }
        }

        ResultLob resultOut = ResultLob.newLobSetCharsRequest(id, pos, chars);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        this.length = ((ResultLob) resultIn).getBlockLength();
    }

    public long position(SessionInterface session, String searchstr,
                         long start) {

        ResultLob resultOut = ResultLob.newLobGetCharPatternPositionRequest(id,
            searchstr.toCharArray(), start);
        Result resultIn = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        return ((ResultLob) resultIn).getOffset();
    }

    public long position(SessionInterface session, ClobData searchstr,
                         long start) {

        ResultLob resultOut = ResultLob.newLobGetCharPatternPositionRequest(id,
            searchstr.getId(), start);
        Result resultIn = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        return ((ResultLob) resultIn).getOffset();
    }

    /** @todo - implement the next method call in Session */
    public long nonSpaceLength(SessionInterface session) {

        ResultLob resultOut = ResultLob.newLobGetTruncateLength(id);
        Result    resultIn  = session.execute(resultOut);

        if (resultIn.isError()) {
            throw resultIn.getException();
        }

        return ((ResultLob) resultIn).getBlockLength();
    }

    public Reader getCharacterStream(SessionInterface session, long pos,
                                     long length) {
        return new ClobInputStream(session, this, pos, length);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }

    public void setSession(SessionInterface session) {}

    public boolean isBinary() {
        return false;
    }

    public boolean equals(Object other) {

        if (other instanceof ClobDataID) {
            return id == ((ClobDataID) other).id;
        }

        return false;
    }

    public int hashCode() {
        return (int) id;
    }
}
