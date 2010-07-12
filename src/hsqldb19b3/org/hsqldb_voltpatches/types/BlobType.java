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

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.StringConverter;

public final class BlobType extends BinaryType {

    public static final long maxBlobPrecision = 1024L * 1024 * 1024 * 1024;
    public static final int  defaultBlobSize  = 1024 * 1024 * 16;

    public BlobType() {
        super(Types.SQL_BLOB, defaultBlobSize);
    }

    public BlobType(long precision) {
        super(Types.SQL_BLOB, precision);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {
        return Types.BLOB;
    }

    public String getJDBCClassName() {
        return "java.sql.Blob";
    }

    public String getNameString() {
        return Tokens.T_BLOB;
    }

    public String getFullNameString() {
        return "BINARY LARGE OBJECT";
    }

    public String getDefinition() {

        long   factor     = precision;
        String multiplier = null;

        if (precision % (1024 * 1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024 * 1024);
            multiplier = Tokens.T_G_FACTOR;
        } else if (precision % (1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024);
            multiplier = Tokens.T_M_FACTOR;
        } else if (precision % (1024) == 0) {
            factor     = precision / (1024);
            multiplier = Tokens.T_K_FACTOR;
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(getNameString());
        sb.append('(');
        sb.append(factor);

        if (multiplier != null) {
            sb.append(multiplier);
        }

        sb.append(')');

        return sb.toString();
    }

    public boolean isBinaryType() {
        return true;
    }

    public boolean isLobType() {
        return true;
    }

    /** @todo implement comparison */
    public int compare(Object a, Object b) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        long aId = ((BlobData) a).getId();
        long bId = ((BlobData) b).getId();

        return (aId > bId) ? 1
                           : (bId > aId ? -1
                                        : 0);
    }

    /** @todo - implement */
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return a;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return null;
        }

        if (otherType.typeCode == Types.SQL_BLOB) {
            return a;
        }

        if (otherType.typeCode == Types.SQL_BINARY
                || otherType.typeCode == Types.SQL_VARBINARY) {
            return new BinaryData(((BinaryData) a).data, false);
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        // conversion to Blob via PreparedStatement.setObject();
        if (a instanceof byte[]) {
            return new BinaryData((byte[]) a, false);
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        byte[] bytes = ((BlobData) a).getBytes();

        return StringConverter.byteArrayToHexString(bytes);
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        byte[] bytes = ((BlobData) a).getBytes();

        return StringConverter.byteArrayToSQLHexString(bytes);
    }
}
