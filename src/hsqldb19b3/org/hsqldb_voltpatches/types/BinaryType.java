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
import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.StringConverter;

/**
 * Type implementation for BINARY, VARBINARY and (part) BLOB.<p>

 * SQL:2008 Standard  specifies silent truncation of zero bytes at the end of the
 * binary strings used for assignment and contatenation.<p>
 *
 * * A binary string of type BINARY VALYING and BLOB when assigned to a column
 * of similar type but shorter maximum length.<p?
 *
 * * The Second operand of a concatenation when the length of the result exceeds
 * the maximum implementation-dependent length of BINARY VARYING and BLOB
 * binary strings.<p>
 *
 * The behaviour is similar to trimming of space characters from strings of
 * CHARACTER VARYING and CLOB types.<p>
 *
 * <p>
 * In most real-world use-cases, all the bytes of variable-length binary values
 * stored in a database are significant and should not be discarded.<p>
 *
 * HSQLDB follows the Standard completely, despite this inadequecy.<p>
 *
 * Comparison of binary values follows the Standard. When two values are not
 * the same length and all the bytes of the shorter value equal the initial
 * sequence of bytes of the longer value, then the shorter value is the smaller.
 * The Standard treats this determination as implementation dependent.<p>
 *
 * BIT types, which were part of the SQL:1999, can be converted to and from
 * BINARY. The BIT strings may be padded with zero bits for byte alignment.<p>
 *
 * As an extension to the Standard, HSQLDB supports cast from CHARACTER types
 * to BINARY. The length of the string must be even and all character
 * must be hexadecimal characters.<p>
 *
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class BinaryType extends Type {

    static final long maxBinaryPrecision = Integer.MAX_VALUE;

    protected BinaryType(int type, long precision) {
        super(Types.SQL_VARBINARY, type, precision, 0);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {
        return typeCode == Types.SQL_BINARY ? Types.BINARY
                                            : Types.VARBINARY;
    }

    public String getJDBCClassName() {
        return "[B";
    }

    public String getNameString() {
        return typeCode == Types.SQL_BINARY ? Tokens.T_BINARY
                                            : Tokens.T_VARBINARY;
    }

    public String getNameFullString() {
        return typeCode == Types.SQL_BINARY ? Tokens.T_BINARY
                                            : "BINARY VARYING";
    }

    public String getDefinition() {

        if (precision == 0) {
            return getNameString();
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(getNameString());
        sb.append('(');
        sb.append(precision);
        sb.append(')');

        return sb.toString();
    }

    public boolean isBinaryType() {
        return true;
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean requiresPrecision() {
        return typeCode == Types.SQL_VARBINARY;
    }

    /**
     * relaxes the SQL standard list to avoid problems with covnersion of
     * literals and java method parameter type issues
     */
    public int precedenceDegree(Type other) {

        if (other.typeCode == typeCode) {
            return 0;
        }

        if (!other.isBinaryType()) {
            return Integer.MIN_VALUE;
        }

        switch (typeCode) {

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return Integer.MIN_VALUE;

            case Types.SQL_BINARY :
                return other.typeCode == Types.SQL_BLOB ? 4
                                                        : 2;

            case Types.SQL_VARBINARY :
                return other.typeCode == Types.SQL_BLOB ? 4
                                                        : 2;

            case Types.SQL_BLOB :
                return other.typeCode == Types.SQL_BINARY ? -4
                                                          : -2;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public Type getAggregateType(Type other) {

        if (typeCode == other.typeCode) {
            return precision >= other.precision ? this
                                                : other;
        }

        if (other.isCharacterType()) {
            return other.getAggregateType(this);
        }

        switch (other.typeCode) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING : {
                long bytePrecision = (other.precision + 7) / 8;

                return precision >= bytePrecision ? this
                                                  : getBinaryType(
                                                      this.typeCode,
                                                      bytePrecision);
            }
            case Types.SQL_BINARY :
                return precision >= other.precision ? this
                                                    : getBinaryType(typeCode,
                                                    other.precision);

            case Types.SQL_VARBINARY :
                if (typeCode == Types.SQL_BLOB) {
                    return precision >= other.precision ? this
                                                        : getBinaryType(
                                                        typeCode,
                                                        other.precision);
                } else {
                    return other.precision >= precision ? other
                                                        : getBinaryType(
                                                        other.typeCode,
                                                        precision);
                }
            case Types.SQL_BLOB :
                return other.precision >= precision ? other
                                                    : getBinaryType(
                                                    other.typeCode, precision);

            default :
                throw Error.error(ErrorCode.X_42562);
        }
    }

    /**
     * Returns type for concat
     */
    public Type getCombinedType(Type other, int operation) {

        if (operation != OpTypes.CONCAT) {
            return getAggregateType(other);
        }

        Type newType;
        long newPrecision = this.precision + other.precision;

        switch (other.typeCode) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                newPrecision = this.precision + (other.precision + 7) / 8;
                newType      = this;
                break;

            case Types.SQL_BINARY :
                newType = this;
                break;

            case Types.SQL_VARBINARY :
                newType = (typeCode == Types.SQL_BLOB) ? this
                                                       : other;
                break;

            case Types.SQL_BLOB :
                newType = other;
                break;

            default :
                throw Error.error(ErrorCode.X_42561);
        }

        if (newPrecision > maxBinaryPrecision) {
            if (typeCode == Types.SQL_BINARY) {

                // Standard disallows type length reduction
                throw Error.error(ErrorCode.X_42570);
            } else if (typeCode == Types.SQL_VARBINARY) {
                newPrecision = maxBinaryPrecision;
            }
        }

        return getBinaryType(newType.typeCode, newPrecision);
    }

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

        if (a instanceof BinaryData && b instanceof BinaryData) {
            byte[] data1  = ((BinaryData) a).getBytes();
            byte[] data2  = ((BinaryData) b).getBytes();
            int    length = data1.length > data2.length ? data2.length
                                                        : data1.length;

            for (int i = 0; i < length; i++) {
                if (data1[i] == data2[i]) {
                    continue;
                }

                return (((int) data1[i]) & 0xff) > (((int) data2[i]) & 0xff)
                       ? 1
                       : -1;
            }

            if (data1.length == data2.length) {
                return 0;
            }

            return data1.length > data2.length ? 1
                                               : -1;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "BinaryType");
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return castOrConvertToType(session, a, this, false);
    }

    public Object castToType(SessionInterface session, Object a,
                             Type otherType) {
        return castOrConvertToType(session, a, otherType, true);
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {
        return castOrConvertToType(session, a, otherType, false);
    }

    public Object convertJavaToSQL(SessionInterface session, Object a) {

        if (a instanceof byte[]) {
            return new BinaryData((byte[]) a, true);
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {
        return ((BlobData) a).getBytes();
    }

    Object castOrConvertToType(SessionInterface session, Object a,
                               Type otherType, boolean cast) {

        BlobData b;

        if (a == null) {
            return null;
        }

        switch (otherType.typeCode) {

            // non-SQL feature, for compatibility with previous versions
            case Types.SQL_VARCHAR :
            case Types.SQL_CHAR : {
                b = session.getScanner().convertToBinary((String) a);
                otherType = getBinaryType(Types.SQL_VARBINARY,
                                          b.length(session));

                break;
            }
            case Types.SQL_BIT : {
                b = (BlobData) a;
                otherType = getBinaryType(Types.SQL_VARBINARY,
                                          b.length(session));

                break;
            }
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                b = (BlobData) a;
                break;

            default :
                throw Error.error(ErrorCode.X_22501);
        }

        if (precision == 0) {
            return b;    // never a blob
        }

        if (b.length(session) > precision
                && b.nonZeroLength(session) > precision) {
            if (!cast) {
                throw Error.error(ErrorCode.X_22001);
            }

            session.addWarning(Error.error(ErrorCode.W_01004));
        }

        if (otherType.typeCode == Types.SQL_BLOB) {
            byte[] bytes = b.getBytes(session, 0, (int) precision);

            b = new BinaryData(bytes, false);
        }

        switch (typeCode) {

            case Types.SQL_BINARY : {
                if (b.length(session) > precision) {
                    byte[] data = b.getBytes(session, 0, (int) precision);

                    b = new BinaryData(data, false);
                } else if (b.length(session) < precision) {
                    byte[] data = (byte[]) ArrayUtil.resizeArray(b.getBytes(),
                        (int) precision);

                    b = new BinaryData(data, false);
                }

                return b;
            }
            case Types.SQL_VARBINARY : {
                if (b.length(session) > precision) {
                    byte[] data = b.getBytes(session, 0, (int) precision);

                    b = new BinaryData(data, false);
                }

                return b;
            }
            default :
        }

        throw Error.error(ErrorCode.X_22501);
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        if (a instanceof byte[]) {
            return new BinaryData((byte[]) a, false);
        } else if (a instanceof BinaryData) {
            return a;
        } else if (a instanceof String) {
            return castOrConvertToType(session, a, Type.SQL_VARCHAR, false);
        }

        throw Error.error(ErrorCode.X_22501);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return StringConverter.byteArrayToHexString(((BlobData) a).getBytes());
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        return StringConverter.byteArrayToSQLHexString(
            ((BinaryData) a).getBytes());
    }

    public boolean canConvertFrom(Type otherType) {
        return otherType.typeCode == Types.SQL_ALL_TYPES
               || otherType.isBinaryType() || otherType.isCharacterType();
    }

    public long position(SessionInterface session, BlobData data,
                         BlobData otherData, Type otherType, long offset) {

        if (data == null || otherData == null) {
            return -1L;
        }

        long otherLength = ((BlobData) data).length(session);

        if (offset + otherLength > data.length(session)) {
            return -1;
        }

        return data.position(session, otherData, offset);
    }

    public BlobData substring(SessionInterface session, BlobData data,
                              long offset, long length, boolean hasLength) {

        long end;
        long dataLength = data.length(session);

        if (hasLength) {
            end = offset + length;
        } else {
            end = dataLength > offset ? dataLength
                                      : offset;
        }

        if (offset > end) {
            throw Error.error(ErrorCode.X_22011);
        }

        if (offset > end || end < 0) {

            // return zero length data
            offset = 0;
            end    = 0;
        }

        if (offset < 0) {
            offset = 0;
        }

        if (end > dataLength) {
            end = dataLength;
        }

        length = end - offset;

        // change method signature to take long
        byte[] bytes = ((BlobData) data).getBytes(session, offset,
            (int) length);

        return new BinaryData(bytes, false);
    }

    int getRightTrimSize(BlobData data) {

        byte[] bytes    = data.getBytes();
        int    endindex = bytes.length;

        for (--endindex; endindex >= 0 && bytes[endindex] == 0; endindex--) {}

        return ++endindex;
    }

    public BlobData trim(Session session, BlobData data, int trim,
                         boolean leading, boolean trailing) {

        if (data == null) {
            return null;
        }

        byte[] bytes    = ((BlobData) data).getBytes();
        int    endindex = bytes.length;

        if (trailing) {
            for (--endindex; endindex >= 0 && bytes[endindex] == trim;
                    endindex--) {}

            endindex++;
        }

        int startindex = 0;

        if (leading) {
            while (startindex < endindex && bytes[startindex] == trim) {
                startindex++;
            }
        }

        byte[] newBytes = bytes;

        if (startindex != 0 || endindex != bytes.length) {
            newBytes = new byte[endindex - startindex];

            System.arraycopy(bytes, startindex, newBytes, 0,
                             endindex - startindex);
        }

        if (typeCode == Types.SQL_BLOB) {
            BlobData blob = session.createBlob(newBytes.length);

            blob.setBytes(session, 0, newBytes);

            return blob;
        } else {
            return new BinaryData(newBytes, newBytes == bytes);
        }
    }

    public BlobData overlay(Session session, BlobData data, BlobData overlay,
                            long offset, long length, boolean hasLength) {

        if (data == null || overlay == null) {
            return null;
        }

        if (!hasLength) {
            length = ((BlobData) overlay).length(session);
        }

        switch (typeCode) {

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY : {
                BinaryData binary =
                    new BinaryData(session,
                                   substring(session, data, 0, offset, true),
                                   overlay);

                binary = new BinaryData(session, binary,
                                        substring(session, data,
                                                  offset + length, 0, false));

                return binary;
            }
            case Types.SQL_BLOB : {
                byte[] bytes = substring(session, data, 0, offset,
                                         false).getBytes();
                long blobLength = data.length(session)
                                  + overlay.length(session) - length;
                BlobData blob = session.createBlob(blobLength);

                blob.setBytes(session, 0, bytes);
                blob.setBytes(session, blob.length(session),
                              overlay.getBytes());

                bytes = substring(session, data, offset + length, 0,
                                  false).getBytes();

                blob.setBytes(session, blob.length(session), bytes);

                return blob;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "BinaryType");
        }
    }

    public Object concat(Session session, Object a, Object b) {

        if (a == null || b == null) {
            return null;
        }

        long length = ((BlobData) a).length(session)
                      + ((BlobData) b).length(session);

        if (length > precision) {
            throw Error.error(ErrorCode.X_22001);
        }

        if (typeCode == Types.SQL_BLOB) {
            BlobData blob = session.createBlob(length);

            blob.setBytes(session, 0, ((BlobData) b).getBytes());
            blob.setBytes(session, ((BlobData) a).length(session),
                          ((BlobData) b).getBytes());

            return blob;
        } else {
            return new BinaryData(session, (BlobData) a, (BlobData) b);
        }
    }

    /** @todo check and adjust max precision */
    public static BinaryType getBinaryType(int type, long precision) {

        switch (type) {

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                return new BinaryType(type, precision);

            case Types.SQL_BLOB :
                return new BlobType(precision);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "BinaryType");
        }
    }
}
