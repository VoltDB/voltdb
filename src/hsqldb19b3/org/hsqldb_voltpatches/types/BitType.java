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
import org.hsqldb_voltpatches.store.BitMap;

/**
 *
 * Operations allowed on BIT strings are CONCAT, SUBSTRING, POSITION,
 * BIT_LENGTH and OCTECT_LENGTH.<p>
 *
 * BIT values can be cast to BINARY and vice-versa. In casts, BIT values are
 * converted to their counterpart BINARY values by treating each set of 8 bits
 * or less as a signle byte. The first bit of a BIT string is treated as the most
 * significan bit of the resutling byte value. Binary values are converted by
 * treating the bits in the sequence of bytes as sequence of bits in the BIT
 * string<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class BitType extends BinaryType {

    static final long maxBitPrecision = 1024;

    public BitType(int type, long precision) {
        super(type, precision);
    }

    public int displaySize() {
        return (int) precision;
    }

    public int getJDBCTypeCode() {
        return Types.BIT;
    }

    public String getJDBCClassName() {
        return "[B";
    }

    public int getSQLGenericTypeCode() {
        return typeCode;
    }

    public String getNameString() {
        return typeCode == Types.SQL_BIT ? Tokens.T_BIT
                                         : "BIT VARYING";
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

    public boolean isBitType() {
        return true;
    }

    public boolean requiresPrecision() {
        return typeCode == Types.SQL_BIT_VARYING;
    }

    public Type getAggregateType(Type other) {

        if (typeCode == other.typeCode) {
            return precision >= other.precision ? this
                                                : other;
        }

        switch (other.typeCode) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BIT :
                return precision >= other.precision ? this
                                                    : getBitType(typeCode,
                                                    other.precision);

            case Types.SQL_BIT_VARYING :
                return other.precision >= precision ? other
                                                    : getBitType(
                                                    other.typeCode, precision);

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                return other;

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
        long newPrecision = precision + other.precision;

        switch (other.typeCode) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BIT :
                newType = this;
                break;

            case Types.SQL_BIT_VARYING :
                newType = other;
                break;

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                return other.getCombinedType(this, operation);

            default :
                throw Error.error(ErrorCode.X_42562);
        }

        if (newPrecision > maxBitPrecision) {
            if (typeCode == Types.SQL_BIT) {

                // Standard disallows type length reduction
                throw Error.error(ErrorCode.X_42570);
            }

            newPrecision = maxBitPrecision;
        }

        return getBitType(newType.typeCode, newPrecision);
    }

    public int compare(Object a, Object b) {

        int i = super.compare(a, b);

        if (i == 0) {
            if (((BinaryData) a).bitLength(null) == 0) {
                return 0;
            }

            return ((BinaryData) a).bitLength(null)
                   > ((BinaryData) b).bitLength(null) ? 1
                                                      : -1;
        }

        return i;
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return castOrConvertToType(null, a, this, false);
    }

    public Object castToType(SessionInterface session, Object a,
                             Type otherType) {
        return castOrConvertToType(session, a, otherType, true);
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {
        return castOrConvertToType(session, a, otherType, false);
    }

    Object castOrConvertToType(SessionInterface session, Object a,
                               Type otherType, boolean cast) {

        BlobData b;

        if (a == null) {
            return null;
        }

        switch (otherType.typeCode) {

            case Types.SQL_VARCHAR :
            case Types.SQL_CHAR : {
                b = session.getScanner().convertToBit((String) a);
                otherType = getBitType(Types.SQL_BIT_VARYING,
                                       b.length(session));

                break;
            }
            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                b = (BlobData) a;
                break;

            default :
                throw Error.error(ErrorCode.X_22501);
        }

        // no special 0 bit consideration
        if (b.bitLength(session) > precision) {
            if (!cast) {
                throw Error.error(ErrorCode.X_22001);
            }

            session.addWarning(Error.error(ErrorCode.W_01004));
        }

        int bytePrecision = (int) ((precision + 7) / 8);

        if (otherType.typeCode == Types.SQL_BLOB) {
            byte[] bytes = b.getBytes(session, 0, bytePrecision);

            b = new BinaryData(bytes, precision);
        }

        switch (typeCode) {

            case Types.SQL_BIT : {
                if (b.bitLength(session) == precision) {
                    return b;
                }

                if (b.length(session) > bytePrecision) {
                    byte[] data = b.getBytes(session, 0, bytePrecision);

                    b = new BinaryData(data, precision);
                } else if (b.length(session) <= bytePrecision) {
                    byte[] data = (byte[]) ArrayUtil.resizeArray(b.getBytes(),
                        bytePrecision);

                    b = new BinaryData(data, precision);
                }

                break;
            }
            case Types.SQL_BIT_VARYING : {
                if (b.bitLength(session) <= precision) {
                    return b;
                }

                if (b.length(session) > bytePrecision) {
                    byte[] data = b.getBytes(session, 0, bytePrecision);

                    b = new BinaryData(data, precision);
                }

                break;
            }
            default :
                throw Error.error(ErrorCode.X_22501);
        }

        byte[] data = b.getBytes();

        for (int i = (int) precision; i < b.length(session) * 8; i++) {
            BitMap.unset(data, i);
        }

        return b;
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        if (a instanceof byte[]) {
            BinaryData data = new BinaryData((byte[]) a, ((byte[]) a).length);

            return convertToTypeLimits(session, data);
        } else if (a instanceof BinaryData) {
            return convertToTypeLimits(session, a);
        } else if (a instanceof String) {
            return convertToType(session, a, Type.SQL_VARCHAR);
        }

        throw Error.error(ErrorCode.X_22501);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return StringConverter.byteArrayToBitString(
            ((BinaryData) a).getBytes(),
            (int) ((BinaryData) a).bitLength(null));
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
               || otherType.isBinaryType();
    }

    /** @todo - implement */
    public long position(SessionInterface session, BlobData data,
                         BlobData otherData, Type otherType, long offset) {

        if (data == null || otherData == null) {
            return -1L;
        }

        long otherLength = ((BlobData) data).bitLength(session);

        if (offset + otherLength > data.bitLength(session)) {
            return -1;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "BitType");
    }

    public BlobData substring(SessionInterface session, BlobData data,
                              long offset, long length, boolean hasLength) {

        long end;
        long dataLength = data.bitLength(session);

        if (hasLength) {
            end = offset + length;
        } else {
            end = dataLength > offset ? dataLength
                                      : offset;
        }

        if (end < offset) {
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

        byte[] dataBytes = data.getBytes();
        byte[] bytes     = new byte[(int) (length + 7) / 8];

        for (int i = (int) offset; i < end; i++) {
            if (BitMap.isSet(dataBytes, i)) {
                BitMap.set(bytes, i - (int) offset);
            }
        }

        return new BinaryData(bytes, length);
    }

    int getRightTrimSize(BinaryData data) {

        int    i     = (int) data.bitLength(null) - 1;
        byte[] bytes = data.getBytes();

        for (; i >= 0; i--) {
            if (BitMap.isSet(bytes, i)) {
                break;
            }
        }

        return i + 1;
    }

    public BlobData overlay(Session session, BlobData value, BlobData overlay,
                            long offset, long length, boolean hasLength) {

        if (value == null || overlay == null) {
            return null;
        }

        if (!hasLength) {
            length = ((BlobData) overlay).bitLength(session);
        }

        switch (typeCode) {

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING : {
                byte[] data =
                    (byte[]) ArrayUtil.duplicateArray(value.getBytes());
                byte[] overlaydata = overlay.getBytes();

                for (int i = 0, pos = (int) offset; i < length;
                        pos += 8, i++) {
                    int count = 8;

                    if (length - pos < 8) {
                        count = (int) length - pos;
                    }

                    BitMap.overlay(data, pos, overlaydata[i], count);
                }

                BinaryData binary = new BinaryData(data,
                                                   value.bitLength(session));

                return binary;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "BinaryType");
        }
    }

    public Object concat(Session session, Object a, Object b) {

        if (a == null || b == null) {
            return null;
        }

        long length = ((BlobData) a).bitLength(session)
                      + ((BlobData) b).bitLength(session);

        if (length > Integer.MAX_VALUE) {
            throw Error.error(ErrorCode.W_01000);
        }

        byte[] aData   = ((BlobData) a).getBytes();
        byte[] bData   = ((BlobData) b).getBytes();
        int    aLength = (int) ((BlobData) a).bitLength(session);
        int    bLength = (int) ((BlobData) b).bitLength(session);
        byte[] bytes   = new byte[(int) (length + 7) / 8];

        System.arraycopy(aData, 0, bytes, 0, aData.length);

        for (int i = 0; i < bLength; i++) {
            if (BitMap.isSet(bData, i)) {
                BitMap.set(bytes, aLength + i);
            }
        }

        return new BinaryData(bytes, length);
    }

    public static BinaryType getBitType(int type, long precision) {

        switch (type) {

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return new BitType(type, precision);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "BitType");
        }
    }
}
