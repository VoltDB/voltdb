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

import java.math.BigDecimal;

import org.hsqldb_voltpatches.HsqlDateTime;
import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.lib.java.JavaSystem;

/**
 * Type subclass for CHARACTER, VARCHAR, etc.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class CharacterType extends Type {

    static final int  defaultCharPrecision    = 256;
    static final int  defaultVarcharPrecision = 32 * 1024;
    public static final long maxCharPrecision        = Integer.MAX_VALUE;
    Collation         collation;
    Charset           charset;
    String            nameString;
    // A VoltDB extension to support character columns sized in bytes
    public boolean           inBytes = false;
    // End of VoltDB extension

    public CharacterType(Collation collation, int type, long precision) {

        super(Types.SQL_VARCHAR, type, precision, 0);

        if (collation == null) {
            collation = Collation.getDefaultInstance();
        }

        this.collation = collation;
        this.charset   = Charset.getDefaultInstance();
        nameString     = getNameStringPrivate();
    }

    /**
     * Always ASCII collation
     */
    public CharacterType(int type, long precision) {

        super(Types.SQL_VARCHAR, type, precision, 0);

        this.collation = Collation.getDefaultInstance();
        this.charset   = Charset.getDefaultInstance();
        nameString     = getNameStringPrivate();
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {

        switch (typeCode) {

            case Types.SQL_CHAR :
                return Types.CHAR;

            case Types.SQL_VARCHAR :
                return Types.VARCHAR;

            case Types.SQL_CLOB :
                return Types.CLOB;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public Class getJDBCClass() {
        return String.class;
    }

    public String getJDBCClassName() {
        return "java.lang.String";
    }

    public int getSQLGenericTypeCode() {
        return typeCode == Types.SQL_CHAR ? typeCode
                                          : Types.SQL_VARCHAR;
    }

    public String getNameString() {
        return nameString;
    }

    private String getNameStringPrivate() {

        switch (typeCode) {

            case Types.SQL_CHAR :
                return Tokens.T_CHARACTER;

            case Types.SQL_VARCHAR :
                return Tokens.T_VARCHAR;

            case Types.SQL_CLOB :
                return Tokens.T_CLOB;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public String getFullNameString() {

        switch (typeCode) {

            case Types.SQL_CHAR :
                return Tokens.T_CHARACTER;

            case Types.SQL_VARCHAR :
                return "CHARACTER VARYING";

            case Types.SQL_CLOB :
                return "CHARACTER LARGE OBJECT";

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
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

    public boolean isCharacterType() {
        return true;
    }

    public long getMaxPrecision() {
        return maxCharPrecision;
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean requiresPrecision() {
        return typeCode == Types.SQL_VARCHAR;
    }

    public int precedenceDegree(Type other) {

        if (other.typeCode == typeCode) {
            return 0;
        }

        if (!other.isCharacterType()) {
            return Integer.MIN_VALUE;
        }

        switch (typeCode) {

            case Types.SQL_CHAR :
                return other.typeCode == Types.SQL_CLOB ? 4
                                                        : 2;

            case Types.SQL_VARCHAR :
                return other.typeCode == Types.SQL_CLOB ? 4
                                                        : 2;

            case Types.SQL_CLOB :
                return other.typeCode == Types.SQL_CHAR ? -4
                                                        : -2;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public Type getAggregateType(Type other) {

        if (other == null) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        if (typeCode == other.typeCode) {
            return precision >= other.precision ? this
                                                : other;
        }

        switch (other.typeCode) {

            case Types.SQL_CHAR :
                return precision >= other.precision ? this
                                                    : getCharacterType(
                                                    typeCode, other.precision,
                                                    other.getCollation());

            case Types.SQL_VARCHAR :
                if (typeCode == Types.SQL_CLOB) {
                    return precision >= other.precision ? this
                                                        : getCharacterType(
                                                        typeCode,
                                                        other.precision,
                                                        other.getCollation());
                } else {
                    return other.precision >= precision ? other
                                                        : getCharacterType(
                                                        other.typeCode,
                                                        precision,
                                                        other.getCollation());
                }
            case Types.SQL_CLOB :
                return other.precision >= precision ? other
                                                    : getCharacterType(
                                                    other.typeCode, precision,
                                                    other.getCollation());

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
            case Types.SQL_BLOB :
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.OTHER :
                throw Error.error(ErrorCode.X_42562);
            default :

                /**
                 * @todo - this seems to be allowed in SQL-92 (is in NIST)
                 * but is disallowed in SQL:2003
                 * need to make dependent on a database property
                 */
/*
                int length = other.displaySize();

                return getCharacterType(Types.SQL_VARCHAR,
                                        length).getAggregateType(this);
*/
                throw Error.error(ErrorCode.X_42562);
        }
    }

    /**
     * For concatenation
     */
    public Type getCombinedType(Session session, Type other, int operation) {

        if (operation != OpTypes.CONCAT) {
            return getAggregateType(other);
        }

        Type newType;
        long newPrecision = this.precision + other.precision;

        switch (other.typeCode) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_CHAR :
                newType = this;
                break;

            case Types.SQL_VARCHAR :
                newType = (typeCode == Types.SQL_CLOB) ? this
                                                       : other;
                break;

            case Types.SQL_CLOB :
                newType = other;
                break;

            default :
                throw Error.error(ErrorCode.X_42562);
        }

        if (newPrecision > maxCharPrecision) {
            if (typeCode == Types.SQL_BINARY) {

                // Standard disallows type length reduction
                // throw Error.error(ErrorCode.X_42570);
                newPrecision = maxCharPrecision;
            } else if (typeCode == Types.SQL_CHAR) {
                newPrecision = maxCharPrecision;
            } else if (typeCode == Types.SQL_VARCHAR) {
                newPrecision = maxCharPrecision;
            }
        }

        return getCharacterType(newType.typeCode, newPrecision, collation);
    }

    public int compare(Session session, Object a, Object b) {
        return compare(session, a, b, OpTypes.EQUAL);
    }

    public int compare(Session session, Object a, Object b, int opType) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        if (b instanceof ClobData) {
            return -session.database.lobManager.compare(collation,
                    (ClobData) b, (String) a);
        }

        String as = (String) a;
        String bs = (String) b;
        int    la = as.length();
        int    lb = bs.length();

        if (la == lb) {

            //
        } else if (la > lb) {
            if (collation.isPadSpace()
                    && opType != OpTypes.GREATER_EQUAL_PRE) {
                char[] buffer = new char[la];

                bs.getChars(0, lb, buffer, 0);
                ArrayUtil.fillArray(buffer, lb, ' ');

                bs = String.valueOf(buffer);
            }
        } else {
            if (collation.isPadSpace()
                    && opType != OpTypes.GREATER_EQUAL_PRE) {
                char[] buffer = new char[lb];

                as.getChars(0, la, buffer, 0);
                ArrayUtil.fillArray(buffer, la, ' ');

                as = String.valueOf(buffer);
            }
        }

        return collation.compare(as, bs);
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        if (precision == 0) {
            return a;
        }

        switch (typeCode) {

            case Types.SQL_CHAR : {
                int slen = ((String) a).length();

                if (slen == precision) {
                    return a;
                }

                if (slen > precision) {
                    if (getRightTrimSise((String) a, ' ') <= precision) {
                        return ((String) a).substring(0, (int) precision);
                    } else {
                        throw Error.error(ErrorCode.X_22001);
                    }
                }

                char[] b = new char[(int) precision];

                ((String) a).getChars(0, slen, b, 0);

                for (int i = slen; i < precision; i++) {
                    b[i] = ' ';
                }

                return new String(b);
            }
            case Types.SQL_VARCHAR : {
                int slen = ((String) a).length();

                if (slen > precision) {
                    if (getRightTrimSise((String) a, ' ') <= precision) {
                        return ((String) a).substring(0, (int) precision);
                    } else {
                        throw Error.error(ErrorCode.X_22001);
                    }
                }

                return a;
            }
            case Types.SQL_CLOB : {
                ClobData clob = (ClobData) a;

                if (clob.length(session) > precision) {
                    throw Error.error(ErrorCode.X_22001);
                }

                return a;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public Object castToType(SessionInterface session, Object a,
                             Type otherType) {

        if (a == null) {
            return a;
        }

        return castOrConvertToType(session, a, otherType, true);
    }

    public Object castOrConvertToType(SessionInterface session, Object a,
                                      Type otherType, boolean cast) {

        switch (otherType.typeCode) {

            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR : {
                int length = ((String) a).length();

                if (precision != 0 && length > precision) {
                    if (StringUtil.rightTrimSize((String) a) > precision) {
                        if (!cast) {
                            throw Error.error(ErrorCode.X_22001);
                        }

                        session.addWarning(Error.error(ErrorCode.W_01004));
                    }

                    a = ((String) a).substring(0, (int) precision);
                }

                switch (typeCode) {

                    case Types.SQL_CHAR :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_VARCHAR :
                        return a;

                    case Types.SQL_CLOB : {
                        ClobData clob =
                            session.createClob(((String) a).length());

                        clob.setString(session, 0, (String) a);

                        return clob;
                    }
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "CharacterType");
                }
            }
            case Types.SQL_CLOB : {
                long length = ((ClobData) a).length(session);

                if (precision != 0 && length > precision) {

                    // todo nonSpaceLength() not yet implemented for CLOB
                    if (!cast) {
                        throw Error.error(ErrorCode.X_22001);
                    }

                    session.addWarning(Error.error(ErrorCode.W_01004));
                }

                switch (typeCode) {

                    case Types.SQL_CHAR :
                    case Types.SQL_VARCHAR : {
                        if (length > maxCharPrecision) {
                            if (!cast) {
                                throw Error.error(ErrorCode.X_22001);
                            }

                            length = maxCharPrecision;
                        }

                        a = ((ClobData) a).getSubString(session, 0,
                                                        (int) length);

                        return convertToTypeLimits(session, a);
                    }
                    case Types.SQL_CLOB : {
                        if (precision != 0 && length > precision) {
                            return ((ClobData) a).getClob(session, 0,
                                                          precision);
                        }

                        return a;
                    }
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "CharacterType");
                }
            }
            case Types.OTHER :
                throw Error.error(ErrorCode.X_42561);
            case Types.SQL_BLOB :
                long blobLength = ((BlobData) a).length(session);

                if (precision != 0 && blobLength * 2 > precision) {
                    throw Error.error(ErrorCode.X_22001);
                }

                byte[] bytes = ((BlobData) a).getBytes(session, 0,
                                                       (int) blobLength);

                a = StringConverter.byteArrayToHexString(bytes);

                return convertToTypeLimits(session, a);

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            default :
                String s = otherType.convertToString(a);

                if (precision != 0 && s.length() > precision) {
                    throw Error.error(ErrorCode.X_22001);
                }

                a = s;

                return convertToTypeLimits(session, a);
        }
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return a;
        }

        return castOrConvertToType(session, a, otherType, false);
    }

    public Object convertToTypeJDBC(SessionInterface session, Object a,
                                    Type otherType) {

        if (a == null) {
            return a;
        }

        if (otherType.typeCode == Types.SQL_BLOB) {
            throw Error.error(ErrorCode.X_42561);
        }

        return convertToType(session, a, otherType);
    }

    /**
     * Relaxes SQL parameter type enforcement, allowing long strings.
     */
    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        String s;

        if (a instanceof Boolean) {
            s = a.toString();
        } else if (a instanceof BigDecimal) {
            s = JavaSystem.toString((BigDecimal) a);
        } else if (a instanceof Number) {
            s = a.toString();    // use shortcut
        } else if (a instanceof String) {
            s = (String) a;
        } else if (a instanceof java.sql.Date) {
            s = ((java.sql.Date) a).toString();
        } else if (a instanceof java.sql.Time) {
            s = ((java.sql.Time) a).toString();
        } else if (a instanceof java.sql.Timestamp) {
            s = ((java.sql.Timestamp) a).toString();
        } else if (a instanceof java.util.Date) {
            s = HsqlDateTime.getTimestampString(((java.util.Date) a).getTime());
        } else {
            throw Error.error(ErrorCode.X_42561);
        }

        return s;

        // return convertToType(session, a, Type.SQL_VARCHAR);
    }

    public Object convertJavaToSQL(SessionInterface session, Object a) {
        return convertToDefaultType(session, a);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_CHAR : {
                int slen = ((String) a).length();

                if (precision == 0 || slen == precision) {
                    return (String) a;
                }

                char[] b = new char[(int) precision];

                ((String) a).getChars(0, slen, b, 0);

                for (int i = slen; i < precision; i++) {
                    b[i] = ' ';
                }

                return new String(b);
            }
            case Types.SQL_VARCHAR : {
                return (String) a;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return Tokens.T_NULL;
        }

        String s = convertToString(a);

        return StringConverter.toQuotedString(s, '\'', true);
    }

    public boolean canConvertFrom(Type otherType) {
        return !otherType.isObjectType() && !otherType.isArrayType();
    }

    public int canMoveFrom(Type otherType) {

        if (otherType == this) {
            return 0;
        }

        if (!otherType.isCharacterType()) {
            return -1;
        }

        switch (typeCode) {

            case Types.SQL_VARCHAR : {
                if (otherType.typeCode == typeCode) {
                    return precision >= otherType.precision ? 0
                                                            : 1;
                }

                if (otherType.typeCode == Types.SQL_CHAR) {
                    return precision >= otherType.precision ? 0
                                                            : -1;
                }

                return -1;
            }
            case Types.SQL_CLOB : {
                if (otherType.typeCode == Types.SQL_CLOB) {
                    return precision >= otherType.precision ? 0
                                                            : 1;
                }

                return -1;
            }
            case Types.SQL_CHAR : {
                return otherType.typeCode == Types.SQL_CHAR
                       && precision == otherType.precision ? 0
                                                           : -1;
            }
            default :
                return -1;
        }
    }

    public Collation getCollation() {
        return collation;
    }

    public Charset getCharacterSet() {
        return charset;
    }

    public long position(SessionInterface session, Object data,
                         Object otherData, Type otherType, long offset) {

        if (data == null || otherData == null) {
            return -1L;
        }

        if (otherType.typeCode == Types.SQL_CLOB) {
            long otherLength = ((ClobData) otherData).length(session);

            if (offset + otherLength > ((String) data).length()) {
                return -1;
            }

            if (otherLength > Integer.MAX_VALUE) {
                throw Error.error(ErrorCode.X_22026);
            }

            String otherString = ((ClobData) otherData).getSubString(session,
                0, (int) otherLength);

            return ((String) data).indexOf(otherString, (int) offset);
        } else if (otherType.isCharacterType()) {
            long otherLength = ((String) otherData).length();

            if (offset + otherLength > ((String) data).length()) {
                return -1;
            }

            return ((String) data).indexOf((String) otherData, (int) offset);
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public Object substring(SessionInterface session, Object data,
                            long offset, long length, boolean hasLength,
                            boolean trailing) {

        long end;
        long dataLength = typeCode == Types.SQL_CLOB
                          ? ((ClobData) data).length(session)
                          : ((String) data).length();

        if (trailing) {
            end = dataLength;

            if (length > dataLength) {
                offset = 0;
                length = dataLength;
            } else {
                offset = dataLength - length;
            }
        } else if (hasLength) {
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

        if (data instanceof String) {
            return ((String) data).substring((int) offset,
                                             (int) (offset + length));
        } else if (data instanceof ClobData) {
            ClobData clob = session.createClob(length);

            if (length > Integer.MAX_VALUE) {
                throw Error.error(ErrorCode.X_22001);
            }

            /** @todo - change to support long strings */
            String result = ((ClobData) data).getSubString(session, offset,
                (int) length);

            clob.setString(session, 0, result);

            return clob;
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    /**
     * Memory limits apply to Upper and Lower implementations with Clob data
     */
    public Object upper(Session session, Object data) {

        if (data == null) {
            return null;
        }

        if (typeCode == Types.SQL_CLOB) {
            String result = ((ClobData) data).getSubString(session, 0,
                (int) ((ClobData) data).length(session));

            result = collation.toUpperCase(result);

            ClobData clob = session.createClob(result.length());

            clob.setString(session, 0, result);

            return clob;
        }

        return collation.toUpperCase((String) data);
    }

    public Object lower(Session session, Object data) {

        if (data == null) {
            return null;
        }

        if (typeCode == Types.SQL_CLOB) {
            String result = ((ClobData) data).getSubString(session, 0,
                (int) ((ClobData) data).length(session));

            result = collation.toLowerCase(result);

            ClobData clob = session.createClob(result.length());

            clob.setString(session, 0, result);

            return clob;
        }

        return collation.toLowerCase((String) data);
    }

    public Object trim(SessionInterface session, Object data, char trim,
                       boolean leading, boolean trailing) {

        if (data == null) {
            return null;
        }

        String s;

        if (typeCode == Types.SQL_CLOB) {
            long length = ((ClobData) data).length(session);

            if (length > Integer.MAX_VALUE) {
                throw Error.error(ErrorCode.X_22026);
            }

            s = ((ClobData) data).getSubString(session, 0, (int) length);
        } else {
            s = (String) data;
        }

        int endindex = s.length();

        if (trailing) {
            for (--endindex; endindex >= 0 && s.charAt(endindex) == trim;
                    endindex--) {}

            endindex++;
        }

        int startindex = 0;

        if (leading) {
            while (startindex < endindex && s.charAt(startindex) == trim) {
                startindex++;
            }
        }

        /** @todo - change to support long strings */
        if (startindex == 0 && endindex == s.length()) {}
        else {
            s = s.substring(startindex, endindex);
        }

        if (typeCode == Types.SQL_CLOB) {
            ClobData clob = session.createClob(s.length());

            clob.setString(session, 0, s);

            return clob;
        } else {
            return s;
        }
    }

    public Object overlay(SessionInterface session, Object data,
                          Object overlay, long offset, long length,
                          boolean hasLength) {

        if (data == null || overlay == null) {
            return null;
        }

        if (!hasLength) {
            length = typeCode == Types.SQL_CLOB
                     ? ((ClobData) overlay).length(session)
                     : ((String) overlay).length();
        }

        Object temp = concat(null,
                             substring(session, data, 0, offset, true, false),
                             overlay);

        return concat(null, temp,
                      substring(session, data, offset + length, 0, false,
                                false));
    }

    public Object concat(Session session, Object a, Object b) {

        if (a == null || b == null) {
            return null;
        }

        String left;
        String right;

        if (a instanceof ClobData) {
            left = ((ClobData) a).getSubString(
                session, 0, (int) ((ClobData) a).length(session));
        } else {
            left = (String) a;
        }

        if (b instanceof ClobData) {
            right = ((ClobData) b).getSubString(
                session, 0, (int) ((ClobData) b).length(session));
        } else {
            right = (String) b;
        }

        if (typeCode == Types.SQL_CLOB) {
            ClobData clob = session.createClob(left.length() + right.length());

            clob.setString(session, 0, left);
            clob.setString(session, left.length(), right);

            return clob;
        } else {
            return left + right;
        }
    }

    public long size(SessionInterface session, Object data) {

        if (typeCode == Types.SQL_CLOB) {
            return ((ClobData) data).length(session);
        }

        return ((String) data).length();
    }

    /**
     * Matches the string against array containing part strings. Null element
     * in array indicates skip one character. Empty string in array indicates
     * skip any number of characters.
     */
    public Boolean match(Session session, String string, String[] array) {

        if (string == null || array == null) {
            return null;
        }

        String  s      = null;
        int     offset = 0;
        boolean match  = true;

        for (int i = 0; i < array.length; i++) {
            if (array[i] == null) {

                // single char skip
                offset++;

                match = true;
            } else if (array[i].length() == 0) {

                // string skip
                match = false;
            }

            if (match) {
                if (offset + array[i].length() > string.length()) {
                    return Boolean.FALSE;
                }

                s = string.substring(offset, offset + array[i].length());

                if (collation.compare(s, array[i]) != 0) {
                    return Boolean.FALSE;
                }

                offset += array[i].length();
            } else {
                int index = string.indexOf(array[i], offset);

                if (index < 0) {
                    return Boolean.FALSE;
                }

                offset = index + array[i].length();
                match  = true;
            }
        }

        return Boolean.TRUE;
    }

    public Type getCharacterType(long length) {

        if (length == precision) {
            return this;
        }

        return getCharacterType(this.typeCode, length, this.collation);
    }

    public static int getRightTrimSise(String s, char trim) {

        int endindex = s.length();

        for (--endindex; endindex >= 0 && s.charAt(endindex) == trim;
                endindex--) {}

        endindex++;

        return endindex;
    }

    private static final int fixedTypesLength = 32;
    static CharacterType[]   charArray = new CharacterType[fixedTypesLength];

    static {
        for (int i = 0; i < charArray.length; i++) {
            charArray[i] = new CharacterType(Types.SQL_CHAR, i);
        }
    }

    public static CharacterType getCharacterType(int type, long length) {

        switch (type) {

            case Types.SQL_CHAR :
                if (length < fixedTypesLength) {
                    return charArray[(int) length];
                }

            // fall through
            case Types.SQL_VARCHAR :
                return new CharacterType(type, (int) length);

            case Types.SQL_CLOB :
                return new ClobType(length);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }

    public static CharacterType getCharacterType(int type, long length,
            Collation collation) {

        if (collation == null) {
            collation = Collation.getDefaultInstance();
        }

        switch (type) {

            case Types.SQL_VARCHAR :
            case Types.SQL_CHAR :
                return new CharacterType(collation, type, (int) length);

            case Types.SQL_CLOB :
                CharacterType typeObject = new ClobType(length);

                typeObject.collation = collation;

                return typeObject;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "CharacterType");
        }
    }
}
