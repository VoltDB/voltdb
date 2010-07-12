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

import java.io.Serializable;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.StringConverter;

/**
 * Type implementation for OTHER type.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class OtherType extends Type {

    static final OtherType otherType = new OtherType();

    private OtherType() {
        super(Types.OTHER, Types.OTHER, 0, 0);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {
        return typeCode;
    }

    public String getJDBCClassName() {
        return "java.lang.Object";
    }

    public int getSQLGenericTypeCode() {

        // return Types.SQL_UDT;
        return typeCode;
    }

    public int typeCode() {

        // return Types.SQL_UDT;
        return typeCode;
    }

    public String getNameString() {
        return Tokens.T_OTHER;
    }

    public String getDefinition() {
        return Tokens.T_OTHER;
    }

    public Type getAggregateType(Type other) {

        if (typeCode == other.typeCode) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        throw Error.error(ErrorCode.X_42562);
    }

    public Type getCombinedType(Type other, int operation) {
        return this;
    }

    public int compare(Object a, Object b) {

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        return 0;
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return a;
    }

    // to review - if conversion is supported, then must be serializable and wappred
    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {
        return a;
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a instanceof Serializable) {
            return a;
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return StringConverter.byteArrayToHexString(
            ((JavaObjectData) a).getBytes());
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        return StringConverter.byteArrayToSQLHexString(
            ((JavaObjectData) a).getBytes());
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {
        return ((JavaObjectData) a).getObject();
    }

    public boolean canConvertFrom(Type otherType) {

        if (otherType.typeCode == typeCode) {
            return true;
        }

        if (otherType.typeCode == Types.SQL_CHAR
                || otherType.typeCode == Types.SQL_VARCHAR) {
            return true;
        }

        if (otherType.isNumberType()) {
            return true;
        }

        return false;
    }

    public boolean isObjectType() {
        return true;
    }

    public static OtherType getOtherType() {
        return otherType;
    }
}
