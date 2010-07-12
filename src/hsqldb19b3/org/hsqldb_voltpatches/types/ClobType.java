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
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.StringConverter;

/**
 * Type subclass CLOB data.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class ClobType extends CharacterType {

    static final long maxClobPrecision = 1024L * 1024 * 1024 * 1024;
    static final int  defaultClobSize  = 1024 * 1024 * 16;

    public ClobType() {
        super(Types.SQL_CLOB, defaultClobSize);
    }

    public ClobType(long precision) {
        super(Types.SQL_CLOB, precision);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {
        return Types.CLOB;
    }

    public String getJDBCClassName() {
        return "java.sql.Clob";
    }

    public int getSQLGenericTypeCode() {
        return typeCode;
    }

    public String getDefinition() {

        long   factor     = precision;
        String multiplier = null;

        if (precision % (1024) == 0) {
            if (precision % (1024 * 1024 * 1024) == 0) {
                factor     = precision / (1024 * 1024 * 1024);
                multiplier = Tokens.T_G_FACTOR;
            } else if (precision % (1024 * 1024) == 0) {
                factor     = precision / (1024 * 1024);
                multiplier = Tokens.T_M_FACTOR;
            } else {
                factor     = precision / (1024);
                multiplier = Tokens.T_K_FACTOR;
            }
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

        long aId = ((ClobData) a).getId();
        long bId = ((ClobData) b).getId();

        return (aId > bId) ? 1
                           : (bId > aId ? -1
                                        : 0);
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof ClobData) {
            return a;
        }

        if (a instanceof String) {
            ClobData clob = session.createClob(((String) a).length());

            clob.setString(session, 0, (String) a);

            return clob;
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return ((ClobData) a).toString();
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        String s = convertToString(a);

        return StringConverter.toQuotedString(s, '\'', true);
    }

    public long position(SessionInterface session, Object data,
                         Object otherData, Type otherType, long start) {

        if (otherType.typeCode == Types.SQL_CLOB) {
            return ((ClobData) data).position(session, (ClobData) otherData,
                                              start);
        } else if (otherType.isCharacterType()) {
            return ((ClobData) data).position(session, (String) otherData,
                                              start);
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "ClobType");
        }
    }
}
