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

/**
 * Type subclass for untyped NULL values.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class NullType extends Type {

    static final NullType nullType = new NullType();

    private NullType() {
        super(Types.SQL_ALL_TYPES, Types.SQL_ALL_TYPES, 0, 0);
    }

    public int displaySize() {
        return 4;
    }

    public int getJDBCTypeCode() {
        return typeCode;
    }

    public String getJDBCClassName() {
        return "java.lang.Void";
    }

    public String getNameString() {
        return Tokens.T_NULL;
    }

    public String getDefinition() {
        return Tokens.T_NULL;
    }

    public Type getAggregateType(Type other) {
        return other;
    }

    public Type getCombinedType(Type other, int operation) {
        return other;
    }

    public int compare(Object a, Object b) {
        throw Error.runtimeError(ErrorCode.U_S0500, "NullType");
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return null;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {
        return null;
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {
        return null;
    }

    public String convertToString(Object a) {
        throw Error.runtimeError(ErrorCode.U_S0500, "NullType");
    }

    public String convertToSQLString(Object a) {
        throw Error.runtimeError(ErrorCode.U_S0500, "NullType");
    }

    public boolean canConvertFrom(Type otherType) {
        return true;
    }

    public static Type getNullType() {
        return nullType;
    }
}
