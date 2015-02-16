/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;
import org.hsqldb_voltpatches.types.UserTypeModifier;

/**
 * Information Schema types.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class TypeInvariants {

    public static final Type CARDINAL_NUMBER;
    public static final Type YES_OR_NO;
    public static final Type CHARACTER_DATA;
    public static final Type SQL_IDENTIFIER;
    public static final Type TIME_STAMP;

    static {
        HsqlName name;

        name = HsqlNameManager.newInfoSchemaObjectName("CARDINAL_NUMBER",
                false, SchemaObject.DOMAIN);
        CARDINAL_NUMBER = new NumberType(Types.SQL_BIGINT, 0, 0);
        CARDINAL_NUMBER.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, CARDINAL_NUMBER);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("YES_OR_NO", false,
                SchemaObject.DOMAIN);
        YES_OR_NO = new CharacterType(Types.SQL_VARCHAR, 3);
        YES_OR_NO.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, YES_OR_NO);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("CHARACTER_DATA",
                false, SchemaObject.DOMAIN);
        CHARACTER_DATA = new CharacterType(Types.SQL_VARCHAR, (1 << 16));
        CHARACTER_DATA.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, CHARACTER_DATA);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("SQL_IDENTIFIER",
                false, SchemaObject.DOMAIN);
        SQL_IDENTIFIER = new CharacterType(Types.SQL_VARCHAR, 128);
        SQL_IDENTIFIER.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, SQL_IDENTIFIER);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("TIME_STAMP", false,
                SchemaObject.DOMAIN);
        TIME_STAMP = new DateTimeType(Types.SQL_TIMESTAMP,
                                      Types.SQL_TIMESTAMP, 6);
        TIME_STAMP.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, TIME_STAMP);
    }
}
