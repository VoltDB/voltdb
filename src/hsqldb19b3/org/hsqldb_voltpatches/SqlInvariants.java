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


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.UserTypeModifier;

/**
 * Invariant schema objects.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class SqlInvariants {

    /**
     * The role name reserved for authorization of INFORMATION_SCHEMA and
     * system objects.
     */
    public static final String SYSTEM_AUTHORIZATION_NAME = "_SYSTEM";

    /** The role name reserved for ADMIN users. */
    public static final String DBA_ADMIN_ROLE_NAME = "DBA";

    /** The role name allowing schema creation for users. */
    public static final String SCHEMA_CREATE_ROLE_NAME = "CREATE_SCHEMA";

    /** The role name allowing switching authorisation for users. */
    public static final String CHANGE_AUTH_ROLE_NAME = "CHANGE_AUTHORIZATION";

    //
    public static final String SYSTEM_SUBQUERY = "SYSTEM_SUBQUERY";

    /** The role name reserved for the special PUBLIC pseudo-user. */
    public static final String   PUBLIC_ROLE_NAME   = "PUBLIC";
    public static final String   SYSTEM_SCHEMA      = "SYSTEM_SCHEMA";
    public static final String   LOBS_SCHEMA        = "SYSTEM_LOBS";
    public static final String   DEFINITION_SCHEMA  = "DEFINITION_SCHEMA";
    public static final String   INFORMATION_SCHEMA = "INFORMATION_SCHEMA";
    public static final String   SQLJ_SCHEMA        = "SQLJ";
    public static final String   PUBLIC_SCHEMA      = "PUBLIC";
    public static final String   CLASSPATH_NAME     = "CLASSPATH";
    public static final HsqlName INFORMATION_SCHEMA_HSQLNAME;
    public static final HsqlName SYSTEM_SCHEMA_HSQLNAME;
    public static final HsqlName LOBS_SCHEMA_HSQLNAME;
    public static final HsqlName SQLJ_SCHEMA_HSQLNAME;
    public static final HsqlName SYSTEM_SUBQUERY_HSQLNAME;

    static {
        INFORMATION_SCHEMA_HSQLNAME =
            HsqlNameManager.newSystemObjectName(INFORMATION_SCHEMA,
                SchemaObject.SCHEMA);
        SYSTEM_SCHEMA_HSQLNAME =
            HsqlNameManager.newSystemObjectName(SYSTEM_SCHEMA,
                SchemaObject.SCHEMA);
        LOBS_SCHEMA_HSQLNAME = HsqlNameManager.newSystemObjectName(LOBS_SCHEMA,
                SchemaObject.SCHEMA);
        SQLJ_SCHEMA_HSQLNAME = HsqlNameManager.newSystemObjectName(SQLJ_SCHEMA,
                SchemaObject.SCHEMA);
        SYSTEM_SUBQUERY_HSQLNAME =
            HsqlNameManager.newSystemObjectName(SYSTEM_SUBQUERY,
                SchemaObject.TABLE);

        SYSTEM_SUBQUERY_HSQLNAME.setSchemaIfNull(SYSTEM_SCHEMA_HSQLNAME);
    }

    public static final Charset SQL_TEXT;
    public static final Charset SQL_IDENTIFIER_CHARSET;
    public static final Charset SQL_CHARACTER;
    public static final Charset ASCII_GRAPHIC;    // == GRAPHIC_IRV
    public static final Charset GRAPHIC_IRV;
    public static final Charset ASCII_FULL;       // == ISO8BIT
    public static final Charset ISO8BIT;
    public static final Charset LATIN1;
    public static final Charset UTF32;
    public static final Charset UTF16;
    public static final Charset UTF8;

    static {
        HsqlName name;

        name = HsqlNameManager.newInfoSchemaObjectName("SQL_TEXT", false,
                SchemaObject.CHARSET);
        SQL_TEXT = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("SQL_IDENTIFIER",
                false, SchemaObject.CHARSET);
        SQL_IDENTIFIER_CHARSET = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("SQL_CHARACTER", false,
                SchemaObject.CHARSET);
        SQL_CHARACTER = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("LATIN1", false,
                SchemaObject.CHARSET);
        LATIN1 = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("ASCII_GRAPHIC", false,
                SchemaObject.CHARSET);
        ASCII_GRAPHIC = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("GRAPHIC_IRV", false,
                SchemaObject.CHARSET);
        GRAPHIC_IRV = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("ASCII_FULL", false,
                SchemaObject.CHARSET);
        ASCII_FULL = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("ISO8BIT", false,
                SchemaObject.CHARSET);
        ISO8BIT = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("UTF32", false,
                SchemaObject.CHARSET);
        UTF32 = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("UTF16", false,
                SchemaObject.CHARSET);
        UTF16 = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("UTF8", false,
                SchemaObject.CHARSET);
        UTF8 = new Charset(name);
        /*
         * Foundattion 4.2.1
         * Character sets defined by standards or by SQL-implementations reside
         * in the Information Schema (named INFORMATION_SCHEMA) in each catalog,
         * as do collations defined by standards and collations,
         * transliterations, and transcodings defined by SQL implementations.
         */
    }

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

    public static final void checkSchemaNameNotSystem(String name) {

        if (isSchemaNameSystem(name)) {
            throw Error.error(ErrorCode.X_42503, name);
        }
    }

    public static final boolean isSchemaNameSystem(String name) {

        if (SqlInvariants.DEFINITION_SCHEMA.equals(name)
                || SqlInvariants.INFORMATION_SCHEMA.equals(name)
                || SqlInvariants.SYSTEM_SCHEMA.equals(name)
                || SqlInvariants.SQLJ_SCHEMA.equals(name)) {
            return true;
        }

        return false;
    }

    public static final boolean isSchemaNameSystem(HsqlName name) {

        if (name.schema != null) {
            name = name.schema;
        }

        if (SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.equals(name)
                || SqlInvariants.SYSTEM_SCHEMA_HSQLNAME.equals(name)
                || SqlInvariants.SQLJ_SCHEMA_HSQLNAME.equals(name)) {
            return true;
        }

        return false;
    }
}
