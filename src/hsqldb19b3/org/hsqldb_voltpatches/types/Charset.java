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


package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SqlInvariants;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;

/**
 * Implementation of CHARACTER SET objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class Charset implements SchemaObject {

    public static final int[][] uppercaseLetters   = new int[][] {
        {
            'A', 'Z'
        }
    };
    public static final int[][] unquotedIdentifier = new int[][] {
        {
            '0', '9'
        }, {
            'A', 'Z'
        }, {
            '_', '_'
        }
    };
    public static final int[][] basicIdentifier    = new int[][] {
        {
            '0', '9'
        }, {
            'A', 'Z'
        }, {
            '_', '_'
        }, {
            'a', 'z'
        }
    };
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

    HsqlName        name;
    public HsqlName base;

    //
    int[][] ranges;

    public Charset(HsqlName name) {
        this.name = name;
    }

    public int getType() {
        return SchemaObject.CHARSET;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {

        OrderedHashSet set = new OrderedHashSet();

        set.add(base);

        return set;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ').append(
            Tokens.T_CHARACTER).append(' ').append(Tokens.T_SET).append(' ');

        if (SqlInvariants.INFORMATION_SCHEMA.equals(name.schema.name)) {
            sb.append(name.getStatementName());
        } else {
            sb.append(name.getSchemaQualifiedStatementName());
        }

        sb.append(' ').append(Tokens.T_AS).append(' ').append(Tokens.T_GET);
        sb.append(' ');

        if (SqlInvariants.INFORMATION_SCHEMA.equals(base.schema.name)) {
            sb.append(base.getStatementName());
        } else {
            sb.append(base.getSchemaQualifiedStatementName());
        }

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    public static boolean isInSet(String value, int[][] ranges) {

        int length = value.length();

        mainLoop:
        for (int index = 0; index < length; index++) {
            int ch = value.charAt(index);

            for (int i = 0; i < ranges.length; i++) {
                if (ch > ranges[i][1]) {
                    continue;
                }

                if (ch < ranges[i][0]) {
                    return false;
                }

                continue mainLoop;
            }

            return false;
        }

        return true;
    }

    public static boolean startsWith(String value, int[][] ranges) {

        int ch = value.charAt(0);

        for (int i = 0; i < ranges.length; i++) {
            if (ch > ranges[i][1]) {
                continue;
            }

            if (ch < ranges[i][0]) {
                return false;
            }

            return true;
        }

        return false;
    }

    public static Charset getDefaultInstance() {
        return SQL_TEXT;
    }
}
