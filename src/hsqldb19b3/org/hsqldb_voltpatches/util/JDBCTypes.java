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


package org.hsqldb_voltpatches.util;

import java.util.Hashtable;

/**
 * Base class for conversion from a different databases
 *
 * @author Nicolas BAZIN
 * @version 1.7.0
 */
class JDBCTypes {

    public static final int JAVA_OBJECT = 2000;
    public static final int DISTINCT    = 2001;
    public static final int STRUCT      = 2002;
    public static final int ARRAY       = 2003;
    public static final int BLOB        = 2004;
    public static final int CLOB        = 2005;
    public static final int REF         = 2006;
    private Hashtable       hStringJDBCtypes;
    private Hashtable       hIntJDBCtypes;

    JDBCTypes() {

        hStringJDBCtypes = new Hashtable();
        hIntJDBCtypes    = new Hashtable();

        hStringJDBCtypes.put(new Integer(ARRAY), "ARRAY");
        hStringJDBCtypes.put(new Integer(BLOB), "BLOB");
        hStringJDBCtypes.put(new Integer(CLOB), "CLOB");
        hStringJDBCtypes.put(new Integer(DISTINCT), "DISTINCT");
        hStringJDBCtypes.put(new Integer(JAVA_OBJECT), "JAVA_OBJECT");
        hStringJDBCtypes.put(new Integer(REF), "REF");
        hStringJDBCtypes.put(new Integer(STRUCT), "STRUCT");

        //
        hStringJDBCtypes.put(new Integer(java.sql.Types.BIGINT), "BIGINT");
        hStringJDBCtypes.put(new Integer(java.sql.Types.BINARY), "BINARY");
        hStringJDBCtypes.put(new Integer(java.sql.Types.BIT), "BIT");
        hStringJDBCtypes.put(new Integer(java.sql.Types.CHAR), "CHAR");
        hStringJDBCtypes.put(new Integer(java.sql.Types.DATE), "DATE");
        hStringJDBCtypes.put(new Integer(java.sql.Types.DECIMAL), "DECIMAL");
        // A VoltDB extension to alias FLOAT to DOUBLE
        hStringJDBCtypes.put(new Integer(java.sql.Types.DOUBLE), "FLOAT");
        /* disable 1 line ...
        hStringJDBCtypes.put(new Integer(java.sql.Types.DOUBLE), "DOUBLE");
        ... disabled 1 line */
        // End of VoltDB extension
        hStringJDBCtypes.put(new Integer(java.sql.Types.FLOAT), "FLOAT");
        hStringJDBCtypes.put(new Integer(java.sql.Types.INTEGER), "INTEGER");
        hStringJDBCtypes.put(new Integer(java.sql.Types.LONGVARBINARY),
                             "LONGVARBINARY");
        hStringJDBCtypes.put(new Integer(java.sql.Types.LONGVARCHAR),
                             "LONGVARCHAR");
        hStringJDBCtypes.put(new Integer(java.sql.Types.NULL), "NULL");
        hStringJDBCtypes.put(new Integer(java.sql.Types.NUMERIC), "NUMERIC");
        hStringJDBCtypes.put(new Integer(java.sql.Types.OTHER), "OTHER");
        hStringJDBCtypes.put(new Integer(java.sql.Types.REAL), "REAL");
        hStringJDBCtypes.put(new Integer(java.sql.Types.SMALLINT),
                             "SMALLINT");
        hStringJDBCtypes.put(new Integer(java.sql.Types.TIME), "TIME");
        hStringJDBCtypes.put(new Integer(java.sql.Types.TIMESTAMP),
                             "TIMESTAMP");
        hStringJDBCtypes.put(new Integer(java.sql.Types.TINYINT), "TINYINT");
        hStringJDBCtypes.put(new Integer(java.sql.Types.VARBINARY),
                             "VARBINARY");
        hStringJDBCtypes.put(new Integer(java.sql.Types.VARCHAR), "VARCHAR");

        //
        hIntJDBCtypes.put("ARRAY", new Integer(ARRAY));
        hIntJDBCtypes.put("BLOB", new Integer(BLOB));
        hIntJDBCtypes.put("CLOB", new Integer(CLOB));
        hIntJDBCtypes.put("DISTINCT", new Integer(DISTINCT));
        hIntJDBCtypes.put("JAVA_OBJECT", new Integer(JAVA_OBJECT));
        hIntJDBCtypes.put("REF", new Integer(REF));
        hIntJDBCtypes.put("STRUCT", new Integer(STRUCT));

        //
        hIntJDBCtypes.put("BIGINT", new Integer(java.sql.Types.BIGINT));
        hIntJDBCtypes.put("BINARY", new Integer(java.sql.Types.BINARY));
        hIntJDBCtypes.put("BIT", new Integer(java.sql.Types.BIT));
        hIntJDBCtypes.put("CHAR", new Integer(java.sql.Types.CHAR));
        hIntJDBCtypes.put("DATE", new Integer(java.sql.Types.DATE));
        hIntJDBCtypes.put("DECIMAL", new Integer(java.sql.Types.DECIMAL));
        // A VoltDB extension to alias FLOAT to DOUBLE
        hIntJDBCtypes.put("FLOAT", new Integer(java.sql.Types.DOUBLE));
        /* disable 1 line ...
        hIntJDBCtypes.put("DOUBLE", new Integer(java.sql.Types.DOUBLE));
        ... disabled 1 line */
        // End of VoltDB extension
        hIntJDBCtypes.put("FLOAT", new Integer(java.sql.Types.FLOAT));
        hIntJDBCtypes.put("INTEGER", new Integer(java.sql.Types.INTEGER));
        hIntJDBCtypes.put("LONGVARBINARY",
                          new Integer(java.sql.Types.LONGVARBINARY));
        hIntJDBCtypes.put("LONGVARCHAR",
                          new Integer(java.sql.Types.LONGVARCHAR));
        hIntJDBCtypes.put("NULL", new Integer(java.sql.Types.NULL));
        hIntJDBCtypes.put("NUMERIC", new Integer(java.sql.Types.NUMERIC));
        hIntJDBCtypes.put("OTHER", new Integer(java.sql.Types.OTHER));
        hIntJDBCtypes.put("REAL", new Integer(java.sql.Types.REAL));
        hIntJDBCtypes.put("SMALLINT", new Integer(java.sql.Types.SMALLINT));
        hIntJDBCtypes.put("TIME", new Integer(java.sql.Types.TIME));
        hIntJDBCtypes.put("TIMESTAMP", new Integer(java.sql.Types.TIMESTAMP));
        hIntJDBCtypes.put("TINYINT", new Integer(java.sql.Types.TINYINT));
        hIntJDBCtypes.put("VARBINARY", new Integer(java.sql.Types.VARBINARY));
        hIntJDBCtypes.put("VARCHAR", new Integer(java.sql.Types.VARCHAR));
    }

    public Hashtable getHashtable() {
        return hStringJDBCtypes;
    }

    public String toString(int type) {
        return (String) hStringJDBCtypes.get(new Integer(type));
    }

    public int toInt(String type) throws Exception {

        Integer tempInteger = (Integer) hIntJDBCtypes.get(type);

        return tempInteger.intValue();
    }
}
