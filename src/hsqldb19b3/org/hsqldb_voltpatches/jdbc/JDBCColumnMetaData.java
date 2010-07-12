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


package org.hsqldb_voltpatches.jdbc;

import java.lang.reflect.Field;

// fredt@users - 1.9.0 rewritten as simple structure derived from JDBCResultSetMetaData

/**
 * Provides a site for holding the ResultSetMetaData for individual ResultSet
 * columns. In 1.9.0 it is implemented as a simple data structure derived
 * from calls to JDBCResultSetMetaData methods.
 * purposes.<p>
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since HSQLDB 1.7.2
 */
public final class JDBCColumnMetaData {

    /** The column's table's catalog name. */
    public String catalogName;

    /**
     * The fully-qualified name of the Java class whose instances are
     * manufactured if the method ResultSet.getObject is called to retrieve
     * a value from the column.
     */
    public String columnClassName;

    /** The column's normal max width in chars. */
    public int columnDisplaySize;

    /** The suggested column title for use in printouts and displays. */
    public String columnLabel;

    /** The column's name. */
    public String columnName;

    /** The column's SQL type. */
    public int columnType;

    /** The column's value's number of decimal digits. */
    public int precision;

    /** The column's value's number of digits to right of the decimal point. */
    public int scale;

    /** The column's table's schema. */
    public String schemaName;

    /** The column's table's name. */
    public String tableName;

    /** Whether the value of the column are automatically numbered. */
    public boolean isAutoIncrement;

    /** Whether the column's value's case matters. */
    public boolean isCaseSensitive;

    /** Whether the values in the column are cash values. */
    public boolean isCurrency;

    /** Whether a write on the column will definitely succeed. */
    public boolean isDefinitelyWritable;

    /** The nullability of values in the column. */
    public int isNullable;

    /** Whether the column's values are definitely not writable. */
    public boolean isReadOnly;

    /** Whether the column's values can be used in a where clause. */
    public boolean isSearchable;

    /** Whether values in the column are signed numbers. */
    public boolean isSigned;

    /** Whether it is possible for a write on the column to succeed. */
    public boolean isWritable;

    /**
     * Retrieves a String representation of this object.
     *
     * @return a Sring representation of this object
     */
    public String toString() {

        try {
            return toStringImpl();
        } catch (Exception e) {
            return super.toString() + "[" + e + "]";
        }
    }

    /**
     * Provides the implementation of the toString() method.
     *
     * @return a Sring representation of this object
     */
    private String toStringImpl() throws Exception {

        StringBuffer sb;
        Field[]      fields;
        Field        field;

        sb = new StringBuffer();

        sb.append('[');

        fields = getClass().getFields();

        int len = fields.length;

        for (int i = 0; i < len; i++) {
            field = fields[i];

            sb.append(field.getName());
            sb.append('=');
            sb.append(field.get(this));

            if (i + 1 < len) {
                sb.append(',');
                sb.append(' ');
            }
        }
        sb.append(']');

        return sb.toString();
    }
}
