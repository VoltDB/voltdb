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

import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.StringConverter;

import java.io.IOException;
import java.sql.RowId;
import java.sql.SQLException;
import java.util.Arrays;

/* $Id: JDBCRowId.java 2946 2009-03-22 17:44:48Z fredt $ */

//boucherb@users 20060522 - doc 1.8.1 full synch up to Mustang Build 84

/**
 *
 * The representation (mapping) in the Java programming language of an SQL ROWID
 * value. An SQL ROWID is a built-in type, a value of which can be thought of as
 * an address  for its identified row in a database table. Whether that address
 * is logical or, in any  respects, physical is determined by its originating data
 * source.
 * <p>
 * Methods in the interfaces <code>ResultSet</code>, <code>CallableStatement</code>,
 * and <code>PreparedStatement</code>, such as <code>getRowId</code> and <code>setRowId</code>
 * allow a programmer to access a SQL <code>ROWID</code>  value. The <code>RowId</code>
 * interface provides a method
 * for representing the value of the <code>ROWID</code> as a byte array or as a
 * <code>String</code>.
 * <p>
 * The method <code>getRowIdLifetime</code> in the interface <code>DatabaseMetaData</code>,
 * can be used
 * to determine if a <code>RowId</code> object remains valid for the duration of the transaction in
 * which  the <code>RowId</code> was created, the duration of the session in which
 * the <code>RowId</code> was created,
 * or, effectively, for as long as its identified row is not deleted. In addition
 * to specifying the duration of its valid lifetime outside its originating data
 * source, <code>getRowIdLifetime</code> specifies the duration of a <code>ROWID</code>
 * value's valid lifetime
 * within its originating data source. In this, it differs from a large object,
 * because there is no limit on the valid lifetime of a large  object within its
 * originating data source.
 * <p>
 * All methods on the <code>RowId</code> interface must be fully implemented if the
 * JDBC driver supports the data type.
 *
 * @see java.sql.DatabaseMetaData
 * @since JDK 1.6, HSQLDB 1.9.0
 * @author boucherb@users
 */
public final class JDBCRowId implements RowId {

    private int hash;

    // ------------------------- Internal Implementation -----------------------
    private final byte[] id;

    /**
     * Constructs a new JDBCRowId instance wrapping the given octet sequence. <p>
     *
     * This constructor may be used internally to retrieve result set values as
     * RowId objects, yet it also may need to be public to allow access from
     * other packages. As such (in the interest of efficiency) this object
     * maintains a reference to the given octet sequence rather than making a
     * copy; special care should be taken by extenal clients never to use this
     * constructor with a byte array object that may later be modified
     * extenally.
     *
     * @param id the octet sequence representing the Rowid value
     * @throws SQLException if the argument is null
     */
    public JDBCRowId(final byte[] id) throws SQLException {

        if (id == null) {
            throw Util.nullArgument("id");
        }
        this.id = id;
    }

    /**
     * Constructs a new JDBCRowId instance whose internal octet sequence is
     * is a copy of the octet sequence of the given RowId object. <p>
     *
     * @param id the octet sequence representing the Rowid value
     * @throws SQLException if the argument is null
     */
    public JDBCRowId(RowId id) throws SQLException {
        this(id.getBytes());
    }

    /**
     * Constructs a new JDBCRowId instance whose internal octet sequence is
     * is that represented by the given hexidecimal character sequence. <p>
     * @param hex the hexadecimal character sequence from which to derive
     *        the internal octet sequence
     * @throws java.sql.SQLException if the argument is null or is not a valid
     *         hexadecimal character sequence
     */
    public JDBCRowId(final String hex) throws SQLException {

        if (hex == null) {
            throw Util.nullArgument("hex");
        }

        try {
            this.id = StringConverter.hexStringToByteArray(hex);
        } catch (IOException e) {
            throw Util.sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
                                    "hex: " + e);

            // .illegalHexadecimalCharacterSequenceArgumentException("hex", e);
        }
    }

    /**
     * Compares this <code>RowId</code> to the specified object. The result is
     * <code>true</code> if and only if the argument is not null and is a RowId
     * object that represents the same ROWID as  this object.
     * <p>
     * It is important
     * to consider both the origin and the valid lifetime of a <code>RowId</code>
     * when comparing it to another <code>RowId</code>. If both are valid, and
     * both are from the same table on the same data source, then if they are equal
     * they identify
     * the same row; if one or more is no longer guaranteed to be valid, or if
     * they originate from different data sources, or different tables on the
     * same data source, they  may be equal but still
     * not identify the same row.
     *
     * @param obj the <code>Object</code> to compare this <code>RowId</code> object
     *     against.
     * @return true if the <code>RowId</code>s are equal; false otherwise
     * @since JDK 1.6, HSQLDB 1.9.0
     */
    public boolean equals(Object obj) {
        return (obj instanceof JDBCRowId)
               && Arrays.equals(this.id, ((JDBCRowId) obj).id);
    }

    /**
     * Returns an array of bytes representing the value of the SQL <code>ROWID</code>
     * designated by this <code>java.sql.RowId</code> object.
     *
     * @return an array of bytes, whose length is determined by the driver supplying
     *     the connection, representing the value of the ROWID designated by this
     *     java.sql.RowId object.
     */
    public byte[] getBytes() {
        return id.clone();
    }

    /**
     * Returns a String representing the value of the SQL ROWID designated by this
     * <code>java.sql.RowId</code> object.
     * <p>
     * Like <code>java.sql.Date.toString()</code>
     * returns the contents of its DATE as the <code>String</code> "2004-03-17"
     * rather than as  DATE literal in SQL (which would have been the <code>String</code>
     * DATE "2004-03-17"), toString()
     * returns the contents of its ROWID in a form specific to the driver supplying
     * the connection, and possibly not as a <code>ROWID</code> literal.
     *
     * @return a String whose format is determined by the driver supplying the
     *     connection, representing the value of the <code>ROWID</code> designated
     *     by this <code>java.sql.RowId</code>  object.
     */
    public String toString() {
        return StringConverter.byteArrayToHexString(id);
    }

    /**
     * Returns a hash code value of this <code>RowId</code> object.
     *
     * @return a hash code for the <code>RowId</code>
     */
    public int hashCode() {

        if (hash == 0) {
            hash = Arrays.hashCode(id);
        }

        return hash;
    }

    /**
     * Direct access to id bytes for subclassing.
     *
     * @return direct reference to id bytes.
     */
    Object id() {
        return id;
    }
}
