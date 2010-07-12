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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.SQLException;

/* $Id: JDBCClob.java 2944 2009-03-21 22:53:43Z fredt $ */

// boucherb@users 2004-03/04-xx - doc 1.7.2 - javadocs updated; methods put in
//                                            correct (historical, interface
//                                            declared) order
// boucherb@users 2004-03/04-xx - patch 1.7.2 - null check for constructor (a
//                                              null CLOB value is Java null,
//                                              not a Clob object with null
//                                              data);moderate thread safety;
//                                              simplification; optimization
//                                              of operations between jdbcClob
//                                              instances
// boucherb@users 2005-12-07    - patch 1.8.0.x - initial JDBC 4.0 support work
// boucherb@users 2006-05-22    - doc   1.9.0 - full synch up to Mustang Build 84
//                              - patch 1.9.0 - setAsciiStream &
//                                              setCharacterStream improvement
// patch 1.9.0
// - full synch up to Mustang b90
// - better bounds checking

/**
 * The mapping in the Java<sup><font size=-2>TM</font></sup> programming language
 * for the SQL <code>CLOB</code> type.
 * An SQL <code>CLOB</code> is a built-in type
 * that stores a Character Large Object as a column value in a row of
 * a database table.
 * By default drivers implement a <code>Clob</code> object using an SQL
 * <code>locator(CLOB)</code>, which means that a <code>Clob</code> object
 * contains a logical pointer to the SQL <code>CLOB</code> data rather than
 * the data itself. A <code>Clob</code> object is valid for the duration
 * of the transaction in which it was created.
 * <P>The <code>Clob</code> interface provides methods for getting the
 * length of an SQL <code>CLOB</code> (Character Large Object) value,
 * for materializing a <code>CLOB</code> value on the client, and for
 * searching for a substring or <code>CLOB</code> object within a
 * <code>CLOB</code> value.
 * Methods in the interfaces {@link java.sql.ResultSet},
 * {@link java.sql.CallableStatement}, and {@link java.sql.PreparedStatement}, such as
 * <code>getClob</code> and <code>setClob</code> allow a programmer to
 * access an SQL <code>CLOB</code> value.  In addition, this interface
 * has methods for updating a <code>CLOB</code> value.
 * <p>
 * All methods on the <code>Clob</code> interface must be fully implemented if the
 * JDBC driver supports the data type.
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * Previous to 1.9.0, the HSQLDB driver did not implement Clob using an SQL
 * locator(CLOB).  That is, an HSQLDB Clob object did not contain a logical
 * pointer to SQL CLOB data; rather it directly contained a representation of
 * the data (a String). As a result, an HSQLDB Clob object was itself
 * valid beyond the duration of the transaction in which is was created,
 * although it did not necessarily represent a corresponding value
 * on the database. Also, the interface methods for updating a CLOB value
 * were unsupported, with the exception of the truncate method,
 * in that it could be used to truncate the local value. <p>
 *
 * Starting with 1.9.0, the HSQLDB driver fully supports both local and remote
 * SQL CLOB data implementations, meaning that an HSQLDB Clob object <em>may</em>
 * contain a logical pointer to remote SQL CLOB data (see {@link JDBCClobClient
 * JDBCClobClient}) or it may directly contain a local representation of the
 * data (as implemented in this class).  In particular, when the product is built
 * under JDK 1.6+ and the Clob instance is constructed as a result of calling
 * JDBCConnection.createClob(), then the resulting Clob instance is initially
 * disconnected (is not bound to the tranaction scope of the vending Connection
 * object), the data is contained directly and all interface methods for
 * updating the CLOB value are supported for local use until the first
 * invocation of free(); otherwise, an HSQLDB Clob's implementation is
 * determined at runtime by the driver, it is typically not valid beyond
 * the duration of the transaction in which is was created, and there no
 * standard way to query whether it represents a local or remote value.<p>
 *
 * </div>
 * <!-- end release-specific documentation -->
 *
 * @author  boucherb@users
 * @version 1.9.0
 * @since JDK 1.2, HSQLDB 1.7.2
 * @revised JDK 1.6, HSQLDB 1.9.0
 */
public class JDBCClob implements Clob {

    /**
     * Retrieves the number of characters
     * in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     *
     * @return length of the <code>CLOB</code> in characters
     * @exception SQLException if there is an error accessing the
     *            length of the <code>CLOB</code> value
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long length() throws SQLException {

        final String ldata = data;

        checkValid(ldata);

        return ldata.length();
    }

    /**
     * Retrieves a copy of the specified substring
     * in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * The substring begins at position
     * <code>pos</code> and has up to <code>length</code> consecutive
     * characters.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * The official specification above is ambiguous in that it does not
     * precisely indicate the policy to be observed when
     * pos > this.length() - length.  One policy would be to retrieve the
     * characters from pos to this.length().  Another would be to throw
     * an exception.  HSQLDB observes the second policy. <p>
     *
     * <b>Note</b><p>
     *
     * Depending java.lang.String implementation, the returned value
     * may be sharing the underlying (and possibly much larger) character
     * buffer.  This facilitates much faster operation and will save memory
     * if many transient substrings are to be retrieved during processing, but
     * it has memory management implications should retrieved substrings be
     * required to survive for any non-trivial duration.  It is left up to the
     * client to decide how to handle the trade-off (whether to make an isolated
     * copy of the returned substring or risk that more memory remains allocated
     * than is absolutely reuired).
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the first character of the substring to be extracted.
     *            The first character is at position 1.
     * @param length the number of consecutive characters to be copied
     * @return a <code>String</code> that is the specified substring in
     *         the <code>CLOB</code> value designated by this <code>Clob</code> object
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if pos is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public String getSubString(long pos,
                               final int length) throws SQLException {

        final String ldata = data;

        checkValid(ldata);

        final int dlen = ldata.length();

        if (pos < MIN_POS || pos > dlen) {
            Util.outOfRangeArgument("pos: " + pos);
        }
        pos--;

        if (length < 0 || length > dlen - pos) {
            throw Util.outOfRangeArgument("length: " + length);
        }

        return (pos == 0 && length == dlen) ? ldata
                : ldata.substring((int) pos, (int) pos + length);
    }

    /**
     * Retrieves the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as a <code>java.io.Reader</code> object (or as a stream of
     * characters).
     *
     * @return a <code>java.io.Reader</code> object containing the
     *         <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setCharacterStream
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public java.io.Reader getCharacterStream() throws SQLException {

        final String ldata = data;

        checkValid(ldata);

        return new StringReader(ldata);
    }

    /**
     * Retrieves the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as an ascii stream.
     *
     * @return a <code>java.io.InputStream</code> object containing the
     *         <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setAsciiStream
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public java.io.InputStream getAsciiStream() throws SQLException {

        checkValid(data);

        try {
            return new ByteArrayInputStream(data.getBytes("US-ASCII"));
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Retrieves the character position at which the specified substring
     * <code>searchstr</code> appears in the SQL <code>CLOB</code> value
     * represented by this <code>Clob</code> object.  The search
     * begins at position <code>start</code>.
     *
     * @param searchstr the substring for which to search
     * @param start the position at which to begin searching; the first position
     *              is 1
     * @return the position at which the substring appears or -1 if it is not
     *         present; the first position is 1
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if start is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long position(final String searchstr,
                         long start) throws SQLException {

        final String ldata = data;

        checkValid(ldata);

        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        }

        if (searchstr == null || start > MAX_POS) {
            return -1;
        }

        final int pos = ldata.indexOf(searchstr, (int) --start);

        return (pos < 0) ? -1
                         : pos + 1;
    }

    /**
     * Retrieves the character position at which the specified
     * <code>Clob</code> object <code>searchstr</code> appears in this
     * <code>Clob</code> object.  The search begins at position
     * <code>start</code>.
     *
     * @param searchstr the <code>Clob</code> object for which to search
     * @param start the position at which to begin searching; the first
     *              position is 1
     * @return the position at which the <code>Clob</code> object appears
     *              or -1 if it is not present; the first position is 1
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if start is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long position(final Clob searchstr,
                         long start) throws SQLException {

        final String ldata = data;

        checkValid(ldata);

        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        }

        if (searchstr == null) {
            return -1;
        }

        final long dlen  = ldata.length();
        final long sslen = searchstr.length();

        start--;

// This is potentially much less expensive than materializing a large
// substring from some other vendor's CLOB.  Indeed, we should probably
// do the comparison piecewise, using an in-memory buffer (or temp-files
// when available), if it is detected that the input CLOB is very long.
        if (start > dlen - sslen) {
            return -1;
        }

        // by now, we know sslen and start are both < Integer.MAX_VALUE
        String s;

        if (searchstr instanceof JDBCClob) {
            s = ((JDBCClob) searchstr).data();
        } else {
            s = searchstr.getSubString(1L, (int) sslen);
        }

        final int pos = ldata.indexOf(s, (int) start);

        return (pos < 0) ? -1
                         : pos + 1;
    }

    //---------------------------- jdbc 3.0 -----------------------------------

    /**
     * Writes the given Java <code>String</code> to the <code>CLOB</code>
     * value that this <code>Clob</code> object designates at the position
     * <code>pos</code>. The string will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Clob</code> value is reached
     * while writing the given string, then the length of the <code>Clob</code>
     * value will be increased to accomodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.9.0 this feature is supported. <p>
     *
     * When built under JDK 1.6+ and the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in the
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propogate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * No attempt is made to ensure precise thread safety. Instead, volatile
     * member field and local variable snapshot isolation semantics are
     * implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, however, if an application may perform concurrent
     * JDBCClob modifications and the integrity of the application depends on
     * total order Clob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position at which to start writing to the <code>CLOB</code>
     *         value that this <code>Clob</code> object represents;
     * The first position is 1
     * @param str the string to be written to the <code>CLOB</code>
     *        value that this <code>Clob</code> designates
     * @return the number of characters written
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if pos is less than 1
     *
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public int setString(long pos, String str) throws SQLException {

        if (str == null) {
            throw Util.nullArgument("str");
        }

        return setString(pos, str, 0, str.length());
    }

    /**
     * Writes <code>len</code> characters of <code>str</code>, starting
     * at character <code>offset</code>, to the <code>CLOB</code> value
     * that this <code>Clob</code> represents.  The string will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Clob</code> value is reached
     * while writing the given string, then the length of the <code>Clob</code>
     * value will be increased to accomodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.9.0 this feature is supported. <p>
     *
     * When built under JDK 1.6+ and the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propogate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * If the value specified for <code>pos</code>
     * is greater than the length of the <code>CLOB</code> value, then
     * the <code>CLOB</code> value is extended in length to accept the
     * written characters and the undefined region up to <code>pos</code> is
     * filled with (char)0. <p>
     *
     * No attempt is made to ensure precise thread safety. Instead, volatile
     * member field and local variable snapshot isolation semantics are
     * implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, however, if an application may perform concurrent
     * JDBCClob modifications and the integrity of the application depends on
     * total order Clob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position at which to start writing to this
     *        <code>CLOB</code> object; The first position  is 1
     * @param str the string to be written to the <code>CLOB</code>
     *        value that this <code>Clob</code> object represents
     * @param offset the offset into <code>str</code> to start reading
     *        the characters to be written
     * @param len the number of characters to be written
     * @return the number of characters written
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if pos is less than 1
     *
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public int setString(long pos, String str, int offset,
                         int len) throws SQLException {

        if (!this.createdByConnection) {

            /** @todo - better error message */
            throw Util.notSupported();
        }

        String ldata = this.data;

        checkValid(ldata);

        if (str == null) {
            throw Util.nullArgument("str");
        }

        final int strlen = str.length();

        if (offset < 0 || offset > strlen) {
            throw Util.outOfRangeArgument("offset: " + offset);
        }

        if (len > strlen - offset) {
            throw Util.outOfRangeArgument("len: " + len);
        }

        if (pos < MIN_POS || pos > 1L + (Integer.MAX_VALUE - len)) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        final int    dlen = ldata.length();
        final int    ipos = (int) (pos - 1);
        StringBuffer sb;

        if (ipos > dlen - len) {
            sb = new StringBuffer(ipos + len);

            sb.append(ldata.substring(0, ipos));

            ldata = null;

            sb.append(str.substring(offset, offset + len));

            str = null;
        } else {
            sb    = new StringBuffer(ldata);
            ldata = null;

            for (int i = ipos, j = 0; j < len; i++, j++) {
                sb.setCharAt(i, str.charAt(offset + j));
            }
            str = null;
        }

        // paranoia, in case somone free'd us during the copies.
        checkValid(this.data);

        this.data = sb.toString();

        return len;
    }

    /**
     * Retrieves a stream to be used to write Ascii characters to the
     * <code>CLOB</code> value that this <code>Clob</code> object represents,
     * starting at position <code>pos</code>.  Characters written to the stream
     * will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Clob</code> value is reached
     * while writing characters to the stream, then the length of the <code>Clob</code>
     * value will be increased to accomodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater than the length of the <code>CLOB</code> value, then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.9.0 this feature is supported. <p>
     *
     * When built under JDK 1.6+ and the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propogate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * The data written to the stream does not appear in this
     * Clob until the stream is closed. <p>
     *
     * When the stream is closed, if the value specified for <code>pos</code>
     * is greater than the length of the <code>CLOB</code> value, then
     * the <code>CLOB</code> value is extended in length to accept the
     * written characters and the undefined region up to <code>pos</code> is
     * filled with (char)0. <p>
     *
     * Also, no attempt is made to ensure precise thread safety. Instead,
     * volatile member field and local variable snapshot isolation semantics
     * are implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, however, if an application may perform concurrent
     * JDBCClob modifications and the integrity of the application depends on
     * total order Clob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position at which to start writing to this
     *        <code>CLOB</code> object; The first position is 1
     * @return the stream to which ASCII encoded characters can be written
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if pos is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getAsciiStream
     *
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public java.io.OutputStream setAsciiStream(
            final long pos) throws SQLException {

        if (!this.createdByConnection) {

            /** @todo - Better error message */
            throw Util.notSupported();
        }
        checkValid(this.data);

        if (pos < MIN_POS || pos > MAX_POS) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        return new java.io.ByteArrayOutputStream() {

            public synchronized void close() throws java.io.IOException {

                try {
                    JDBCClob.this.setString(pos,
                            new String(toByteArray(), "US-ASCII"));
                } catch (SQLException se) {
                    throw new java.io.IOException(se.toString());
                } finally {
                    super.close();
                }
            }
        };
    }

    /**
     * Retrieves a stream to be used to write a stream of Unicode characters
     * to the <code>CLOB</code> value that this <code>Clob</code> object
     * represents, at position <code>pos</code>. Characters written to the stream
     * will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Clob</code> value is reached
     * while writing characters to the stream, then the length of the <code>Clob</code>
     * value will be increased to accomodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.9.0 this feature is supported. <p>
     *
     * When built under JDK 1.6+ and the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propogate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * The data written to the stream does not appear in this
     * Clob until the stream is closed. <p>
     *
     * When the stream is closed, if the value specified for <code>pos</code>
     * is greater than the length of the <code>CLOB</code> value, then
     * the <code>CLOB</code> value is extended in length to accept the
     * written characters and the undefined region up to <code>pos</code> is
     * filled with (char)0. <p>
     *
     * Also, no attempt is made to ensure precise thread safety. Instead,
     * volatile member field and local variable snapshot isolation semantics
     * are implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, however, if an application may perform concurrent
     * JDBCClob modifications and the integrity of the application depends on
     * total order Clob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param  pos the position at which to start writing to the
     *        <code>CLOB</code> value; The first position is 1
     *
     * @return a stream to which Unicode encoded characters can be written
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if pos is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getCharacterStream
     *
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public java.io.Writer setCharacterStream(
            final long pos) throws SQLException {

        if (!this.createdByConnection) {

            /** @todo - better error message */
            throw Util.notSupported();
        }
        checkValid(this.data);

        if (pos < MIN_POS || pos > MAX_POS) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        return new java.io.StringWriter() {

            public synchronized void close() throws java.io.IOException {

                try {
                    JDBCClob.this.setString(pos, toString());
                } catch (SQLException se) {
                    throw new java.io.IOException(se.toString());
                } finally {
                    super.close();
                }
            }
        };
    }

    /**
     * Truncates the <code>CLOB</code> value that this <code>Clob</code>
     * designates to have a length of <code>len</code>
     * characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>len</code>
     * is greater than the length of the <code>CLOB</code> value, then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.9.0 this feature is fully supported. <p>
     *
     * When built under JDK 1.6+ and the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Blob instances. To propogate the truncated clob value to
     * a database in this case, it is required to supply the Clob instance to
     * an updating or inserting setXXX method of a Prepared or Callable
     * Statement, or to supply the Blob instance to an updateXXX method of an
     * updateable ResultSet. <p>
     *
     * <b>Implementation Notes:</b> <p>
     *
     * HSQLDB throws an SQLException if the specified <tt>len</tt> is greater
     * than the value returned by {@link #length() length}. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param len the length, in characters, to which the <code>CLOB</code> value
     *        should be truncated
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if len is less than 0
     *
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public void truncate(final long len) throws SQLException {

        final String ldata = this.data;

        this.checkValid(ldata);

        final long dlen  = ldata.length();
        final long chars = len;

        if (chars == dlen) {

            // nothing has changed, so there's nothing to be done
        } else if (len < 0 || chars > dlen) {
            throw Util.outOfRangeArgument("len: " + len);
        } else {

            // use new String() to ensure we get rid of slack
            data = new String(ldata.substring(0, (int) chars));
        }
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * This method frees the <code>Clob</code> object and releases the resources the resources
     * that it holds.  The object is invalid once the <code>free</code> method
     * is called.
     * <p>
     * After <code>free</code> has been called, any attempt to invoke a
     * method other than <code>free</code> will result in a <code>SQLException</code>
     * being thrown.  If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     * <p>
     * @throws SQLException if an error occurs releasing
     * the Clob's resources
     *
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
    public void free() throws SQLException {
        this.data = null;
    }

    /**
     * Returns a <code>Reader</code> object that contains a partial <code>Clob</code> value, starting
     * with the character specified by pos, which is length characters in length.
     *
     * @param pos the offset to the first character of the partial value to
     * be retrieved.  The first character in the Clob is at position 1.
     * @param length the length in characters of the partial value to be retrieved.
     * @return <code>Reader</code> through which the partial <code>Clob</code> value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the number of
     * characters in the <code>Clob</code> or if pos + length is greater than the number of
     * characters in the <code>Clob</code>
     *
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
    public Reader getCharacterStream(long pos,
                                     long length) throws SQLException {

        if (length > Integer.MAX_VALUE) {
            throw Util.outOfRangeArgument("length: " + length);
        }

        return new StringReader(getSubString(pos, (int) length));
    }

    // ---------------------- internal implementation --------------------------
    private static final long MIN_POS = 1L;
    private static final long MAX_POS = 1L + (long) Integer.MAX_VALUE;
    private volatile String   data;
    private final boolean     createdByConnection;

    /**
     * Constructs a new JDBCClob object wrapping the given character
     * sequence. <p>
     *
     * This constructor is used internally to retrieve result set values as
     * Clob objects, yet it must be public to allow access from other packages.
     * As such (in the interest of efficiency) this object maintains a reference
     * to the given String object rather than making a copy and so it is
     * gently suggested (in the interest of effective memory management) that
     * extenal clients using this constructor either take pause to consider
     * the implications or at least take care to provide a String object whose
     * internal character buffer is not much larger than required to represent
     * the value.
     *
     * @param data the character sequence representing the Clob value
     * @throws SQLException if the argument is null
     */
    public JDBCClob(final String data) throws SQLException {

        this.init(data);

        this.createdByConnection = false;
    }

    protected JDBCClob() {
        this.data                = "";
        this.createdByConnection = true;
    }

    protected void init(String data) throws SQLException {

        if (data == null) {
            throw Util.nullArgument("data");
        }
        this.data = data;
    }

    protected void checkValid(final Object data) {

        if (data == null) {
            throw new RuntimeException("null data");
        }
    }

    protected String data() throws SQLException {

        final String ldata = data;

        checkValid(ldata);

        return ldata;
    }
}
