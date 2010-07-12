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
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

// boucherb@users 2004-04-xx - patch 1.7.2 - position and truncate methods
//                             implemented; minor changes for moderate thread
//                             safety and optimal performance
// boucherb@users 2004-04-xx - doc 1.7.2 - javadocs updated; methods put in
//                             correct (historical, interface declared) order
// boucherb@users 2005-12-07 - patch 1.8.0.x - initial JDBC 4.0 support work
// boucherb@users 2006-05-22 - doc 1.9.0     - full synch up to Mustang Build 84
//                           - patch 1.9.0   - setBinaryStream improvement
// patch 1.9.0
//  - fixed invalid reference to new BinaryStream(...) in getBinaryStream
//
// patch 1.9.0 - full synch up to Mustang b90
//             - better bounds checking
//             - added support for clients to decide whether getBinaryStream
//               uses copy of internal byte buffer

/**
 * The representation (mapping) in
 * the Java<sup><font size=-2>TM</font></sup> programming
 * language of an SQL
 * <code>BLOB</code> value.  An SQL <code>BLOB</code> is a built-in type
 * that stores a Binary Large Object as a column value in a row of
 * a database table. By default drivers implement <code>Blob</code> using
 * an SQL <code>locator(BLOB)</code>, which means that a
 * <code>Blob</code> object contains a logical pointer to the
 * SQL <code>BLOB</code> data rather than the data itself.
 * A <code>Blob</code> object is valid for the duration of the
 * transaction in which is was created.
 *
 * <P>Methods in the interfaces {@link java.sql.ResultSet},
 * {@link java.sql.CallableStatement}, and {@link java.sql.PreparedStatement}, such as
 * <code>getBlob</code> and <code>setBlob</code> allow a programmer to
 * access an SQL <code>BLOB</code> value.
 * The <code>Blob</code> interface provides methods for getting the
 * length of an SQL <code>BLOB</code> (Binary Large Object) value,
 * for materializing a <code>BLOB</code> value on the client, and for
 * determining the position of a pattern of bytes within a
 * <code>BLOB</code> value. In addition, this interface has methods for updating
 * a <code>BLOB</code> value.
 * <p>
 * All methods on the <code>Blob</code> interface must be fully implemented if the
 * JDBC driver supports the data type.
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * Previous to 1.9.0, the HSQLDB driver did not implement Blob using an SQL
 * locator(BLOB).  That is, an HSQLDB Blob object did not contain a logical
 * pointer to SQL BLOB data; rather it directly contained a representation of
 * the data (a byte array). As a result, an HSQLDB Blob object was itself
 * valid beyond the duration of the transaction in which is was created,
 * although it did not necessarily represent a corresponding value
 * on the database. Also, the interface methods for updating a BLOB value
 * were unsupported, with the exception of the truncate method,
 * in that it could be used to truncate the local value. <p>
 *
 * Starting with 1.9.0, the HSQLDB driver fully supports both local and remote
 * SQL BLOB data implementations, meaning that an HSQLDB Blob object <em>may</em>
 * contain a logical pointer to remote SQL BLOB data (see {@link JDBCBlobClient
 * JDBCBlobClient}) or it may directly contain a local representation of the
 * data (as implemented in this class).  In particular, when the product is built
 * under JDK 1.6+ and the Blob instance is constructed as a result of calling
 * JDBCConnection.createBlob(), then the resulting Blob instance is initially
 * disconnected (is not bound to the tranaction scope of the vending Connection
 * object), the data is contained directly and all interface methods for
 * updating the BLOB value are supported for local use until the first
 * invocation of free(); otherwise, an HSQLDB Blob's implementation is
 * determined at runtime by the driver, it is typically not valid beyond the
 * duration of the transaction in which is was created, and there no
 * standard way to query whether it represents a local or remote
 * value.<p>
 *
 * </div>
 * <!-- end Release-specific documentation -->
 *
 * @author james house jhouse@part.net
 * @author boucherb@users
 * @version 1.9.0
 * @since JDK 1.2, HSQLDB 1.7.2
 * @revised JDK 1.6, HSQLDB 1.9.0
 */
public class JDBCBlob implements Blob {

    /**
     * Returns the number of bytes in the <code>BLOB</code> value
     * designated by this <code>Blob</code> object.
     * @return length of the <code>BLOB</code> in bytes
     * @exception SQLException if there is an error accessing the
     * length of the <code>BLOB</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long length() throws SQLException {

        final byte[] ldata = data;

        checkValid(ldata);

        return ldata.length;
    }

    /**
     * Retrieves all or part of the <code>BLOB</code>
     * value that this <code>Blob</code> object represents, as an array of
     * bytes.  This <code>byte</code> array contains up to <code>length</code>
     * consecutive bytes starting at position <code>pos</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * The official specification above is ambiguous in that it does not
     * precisely indicate the policy to be observed when
     * pos > this.length() - length.  One policy would be to retrieve the
     * octets from pos to this.length().  Another would be to throw an
     * exception.  HSQLDB observes the second policy.
     * </div> <!-- end release-specific documentation -->
     *
     * @param pos the ordinal position of the first byte in the
     *        <code>BLOB</code> value to be extracted; the first byte is at
     *        position 1
     * @param length the number of consecutive bytes to be copied
     * @return a byte array containing up to <code>length</code>
     *         consecutive bytes from the <code>BLOB</code> value designated
     *         by this <code>Blob</code> object, starting with the
     *         byte at position <code>pos</code>
     * @exception SQLException  if there is an error accessing the BLOB value;
     *            if <code>pos</code> is less than 1 or <code>length</code> is
     *            less than 0.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBytes
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public byte[] getBytes(long pos, final int length) throws SQLException {

        final byte[] ldata = data;

        checkValid(ldata);

        final int dlen = ldata.length;

        if (pos < MIN_POS || pos > MIN_POS + dlen) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        pos--;

        if (length < 0 || length > dlen - pos) {
            throw Util.outOfRangeArgument("length: " + length);
        }

        final byte[] out = new byte[length];

        System.arraycopy(ldata, (int) pos, out, 0, length);

        return out;
    }

    /**
     * Retrieves the <code>BLOB</code> value designated by this
     * <code>Blob</code> instance as a stream.
     *
     * @return a stream containing the <code>BLOB</code> data
     * @exception SQLException if there is an error accessing the
     *            <code>BLOB</code> value
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBinaryStream
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public InputStream getBinaryStream() throws SQLException {

        final byte[] ldata = data;

        checkValid(ldata);

        return new ByteArrayInputStream(ldata);
    }

    /**
     * Retrieves the byte position at which the specified byte array
     * <code>pattern</code> begins within the <code>BLOB</code>
     * value that this <code>Blob</code> object represents.  The
     * search for <code>pattern</code> begins at position
     * <code>start</code>.
     *
     * @param pattern the byte array for which to search
     * @param start the position at which to begin searching; the
     *        first position is 1
     * @return the position at which the pattern appears, else -1
     * @exception SQLException if there is an error accessing the
     * <code>BLOB</code> or if start is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long position(final byte[] pattern,
                         long start) throws SQLException {

        final byte[] ldata = data;

        checkValid(ldata);

        final int dlen = ldata.length;

        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if (start > dlen || pattern == null) {
            return -1L;
        } else {
            start--;
        }

        final int plen = pattern.length;

        if (plen == 0 || start > dlen - plen) {
            return -1L;
        }

        final int  stop = dlen - plen;
        final byte b0   = pattern[0];

        outer_loop:
        for (int i = (int) start; i <= stop; i++) {
            if (ldata[i] != b0) {
                continue;
            }

            int     len     = plen;
            int     doffset = i;
            int     poffset = 0;
            boolean match   = true;

            while (len-- > 0) {
                if (ldata[doffset++] != pattern[poffset++]) {
                    continue outer_loop;
                }
            }

            return (i + 1);
        }

        return -1L;
    }

    /**
     * Retrieves the byte position in the <code>BLOB</code> value
     * designated by this <code>Blob</code> object at which
     * <code>pattern</code> begins.  The search begins at position
     * <code>start</code>.
     *
     * @param pattern the <code>Blob</code> object designating
     * the <code>BLOB</code> value for which to search
     * @param start the position in the <code>BLOB</code> value
     *        at which to begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     * @exception SQLException if there is an error accessing the
     *            <code>BLOB</code> value or if start is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long position(final Blob pattern, long start) throws SQLException {

        final byte[] ldata = data;

        checkValid(ldata);

        final int dlen = ldata.length;

        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if (start > dlen || pattern == null) {
            return -1L;
        } else {
            start--;
        }

        final long plen = pattern.length();

        if (plen == 0 || start > ((long) dlen) - plen) {
            return -1L;
        }

        // by now, we know plen <= Integer.MAX_VALUE
        final int iplen = (int) plen;
        byte[]    bap;

        if (pattern instanceof JDBCBlob) {
            bap = ((JDBCBlob) pattern).data();
        } else {
            bap = pattern.getBytes(1L, iplen);
        }

        final int  stop = dlen - iplen;
        final byte b0   = bap[0];

        outer_loop:
        for (int i = (int) start; i <= stop; i++) {
            if (ldata[i] != b0) {
                continue;
            }

            int len     = iplen;
            int doffset = i;
            int poffset = 0;

            while (len-- > 0) {
                if (ldata[doffset++] != bap[poffset++]) {
                    continue outer_loop;
                }
            }

            return i + 1;
        }

        return -1L;
    }

    // -------------------------- JDBC 3.0 -----------------------------------

    /**
     * Writes the given array of bytes to the <code>BLOB</code> value that
     * this <code>Blob</code> object represents, starting at position
     * <code>pos</code>, and returns the number of bytes written.
     * The array of bytes will overwrite the existing bytes
     * in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached
     * while writing the array of bytes, then the length of the <code>Blob</code>
     * value will be increased to accomodate the extra bytes.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
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
     * When built under JDK 1.6+ and the Blob instance is constructed as a
     * result of calling JDBCConnection.createBlob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createBlob() constructs disconnected,
     * initially empty Blob instances. To propogate the Blob value to a database
     * in this case, it is required to supply the Blob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Blob instance to an updateXXX method of an updateable
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
     * JDBCBlob modifications and the integrity of the application depends on
     * total order Blob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position in the <code>BLOB</code> object at which
     *        to start writing; the first position is 1
     * @param bytes the array of bytes to be written to the <code>BLOB</code>
     *        value that this <code>Blob</code> object represents
     * @return the number of bytes written
     * @exception SQLException if there is an error accessing the
     *            <code>BLOB</code> value or if pos is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBytes
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {

        if (bytes == null) {
            throw Util.nullArgument("bytes");
        }

        return (setBytes(pos, bytes, 0, bytes.length));
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the
     * <code>BLOB</code> value that this <code>Blob</code> object represents
     * and returns the number of bytes written.
     * Writing starts at position <code>pos</code> in the <code>BLOB</code>
     * value; <code>len</code> bytes from the given byte array are written.
     * The array of bytes will overwrite the existing bytes
     * in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached
     * while writing the array of bytes, then the length of the <code>Blob</code>
     * value will be increased to accomodate the extra bytes.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
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
     * When built under JDK 1.6+ and the Blob instance is constructed as a
     * result of calling JDBCConnection.createBlob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createBlob() constructs disconnected,
     * initially empty Blob instances. To propogate the Blob value to a database
     * in this case, it is required to supply the Blob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Blob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * If the value specified for <code>pos</code>
     * is greater than the length of the <code>BLOB</code> value, then
     * the <code>BLOB</code> value is extended in length to accept the
     * written octets and the undefined region up to <code>pos</code> is
     * filled with (byte)0. <p>
     *
     * No attempt is made to ensure precise thread safety. Instead, volatile
     * member field and local variable snapshot isolation semantics are
     * implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, however, if an application may perform concurrent
     * JDBCBlob modifications and the integrity of the application depends on
     * total order Blob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position in the <code>BLOB</code> object at which
     *        to start writing; the first position is 1
     * @param bytes the array of bytes to be written to this <code>BLOB</code>
     *        object
     * @param offset the offset into the array <code>bytes</code> at which
     *        to start reading the bytes to be set
     * @param len the number of bytes to be written to the <code>BLOB</code>
     *        value from the array of bytes <code>bytes</code>
     * @return the number of bytes written
     * @exception SQLException if there is an error accessing the
     *            <code>BLOB</code> value or if pos is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBytes
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public int setBytes(long pos, byte[] bytes, int offset,
                        int len) throws SQLException {

        if (!this.createdByConnection) {

            /** @todo - better error message */
            throw Util.notSupported();
        }

        if (bytes == null) {
            throw Util.nullArgument("bytes");
        }

        if (offset < 0 || offset > bytes.length) {
            throw Util.outOfRangeArgument("offset: " + offset);
        }

        if (len > bytes.length - offset) {
            throw Util.outOfRangeArgument("len: " + len);
        }

        if (pos < MIN_POS || pos > 1L + (Integer.MAX_VALUE - len)) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        pos--;

        byte[] ldata = this.data;

        checkValid(ldata);

        final int dlen = ldata.length;

        if ((pos + len) > dlen) {
            byte[] temp = new byte[(int) pos + len];

            System.arraycopy(ldata, 0, temp, 0, dlen);

            ldata = temp;
            temp  = null;
        }

        System.arraycopy(bytes, offset, ldata, (int) pos, len);

        // paranoia, in case somone free'd us during the array copies.
        checkValid(this.data);

        this.data = ldata;

        return len;
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code>
     * value that this <code>Blob</code> object represents.  The stream begins
     * at position <code>pos</code>.
     * The  bytes written to the stream will overwrite the existing bytes
     * in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached
     * while writing to the stream, then the length of the <code>Blob</code>
     * value will be increased to accomodate the extra bytes.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
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
     * When built under JDK 1.6+ and the Blob instance is constructed as a
     * result of calling JDBCConnection.createBlob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createBlob() constructs disconnected,
     * initially empty Blob instances. To propogate the Blob value to a database
     * in this case, it is required to supply the Blob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Blob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * The data written to the stream does not appear in this
     * Blob until the stream is closed <p>
     *
     * When the stream is closed, if the value specified for <code>pos</code>
     * is greater than the length of the <code>BLOB</code> value, then
     * the <code>BLOB</code> value is extended in length to accept the
     * written octets and the undefined region up to <code>pos</code> is
     * filled with (byte)0. <p>
     *
     * Also, no attempt is made to ensure precise thread safety. Instead,
     * volatile member field and local variable snapshot isolation semantics
     * are implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, however, if an application may perform concurrent
     * JDBCBlob modifications and the integrity of the application depends on
     * total order Blob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position in the <code>BLOB</code> value at which
     *        to start writing; the first position is 1
     * @return a <code>java.io.OutputStream</code> object to which data can
     *         be written
     * @exception SQLException if there is an error accessing the
     *            <code>BLOB</code> value or if pos is less than 1
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBinaryStream
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public OutputStream setBinaryStream(final long pos) throws SQLException {

        if (!this.createdByConnection) {

            /** @todo - Better error message */
            throw Util.notSupported();
        }

        if (pos < MIN_POS || pos > MAX_POS) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        checkValid(this.data);

        return new java.io.ByteArrayOutputStream() {

            public synchronized void close() throws java.io.IOException {

                try {
                    JDBCBlob.this.setBytes(pos, toByteArray());
                } catch (SQLException se) {
                    throw new java.io.IOException(se.toString());
                } finally {
                    super.close();
                }
            }
        };
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code>
     * object represents to be <code>len</code> bytes in length.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
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
     * When built under JDK 1.6+ and the Blob instance is constructed as a
     * result of calling JDBCConnection.createBlob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createBlob() constructs disconnected,
     * initially empty Blob instances. To propogate the truncated Blob value to
     * a database in this case, it is required to supply the Blob instance to
     * an updating or inserting setXXX method of a Prepared or Callable
     * Statement, or to supply the Blob instance to an updateXXX method of an
     * updateable ResultSet. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param len the length, in bytes, to which the <code>BLOB</code> value
     *        that this <code>Blob</code> object represents should be truncated
     * @exception SQLException if there is an error accessing the
     *            <code>BLOB</code> value or if len is less than 0
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     * @revised JDK 1.6, HSQLDB 1.9.0
     */
    public void truncate(final long len) throws SQLException {

        final byte[] ldata = data;

        checkValid(ldata);

        if (len < 0 || len > ldata.length) {
            throw Util.outOfRangeArgument("len: " + len);
        }

        if (len == ldata.length) {
            return;
        }

        byte[] newData = new byte[(int) len];

        System.arraycopy(ldata, 0, newData, 0, (int) len);
        checkValid(data);    // limit possible race-condition with free()

        data = newData;
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * This method frees the <code>Blob</code> object and releases the resources that
     * it holds. The object is invalid once the <code>free</code>
     * method is called.
     * <p>
     * After <code>free</code> has been called, any attempt to invoke a
     * method other than <code>free</code> will result in a <code>SQLException</code>
     * being thrown.  If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     * <p>
     *
     * @throws SQLException if an error occurs releasing
     * the Blob's resources
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
    public void free() throws SQLException {
        this.data = null;
    }

    /**
     * Returns an <code>InputStream</code> object that contains a partial <code>Blob</code> value,
     * starting  with the byte specified by pos, which is length bytes in length.
     *
     * @param pos the offset to the first byte of the partial value to be retrieved.
     *  The first byte in the <code>Blob</code> is at position 1
     * @param length the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial <code>Blob</code> value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the number of bytes
     * in the <code>Blob</code> or if pos + length is greater than the number of bytes
     * in the <code>Blob</code>
     *
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
    public InputStream getBinaryStream(long pos,
                                       long length) throws SQLException {

        final byte[] ldata = data;

        checkValid(ldata);

        final int dlen = ldata.length;

        if (pos < MIN_POS || pos > dlen) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        pos--;

        if (length < 0 || length > dlen - pos) {
            throw Util.outOfRangeArgument("length: " + length);
        }

        if (pos == 0 && length == dlen) {
            return new ByteArrayInputStream(ldata);
        }

        // Let client decide on policy.
        //
        // Zero-copy is (possibly much) faster because it does
        // not allocate extra memory and does not involve copy
        // processing.
        //
        // However, because it could lead to unexpected memory,
        // stress, it is not polite to always pass back a stream
        // whose buffer is the full size required to represent the
        // underlying BLOB value.
//        if (isGetBinaryStreamUsesCopy()) {
        final byte[] out = new byte[(int) length];

        System.arraycopy(ldata, (int) pos, out, 0, (int) length);

        return new ByteArrayInputStream(out);

//        } else {
//            return new BinaryInputStream(ldata, pos, length);
//        }
    }

    /**
     * Assigns whether getBinaryStream() uses a copy of
     * the underlying byte[].
     *
     * @param b when true, then getBinaryStream() uses a copy of
     * the underlying byte[]
     */
    public void setGetBinaryStreamUsesCopy(boolean b) {
        getBinaryStreamUsesCopy = b;
    }

    /**
     * Retrieves whether getBinaryStream() uses a copy of
     * the underlying buffer.
     *
     * @return true if getBinaryStream() uses a copy of
     * the underlying buffer; else false
     */
    public boolean isGetBinaryStreamUsesCopy() {
        return getBinaryStreamUsesCopy;
    }

    // ---------------------- internal implementation --------------------------
    public static final long MIN_POS = 1L;
    public static final long MAX_POS = 1L + (long) Integer.MAX_VALUE;
    private volatile byte[]  data;
    private final boolean    createdByConnection;
    private boolean          getBinaryStreamUsesCopy;

    /**
     * Constructs a new JDBCBlob instance wrapping the given octet sequence. <p>
     *
     * This constructor is used internally to retrieve result set values as
     * Blob objects, yet it must be public to allow access from other packages.
     * As such (in the interest of efficiency) this object maintains a reference
     * to the given octet sequence rather than making a copy; special care
     * should be taken by extenal clients never to use this constructor with a
     * byte array object that may later be modified extenally.
     *
     * @param data the octet sequence representing the Blob value
     * @throws SQLException if the argument is null
     */
    public JDBCBlob(final byte[] data) throws SQLException {

        init(data);

        this.createdByConnection = false;
    }

    protected JDBCBlob() {
        this.data                = new byte[0];
        this.createdByConnection = true;
    }

    protected void init(byte[] data) throws SQLException {

        if (data == null) {
            throw Util.nullArgument();
        }

        this.data = data;    // (byte[]) data.clone();
    }

    protected void checkValid(final Object data) {

        if (data == null) {
            throw new RuntimeException("null data");
        }
    }

    protected byte[] data() throws SQLException {

        byte[] ldata = data;

        checkValid(ldata);

        return ldata;
    }
}
