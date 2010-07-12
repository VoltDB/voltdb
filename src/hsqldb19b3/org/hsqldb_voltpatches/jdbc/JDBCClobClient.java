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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.types.ClobDataID;

public class JDBCClobClient implements Clob {

    /**
     * Retrieves the <code>CLOB</code> value designated by this
     * <code>Clob</code> object as an ascii stream.
     *
     * @return a <code>java.io.InputStream</code> object containing the
     *   <code>CLOB</code> data
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized InputStream getAsciiStream() throws SQLException {
        throw Util.notSupported();
    }

    /**
     * Retrieves the <code>CLOB</code> value designated by this
     * <code>Clob</code> object as a <code>java.io.Reader</code> object (or
     * as a stream of characters).
     *
     * @return a <code>java.io.Reader</code> object containing the
     *   <code>CLOB</code> data
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized Reader getCharacterStream() throws SQLException {
        return new ClobInputStream(this, 0, length(),
                                   session.getStreamBlockSize());
    }

    /**
     * Retrieves a copy of the specified substring in the <code>CLOB</code>
     * value designated by this <code>Clob</code> object.
     *
     * @param pos the first character of the substring to be extracted. The
     *   first character is at position 1.
     * @param length the number of consecutive characters to be copied
     * @return a <code>String</code> that is the specified substring in the
     *   <code>CLOB</code> value designated by this <code>Clob</code> object
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized String getSubString(long pos,
            int length) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw Util.outOfRangeArgument();
        }

        try {
            return clob.getSubString(session, pos - 1, length);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * Retrieves the number of characters in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     *
     * @return length of the <code>CLOB</code> in characters
     * @throws SQLException if there is an error accessing the length of the
     *   <code>CLOB</code> value
     */
    public synchronized long length() throws SQLException {

        try {
            return clob.length(session);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * Retrieves the character position at which the specified substring
     * <code>searchstr</code> appears in the SQL <code>CLOB</code> value
     * represented by this <code>Clob</code> object.
     *
     * @param searchstr the substring for which to search
     * @param start the position at which to begin searching; the first
     *   position is 1
     * @return the position at which the substring appears or -1 if it is
     *   not present; the first position is 1
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized long position(String searchstr,
                                      long start) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw Util.outOfRangeArgument();
        }

        try {
            return clob.position(session, searchstr, start - 1);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * Retrieves the character position at which the specified
     * <code>Clob</code> object <code>searchstr</code> appears in this
     * <code>Clob</code> object.
     *
     * @param searchstr the <code>Clob</code> object for which to search
     * @param start the position at which to begin searching; the first
     *   position is 1
     * @return the position at which the <code>Clob</code> object appears or
     *   -1 if it is not present; the first position is 1
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized long position(Clob searchstr,
                                      long start) throws SQLException {
        return position(searchstr.getSubString(0, (int) searchstr.length()),
                        start);
    }

    /**
     * Retrieves a stream to be used to write Ascii characters to the
     * <code>CLOB</code> value that this <code>Clob</code> object represents,
     * starting at position <code>pos</code>.
     *
     * @param pos the position at which to start writing to this
     *   <code>CLOB</code> object
     * @return the stream to which ASCII encoded characters can be written
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized OutputStream setAsciiStream(
            long pos) throws SQLException {
        throw Util.notSupported();
    }

    /**
     * Retrieves a stream to be used to write a stream of Unicode characters
     * to the <code>CLOB</code> value that this <code>Clob</code> object
     * represents, at position <code>pos</code>.
     *
     * @param pos the position at which to start writing to the
     *   <code>CLOB</code> value
     * @return a stream to which Unicode encoded characters can be written
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized Writer setCharacterStream(
            long pos) throws SQLException {
        throw Util.notSupported();
    }

    /**
     * Writes the given Java <code>String</code> to the <code>CLOB</code>
     * value that this <code>Clob</code> object designates at the position
     * <code>pos</code>.
     *
     * @param pos the position at which to start writing to the
     *   <code>CLOB</code> value that this <code>Clob</code> object
     *   represents
     * @param str the string to be written to the <code>CLOB</code> value
     *   that this <code>Clob</code> designates
     * @return the number of characters written
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized int setString(long pos,
                                      String str) throws SQLException {
        return setString(pos, str, 0, str.length());
    }

    /**
     * Writes <code>len</code> characters of <code>str</code>, starting at
     * character <code>offset</code>, to the <code>CLOB</code> value that
     * this <code>Clob</code> represents.
     *
     * @param pos the position at which to start writing to this
     *   <code>CLOB</code> object
     * @param str the string to be written to the <code>CLOB</code> value
     *   that this <code>Clob</code> object represents
     * @param offset the offset into <code>str</code> to start reading the
     *   characters to be written
     * @param len the number of characters to be written
     * @return the number of characters written
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized int setString(long pos, String str, int offset,
                                      int len) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, pos - 1, len)) {
            throw Util.outOfRangeArgument();
        }

        try {
            return clob.setString(session, pos - 1, str, offset, len);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * Truncates the <code>CLOB</code> value that this <code>Clob</code>
     * designates to have a length of <code>len</code> characters.
     *
     * @param len the length, in bytes, to which the <code>CLOB</code> value
     *   should be truncated
     * @throws SQLException if there is an error accessing the
     *   <code>CLOB</code> value
     */
    public synchronized void truncate(long len) throws SQLException {

        try {
            clob.truncate(session, len);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
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
    public synchronized void free() throws SQLException {
        isClosed = true;
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
    public synchronized Reader getCharacterStream(long pos,
            long length) throws SQLException {
        return new ClobInputStream(this, pos - 1, length,
                                   session.getStreamBlockSize());
    }

    char[] getChars(long position, int length) throws SQLException {

        try {
            return clob.getChars(session, position - 1, length);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    //
    ClobDataID       clob;
    SessionInterface session;
    boolean          isClosed;

    JDBCClobClient(SessionInterface session, ClobDataID clob) {
        this.session = session;
        this.clob    = clob;
    }

    public synchronized boolean isClosed() {
        return isClosed;
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}
