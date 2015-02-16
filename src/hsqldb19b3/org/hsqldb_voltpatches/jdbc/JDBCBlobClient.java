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


package org.hsqldb_voltpatches.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.BlobInputStream;

/**
 * A wrapper for HSQLDB BlobData objects.
 *
 * Instances of this class are returnd by calls to ResultSet methods.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.6
 * @since 1.9.0
 */
public class JDBCBlobClient implements Blob {

    /**
     * Returns the number of bytes in the <code>BLOB</code> value designated
     * by this <code>Blob</code> object.
     *
     * @return length of the <code>BLOB</code> in bytes
     * @throws SQLException if there is an error accessing the length of the
     *   <code>BLOB</code>
     */
    public synchronized long length() throws SQLException {

        try {
            return blob.length(session);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves all or part of the <code>BLOB</code> value that this
     * <code>Blob</code> object represents, as an array of bytes.
     *
     * @param pos the ordinal position of the first byte in the
     *   <code>BLOB</code> value to be extracted; the first byte is at
     *   position 1
     * @param length the number of consecutive bytes to be copied
     * @return a byte array containing up to <code>length</code> consecutive
     *   bytes from the <code>BLOB</code> value designated by this
     *   <code>Blob</code> object, starting with the byte at position
     *   <code>pos</code>
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code> value
     */
    public synchronized byte[] getBytes(long pos,
                                        int length) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        try {
            return blob.getBytes(session, pos - 1, length);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the <code>BLOB</code> value designated by this
     * <code>Blob</code> instance as a stream.
     *
     * @return a stream containing the <code>BLOB</code> data
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code> value
     */
    public synchronized InputStream getBinaryStream() throws SQLException {
        return new BlobInputStream(session, blob, 0, length());
    }

    /**
     * Retrieves the byte position at which the specified byte array
     * <code>pattern</code> begins within the <code>BLOB</code> value that
     * this <code>Blob</code> object represents.
     *
     * @param pattern the byte array for which to search
     * @param start the position at which to begin searching; the first
     *   position is 1
     * @return the position at which the pattern appears, else -1
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code>
     */
    public synchronized long position(byte[] pattern,
                                      long start) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        try {
            long position = blob.position(session, pattern, start - 1);

            if (position >= 0) {
                position++;
            }

            return position;
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the byte position in the <code>BLOB</code> value designated
     * by this <code>Blob</code> object at which <code>pattern</code> begins.
     *
     * @param pattern the <code>Blob</code> object designating the
     *   <code>BLOB</code> value for which to search
     * @param start the position in the <code>BLOB</code> value at which to
     *   begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code> value
     */
    public synchronized long position(Blob pattern,
                                      long start) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        if (pattern instanceof JDBCBlobClient) {
            BlobDataID searchClob = ((JDBCBlobClient) pattern).blob;

            try {
                long position = blob.position(session, searchClob, start - 1);

                if (position >= 0) {
                    position++;
                }

                return position;
            } catch (HsqlException e) {
                throw JDBCUtil.sqlException(e);
            }
        }

        if (!isInLimits(Integer.MAX_VALUE, 0, pattern.length())) {
            throw JDBCUtil.outOfRangeArgument();
        }

        byte[] bytePattern = pattern.getBytes(1, (int) pattern.length());

        return position(bytePattern, start);
    }

    /**
     * Writes the given array of bytes to the <code>BLOB</code> value that
     * this <code>Blob</code> object represents, starting at position
     * <code>pos</code>, and returns the number of bytes written.
     *
     * @param pos the position in the <code>BLOB</code> object at which to
     *   start writing
     * @param bytes the array of bytes to be written to the
     *   <code>BLOB</code> value that this <code>Blob</code> object
     *   represents
     * @return the number of bytes written
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code> value
     */
    public synchronized int setBytes(long pos,
                                     byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the
     * <code>BLOB</code> value that this <code>Blob</code> object represents
     * and returns the number of bytes written.
     *
     * @param pos the position in the <code>BLOB</code> object at which to
     *   start writing
     * @param bytes the array of bytes to be written to this
     *   <code>BLOB</code> object
     * @param offset the offset into the array <code>bytes</code> at which
     *   to start reading the bytes to be set
     * @param len the number of bytes to be written to the <code>BLOB</code>
     *   value from the array of bytes <code>bytes</code>
     * @return the number of bytes written
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code> value
     */
    public synchronized int setBytes(long pos, byte[] bytes, int offset,
                                     int len) throws SQLException {

        if (!isInLimits(bytes.length, offset, len)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        if (!isInLimits(Long.MAX_VALUE, pos - 1, len)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        if (!isWritable) {
            throw JDBCUtil.notUpdatableColumn();
        }

        startUpdate();

        try {
            blob.setBytes(session, pos - 1, bytes, offset, len);

            return len;
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code>
     * value that this <code>Blob</code> object represents.
     *
     * @param pos the position in the <code>BLOB</code> value at which to
     *   start writing
     * @return a <code>java.io.OutputStream</code> object to which data can
     *   be written
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code> value
     */
    public synchronized OutputStream setBinaryStream(long pos)
    throws SQLException {
        throw JDBCUtil.notSupported();
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code>
     * object represents to be <code>len</code> bytes in length.
     *
     * @param len the length, in bytes, to which the <code>BLOB</code> value
     *   that this <code>Blob</code> object represents should be truncated
     * @throws SQLException if there is an error accessing the
     *   <code>BLOB</code> value
     */
    public synchronized void truncate(long len) throws SQLException {

        try {
            blob.truncate(session, len);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

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
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void free() throws SQLException {
        isClosed = true;
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
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized InputStream getBinaryStream(long pos,
            long length) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        return new BlobInputStream(session, blob, pos - 1, length);
    }

    //--
    BlobDataID       originalBlob;
    BlobDataID       blob;
    SessionInterface session;
    int              colIndex;
    private boolean  isClosed;
    private boolean  isWritable;
    JDBCResultSet    resultSet;

    public JDBCBlobClient(SessionInterface session, BlobDataID blob) {
        this.session = session;
        this.blob    = blob;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public BlobDataID getBlob() {
        return blob;
    }

    public synchronized void setWritable(JDBCResultSet result, int index) {

        isWritable = true;
        resultSet  = result;
        colIndex   = index;
    }

    public synchronized void clearUpdates() {

        if (originalBlob != null) {
            blob         = originalBlob;
            originalBlob = null;
        }
    }

    private void startUpdate() throws SQLException {

        if (originalBlob != null) {
            return;
        }

        originalBlob = blob;
        blob         = (BlobDataID) blob.duplicate(session);

        resultSet.startUpdate(colIndex + 1);

        resultSet.preparedStatement.parameterValues[colIndex] = blob;
        resultSet.preparedStatement.parameterSet[colIndex]    = Boolean.TRUE;
    }

    private void checkClosed() throws SQLException {

        if (isClosed) {
            throw JDBCUtil.sqlException(ErrorCode.X_07501);
        }
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}
