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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.sql.Clob;
import java.sql.SQLException;

import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.types.ClobDataID;
import org.hsqldb_voltpatches.types.ClobInputStream;

/**
 * A wrapper for HSQLDB ClobData objects.
 *
 * Instances of this class are returned by calls to ResultSet methods.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.6
 * @since  JDK 1.2, HSQLDB 1.9.0
 */
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

        checkClosed();

        return new InputStream() {

            private final byte[] oneChar = new byte[1];
            private boolean      m_closed;

            // better size than 8192 for network connections.
            private CharBuffer m_charBuffer =
                (CharBuffer) CharBuffer.allocate(64 * 1024).flip();
            private ByteBuffer m_byteBuffer = ByteBuffer.allocate(1024);
            private Charset    m_charset    = charsetForName("US-ASCII");
            private CharsetEncoder m_encoder =
                m_charset.newEncoder().onMalformedInput(
                    CodingErrorAction.REPLACE).onUnmappableCharacter(
                    CodingErrorAction.REPLACE);
            private Reader m_reader = clob.getCharacterStream(session);
            public int read() throws IOException {

                if (isEOF()) {
                    return -1;
                }

                synchronized (oneChar) {
                    int charsRead = read(oneChar, 0, 1);

                    return charsRead == 1 ? oneChar[0]
                            : -1;
                }
            }
            public int read(byte b[], int off, int len) throws IOException {

                checkClosed();

                if (isEOF()) {
                    return -1;
                }

                final CharBuffer cb = m_charBuffer;

                //
                int charsRead;
                int bytesRead;

                if (cb.remaining() == 0) {
                    cb.clear();

                    charsRead = m_reader.read(cb);

                    cb.flip();

                    if (charsRead < 0) {
                        setEOF();

                        return -1;
                    } else if (charsRead == 0) {
                        return 0;
                    }
                }

                final ByteBuffer bb = (m_byteBuffer.capacity() < len)
                                      ? ByteBuffer.allocate(len)
                                      : m_byteBuffer;

                // Since ASCII is single-byte, retrict encoder character consumption
                // to at most 'len' characters' to produce at most len ASCII
                // characters
                int cbLimit     = cb.limit();
                int cbPosistion = cb.position();

                cb.limit(cbPosistion + len);
                bb.clear();

                int         bbPosition = bb.position();
                CoderResult result     = m_encoder.encode(cb, bb, false);

                if (bbPosition == bb.position() && result.isUnderflow()) {

                    // surrogate character time
                    cb.limit(cb.limit() + 1);
                    m_encoder.encode(cb, bb, false);
                }

                // Restore the old limit so the buffer gets topped up
                // when required.
                cb.limit(cbLimit);
                bb.flip();

                bytesRead = bb.limit();

                if (bytesRead == 0) {
                    setEOF();

                    return -1;
                }
                m_byteBuffer = bb;

                bb.get(b, off, bytesRead);

                return bytesRead;
            }
            public void close() throws IOException {

                boolean isClosed = m_closed;

                if (!isClosed) {
                    m_closed     = true;
                    m_charBuffer = null;
                    m_charset    = null;
                    m_encoder    = null;

                    try {
                        m_reader.close();
                    } catch (Exception ex) {
                    }
                }
            }
            private boolean isEOF() {

                final Reader reader = m_reader;

                return (reader == null);
            }
            private void setEOF() {

                final Reader reader = m_reader;

                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException iOException) {
                    }
                }
                m_reader = null;
            }
            private void checkClosed() throws IOException {

                if (JDBCClobClient.this.isClosed()) {
                    try {
                        this.close();
                    } catch (Exception ex) {
                    }
                }

                if (m_closed) {
                    throw new IOException("The stream is closed.");
                }
            }
        };
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

        checkClosed();

        return new ClobInputStream(session, clob, 0, length());
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

        checkClosed();

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        try {
            return clob.getSubString(session, pos - 1, length);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
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

        checkClosed();

        try {
            return clob.length(session);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
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
            throw JDBCUtil.outOfRangeArgument();
        }
        checkClosed();

        try {
            return clob.position(session, searchstr, start - 1);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
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

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        if (searchstr instanceof JDBCClobClient) {
            ClobDataID searchClob = ((JDBCClobClient) searchstr).clob;

            try {
                return clob.position(session, searchClob, start - 1);
            } catch (HsqlException e) {
                throw JDBCUtil.sqlException(e);
            }
        }

        return position(searchstr.getSubString(1, (int) searchstr.length()),
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
            final long pos) throws SQLException {

        checkClosed();

        if (pos < 1) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        if (!isWritable) {
            throw JDBCUtil.notUpdatableColumn();
        }
        startUpdate();

        return new OutputStream() {

            private long    m_position = pos - 1;
            private Charset m_charset  = charsetForName("US-ASCII");
            private CharsetDecoder m_decoder =
                m_charset.newDecoder().onMalformedInput(
                    CodingErrorAction.REPLACE).onUnmappableCharacter(
                    CodingErrorAction.REPLACE);
            private CharBuffer   m_charBuffer = CharBuffer.allocate(64 * 1024);
            private ByteBuffer   m_byteBuffer = ByteBuffer.allocate(1024);
            private final byte[] oneByte      = new byte[1];
            private boolean      m_closed;
            public void write(int b) throws IOException {

                synchronized (oneByte) {
                    oneByte[0] = (byte) b;

                    this.write(oneByte, 0, 1);
                }
            }
            public void write(byte b[], int off, int len) throws IOException {

                checkClosed();

                final ByteBuffer bb = (m_byteBuffer.capacity() < len)
                                      ? ByteBuffer.allocate(len)
                                      : m_byteBuffer;

                if (m_charBuffer.remaining() < len) {
                    flush0();
                }

                final CharBuffer cb = m_charBuffer.capacity() < len
                                      ? CharBuffer.allocate(len)
                                      : m_charBuffer;

                bb.clear();
                bb.put(b, off, len);
                bb.flip();
                m_decoder.decode(bb, cb, false);

                if (cb.remaining() == 0) {
                    flush();
                }
            }
            public void flush() throws IOException {
                checkClosed();
                flush0();
            }
            public void close() throws IOException {

                if (!m_closed) {
                    try {
                        flush0();
                    } finally {
                        m_closed     = true;
                        m_byteBuffer = null;
                        m_charBuffer = null;
                        m_charset    = null;
                        m_decoder    = null;
                    }
                }
            }
            private void checkClosed() throws IOException {

                if (JDBCClobClient.this.isClosed()) {
                    try {
                        close();
                    } catch (Exception ex) {
                    }
                }

                if (m_closed) {
                    throw new IOException("The stream is closed.");
                }
            }
            private void flush0() throws IOException {

                final CharBuffer cb = m_charBuffer;

                cb.flip();

                final char[] chars = new char[cb.length()];

                cb.get(chars);
                cb.clear();

                try {
                    clob.setChars(session, m_position, chars, 0, chars.length);
                } catch (Exception e) {
                    throw new IOException(e.toString());
                }
                m_position += chars.length;
            }
        };
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
            final long pos) throws SQLException {

        checkClosed();

        if (pos < 1) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        if (!isWritable) {
            throw JDBCUtil.notUpdatableColumn();
        }
        startUpdate();

        return new Writer() {

            private long    m_clobPosition = pos - 1;
            private boolean m_closed;
            public void write(char[] cbuf, int off,
                              int len) throws IOException {

                checkClosed();
                clob.setChars(session, m_clobPosition, cbuf, off, len);

                m_clobPosition += len;
            }
            public void flush() throws IOException {

                // no-op
            }
            @Override
            public void close() throws IOException {
                m_closed = true;
            }
            private void checkClosed() throws IOException {

                if (m_closed || JDBCClobClient.this.isClosed()) {
                    throw new IOException("The stream is closed");
                }
            }
        };
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

        if (!isInLimits(str.length(), offset, len)) {
            throw JDBCUtil.outOfRangeArgument();
        }
        checkClosed();

        if (pos < 1) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        if (!isWritable) {
            throw JDBCUtil.notUpdatableColumn();
        }
        startUpdate();

        str = str.substring(offset, offset + len);

        try {
            clob.setString(session, pos - 1, str);

            return len;
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
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

        if (len < 0) {
            throw JDBCUtil.outOfRangeArgument("len: " + len);
        }
        checkClosed();

        try {
            clob.truncate(session, len);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
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
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void free() throws SQLException {

        isClosed = true;
        clob     = null;
        session  = null;
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
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized Reader getCharacterStream(long pos,
            long length) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw JDBCUtil.outOfRangeArgument();
        }
        checkClosed();

        return new ClobInputStream(session, clob, pos - 1, length);
    }

    char[] getChars(long position, int length) throws SQLException {

        try {
            return clob.getChars(session, position - 1, length);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    //
    ClobDataID       originalClob;
    ClobDataID       clob;
    SessionInterface session;
    int              colIndex;
    private boolean  isClosed;
    private boolean  isWritable;
    JDBCResultSet    resultSet;

    public JDBCClobClient(SessionInterface session, ClobDataID clob) {
        this.session = session;
        this.clob    = clob;
    }

    public ClobDataID getClob() {
        return clob;
    }

    public synchronized boolean isClosed() {
        return isClosed;
    }

    public synchronized void setWritable(JDBCResultSet result, int index) {

        isWritable = true;
        resultSet  = result;
        colIndex   = index;
    }

    public synchronized void clearUpdates() {

        if (originalClob != null) {
            clob         = originalClob;
            originalClob = null;
        }
    }

    private void startUpdate() throws SQLException {

        if (originalClob != null) {
            return;
        }
        originalClob = clob;
        clob         = (ClobDataID) clob.duplicate(session);

        resultSet.startUpdate(colIndex + 1);

        resultSet.preparedStatement.parameterValues[colIndex] = clob;
        resultSet.preparedStatement.parameterSet[colIndex]    = Boolean.TRUE;
    }

    private void checkClosed() throws SQLException {

        if (isClosed) {
            throw JDBCUtil.sqlException(ErrorCode.X_07501);
        }
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return fullLength >= 0 && pos >= 0 && len >= 0
               && pos <= fullLength - len;
    }

    protected static Charset charsetForName(
            final String charsetName) throws SQLException {

        String csn = charsetName;

        if (csn == null) {
            csn = Charset.defaultCharset().name();
        }

        try {
            if (Charset.isSupported(csn)) {
                return Charset.forName(csn);
            }
        } catch (IllegalCharsetNameException x) {
        }

        throw JDBCUtil.sqlException(new UnsupportedEncodingException(csn));
    }
}
