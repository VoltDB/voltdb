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


package org.hsqldb_voltpatches.lib;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/* $Id: ClosableByteArrayOutputStream.java 2946 2009-03-22 17:44:48Z fredt $ */

/**
 * @todo - finer-grained synchronization to reduce average
 * potential monitor contention
 */

/**
 * Provides true Closable semantics ordinarily missing in a
 * {@link java.io.ByteArrayOutputStream}. <p>
 *
 * Accumulates output in a byte array that automatically grows as needed.<p>
 *
 * Data is retrieved using <tt>toByteArray()</tt>,
 * <tt>toByteArrayInputStream()</tt>, <tt>toString()</tt> and
 * <tt>toString(encoding)</tt>. <p>
 *
 * {@link #close() Closing} a <tt>ClosableByteArrayOutputStream</tt> prevents
 * further write operations, but all other operations may succeed until after
 * the first invocation of {@link #free() free()}.<p>
 *
 * Freeing a <tt>ClosableByteArrayOutputStream</tt> closes the stream and
 * releases the internal buffer, preventing successful invocation of all
 * operations, with the exception of <tt>size()<tt>, <tt>close()</tt>,
 * <tt>isClosed()</tt>, <tt>free()</tt> and <tt>isFreed()</tt>. <p>
 *
 * This class is especially useful when an accumulating output stream must be
 * handed off to an extenal client under contract that the stream should
 * exhibit true Closable behaviour in response both to internally tracked
 * events and to client invocation of the <tt>OutputStream.close()</tt> method.
 *
 * @author boucherb@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class ClosableByteArrayOutputStream extends OutputStream {

    /**
     * Data buffer.
     */
    protected byte[] buf;

    /**
     * # of valid bytes in buffer.
     */
    protected int count;

    /**
     * Whether this stream is closed.
     */
    protected boolean closed;

    /**
     * Whether this stream is freed.
     */
    protected boolean freed;

    /**
     * Creates a new output stream. <p>
     *
     * The buffer capacity is initially 32 bytes, though its size increases
     * if necessary.
     */
    public ClosableByteArrayOutputStream() {
        this(32);
    }

    /**
     * Creates a new output stream with a buffer capacity of the specified
     * <tt>size</tt>, in bytes.
     *
     * @param size the initial size.
     * @exception IllegalArgumentException if size is negative.
     */
    public ClosableByteArrayOutputStream(int size)
    throws IllegalArgumentException {

        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                                               + size);    // NOI18N
        }

        buf = new byte[size];
    }

    /**
     * Writes the specified single byte.
     *
     * @param b the single byte to be written.
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #close() closed}.
     */
    public synchronized void write(int b) throws IOException {

        checkClosed();

        int newcount = count + 1;

        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }

        buf[count] = (byte) b;
        count      = newcount;
    }

    /**
     * Writes the specified portion of the designated octet sequence. <p>
     *
     * @param b the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #close() closed}.
     */
    public synchronized void write(byte b[], int off,
                                   int len) throws IOException {

        checkClosed();

        if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int newcount = count + len;

        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }

        System.arraycopy(b, off, buf, count, len);

        count = newcount;
    }

    /**
     * By default, does nothing. <p>
     *
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #close() closed}.
     */
    public void flush() throws IOException {
        checkClosed();
    }

    /**
     * Writes the complete contents of this stream's accumulated data to the
     * specified output stream. <p>
     *
     * The operation occurs as if by calling <tt>out.write(buf, 0, count)</tt>.
     *
     * @param out the output stream to which to write the data.
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #free() freed}.
     */
    public synchronized void writeTo(OutputStream out) throws IOException {
        checkFreed();
        out.write(buf, 0, count);
    }

    /**
     * Returns the current capacity of this stream's data buffer.
     *
     * @return  the length of the internal data array
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #free() freed}.
     */
    public synchronized int capacity() throws IOException {

        checkFreed();

        return buf.length;
    }

    /**
     * Resets the <tt>count</tt> field of this output stream to zero, so that
     * all currently accumulated data is effectively discarded. <p>
     *
     * Further write operations will reuse the allocated buffer space. <p>
     *
     * @see #count
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #close() closed}.
     */
    public synchronized void reset() throws IOException {

        checkClosed();

        count = 0;
    }

    /**
     * Attempts to reduce this stream's capacity to its current size. <p>
     *
     * If the data buffer is larger than necessary to hold its current sequence
     * of bytes, then it may be resized to become more space efficient.
     * Calling this method may, but is not required to, affect the value
     * returned by a subsequent call to the {@link #capacity()} method. <p>
     *
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #free() freed}.
     */
    public synchronized void trimToSize() throws IOException {

        checkFreed();

        if (buf.length > count) {
            buf = copyOf(buf, count);
        }
    }

    /**
     * Retrieves a copy of this stream's accumated data, as a byte array.
     *
     * @return a copy of this stream's accumated data, as a byte array.
     * @see #size()
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #free() freed}.
     */
    public synchronized byte[] toByteArray() throws IOException {

        checkFreed();

        return copyOf(buf, count);
    }

    /**
     * Returns the current size of this stream's accumated data.
     *
     * @return the value of the <tt>count</tt> field, which is the number
     *      of valid bytes in this output stream.
     * @see #count
     * @throws java.io.IOException never
     */
    public synchronized int size() throws IOException {
        return count;
    }

    /**
     * Sets the size of this stream's accumulated data. <p>
     *
     * @param   newSize the new size
     * @throws  ArrayIndexOutOfBoundsException if new size is negative
     */
    public synchronized void setSize(int newSize) {

        if (newSize < 0) {
            throw new ArrayIndexOutOfBoundsException(newSize);
        } else if (newSize > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newSize));
        }

        count = newSize;
    }

    /**
     * Performs an effecient (zero-copy) conversion of the data accumulated in
     * this output stream to an input stream. <p>
     *
     * To ensure the future integrity of the resulting input stream, {@link
     * #free() free} is invoked upon this output stream as a side-effect.
     *
     * @return an input stream representing this output stream's accumulated
     *      data
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this output stream has been {@link #free() freed}.
     */
    public synchronized ByteArrayInputStream toByteArrayInputStream()
    throws IOException {

        checkFreed();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf, 0,
            count);

        free();

        return inputStream;
    }

    /**
     * Converts this stream's accumuated data into a string, translating bytes
     * into characters according to the platform's default character encoding.
     *
     * @return String translated from this stream's accumuated data.
     * @throws RuntimeException may be thrown if this output stream has been
     *      {@link #free() freed}.
     */
    public synchronized String toString() {

        try {
            checkFreed();
        } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
        }

        return new String(buf, 0, count);
    }

    /**
     * Converts this stream's accumuated data into a string, translating bytes
     * into characters according to the specified character encoding.
     *
     * @return String translated from the buffer's contents.
     * @param enc a character-encoding name.
     * @throws java.io.IOException may be thrown if this output stream has been
     *      {@link #free() freed}.
     * @throws UnsupportedEncodingException If the named encoding is not
     *      supported.
     */
    public synchronized String toString(String enc)
    throws IOException, UnsupportedEncodingException {

        checkFreed();

        return new String(buf, 0, count, enc);
    }

    /**
     * Closes this object for further writing. <p>
     *
     * Other operations may continue to succeed until after the first invocation
     * of {@link #free() free()}. <p>
     *
     * @throws java.io.IOException if an I/O error occurs (default: never)
     */
    public synchronized void close() throws IOException {
        closed = true;
    }

    /**
     * Retrieves whether this stream is closed. <p>
     * @return <tt>true</tt> if this stream is closed, else <tt>false</tt>
     */
    public synchronized boolean isClosed() {
        return closed;
    }

    /**
     * Closes this object and releases the underlying buffer for
     * garbage collection. <p>
     *
     * @throws java.io.IOException if an I/O error occurs while closing
     *      this stream (default: never).
     */
    public synchronized void free() throws IOException {

        closed = true;
        freed  = true;
        buf    = null;
        count  = 0;
    }

    /**
     * Retrieves whether this stream is freed. <p>
     *
     * @return <tt>true</tt> if this stream is freed; else <tt>false</tt>.
     */
    public synchronized boolean isFreed() {
        return freed;
    }

    /**
     * Tests whether this stream is closed. <p>
     *
     * @throws java.io.IOException if this stream is closed.
     */
    protected synchronized void checkClosed() throws IOException {

        if (closed) {

            throw new IOException("stream is closed.");    // NOI18N
        }
    }

    /**
     * Tests whether this stream is freed. <p>
     *
     * @throws java.io.IOException if this stream is freed.
     */
    protected synchronized void checkFreed() throws IOException {

        if (freed) {
            throw new IOException("stream buffer is freed.");    // NOI18N
        }
    }

    /**
     * Retrieves a copy of <tt>original</tt> with the given
     * <tt>newLength</tt>. <p>
     *
     * @param original the object to copy
     * @param newLength the length of the copy
     * @return copy of <tt>original</tt> with the given <tt>newLength</tt>
     */
    protected byte[] copyOf(byte[] original, int newLength) {

        byte[] copy = new byte[newLength];

        System.arraycopy(original, 0, copy, 0,
                         Math.min(original.length, newLength));

        return copy;
    }
}
