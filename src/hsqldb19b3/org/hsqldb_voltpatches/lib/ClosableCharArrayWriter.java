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

import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * @todo - finer-grained synchronization to reduce average
 * potential monitor contention
 */

/**
 * Provides Closable semantics ordinarily missing in a
 * {@link java.io.CharArrayWriter}. <p>
 *
 * Accumulates output in a character array that automatically grows as needed.<p>
 *
 * Data is retrieved using <tt>toCharArray()</tt>, <tt>toCharArrayReader()</tt>
 * and <tt>toString()</tt>. <p>
 *
 * {@link #close() Closing} a <tt>ClosableCharArrayWriter</tt> prevents
 * further write operations, but all other operations will succeed until after
 * the first invocation of {@link #free() free()}.<p>
 *
 * Freeing a <tt>ClosableCharArrayWriter</tt> closes the writer and
 * releases its internal buffer, preventing successful invocation of all
 * operations, with the exception of <tt>size()<tt>, <tt>close()</tt>,
 * <tt>isClosed()</tt>, <tt>free()</tt> and <tt>isFreed()</tt>. <p>
 *
 * This class is especially useful when an accumulating writer must be
 * handed off to an extenal client under contract that the writer should
 * exhibit true Closable behaviour, both in response to internally tracked
 * events and to client invocation of the <tt>Writer.close()</tt> method.
 *
 * @author boucherb@users
 * @version 1.8.x
 * @since 1.8.x
 */
public class ClosableCharArrayWriter extends Writer {

    /**
     * Data buffer.
     */
    protected char[] buf;

    /**
     * # of valid characters in buffer.
     */
    protected int count;

    /**
     * Whether this writer is closed.
     */
    protected boolean closed;

    /**
     * Whether this writer is freed.
     */
    protected boolean freed;

    /**
     * Creates a new writer. <p>
     *
     * The buffer capacity is initially 32 characters, although its size
     * automatically increases when necessary.
     */
    public ClosableCharArrayWriter() {
        this(32);
    }

    /**
     * Creates a new writer with a buffer capacity of the specified
     * <tt>size</tt>, in characters.
     *
     * @param size the initial size.
     * @exception IllegalArgumentException if <tt>size</tt> is negative.
     */
    public ClosableCharArrayWriter(int size) throws IllegalArgumentException {

        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                                               + size);    // NOI18N
        }

        buf = new char[size];
    }

    /**
     * Writes the specified single character.
     *
     * @param c the single character to be written.
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #close() closed}.
     */
    public synchronized void write(int c) throws IOException {

        checkClosed();

        int newcount = count + 1;

        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }

        buf[count] = (char) c;
        count      = newcount;
    }

    /**
     * Writes the designated portion of the designated character array <p>.
     *
     * @param c the source character sequence.
     * @param off the start offset in the source character sequence.
     * @param len the number of characters to write.
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #close() closed}.
     */
    public synchronized void write(char c[], int off,
                                   int len) throws IOException {

        checkClosed();

        if ((off < 0) || (off > c.length) || (len < 0)
                || ((off + len) > c.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int newcount = count + len;

        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }

        System.arraycopy(c, off, buf, count, len);

        count = newcount;
    }

    /**
     * Efficiently writes the designated portion of the designated string. <p>
     *
     * The operation occurs as if by calling
     * <tt>str.getChars(off, off + len, buf, count)</tt>. <p>
     *
     * @param str the string from which to write
     * @param off the start offset in the string.
     * @param len the number of characters to write.
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #close() closed}.
     */
    public synchronized void write(String str, int off,
                                   int len) throws IOException {

        checkClosed();

        int strlen = str.length();

        if ((off < 0) || (off > strlen) || (len < 0) || ((off + len) > strlen)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int newcount = count + len;

        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }

        str.getChars(off, off + len, buf, count);

        count = newcount;
    }

    /**
     * By default, does nothing. <p>
     *
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #close() closed}.
     */
    public void flush() throws IOException {
        checkClosed();
    }

    /**
     * Writes the complete contents of this writer's buffered data to the
     * specified writer. <p>
     *
     * The operation occurs as if by calling <tt>out.write(buf, 0, count)</tt>.
     *
     * @param out the writer to which to write the data.
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #free() freed}.
     */
    public synchronized void writeTo(Writer out) throws IOException {

        checkFreed();

        if (count > 0) {
            out.write(buf, 0, count);
        }
    }

    /**
     * Returns the current capacity of this writer's data buffer.
     *
     * @return  the current capacity (the length of the internal
     *          data array)
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #free() freed}.
     */
    public synchronized int capacity() throws IOException {

        checkFreed();

        return buf.length;
    }

    /**
     * Resets the <tt>count</tt> field of this writer to zero, so that all
     * currently accumulated output is effectively discarded. Further write
     * operations will reuse the allocated buffer space.
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
     * Attempts to reduce this writer's buffer capacity to its current size. <p>
     *
     * If the buffer is larger than necessary to hold its current sequence of
     * characters, then it may be resized to become more space efficient.
     * Calling this method may, but is not required to, affect the value
     * returned by a subsequent call to the {@link #capacity()} method.
     */
    public synchronized void trimToSize() throws IOException {

        checkFreed();

        if (buf.length > count) {
            buf = copyOf(buf, count);
        }
    }

    /**
     * Creates a newly allocated character array. Its size is the current
     * size of this writer and the valid contents of the buffer
     * have been copied into it.
     *
     * @return the current contents of this writer, as a character array.
     * @see #size()
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #free() freed}.
     */
    public synchronized char[] toCharArray() throws IOException {

        checkFreed();

        return copyOf(buf, count);
    }

    /**
     * Returns the current size of this writer's accumulated character data.
     *
     * @return the value of the <tt>count</tt> field, which is the number
     *      of valid characters accumulated in this writer.
     * @see #count
     * @throws java.io.IOException never
     */
    public synchronized int size() throws IOException {
        return count;
    }

    /**
     * Sets the size of this writer's accumulated character data. <p>
     *
     * @param   newSize the new size of this writer's accumulated data
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
     * Performs an effecient (zero-copy) conversion of the character data
     * accumulated in this writer to a reader. <p>
     *
     * To ensure the integrity of the resulting reader, {@link #free()
     * free} is invoked upon this writer as a side-effect.
     *
     * @return a reader representing this writer's accumulated
     *      character data
     * @throws java.io.IOException if an I/O error occurs.
     *      In particular, an <tt>IOException</tt> may be thrown
     *      if this writer has been {@link #free() freed}.
     */
    public synchronized CharArrayReader toCharArrayReader()
    throws IOException {

        checkFreed();

        CharArrayReader reader = new CharArrayReader(buf, 0, count);

        //System.out.println("toCharArrayReader::buf.length: " + buf.length);
        free();

        return reader;
    }

    /**
     * Converts this writer's accumulated data into a string.
     *
     * @return String constructed from this writer's accumulated data
     * @throws RuntimeException may be thrown if this writer has been
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
     * @return <tt>true</tt> if this writer is closed, else <tt>false</tt>
     */
    public synchronized boolean isClosed() {
        return closed;
    }

    /**
     * Closes this object and releases the underlying buffer for
     * garbage collection. <p>
     *
     * @throws java.io.IOException if an I/O error occurs while closing
     *      this writer (default: never).
     */
    public synchronized void free() throws IOException {

        closed = true;
        freed  = true;
        buf    = null;
        count  = 0;
    }

    /**
     * @return <tt>true</tt> if this writer is freed; else <tt>false</tt>.
     */
    public synchronized boolean isFreed() {
        return freed;
    }

    /**
     * @throws java.io.IOException if this writer is closed.
     */
    protected synchronized void checkClosed() throws IOException {

        if (closed) {
            throw new IOException("writer is closed.");    // NOI18N
        }
    }

    /**
     * @throws java.io.IOException if this writer is freed.
     */
    protected synchronized void checkFreed() throws IOException {

        if (freed) {
            throw new IOException("write buffer is freed.");    // NOI18N
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
    protected char[] copyOf(char[] original, int newLength) {

        char[] copy = new char[newLength];

        System.arraycopy(original, 0, copy, 0,
                         Math.min(original.length, newLength));

        return copy;
    }
}
