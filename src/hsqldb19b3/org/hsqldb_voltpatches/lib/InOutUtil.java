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


package org.hsqldb_voltpatches.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;

/**
 * Input / Output utility
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.1
 * @revised 1.8.0
 * @since 1.7.2
 */
public final class InOutUtil {

    /**
     * Implementation only supports unix line-end format and is suitable for
     * processing HTTP and other network protocol communications. Reads and writes
     * a line of data. Returns the number of bytes read/written.
     */
    public static int readLine(InputStream in,
                               OutputStream out) throws IOException {

        int count = 0;

        for (;;) {
            int b = in.read();

            if (b == -1) {
                break;
            }

            count++;

            out.write(b);

            if (b == '\n') {
                break;
            }
        }

        return count;
    }

    /**
     * Retrieves the serialized form of the specified <CODE>Object</CODE>
     * as an array of bytes.
     *
     * @param s the Object to serialize
     * @return  a static byte array representing the passed Object
     */
    public static byte[] serialize(Serializable s) throws IOException {

        HsqlByteArrayOutputStream bo = new HsqlByteArrayOutputStream();
        ObjectOutputStream        os = new ObjectOutputStream(bo);

        os.writeObject(s);

        return bo.toByteArray();
    }

    /**
     * Deserializes the specified byte array to an
     * <CODE>Object</CODE> instance.
     *
     * @return the Object resulting from deserializing the specified array of bytes
     * @param ba the byte array to deserialize to an Object
     */
    public static Serializable deserialize(byte[] ba)
    throws IOException, ClassNotFoundException {

        HsqlByteArrayInputStream bi = new HsqlByteArrayInputStream(ba);
        ObjectInputStream        is = new ObjectInputStream(bi);

        return (Serializable) is.readObject();
    }


    public static final int DEFAULT_COPY_BUFFER_SIZE = 8192;
    public static final long DEFAULT_COPY_AMOUNT = Long.MAX_VALUE;

    /**
     * @see #copy(java.io.InputStream, java.io.OutputStream, long, int)
     */
    public static long copy(
            final InputStream inputStream,
            final OutputStream outputStream) throws IOException {
        return copy(inputStream, outputStream,
                DEFAULT_COPY_AMOUNT,
                DEFAULT_COPY_BUFFER_SIZE);
    }

    /**
     * @see #copy(java.io.InputStream, java.io.OutputStream, long, int)
     */
    public static long copy(
            final InputStream inputStream,
            final OutputStream outputStream,
            final long amount) throws IOException {
        return copy(inputStream, outputStream, amount,
                DEFAULT_COPY_BUFFER_SIZE);
    }
    
    /**
     * the specified <tt>amount</tt> from the given input stream to the
     * given output stream, using a buffer of the given size.
     *
     * @param inputStream from which to source bytes
     * @param outputStream to which to sink bytes
     * @param amount max # of bytes to transfer.
     * @param bufferSize to use internally
     * @return the number of bytes <i>actually</i> transfered.
     * @throws IOException if any, thrown by either of the given stream objects
     */
    public static long copy(
            final InputStream inputStream,
            final OutputStream outputStream,
            final long amount,
            final int bufferSize) throws IOException {
        //
        int maxBytesToRead = (int) Math.min((long) bufferSize, amount);
        //
        final byte[] buffer = new byte[maxBytesToRead];
        //
        long bytesCopied = 0;
        int bytesRead;        

        while ((bytesCopied < amount) && -1 != (bytesRead =
                inputStream.read(buffer, 0, maxBytesToRead))) {
            //
            outputStream.write(buffer, 0, bytesRead);

            if (bytesRead > Long.MAX_VALUE - bytesCopied) {
                // edge case...
                // extremely unlikely but included for 'correctness'
                bytesCopied = Long.MAX_VALUE;
            } else {
                bytesCopied += bytesRead;
            }

            if (bytesCopied >= amount) {
                return bytesCopied;
            }

            maxBytesToRead = (int) Math.min((long) bufferSize,
                    amount - bytesCopied);
        }

        return bytesCopied;
    }

    /**
     * @see #copy(java.io.Reader, java.io.Writer, long, int)
     */
    public static long copy(
            final Reader reader,
            final Writer writer) throws IOException {
        return copy(reader, writer,
                DEFAULT_COPY_AMOUNT,
                DEFAULT_COPY_BUFFER_SIZE);
    }

    /**
     * @see #copy(java.io.Reader, java.io.Writer, long, int)
     */
    public static long copy(
            final Reader reader,
            final Writer writer,
            final long amount) throws IOException {
        return copy(reader, writer, amount,
                DEFAULT_COPY_BUFFER_SIZE);
    }

    /**
     * the specified <tt>amount</tt> from the given input stream to the
     * given output stream, using a buffer of the given size.
     *
     * @param reader from which to source characters
     * @param writer to which to sink characters
     * @param amount max # of characters to transfer.
     * @param bufferSize to use internally
     * @return the number of characters <i>actually</i> transfered.
     * @throws IOException if any, thrown by either of the given stream objects
     */
    public static long copy(
            final Reader reader,
            final Writer writer,
            final long amount,
            final int bufferSize) throws IOException {
        //
        int maxCharsToRead = (int) Math.min((long) bufferSize, amount);
        //
        final char[] buffer = new char[maxCharsToRead];
        //
        long charsCopied = 0;
        int charsRead;        

        while ((charsCopied < amount) && -1 != (charsRead =
                reader.read(buffer, 0, maxCharsToRead))) {
            //
            writer.write(buffer, 0, charsRead);

            if (charsRead > Long.MAX_VALUE - charsCopied) {
                // edge case...
                // extremely unlikely but included for 'correctness'
                charsCopied = Long.MAX_VALUE;
            } else {
                charsCopied += charsRead;
            }

            if (charsCopied >= amount) {
                return charsCopied;
            }

            maxCharsToRead = (int) Math.min((long) bufferSize,
                    amount - charsCopied);
        }

        return charsCopied;
    }
}
