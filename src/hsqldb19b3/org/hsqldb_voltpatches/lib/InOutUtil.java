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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Input / Output utility
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.8.0
 * @since 1.7.2
 */
public class InOutUtil {

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
}
