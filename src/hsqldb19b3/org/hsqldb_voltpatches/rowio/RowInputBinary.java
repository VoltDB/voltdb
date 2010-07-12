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


package org.hsqldb_voltpatches.rowio;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.BlobData;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.ClobData;
import org.hsqldb_voltpatches.types.ClobDataID;
import org.hsqldb_voltpatches.types.IntervalMonthData;
import org.hsqldb_voltpatches.types.IntervalSecondData;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.JavaObjectData;
import org.hsqldb_voltpatches.types.TimeData;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

/**
 *  Provides methods for reading the data for a row from a
 *  byte array. The format of data is that used for storage of cached
 *  tables by v.1.6.x databases, apart from strings.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class RowInputBinary extends RowInputBase
implements org.hsqldb_voltpatches.rowio.RowInputInterface {

    private RowOutputBinary out;

    public RowInputBinary() {
        super();
    }

    public RowInputBinary(byte[] buf) {
        super(buf);
    }

    /**
     * uses the byte[] buffer from out. At each reset, the buffer is set
     * to the current one for out.
     */
    public RowInputBinary(RowOutputBinary out) {

        super(out.getBuffer());

        this.out = out;
    }

    public int readType() throws IOException {
        return readShort();
    }

    public String readString() throws IOException {

        int    length = readInt();
        String s      = StringConverter.readUTF(buf, pos, length);

        s   = ValuePool.getString(s);
        pos += length;

        return s;
    }

    protected boolean checkNull() throws IOException {

        int b = readByte();

        return b == 0 ? true
                      : false;
    }

    protected String readChar(Type type) throws IOException {
        return readString();
    }

    protected Integer readSmallint() throws IOException {
        return ValuePool.getInt(readShort());
    }

    protected Integer readInteger() throws IOException {
        return ValuePool.getInt(readInt());
    }

    protected Long readBigint() throws IOException {
        return ValuePool.getLong(readLong());
    }

    protected Double readReal() throws IOException {
        return ValuePool.getDouble(readLong());
    }

    protected BigDecimal readDecimal(Type type) throws IOException {

        byte[]     bytes  = readByteArray();
        int        scale  = readInt();
        BigInteger bigint = new BigInteger(bytes);

        return ValuePool.getBigDecimal(new BigDecimal(bigint, scale));
    }

    protected Boolean readBoole() throws IOException {
        return readBoolean() ? Boolean.TRUE
                             : Boolean.FALSE;
    }

    protected TimeData readTime(Type type) throws IOException {

        if (type.typeCode == Types.SQL_TIME) {
            return new TimeData(readInt(), readInt(), 0);
        } else {
            return new TimeData(readInt(), readInt(), readInt());
        }
    }

    protected TimestampData readDate(Type type) throws IOException {

        long date = readLong();

        return new TimestampData(date);
    }

    protected TimestampData readTimestamp(Type type) throws IOException {

        if (type.typeCode == Types.SQL_TIMESTAMP) {
            return new TimestampData(readLong(), readInt());
        } else {
            return new TimestampData(readLong(), readInt(), readInt());
        }
    }

    protected IntervalMonthData readYearMonthInterval(Type type)
    throws IOException {

        long months = readLong();

        return new IntervalMonthData(months, (IntervalType) type);
    }

    protected IntervalSecondData readDaySecondInterval(Type type)
    throws IOException {

        long seconds = readLong();
        int  nanos   = readInt();

        return new IntervalSecondData(seconds, nanos, (IntervalType) type);
    }

    protected Object readOther() throws IOException {
        return new JavaObjectData(readByteArray());
    }

    protected BinaryData readBit() throws IOException {

        int    length = readInt();
        byte[] b      = new byte[(length + 7) / 8];

        readFully(b);

        return new BinaryData(b, length);
    }

    protected BinaryData readBinary() throws IOException {
        return new BinaryData(readByteArray(), false);
    }

    protected ClobData readClob() throws IOException {

        long id = super.readLong();

        return new ClobDataID(id);
    }

    protected BlobData readBlob() throws IOException {

        long id = super.readLong();

        return new BlobDataID(id);
    }

    // helper methods
    public byte[] readByteArray() throws IOException {

        byte[] b = new byte[readInt()];

        readFully(b);

        return b;
    }

    public char[] readCharArray() throws IOException {

        char[] c = new char[readInt()];

        if (count - pos < c.length) {
            pos = count;

            throw new EOFException();
        }

        for (int i = 0; i < c.length; i++) {
            int ch1 = buf[pos++] & 0xff;
            int ch2 = buf[pos++] & 0xff;

            c[i] = (char) ((ch1 << 8) + (ch2));
        }

        return c;
    }

    /**
     *  Used to reset the row, ready for Result data to be written into the
     *  byte[] buffer by an external routine.
     *
     */
    public void resetRow(int rowsize) {

        if (out != null) {
            out.reset(rowsize);

            buf = out.getBuffer();
        }

        super.reset();
    }

    /**
     *  Used to reset the row, ready for a new db row to be written into the
     *  byte[] buffer by an external routine.
     *
     */
    public void resetRow(int filepos, int rowsize) throws IOException {

        if (out != null) {
            out.reset(rowsize);

            buf = out.getBuffer();
        }

        super.resetRow(filepos, rowsize);
    }
}
