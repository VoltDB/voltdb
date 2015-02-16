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


package org.hsqldb_voltpatches.rowio;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.BlobData;
import org.hsqldb_voltpatches.types.ClobData;
import org.hsqldb_voltpatches.types.IntervalMonthData;
import org.hsqldb_voltpatches.types.IntervalSecondData;
import org.hsqldb_voltpatches.types.JavaObjectData;
import org.hsqldb_voltpatches.types.TimeData;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @since 2.0.0
 * @version 1.7.2
 */
public class RowOutputTextLog extends RowOutputBase {

    static byte[] BYTES_NULL;
    static byte[] BYTES_TRUE;
    static byte[] BYTES_FALSE;
    static byte[] BYTES_AND;
    static byte[] BYTES_IS;
    static byte[] BYTES_ARRAY;

    static {
        try {
            BYTES_NULL  = Tokens.T_NULL.getBytes("ISO-8859-1");
            BYTES_TRUE  = Tokens.T_TRUE.getBytes("ISO-8859-1");
            BYTES_FALSE = Tokens.T_FALSE.getBytes("ISO-8859-1");
            BYTES_AND   = " AND ".getBytes("ISO-8859-1");
            BYTES_IS    = " IS ".getBytes("ISO-8859-1");
            BYTES_ARRAY = " ARRAY[".getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            Error.runtimeError(ErrorCode.U_S0500, "RowOutputTextLog");
        }
    }

    public static final int MODE_DELETE = 1;
    public static final int MODE_INSERT = 0;
    private boolean         isWritten;
    private int             logMode;
    private boolean         noSeparators;

    public void setMode(int mode) {
        logMode = mode;
    }

    protected void writeFieldPrefix() {

        if (!noSeparators) {
            if (logMode == MODE_DELETE && isWritten) {
                write(BYTES_AND);
            }
        }
    }

    protected void writeChar(String s, Type t) {

        write('\'');
        StringConverter.stringToUnicodeBytes(this, s, true);
        write('\'');
    }

    protected void writeReal(Double o) {
        writeBytes(Type.SQL_DOUBLE.convertToSQLString(o));
    }

    protected void writeSmallint(Number o) {
        this.writeBytes(o.toString());
    }

    public void writeEnd() {}

    protected void writeBit(BinaryData o) {

        ensureRoom((int) (o.length(null) * 8 + 2));
        write('\'');

        String s = StringConverter.byteArrayToBitString(o.getBytes(),
            (int) o.bitLength(null));

        writeBytes(s);
        write('\'');
    }

    protected void writeBinary(BinaryData o) {

        ensureRoom((int) (o.length(null) * 2 + 2));
        write('\'');
        StringConverter.writeHexBytes(getBuffer(), count, o.getBytes());

        count += (o.length(null) * 2);

        write('\'');
    }

    protected void writeClob(ClobData o, Type type) {
        writeBytes(Long.toString(o.getId()));
    }

    protected void writeBlob(BlobData o, Type type) {
        writeBytes(Long.toString(o.getId()));
    }

    protected void writeArray(Object[] o, Type type) {

        type = type.collectionBaseType();

        noSeparators = true;
        write(BYTES_ARRAY);

        for (int i = 0; i < o.length; i++) {
            if (i > 0) {
                write(',');
            }

            writeData(type, o[i]);
        }

        write(']');
        noSeparators = false;
    }

    public void writeType(int type) {}

    public void writeSize(int size) {}

    public int getSize(Row row) {
        return 0;
    }

    public int getStorageSize(int size) {
        return size;
    }

    protected void writeInteger(Number o) {
        this.writeBytes(o.toString());
    }

    protected void writeBigint(Number o) {
        this.writeBytes(o.toString());
    }

//fredt@users - patch 1108647 by nkowalcz@users (NataliaK) fix for IS NULL
    protected void writeNull(Type type) {

        if (!noSeparators) {
            if (logMode == MODE_DELETE) {
                write(BYTES_IS);
            } else if (isWritten) {
                write(',');
            }

            isWritten = true;
        }

        write(BYTES_NULL);
    }

    protected void writeOther(JavaObjectData o) {

        ensureRoom(o.getBytesLength() * 2 + 2);
        write('\'');
        StringConverter.writeHexBytes(getBuffer(), count, o.getBytes());

        count += o.getBytesLength() * 2;

        write('\'');
    }

    public void writeString(String value) {
        StringConverter.stringToUnicodeBytes(this, value, false);
    }

    protected void writeBoolean(Boolean o) {
        write(o.booleanValue() ? BYTES_TRUE
                               : BYTES_FALSE);
    }

    protected void writeDecimal(BigDecimal o, Type type) {
        writeBytes(type.convertToSQLString(o));
    }

    protected void writeFieldType(Type type) {

        if (!noSeparators) {
            if (logMode == MODE_DELETE) {
                write('=');
            } else if (isWritten) {
                write(',');
            }

            isWritten = true;
        }
    }

    public void writeLong(long value) {
        this.writeBytes(Long.toString(value));
    }

    public void writeIntData(int i, int position) {}

    protected void writeTime(TimeData o, Type type) {

        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }

    protected void writeDate(TimestampData o, Type type) {

        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }

    protected void writeTimestamp(TimestampData o, Type type) {

        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }

    protected void writeYearMonthInterval(IntervalMonthData o, Type type) {

        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }

    protected void writeDaySecondInterval(IntervalSecondData o, Type type) {

        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }

    public void writeShort(int i) {
        writeBytes(Integer.toString(i));
    }

    public void writeInt(int i) {
        writeBytes(Integer.toString(i));
    }

    public void reset() {

        super.reset();

        isWritten = false;
    }

    public RowOutputInterface duplicate() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowOutputText");
    }
}
