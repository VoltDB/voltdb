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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.lib.java.JavaSystem;
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
 * Provides methods for writing the data for a row to a
 * byte array. The new format of data consists of mainly binary values
 * and is not compatible with v.1.6.x databases.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class RowOutputBinary extends RowOutputBase {

    private static final int INT_STORE_SIZE = 4;
    int                      storageSize;
    final int                scale;

    public RowOutputBinary() {

        super();

        scale = 1;
    }

    public RowOutputBinary(int initialSize, int scale) {

        super(initialSize);

        this.scale = scale;
    }

    /**
     * Constructor used for network transmission of result sets
     *
     * @param buffer byte[]
     */
    public RowOutputBinary(byte[] buffer) {

        super(buffer);

        scale = 1;
    }

// fredt@users - comment - methods for writing column type, name and data size
    public void writeIntData(int i, int position) {

        int temp = count;

        count = position;

        writeInt(i);

        if (count < temp) {
            count = temp;
        }
    }

    public void writeEnd() {

        // fredt - this value is used in 1.7.0 when reading back, for a
        // 'data integrity' check
        // has been removed in 1.7.2 as compatibility is no longer necessary
        // writeInt(pos);
        for (; count < storageSize; ) {
            this.write(0);
        }
    }

    public void writeSize(int size) {

        storageSize = size;

        writeInt(size);
    }

    public void writeType(int type) {
        writeShort(type);
    }

    public void writeString(String s) {

        int temp = count;

        writeInt(0);

        if (s != null && s.length() != 0) {
            StringConverter.stringToUTFBytes(s, this);
            writeIntData(count - temp - 4, temp);
        }
    }

    /**
     *  Calculate the size of byte array required to store a row.
     *
     * @param  row - a database row
     * @return  size of byte array
     * @exception  HsqlException When data is inconsistent
     */
    public int getSize(Row row) {

        Object[] data  = row.getData();
        Type[]   types = row.getTable().getColumnTypes();
        int      cols  = row.getTable().getDataColumnCount();

        return INT_STORE_SIZE + getSize(data, cols, types);
    }

    public int getStorageSize(int size) {
        return scale == 1 ? size
                          : ((size + scale - 1) / scale) * scale;
    }

    protected void writeFieldType(Type type) {
        write(1);
    }

    protected void writeNull(Type type) {
        write(0);
    }

    protected void writeChar(String s, Type t) {
        writeString(s);
    }

    protected void writeSmallint(Number o) {
        writeShort(o.intValue());
    }

    protected void writeInteger(Number o) {
        writeInt(o.intValue());
    }

    protected void writeBigint(Number o) {
        writeLong(o.longValue());
    }

    protected void writeReal(Double o) {
        writeLong(Double.doubleToLongBits((o.doubleValue())));
    }

    protected void writeDecimal(BigDecimal o, Type type) {

        int        scale   = o.scale();
        BigInteger bigint  = JavaSystem.unscaledValue(o);
        byte[]     bytearr = bigint.toByteArray();

        writeByteArray(bytearr);
        writeInt(scale);
    }

    protected void writeBoolean(Boolean o) {
        write(o.booleanValue() ? 1
                               : 0);
    }

    protected void writeDate(TimestampData o, Type type) {
        writeLong(o.getSeconds());
    }

    protected void writeTime(TimeData o, Type type) {

        writeInt(o.getSeconds());
        writeInt(o.getNanos());

        if (type.typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
            writeInt(o.getZone());
        }
    }

    protected void writeTimestamp(TimestampData o, Type type) {

        writeLong(o.getSeconds());
        writeInt(o.getNanos());

        if (type.typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
            writeInt(o.getZone());
        }
    }

    protected void writeYearMonthInterval(IntervalMonthData o, Type type) {
        writeLong(o.units);
    }

    protected void writeDaySecondInterval(IntervalSecondData o, Type type) {
        writeLong(o.units);
        writeInt(o.nanos);
    }

    protected void writeOther(JavaObjectData o) {
        writeByteArray(o.getBytes());
    }

    protected void writeBit(BinaryData o) {
        writeInt((int) o.bitLength(null));
        write(o.getBytes(), 0, o.getBytes().length);
    }

    protected void writeBinary(BinaryData o) {
        writeByteArray(o.getBytes());
    }

    protected void writeClob(ClobData o, Type type) {
        writeLong(o.getId());
    }

    protected void writeBlob(BlobData o, Type type) {
        writeLong(o.getId());
    }

// fredt@users - comment - helper and conversion methods
    public void writeByteArray(byte[] b) {
        writeInt(b.length);
        write(b, 0, b.length);
    }

    // fredt@users - comment - helper and conversion methods
    public void writeCharArray(char[] c) {
        writeInt(c.length);
        write(c, 0, c.length);
    }

    /**
     * Calculate the size of byte array required to store a row.
     *
     * @param data - the row data
     * @param l - number of data[] elements to include in calculation
     * @param types - array of java.sql.Types values
     * @return size of byte array
     */
    private static int getSize(Object[] data, int l, Type[] types) {

        int s = 0;

        for (int i = 0; i < l; i++) {
            Object o = data[i];

            s += 1;    // type or null

            if (o != null) {
                switch (types[i].typeCode) {

                    case Types.SQL_ALL_TYPES :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "RowOutputBinary");
                    case Types.SQL_CHAR :
                    case Types.SQL_VARCHAR :
                    case Types.VARCHAR_IGNORECASE :
                        s += 4;
                        s += StringConverter.getUTFSize((String) o);
                        break;

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                        s += 2;
                        break;

                    case Types.SQL_INTEGER :
                        s += 4;
                        break;

                    case Types.SQL_BIGINT :
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        s += 8;
                        break;

                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        s += 8;

                        BigDecimal bigdecimal = (BigDecimal) o;
                        BigInteger bigint =
                            JavaSystem.unscaledValue(bigdecimal);

                        s += bigint.toByteArray().length;
                        break;

                    case Types.SQL_BOOLEAN :
                        s += 1;
                        break;

                    case Types.SQL_DATE :
                        s += 8;
                        break;

                    case Types.SQL_TIME :
                        s += 8;
                        break;

                    case Types.SQL_TIME_WITH_TIME_ZONE :
                        s += 12;
                        break;

                    case Types.SQL_TIMESTAMP :
                        s += 12;
                        break;

                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        s += 16;
                        break;

                    case Types.SQL_INTERVAL_YEAR :
                    case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                    case Types.SQL_INTERVAL_MONTH :
                        s += 8;
                        break;

                    case Types.SQL_INTERVAL_DAY :
                    case Types.SQL_INTERVAL_DAY_TO_HOUR :
                    case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                    case Types.SQL_INTERVAL_DAY_TO_SECOND :
                    case Types.SQL_INTERVAL_HOUR :
                    case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                    case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                    case Types.SQL_INTERVAL_MINUTE :
                    case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                    case Types.SQL_INTERVAL_SECOND :
                        s += 12;
                        break;

                    case Types.SQL_BINARY :
                    case Types.SQL_VARBINARY :
                        s += 4;
                        s += ((BinaryData) o).length(null);
                        break;

                    case Types.SQL_BIT :
                    case Types.SQL_BIT_VARYING :
                        s += 4;
                        s += ((BinaryData) o).length(null);
                        break;

                    case Types.SQL_CLOB :
                    case Types.SQL_BLOB :
                        s += 8;
                        break;

                    case Types.OTHER :
                        JavaObjectData jo = (JavaObjectData) o;

                        s += 4;
                        s += jo.getBytesLength();
                        break;

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "RowOutputBinary");
                }
            }
        }

        return s;
    }

    /**
     * @param  extra amount of extra space
     */
    public void ensureRoom(int extra) {
        super.ensureRoom(extra);
    }

    public void reset() {

        super.reset();

        storageSize = 0;
    }

    public void reset(int newSize) {

        super.reset(newSize);

        storageSize = 0;
    }

    public void setBuffer(byte[] buffer) {

        this.buffer = buffer;

        reset();
    }
}
