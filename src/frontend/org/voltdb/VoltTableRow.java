/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.VoltTypeUtil;

/**
 * <p>Represents the interface to a row in a VoltTable result set.</p>
 *
 * <h3>Accessing Row Fields</h3>
 *
 * <p>Row fields are acessed via methods of the form
 * <code>get</code><i>&lt;Type&gt;</i><code>(</code><i>col_index</i><code>|</code><i>col_name</i><code>)</code>.
 * Note: it is more performant to access rows by column index than by column name.</p>
 *
 * <h3>Advancing and Iterating through a Table</h3>
 *
 * <p>VoltTableRow represents both a row in a table and an iterator for all the
 * rows in the table. For a given table, each VoltTableRow instance has a position
 * value which represents the index represented in the table. To increment the
 * position, call {@link #advanceRow()}. To reset the position, call
 * {@link #resetRowPosition()}, which moves the position before the first row.
 * Note that before you can access fields after a call to resetRowPosition,
 * advanceRow must be called to move to the first row.</p>
 *
 * <h3>Example</h3>
 *
 * <code>
 * VoltTableRow row = table.fetchRow(5);<br>
 * System.out.println(row.getString("foo");<br>
 * row.resetRowPosition();<br>
 * while (row.advanceRow()) {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;System.out.println(row.getLong(7));<br>
 * }
 * </code>
 */
public abstract class VoltTableRow {

    /**
     * Size in bytes of the maximum length for a VoltDB tuple.
     * This is inclusive of the 4-byte row length prefix.
     *
     * 2 megs to allow a max size string/varbinary + length prefix + some other stuff
     */
    public static final int MAX_TUPLE_LENGTH = 2097152;
    /**
     * String representation of <code>MAX_TUPLE_LENGTH</code>.
     */
    public static final String MAX_TUPLE_LENGTH_STR = String.valueOf(MAX_TUPLE_LENGTH / 1024) + "k";

    static final int ROW_HEADER_SIZE = Integer.SIZE/8;
    static final int ROW_COUNT_SIZE = Integer.SIZE/8;
    static final int STRING_LEN_SIZE = Integer.SIZE/8;
    static final int VARBINARY_LEN_SIZE = Integer.SIZE/8;
    static final int INVALID_ROW_INDEX = -1;

    /** Stores the row data (and possibly much more) */
    ByteBuffer m_buffer;
    /** Was the last value retrieved null? */
    boolean m_wasNull = false;
    /** Where in the buffer is the start of the active row's data */
    int m_position = -1;
    /** Offsets of each column in the buffer */
    int[] m_offsets;
    /** Have the offsets been calculated */
    boolean m_hasCalculatedOffsets = false;

    int m_activeRowIndex = INVALID_ROW_INDEX;

    VoltTableRow() {}

    /**
     * Return the {@link VoltType type} of the column with the specified index.
     * @param columnIndex Index of the column
     * @return {@link VoltType VoltType} of the column
     */
    abstract VoltType getColumnType(int columnIndex);

    /**
     * Return the index of the column with the specified index.
     * @param columnName Name of the column
     * @return Index of the column
     */
    abstract int getColumnIndex(String columnName);

    /**
     * Returns the number of columns in the table schema
     * @return Number of columns in the table schema
     */
    abstract int getColumnCount();

    /**
     * Returns the number of rows.
     * @return Number of rows in the table
     */
    abstract int getRowCount();

    abstract int getRowStart();

    /**
     * Clone a row. The new instance returned will have an independent
     * position from the original instance.
     * @return A deep copy of the current <tt>VoltTableRow</tt> instance.
     */
    public abstract VoltTableRow cloneRow();

    private final void ensureCalculatedOffsets() {
        if (m_hasCalculatedOffsets == true)
            return;

        m_offsets[0] = m_position;
        for (int i = 1; i < getColumnCount(); i++) {
            final VoltType type = getColumnType(i - 1);
            // handle variable length types specially
            if ((type == VoltType.STRING) || (type == VoltType.VARBINARY)) {
                final int len = m_buffer.getInt(m_offsets[i - 1]);
                if (len == VoltTable.NULL_STRING_INDICATOR)
                    m_offsets[i] = m_offsets[i - 1] + STRING_LEN_SIZE;
                else if (len < 0)
                    throw new RuntimeException("Invalid object length for column: " + i);
                else
                    m_offsets[i] = m_offsets[i - 1] + len + STRING_LEN_SIZE;
            }
            else {
                m_offsets[i] = m_offsets[i - 1] + type.getLengthInBytesForFixedTypes();
            }
        }
        m_hasCalculatedOffsets = true;
    }

    final int getOffset(int index) {
        ensureCalculatedOffsets();
        assert(index >= 0);
        assert(index < m_offsets.length);
        return m_offsets[index];
    }

    /**
     * Sets the active position indicator so that the next call
     * to {@link #advanceRow()} will make the first record active.
     * This never needs to be called if the table is only going to
     * be scanned once. After this call {@link #getActiveRowIndex()}
     * will return -1 until {@link #advanceRow()} is called.
     */
    public void resetRowPosition() {
        m_activeRowIndex = INVALID_ROW_INDEX;
    }

    /**
     * Get the position in the table of this row instance, starting
     * at zero for the first row.
     * @return The index of the active row or -1 if none.
     */
    public int getActiveRowIndex() {
        return m_activeRowIndex;
    }

    /**
     * Makes the next row active so calls to getXXX() will return
     * values from the current record. At initialization time, the
     * active row index is -1, which is invalid. If advanced past the
     * end of the resultset, {@link #resetRowPosition()} must be
     * called to re-iterate through the rows.
     * @return True if a valid row became active. False otherwise.
     */
    public boolean advanceRow() {
        return advanceToRow(m_activeRowIndex + 1);
    }

    /**
     * Advance to a specific row so calls to getXXX() will return values from
     * the current record. At initialization time, the active row index is -1,
     * which is invalid. If advanced past the end of the resultset,
     * {@link #resetRowPosition()} must be called to re-iterate through the
     * rows.
     *
     * @param rowIndex The row to jump to.
     * @return True if a valid row became active. False otherwise.
     */
    public boolean advanceToRow(int rowIndex) {
        int rows_to_move = rowIndex - m_activeRowIndex;
        m_activeRowIndex = rowIndex;

        if (m_activeRowIndex >= getRowCount())
            return false;
        if (rows_to_move < 0) // this is "advance" to row, don't move backwards
            return false;

        m_hasCalculatedOffsets = false;
        if (m_offsets == null)
            m_offsets = new int[getColumnCount()];

        if (m_activeRowIndex == 0)
            m_position = getRowStart() + ROW_COUNT_SIZE + ROW_HEADER_SIZE;
        else {
            // Move n rows - this code assumes rows can be variable size, so we
            // have to fetch the size of each row in order to advance to the
            // next row
            if (rows_to_move > 0 && m_position < 0)
                throw new RuntimeException(
                        "VoltTableRow is in an invalid state. Consider calling advanceRow().");

            for (int i = 0; i < rows_to_move; i++) {
                int rowlength = m_buffer.getInt(m_position - ROW_HEADER_SIZE);
                if (rowlength <= 0) {
                    throw new RuntimeException("Invalid row length.");
                }

                m_position += (rowlength + ROW_HEADER_SIZE);
            }

            if (m_position >= m_buffer.limit()) {
                throw new RuntimeException("Row length exceeds table boundary.");
            }
        }

        return true;
    }

    /**
     * Retrieve a value from the row by specifying the column index and the {@link VoltType type}.
     * This method is slower then linking directly against the type specific getter. Prefer the
     * type specific getter methods where viable. Looking at the return value is not a reliable
     * way to check if the value is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnIndex Index of the column
     * @param type {@link VoltType} of the value
     * @return The value or <tt>null</tt> if the value is not set.
     * @see #wasNull()
     */
    public final Object get(int columnIndex, VoltType type) {
        Object ret = null;
        switch (type) {
        case TINYINT:
            ret = new Byte((byte)getLong(columnIndex));
            break;
        case SMALLINT:
            ret = new Short((short)getLong(columnIndex));
            break;
        case INTEGER:
            ret = new Integer((int)getLong(columnIndex));
            break;
        case BIGINT:
            ret = getLong(columnIndex);
            break;
        case FLOAT:
            ret = getDouble(columnIndex);
            break;
        case STRING:
            ret = getString(columnIndex);
            break;
        case VARBINARY:
            ret = getVarbinary(columnIndex);
            break;
        case TIMESTAMP:
            ret = getTimestampAsTimestamp(columnIndex);
            break;
        case DECIMAL:
            ret = getDecimalAsBigDecimal(columnIndex);
            break;
        default:
            throw new IllegalArgumentException("Invalid type '" + type + "'");
        }
        return ret;
    }

    /**
     * Retrieve a value from the row by specifying the column name and the
     * {@link VoltType type}. This method is slower then linking directly
     * against the type specific getter. Prefer the type specific getter methods
     * where viable. Looking at the return value is not a reliable way to check
     * if the value is <tt>null</tt>. Use {@link #wasNull()} instead.
     *
     * @param columnName
     *            Name of the column
     * @param type
     *            {@link VoltType} of the value
     * @return The value or <tt>null</tt> if the value is not set.
     * @see #wasNull()
     */
    public final Object get(String columnName, VoltType type) {
        final int colIndex = getColumnIndex(columnName);
        return get(colIndex, type);
    }

    /**
     * Retrieve the <tt>long</tt> value stored in the column specified by index.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     *
     * @param columnIndex
     *            Index of the column
     * @return <tt>long</tt> value stored in the specified column
     * @see #wasNull()
     */
    public final long getLong(int columnIndex) {
        if ((columnIndex >= getColumnCount()) || (columnIndex < 0)) {
            throw new IndexOutOfBoundsException("Column index " + columnIndex + " is type greater than the number of columns");
        }
        final VoltType type = getColumnType(columnIndex);
        if (m_activeRowIndex == INVALID_ROW_INDEX)
            throw new RuntimeException("VoltTableRow.advanceRow() must be called to advance to the first row before any access.");

        switch (type) {
        case TINYINT:
            final byte value1 = m_buffer.get(getOffset(columnIndex));
            m_wasNull = (value1 == VoltType.NULL_TINYINT);
            return value1;
        case SMALLINT:
            final short value2 = m_buffer.getShort(getOffset(columnIndex));
            m_wasNull = (value2 == VoltType.NULL_SMALLINT);
            return value2;
        case INTEGER:
            final int value3 = m_buffer.getInt(getOffset(columnIndex));
            m_wasNull = (value3 == VoltType.NULL_INTEGER);
            return value3;
        case BIGINT:
            final long value4 = m_buffer.getLong(getOffset(columnIndex));
            m_wasNull = (value4 == VoltType.NULL_BIGINT);
            return value4;
        default:
            throw new IllegalArgumentException("getLong() called on non-integral column.");
        }
    }

    /**
     * Retrieve the <tt>long</tt> value stored in the column
     * specified by name. Avoid retrieving via this method as it is slower than specifying the
     * column by index. Use {@link #getLong(int)} instead.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnName Name of the column
     * @return <tt>long</tt> value stored in the specified column
     * @see #wasNull()
     * @see #getLong(int)
     */
    public final long getLong(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getLong(colIndex);
    }

    /**
     * Returns whether last retrieved value was <tt>null</tt>.
     * Some special values that are NOT Java's NULL represents <tt>null</tt> the SQL notion of <tt>null</tt>
     * in our system.
     * @return <tt>true</tt> if the value was <tt>null</tt>, false otherwise.
     */
    public final boolean wasNull() {
        return m_wasNull;
    }

    /**
     * Retrieve the <tt>double</tt> value stored in the column specified by index.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnIndex Index of the column
     * @return <tt>double</tt> value stored in the specified column
     * @see #wasNull()
     */
    public final double getDouble(int columnIndex) {
        validateColumnType(columnIndex, VoltType.FLOAT);
        final double value = m_buffer.getDouble(getOffset(columnIndex));
        m_wasNull = (value <= VoltType.NULL_FLOAT); // see value.h
        return value;
    }

    /**
     * Retrieve the <tt>double</tt> value stored in the column
     * specified by name. Avoid retrieving via this method as it is slower than specifying the
     * column by index. Use {@link #getDouble(int)} instead.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnName Name of the column
     * @return <tt>double</tt> value stored in the specified column
     * @see #wasNull()
     * @see #getDouble(int)
     */
    public final double getDouble(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getDouble(colIndex);
    }

    /**
     * Retrieve the {@link java.lang.String String} value stored in the column specified by index.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead. If at all possible you should use {@link #getStringAsBytes(int)} instead
     * and avoid the overhead of constructing the {@link java.lang.String String} object.
     * @param columnIndex Index of the column
     * @return {@link java.lang.String String} value stored in the specified column
     * @see #wasNull()
     */
    public final String getString(int columnIndex) {
        validateColumnType(columnIndex, VoltType.STRING);
        String retval = readString(getOffset(columnIndex), VoltTable.ROWDATA_ENCODING);
        m_wasNull = (retval == null);
        return retval;
    }

    /**
     * Retrieve the {@link java.lang.String String} value stored in the column
     * specified by name. Avoid retrieving via this method as it is slower than specifying the
     * column by index. Use {@link #getString(int)} instead.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead. If at all possible you should use {@link #getStringAsBytes(int)} instead
     * and avoid the overhead of constructing the {@link java.lang.String String} object.
     * @param columnName Name of the column
     * @return {@link java.lang.String String} value stored in the specified column
     * @see #wasNull()
     * @see #getString(int)
     */
    public final String getString(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getString(colIndex);
    }

    /**
     * Retrieve the <tt>string</tt> value stored in the column specified by index as
     * an array of bytes. Assume UTF-8 encoding for all string values in VoltDB.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnIndex Index of the column
     * @return <tt>string</tt> value stored in the specified column as a <tt>byte[]</tt>
     * @see #wasNull()
     */
    public final byte[] getStringAsBytes(int columnIndex) {
        validateColumnType(columnIndex, VoltType.STRING);
        int pos = m_buffer.position();
        m_buffer.position(getOffset(columnIndex));
        int len = m_buffer.getInt();
        if (len == VoltTable.NULL_STRING_INDICATOR) {
            m_wasNull = true;
            m_buffer.position(pos);
            return null;
        }
        m_wasNull = false;
        byte[] data = new byte[len];
        m_buffer.get(data);
        m_buffer.position(pos);
        return data;
    }

    /**
     * Retrieve the <tt>string</tt> value stored in the column
     * specified by name as an array of bytes. Assume UTF-8 encoding for all
     * string values in VoltDB.Avoid retrieving via this method as it is slower
     * than specifying the column by index. Use {@link #getStringAsBytes(int)} instead.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnName Name of the column
     * @return <tt>string</tt> value stored in the specified column as a <tt>byte[]</tt>
     * @see #wasNull()
     * @see #getStringAsBytes(int)
     */
    public final byte[] getStringAsBytes(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getStringAsBytes(colIndex);
    }

    /**
     * Retrieve the varbinary value stored in the column specified by index.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnIndex Index of the column
     * @return Varbinary value stored in the specified column
     * @see #wasNull()
     */
    public final byte[] getVarbinary(int columnIndex) {
        validateColumnType(columnIndex, VoltType.VARBINARY);
        int pos = m_buffer.position();
        m_buffer.position(getOffset(columnIndex));
        // Sanity check the varbinary size int position.
        if (VARBINARY_LEN_SIZE > m_buffer.remaining()) {
            throw new RuntimeException(String.format(
                    "VoltTableRow::getVarbinary: Can't read varbinary size as %d byte integer " +
                    "from buffer with %d bytes remaining.",
                    VARBINARY_LEN_SIZE, m_buffer.remaining()));
        }
        int len = m_buffer.getInt();
        if (len == VoltTable.NULL_STRING_INDICATOR) {
            m_wasNull = true;
            m_buffer.position(pos);
            return null;
        }
        // Sanity check the size against the remaining buffer size.
        if (len > m_buffer.remaining()) {
            throw new RuntimeException(String.format(
                    "VoltTableRow::getVarbinary: Can't read %d byte varbinary " +
                    "from buffer with %d bytes remaining.", len, m_buffer.remaining()));
        }
        m_wasNull = false;
        byte[] data = new byte[len];
        m_buffer.get(data);
        m_buffer.position(pos);
        return data;
    }

    /**
     * Retrieve the varbinary value stored in the column
     * specified by name. Avoid retrieving via this method as it is slower than specifying the
     * column by index. Use {@link #getVarbinary(int)} instead.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
     * @param columnName Name of the column
     * @return Varbinary value stored in the specified column
     * @see #wasNull()
     * @see #getVarbinary(int)
     */
    public final byte[] getVarbinary(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getVarbinary(colIndex);
    }

    /**
     * Retrieve the <tt>long</tt> timestamp stored in the column specified by index.
     * Note that VoltDB uses GMT universally within its process space. Date objects sent over
     * the wire from clients may seem to be different times because of this, but it is just
     * a time zone offset. Timestamps represent microseconds from epoch.
     * @param columnIndex Index of the column
     * @return <tt>long</tt> timestamp value stored in the specified column
     * @see #wasNull()
     * @see #getTimestampAsTimestamp(int)
     */
    public final long getTimestampAsLong(int columnIndex) {
        validateColumnType(columnIndex, VoltType.TIMESTAMP);
        final long value = m_buffer.getLong(getOffset(columnIndex));
        m_wasNull = (value == Long.MIN_VALUE); // see value.h
        return value;
    }

    /**
     * Retrieve the <tt>long</tt> timestamp value stored in the column
     * specified by name. Note that VoltDB uses GMT universally within its
     * process space. Date objects sent over the wire from clients may seem
     * to be different times because of this, but it is just a time zone offset.
     * Avoid retrieving via this method as it is slower than specifying the
     * column by index. Use {@link #getTimestampAsLong(int)} instead.
     * @param columnName Name of the column
     * @return <tt>long</tt> timestamp value stored in the specified column
     * @see #wasNull()
     * @see #getTimestampAsLong(int)
     * @see #getTimestampAsTimestamp(String)
     */
    public final long getTimestampAsLong(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getTimestampAsLong(colIndex);
    }

    /**
     * Retrieve the {@link org.voltdb.types.TimestampType TimestampType}
     * value stored in the column specified by index.
     * Note that VoltDB uses GMT universally within its process space. Date objects sent over
     * the wire from clients may seem to be different times because of this, but it is just
     * a time zone offset.
     * @param columnIndex Index of the column
     * @return {@link org.voltdb.types.TimestampType TimestampType}
     * value stored in the specified column
     * @see #wasNull()
     */
    public final TimestampType getTimestampAsTimestamp(int columnIndex) {
        final long timestamp = getTimestampAsLong(columnIndex);
        if (m_wasNull) return null;
        return new TimestampType(timestamp);
    }

    /**
     * Retrieve the {@link org.voltdb.types.TimestampType TimestampType} value stored in the column
     * specified by name. Note that VoltDB uses GMT universally within its
     * process space. Date objects sent over the wire from clients may seem
     * to be different times because of this, but it is just a time zone offset.
     * Avoid retrieving via this method as it is slower than specifying the
     * column by index. Use {@link #getTimestampAsTimestamp(int)} instead.
     * @param columnName Name of the column
     * @return {@link org.voltdb.types.TimestampType TimestampType}
     * value stored in the specified column
     * @see #wasNull()
     * @see #getTimestampAsTimestamp(int)
     */
    public final TimestampType getTimestampAsTimestamp(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getTimestampAsTimestamp(colIndex);
    }

    /**
     * Retrieve the <tt>java.sql.Timestamp</tt> equivalent to the value stored in the column specified by index.
     * Note that VoltDB uses GMT universally within its process space. Date objects sent over
     * the wire from clients may seem to be different times because of this, but it is just
     * a time zone offset. VoltDB Timestamps are stored as long integer microseconds from epoch.
     * The resulting value is accurate to no finer than microsecond granularity.
     * @param columnIndex Index of the column
     * @return the <tt>java.sql.Timestamp</tt> equivalent to the value stored in the specified column
     */
    public final java.sql.Timestamp getTimestampAsSqlTimestamp(int columnIndex) {
        final long timestamp = getTimestampAsLong(columnIndex);
        if (m_wasNull) return null;

        return VoltTypeUtil.getSqlTimestampFromMicrosSinceEpoch(timestamp);
    }

    /**
     * Retrieve the <tt>java.sql.Timestamp</tt> equivalent to the value stored in the column specified by name.
     * Note that VoltDB uses GMT universally within its process space. Date objects sent over
     * the wire from clients may seem to be different times because of this, but it is just
     * a time zone offset. VoltDB Timestamps are stored as long integer microseconds from epoch.
     * The resulting value is accurate to no finer than microsecond granularity.
     * Avoid retrieving via this method as it is slower than specifying the
     * column by index. Use {@link #getTimestampAsSqlTimestamp(int)} instead.
     * @param columnName name of the column
     * @return the <tt>java.sql.Timestamp</tt> equivalent to the value stored in the specified column
     */
    public java.sql.Timestamp getTimestampAsSqlTimestamp(String columnName) {
        final int colIndex = getColumnIndex(columnName);
        return getTimestampAsSqlTimestamp(colIndex);
    }

    /**
     * <p>Retrieve the {@link BigDecimal} value stored in the column
     * specified by the index. All DECIMAL types have a fixed
     * scale when represented as BigDecimals.</p>
     *
     * @param columnIndex Index of the column
     * @return BigDecimal representation.
     * @see #wasNull()
     */
    public final BigDecimal getDecimalAsBigDecimal(int columnIndex) {
        validateColumnType(columnIndex, VoltType.DECIMAL);
        final int position = m_buffer.position();
        m_buffer.position(getOffset(columnIndex));
        final BigDecimal bd = VoltDecimalHelper.deserializeBigDecimal(m_buffer);
        m_buffer.position(position);
        m_wasNull = bd == null ? true : false;
        return bd;
    }

    /**
     * Retrieve the {@link BigDecimal} value stored in the column
     * specified by columnName. All DECIMAL types have a
     * fixed scale when represented as BigDecimals.
     * @param columnName Name of the column
     * @return BigDecimal representation.
     * @see #wasNull()
     * @see #getDecimalAsBigDecimal(int)
     */
    public BigDecimal getDecimalAsBigDecimal(String columnName) {
        int colIndex = getColumnIndex(columnName);
        return getDecimalAsBigDecimal(colIndex);
    }

    /**
     *
     * @param columnIndex
     * @param js
     * @throws JSONException
     */
    void putJSONRep(int columnIndex, JSONStringer js) throws JSONException {
        long value; double dvalue;

        VoltType columnType = getColumnType(columnIndex);
        switch (columnType) {
        case TINYINT:
            value = getLong(columnIndex);
            if (value == VoltType.NULL_TINYINT)
                js.value(null);
            else
                js.value(value);
            break;
        case SMALLINT:
            value = getLong(columnIndex);
            if (value == VoltType.NULL_SMALLINT)
                js.value(null);
            else
                js.value(value);
            break;
        case INTEGER:
            value = getLong(columnIndex);
            if (value == VoltType.NULL_INTEGER)
                js.value(null);
            else
                js.value(value);
            break;
        case BIGINT:
            value = getLong(columnIndex);
            if (value == VoltType.NULL_BIGINT)
                js.value(null);
            else
                js.value(value);
            break;
        case TIMESTAMP:
            value = getTimestampAsLong(columnIndex);
            if (value == VoltType.NULL_BIGINT)
                js.value(null);
            else
                js.value(value);
            break;
        case FLOAT:
            dvalue = getDouble(columnIndex);
            if (dvalue == VoltType.NULL_FLOAT)
                js.value(null);
            else
                js.value(dvalue);
            break;
        case STRING:
            js.value(getString(columnIndex));
            break;
        case VARBINARY:
            byte[] bin = getVarbinary(columnIndex);
            js.value(Encoder.hexEncode(bin));
            break;
        case DECIMAL:
            Object dec = getDecimalAsBigDecimal(columnIndex);
            if (dec == null)
                js.value(null);
            else
                js.value(dec.toString());
            break;
        case INVALID:
            break;
        case NULL:
            break;
        case NUMERIC:
            break;
        case VOLTTABLE:
            break;
        }
    }

    /** Validates that type and columnIndex match and are valid. */
    final void validateColumnType(int columnIndex, VoltType... types) {
        if (m_position < 0)
            throw new RuntimeException("VoltTableRow is in an invalid state. Consider calling advanceRow().");

        if ((columnIndex >= getColumnCount()) || (columnIndex < 0)) {
            throw new IndexOutOfBoundsException("Column index " + columnIndex + " is greater than the number of columns");
        }
        final VoltType columnType = getColumnType(columnIndex);
        for (VoltType type : types)
            if (columnType == type)
                return;
        throw new IllegalArgumentException("Column index " + columnIndex + " is type " + columnType);
    }

    /** Reads a string from a buffer with a specific encoding. */
    final String readString(int position, Charset encoding) {
        // Sanity check the string size int position. Note that the eventual
        // m_buffer.get() does check for underflow, getInt() does not.
        if (STRING_LEN_SIZE > m_buffer.limit() - position) {
            throw new RuntimeException(String.format(
                    "VoltTableRow::readString: Can't read string size as %d byte integer " +
                    "from buffer with %d bytes remaining.",
                    STRING_LEN_SIZE, m_buffer.limit() - position));
        }
        final int len = m_buffer.getInt(position);
        //System.out.println(len);

        // check for null string
        if (len == VoltTable.NULL_STRING_INDICATOR)
            return null;

        if (len < 0) {
            throw new RuntimeException("Invalid object length.");
        }

        // Sanity check the size against the remaining buffer size.
        if (position + STRING_LEN_SIZE + len > m_buffer.limit()) {
            throw new RuntimeException(String.format(
                    "VoltTableRow::readString: Can't read %d byte string " +
                    "from buffer with %d bytes remaining.",
                    len, m_buffer.limit() - position - STRING_LEN_SIZE));
        }

        // this is a bit slower than directly getting the array (see below)
        // but that caused bugs
        byte[] stringData = new byte[len];
        int oldPos = m_buffer.position();
        m_buffer.position(position + STRING_LEN_SIZE);
        m_buffer.get(stringData);
        m_buffer.position(oldPos);

        return new String(stringData, encoding);
    }

}
