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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.client.ClientUtils;
import org.voltdb.common.Constants;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;

/*
 * The primary representation of a result set (of tuples) or a temporary
 * table in VoltDB. VoltTable has arbitrary schema, is serializable,
 * and is used from within Stored Procedures and from the client library.
 *

A brief overview of the serialized format:

COLUMN HEADER:
[int: column header size in bytes (non-inclusive)]
[short: num columns]
[byte: column type] * num columns
[string: column name] * num columns
TABLE BODY DATA:
[int: num tuples]
[int: row size (non inclusive), blob row data] * num tuples

Strings are represented as:

[int: length-prefix][bytes: stringdata]

where offset is the starting byte in the raw string bytes buffer, and length is the number of bytes
in the string.

This format allows the entire table to be slurped into memory without having to look at it much.

Note that it is assumed that the pre and post-conditions for all methods are:
1) buffer.position() is at the end of data
2) buffer.limit() is at the end of data

TODO(evanj): In the future, it would be nice to avoid having to copy it in some cases. For
example, most of the data coming from C++ either is consumed immediately by the stored procedure
or is sent immediately to the client. However, it is tough to avoid copying when we don't know for
sure how the results will be used, since the next call to the EE currently reuses the buffer. Also,
the direct ByteBuffers used by the EE are expensive to allocate and free, so we don't want to
rely on the garbage collector.
 */
/**
 * <p>The primary representation of a result set (of tuples) or a temporary
 * table in VoltDB. VoltTable has arbitrary schema, is serializable,
 * and is used from within Stored Procedures and from the client library.</p>
 *
 * <h3>Accessing Rows by Index</h3>
 *
 * <p>Given a VoltTable, individual rows can be accessed via the {@link #fetchRow(int)}
 * method. This method returns a {@link VoltTableRow} instance with position set to
 * the specified row. See VoltTableRow for further information on accessing individual
 * column data within a row.</p>
 *
 * <h3>A VoltTable is also a VoltTableRow</h3>
 *
 * <p>Like a {@link VoltTableRow}, a VoltTable has a current position within its rows.
 * This is because VoltTable is a subclass of VoltTableRow. This allows for easy
 * sequential access of data. Example:</p>
 *
 * <code>
 * while (table.advanceRow()) {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;System.out.println(table.getLong(7));<br>
 * }
 * </code>
 *
 * <h3>Building a Table Dynamically</h3>
 *
 * <p>VoltTables can be constructed on the fly. This can help generate cleaner
 * result sets from stored procedures, or more manageable parameters to them.
 * Example:</p>
 *
 * <code>
 * VoltTable t = new VoltTable(<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;new VoltTable.ColumnInfo("col1", VoltType.BIGINT),<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;new VoltTable.ColumnInfo("col2", VoltType.STRING));<br>
 * t.addRow(15, "sampleString");<br>
 * t.addRow(-9, "moreData");
 * </code>
 */
public final class VoltTable extends VoltTableRow implements JSONString {

    /**
     * Size in bytes of the maximum length for a VoltDB tuple.
     * This value is counted from byte 0 of the header size to the end of row data.
     */
    public static final int MAX_SERIALIZED_TABLE_LENGTH = 10 * 1024 * 1024;
    /**
     * String representation of <code>MAX_SERIALIZED_TABLE_LENGTH</code>.
     */
    public static final String MAX_SERIALIZED_TABLE_LENGTH_STR =
            String.valueOf(MAX_SERIALIZED_TABLE_LENGTH / 1024) + "k";

    static final int NULL_STRING_INDICATOR = -1;
    static final Charset METADATA_ENCODING = Constants.US_ASCII_ENCODING;
    static final Charset ROWDATA_ENCODING = Constants.UTF8ENCODING;

    static final AtomicInteger expandCountDouble = new AtomicInteger(0);

    boolean m_readOnly = false;
    int m_rowStart = -1; // the beginning of the row data (points to before the row count int)
    int m_rowCount = -1;
    int m_colCount = -1;

    // JSON KEYS FOR SERIALIZATION
    static final String JSON_NAME_KEY = "name";
    static final String JSON_TYPE_KEY = "type";
    static final String JSON_SCHEMA_KEY = "schema";
    static final String JSON_DATA_KEY = "data";
    static final String JSON_STATUS_KEY = "status";

    /**
     * <p>Object that represents the name and schema for a {@link VoltTable} column.
     * Primarily used to construct in the constructor {@link VoltTable#VoltTable(ColumnInfo...)}
     * and {@link VoltTable#VoltTable(ColumnInfo[], int)}.</p>
     *
     * <p>Example:<br>
     * <tt>VoltTable t = new VoltTable(<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;new ColumnInfo("foo", VoltType.INTEGER),
     * new ColumnInfo("bar", VoltType.STRING));</tt>
     * </p>
     *
     * <p>Note: VoltDB current supports ASCII encoded column names only. Column values are
     * still UTF-8 encoded.</p>
     */
    public static class ColumnInfo implements Cloneable {

        /**
         * Construct an immutable <tt>ColumnInfo</tt> instance.
         *
         * @param name The name of the column (ASCII).
         * @param type The type of the column. Note that not all types are
         * supported (such as {@link VoltType#INVALID} or {@link VoltType#NUMERIC}.
         */
        public ColumnInfo(String name, VoltType type) {
            this.name = name;
            this.type = type;
            // if you're using this constructor, the values below probably
            // will never be used, but they *are* final.
            size = VoltType.MAX_VALUE_LENGTH;
            nullable = true;
            unique = false;
            defaultValue = NO_DEFAULT_VALUE;
        }

        /**
         * Construct an immutable <tt>ColumnInfo</tt> instance with extra metadata
         * for tests.
         *
         * @param name The name of the column (ASCII).
         * @param type The type of the column. Note that not all types are
         * supported (such as {@link VoltType#INVALID} or {@link VoltType#NUMERIC}.
         * @param size How big should the column be (for strings or binary)
         * @param nullable Is the column nullable.
         * @param unique Is the column unique? (implies index creation)
         * @param defaultValue Default value for column (non string types will be
         * converted from strings - varbinary must be hex-encoded).
         */
        ColumnInfo(String name,
                   VoltType type,
                   int size,
                   boolean nullable,
                   boolean unique,
                   String defaultValue)
        {
            this.name = name;
            this.type = type;
            this.size = size;
            this.nullable = nullable;
            this.unique = unique;
            this.defaultValue = defaultValue;
        }

        /**
         * Construct an immutable <tt>ColumnInfo</tt> instance from JSON.
         *
         * @param jsonCol The JSON object representing the column schema
         * @throws JSONException if the JSON data is not correct.
         */
        ColumnInfo(JSONObject jsonCol) throws JSONException {
            this.name = jsonCol.getString(JSON_NAME_KEY);
            this.type = VoltType.get((byte) jsonCol.getInt(JSON_TYPE_KEY));
            size = VoltType.MAX_VALUE_LENGTH;
            nullable = true;
            unique = false;
            defaultValue = NO_DEFAULT_VALUE;
        }

        /**
         * Basically just suppress CloneNotSupportedException.
         */
        @Override
        public ColumnInfo clone() {
            try {
                return (ColumnInfo) super.clone();
            } catch (CloneNotSupportedException e) {
                assert(false);
                throw new RuntimeException(e);
            }
        }

        // immutable actual data
        final String name;
        final VoltType type;

        // data below not exposed publicly / not serialized / used for test
        final int size;
        final boolean nullable;
        final boolean unique;
        final String defaultValue;

        // pick a random string as sigil for no default
        static final String NO_DEFAULT_VALUE = "!@#$%^&*(!@#$%^&*(";

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            throw new RuntimeException("Didn't expect you to hash this.");
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof VoltTable.ColumnInfo)) {
                return false;
            }
            VoltTable.ColumnInfo other = (VoltTable.ColumnInfo) obj;

            if (nullable != other.nullable) return false;
            if (unique != other.unique) return false;
            if (defaultValue != other.defaultValue) return false;
            if (size != other.size) return false;
            if (type != other.type) return false;
            return name.equals(other.name);
        }
    }

    /**
     * Class to hold additional information and constraints for test code.
     * Basically, a table in a test that has this extra data could re-create
     * it's own DDL. Or it could be randomly filled with data that would be
     * safe to insert into a table in a running VoltDB cluster (unique columns,
     * string size limits, etc...).
     *
     */
    static class ExtraMetadata {
        // kept around if provided for schema enforcement - only really used for test code
        final ColumnInfo[] originalColumnInfos;
        // next two used for test code that generates schema from tables
        final String name;
        int partitionColIndex; // -1 means replicated
        int[] pkeyIndexes;

        ExtraMetadata(String name,
                      int partitionColIndex,
                      int[] pkeyIndexes,
                      ColumnInfo... originalColumnInfos)
        {
            this.name = name;
            this.partitionColIndex = partitionColIndex;
            this.pkeyIndexes = pkeyIndexes.clone();
            this.originalColumnInfos = originalColumnInfos.clone();
        }

        /* (non-Javadoc)
         * @see java.lang.Object#clone()
         */
        @Override
        protected Object clone() {
            try {
                ExtraMetadata cloned = (ExtraMetadata) super.clone();
                for (int i = 0; i < originalColumnInfos.length; i++) {
                    cloned.originalColumnInfos[i] = originalColumnInfos[i].clone();
                }
                return cloned;
            }
            // should never happen
            catch (CloneNotSupportedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }
    }
    final ExtraMetadata m_extraMetadata;

    /**
     * Do nothing constructor that does no initialization or allocation.
     */
    VoltTable() {
        // m_extraMetadata is final and must be set to null
        this((ExtraMetadata) null);
    }

    /**
     * Do nothing constructor that does no initialization or allocation.
     * Does copy over existing values for m_extraMetadata.
     */
    VoltTable(ExtraMetadata extraMetadata) {
        m_extraMetadata = extraMetadata;
    }

    /**
     * Create a table from an existing backing buffer.
     *
     * @param backing The buffer containing the serialized table.
     * @param readOnly Can this table be changed?
     */
    VoltTable(ByteBuffer backing, boolean readOnly) {
        // no test metadata when creating tables from buffers
        m_extraMetadata = null;

        m_buffer = backing;

        // rowstart represents and offset to the start of row data,
        //  but the serialization is the non-inclusive length of the header,
        //  so add 4 bytes.
        m_rowStart = m_buffer.getInt(0) + 4;

        m_colCount = m_buffer.getShort(5);
        m_rowCount = m_buffer.getInt(m_rowStart);
        m_buffer.position(m_buffer.limit());
        m_readOnly = readOnly;

        assert(verifyTableInvariants());
    }

    /**
     * Create an empty table from column schema. While {@link #VoltTable(ColumnInfo...)}
     * is the preferred constructor, this version may reduce the need for an array
     * allocation by allowing the caller to specify only a portion of the given array
     * should be used.
     *
     * @param columns An array of ColumnInfo objects, one per column
     * in the desired order.
     * @param columnCount The number of columns in the array to use.
     */
    public VoltTable(ColumnInfo[] columns, int columnCount) {
        this(null, columns, columnCount);
    }

    /**
     * Create an empty table from column schema given as an array.
     *
     * @param columns An array of ColumnInfo objects, one per column
     * in the desired order.
     */
    public VoltTable(ColumnInfo[] columns) {
        this(null, columns, columns.length);
    }

    /**
     * Given a column and an array of columns, return a new array of columns with the
     * single guy prepended onto the others. This function is used in the constructor
     * below so that one constructor can call another without breaking Java rules about
     * chained constructors being the first thing called.
     */
    private static ColumnInfo[] prependColumn(ColumnInfo firstColumn, ColumnInfo[] columns) {
        int allLen = 1 + columns.length;
        ColumnInfo[] allColumns = new ColumnInfo[allLen];
        allColumns[0] = firstColumn;
        for (int i = 0; i < columns.length; i++) {
            allColumns[i+1] = columns[i];
        }
        return allColumns;
    }

    /**
     * Create an empty table from column schema.
     * Note that while this accepts a varargs set of columns,
     * it requires at least one column to prevent user errors.
     *
     * @param firstColumn The first column of the table.
     * @param columns An array of ColumnInfo objects, one per column
     * in the desired order (can be empty).
     */
    public VoltTable(ColumnInfo firstColumn, ColumnInfo... columns) {
        this(prependColumn(firstColumn, columns));
    }

    /**
     * Constructor that allows for setting extra metadata for test purposes.
     * extraMetadata param can be null.
     */
    VoltTable(ExtraMetadata extraMetadata, ColumnInfo[] columns, int columnCount) {
        // memoize any provided extra metadata for test
        m_extraMetadata = extraMetadata;

        // allocate a 1K table backing for starters
        int allocationSize = 1024;
        m_buffer = ByteBuffer.allocate(allocationSize);

        // while not successful at initializing,
        //  use a bigger and bigger backing
        boolean success = false;
        while (!success) {
            try {
                // inside the try block, do initialization

                m_colCount = columnCount;
                m_rowCount = 0;

                // do some trivial checks to make sure the schema is not totally wrong
                if (columns == null) {
                    throw new RuntimeException("VoltTable(..) constructor passed null schema.");
                }
                if (columnCount <= 0) {
                    throw new RuntimeException("VoltTable(..) constructor requires at least one column.");
                }
                if (columns.length < columnCount) {
                    throw new RuntimeException("VoltTable(..) constructor passed truncated column schema array.");
                }

                // put a dummy value in for header size for now
                m_buffer.putInt(0);

                //Put in 0 for the status code
                m_buffer.put(Byte.MIN_VALUE);

                m_buffer.putShort((short) columnCount);

                for (int i = 0; i < columnCount; i++)
                    m_buffer.put(columns[i].type.getValue());
                for (int i = 0; i < columnCount; i++) {
                    if (columns[i].name == null) {
                        m_buffer.position(0);
                        throw new IllegalArgumentException("VoltTable column names can not be null.");
                    }
                    writeStringToBuffer(columns[i].name, METADATA_ENCODING, m_buffer);
                }
                // write the header size to the first 4 bytes (length-prefixed non-inclusive)
                m_rowStart = m_buffer.position();
                m_buffer.putInt(0, m_rowStart - 4);
                // write the row count to the next 4 bytes after the header
                m_buffer.putInt(0);
                m_buffer.limit(m_buffer.position());

                success = true;
            }
            catch (BufferOverflowException e) {
                // if too small buffer, grow
                allocationSize *= 4;
                m_buffer = ByteBuffer.allocate(allocationSize);
            }
        }
        assert(verifyTableInvariants());
    }

    /**
     * Delete all row data. Column data is preserved.
     * Useful for reusing an <tt>VoltTable</tt>.
     */
    public final void clearRowData() {
        assert(verifyTableInvariants());
        m_buffer.position(m_rowStart);
        m_buffer.putInt(0);
        m_rowCount = 0;
        assert(verifyTableInvariants());
    }

    /**
     * Get a new {@link VoltTableRow} instance with identical position as this table.
     * After the cloning, the new instance and this table can be advanced or reset
     * independently.
     * @return A new {@link VoltTableRow} instance with the same position as this table.
     */
    @Override
    public VoltTableRow cloneRow() {
        Row retval = new Row(m_position);
        retval.m_hasCalculatedOffsets = m_hasCalculatedOffsets;
        retval.m_wasNull = m_wasNull;
        if (m_offsets != null)
            for (int i = 0; i < m_colCount; i++)
                retval.m_offsets[i] = m_offsets[i];
        retval.m_activeRowIndex = m_activeRowIndex;
        return retval;
    }

    /**
     * Representation of a row in an <tt>VoltTable</tt>. Immutable. Values returned by the various
     * getters are SQL <tt>null</tt> (as opposed to Java <tt>null</tt>) when they are Java null <code>((x == null) == true)</code>
     * or {@link #wasNull()} returns <tt>true</tt> when called immediately after the getter that retrieved the value.
     * @see #wasNull()
     */
    final class Row extends VoltTableRow {
        /**
         * A Row instance has no real data, but sits on top of a specific position
         * in the buffer data, interpreting that data into a table row.
         *
         * @param position Offset into the buffer of the start of this row's data.
         * @param offsets Pass in an array sized for the number of columns. This
         * avoids allocating a new object for every row. The reason it can't just
         * use m_schemaOffsets in the RowIterator is that the fetchRow(..) call
         * can create a row without an iterator.
         */
        Row(int position) {
            m_buffer = VoltTable.this.m_buffer;
            assert (position < m_buffer.limit());
            m_position = position;
            m_offsets = new int[m_colCount];
            m_activeRowIndex = -1;
        }

        @Override
        protected int getColumnCount() {
            return VoltTable.this.getColumnCount();
        }

        @Override
        protected int getColumnIndex(String columnName) {
            return VoltTable.this.getColumnIndex(columnName);
        }

        @Override
        protected VoltType getColumnType(int columnIndex) {
            return VoltTable.this.getColumnType(columnIndex);
        }

        @Override
        protected int getRowCount() {
            return VoltTable.this.getRowCount();
        }

        @Override
        protected int getRowStart() {
            return VoltTable.this.getRowStart();
        }

        @Override
        public VoltTableRow cloneRow() {
            Row retval = new Row(m_position);
            retval.m_hasCalculatedOffsets = m_hasCalculatedOffsets;
            retval.m_wasNull = m_wasNull;
            for (int i = 0; i < m_colCount; i++)
                retval.m_offsets[i] = m_offsets[i];
            retval.m_activeRowIndex = m_activeRowIndex;
            return retval;
        }
    }

    /**
     * Return the name of the column with the specified index.
     * @param index Index of the column
     * @return Name of the column with the specified index.
     */
    public final String getColumnName(int index) {
        assert(verifyTableInvariants());
        if ((index < 0) || (index >= m_colCount))
            throw new IllegalArgumentException("Not a valid column index.");

        // move to the start of the list of column names
        // (this could be done faster by just reading the string lengths
        //  and skipping ahead)
        int pos = 4 + 1 + 2 + m_colCount;//headerLength + status code + column count + (m_colCount * colTypeByte)
        String name = null;
        for (int i = 0; i < index; i++)
            pos += m_buffer.getInt(pos) + 4;
        name = readString(pos, METADATA_ENCODING);
        assert(name != null);

        assert(verifyTableInvariants());
        return name;
    }

    @Override
    public final VoltType getColumnType(int index) {
        assert(verifyTableInvariants());
        assert(index < m_colCount);
        // move to the right place
        VoltType retval = VoltType.get(m_buffer.get(4 + 1 + 2 + index));//headerLength + status code + column count
        assert(verifyTableInvariants());
        return retval;
    }

    // package-private methods to get constraint for test code
    final boolean getColumnNullable(int index) {
        if (m_extraMetadata != null) {
            return m_extraMetadata.originalColumnInfos[index].nullable;
        }
        return true;
    }

    // package-private methods to get constraint for test code
    final int getColumnMaxSize(int index) {
        if (m_extraMetadata != null) {
            return m_extraMetadata.originalColumnInfos[index].size;
        }
        return VoltType.MAX_VALUE_LENGTH;
    }

    // package-private methods to get constraint for test code
    final int getColumnPkeyIndex(int index) {
        if (m_extraMetadata != null) {
            for (int i = 0; i < m_extraMetadata.pkeyIndexes.length; i++) {
                if (m_extraMetadata.pkeyIndexes[i] == index) {
                    return i;
                }
            }
        }
        return -1;
    }

    // package-private methods to get constraint for test code
    final int[] getPkeyColumnIndexes() {
        if (m_extraMetadata != null) {
            if (m_extraMetadata.pkeyIndexes != null) {
                return m_extraMetadata.pkeyIndexes;
            }
        }
        return new int[0];
    }

    // package-private methods to get constraint for test code
    final boolean getColumnUniqueness(int index) {
        if (m_extraMetadata != null) {
            return m_extraMetadata.originalColumnInfos[index].unique;
        }
        return false;
    }

    // package-private methods to get constraint for test code
    final String getColumnDefaultValue(int index) {
        if (m_extraMetadata != null) {
            return m_extraMetadata.originalColumnInfos[index].defaultValue;
        }
        return null;
    }

    @Override
    public final int getColumnIndex(String name) {
        assert(verifyTableInvariants());
        for (int i = 0; i < m_colCount; i++) {
            if (getColumnName(i).equalsIgnoreCase(name))
                return i;
        }
        String msg = "No Column named '" + name + "'. Existing columns are:";
        for (int i = 0; i < m_colCount; i++) {
            msg += "[" + i + "]" + getColumnName(i) + ",";
        }
        throw new IllegalArgumentException(msg);
    }

    /**
     * Return a {@link VoltTableRow} instance with the specified index. This method
     * is not performant because it has to scan the length prefix of every row preceding
     * the requested row in order to find the position of the requested row. Use advanceRow
     * or advanceToRow instead.
     * @param index Index of the row
     * @return The requested {@link VoltTableRow Row}.
     * @throws IndexOutOfBoundsException if no row exists at the given index.
     */
    public final VoltTableRow fetchRow(int index) {
        assert(verifyTableInvariants());
        if ((index < 0) || (index >= m_rowCount)) {
            throw new IndexOutOfBoundsException("index = " + index + "; rows = " + m_rowCount);
        }

        int pos = m_rowStart + 4;
        for (int i = 0; i < index; i++) {
            // add 4 bytes as the row size is non-inclusive
            pos += m_buffer.getInt(pos) + 4;
        }
        Row retval = new Row(pos + 4);
        retval.m_activeRowIndex = index;
        return retval;
    }

    /**
     * Append a {@link VoltTableRow row} from another <tt>VoltTable</tt>
     * to this VoltTable instance. Technically, it could be from the same
     * table, but this isn't the common usage.
     * @param row {@link VoltTableRow Row} to add.
     */
    public final void add(VoltTableRow row) {
        assert(verifyTableInvariants());
        final Object[] values = new Object[m_colCount];
        for (int i = 0; i < m_colCount; i++) {
            try {
                values[i] = row.get(i, getColumnType(i));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        addRow(values);
    }

    /**
     * Append a new row to the table using the supplied column values.
     * @param values Values of each column in the row.
     * @throws VoltTypeException  when there are casting/type failures
     *         between an input value and the corresponding column
     */
    public final void addRow(Object... values) {
        if (m_readOnly) {
            throw new IllegalStateException("Table is read-only. Make a copy before changing.");
        }
        assert(verifyTableInvariants());
        if (m_colCount == 0) {
            throw new IllegalStateException("table has no columns defined");
        }
        if (values.length != m_colCount) {
            throw new IllegalArgumentException(values.length + " arguments but table has " + m_colCount + " columns");
        }

        // memoize the start of this row in case we roll back
        final int pos = m_buffer.position();

        try {
            // Allow the buffer to grow to max capacity
            m_buffer.limit(m_buffer.capacity());
            // advance the row size value
            m_buffer.position(pos + 4);

            // where does the type bytes start
            // skip rowstart + status code + colcount
            int typePos = 4 + 1 + 2;

            for (int col = 0; col < m_colCount; col++) {
                Object value = values[col];
                VoltType columnType = VoltType.get(m_buffer.get(typePos + col));

                // schema checking code that is used for some tests
                boolean allowNulls = true;
                int maxColSize = VoltType.MAX_VALUE_LENGTH;
                if (m_extraMetadata != null) {
                    allowNulls = m_extraMetadata.originalColumnInfos[col].nullable;
                    maxColSize = m_extraMetadata.originalColumnInfos[col].size;
                }

                try
                {
                    if (VoltType.isNullVoltType(value))
                    {
                        // schema checking code that is used for some tests
                        // alllowNulls should always be true in production
                        if (allowNulls == false) {
                            throw new IllegalArgumentException(
                                    String.format("Column %s at index %d doesn't allow NULL values.",
                                    getColumnName(col), col));
                        }

                        switch (columnType) {
                        case TINYINT:
                            m_buffer.put(VoltType.NULL_TINYINT);
                            break;
                        case SMALLINT:
                            m_buffer.putShort(VoltType.NULL_SMALLINT);
                            break;
                        case INTEGER:
                            m_buffer.putInt(VoltType.NULL_INTEGER);
                            break;
                        case TIMESTAMP:
                            m_buffer.putLong(VoltType.NULL_BIGINT);
                            break;
                        case BIGINT:
                            m_buffer.putLong(VoltType.NULL_BIGINT);
                            break;
                        case FLOAT:
                            m_buffer.putDouble(VoltType.NULL_FLOAT);
                            break;
                        case STRING:
                            m_buffer.putInt(NULL_STRING_INDICATOR);
                            break;
                        case VARBINARY:
                            m_buffer.putInt(NULL_STRING_INDICATOR);
                            break;
                        case DECIMAL:
                            VoltDecimalHelper.serializeNull(m_buffer);
                            break;

                        default:
                            throw new VoltTypeException("Unsupported type: " +
                                    columnType);
                        }
                    } else {

                        // Allow implicit conversions across all numeric types
                        // except BigDecimal and anything else. Require BigDecimal
                        // and reject Long128. Convert byte[] to VoltType.STRING.
                        // Allow longs to be converted to VoltType.TIMESTAMPS.

                        // In all error paths, catch ClassCastException
                        // and VoltTypeException to restore
                        // the correct table state.
                        // XXX consider adding a fast path that checks for
                        // equivalent types of input and column

                        switch (columnType) {
                        case TINYINT:
                            if (value instanceof BigDecimal)
                                throw new ClassCastException();
                            final Number n1 = (Number) value;
                            if (columnType.wouldCastOverflow(n1))
                            {
                                throw new VoltTypeException("Cast of " +
                                        n1.doubleValue() +
                                        " to " +
                                        columnType.toString() +
                                        " would overflow");
                            }
                            m_buffer.put(n1.byteValue());
                            break;
                        case SMALLINT:
                            if (value instanceof BigDecimal)
                                throw new ClassCastException();
                            final Number n2 = (Number) value;
                            if (columnType.wouldCastOverflow(n2))
                            {
                                throw new VoltTypeException("Cast to " +
                                        columnType.toString() +
                                        " would overflow");
                            }
                            m_buffer.putShort(n2.shortValue());
                            break;
                        case INTEGER:
                            if (value instanceof BigDecimal)
                                throw new ClassCastException();
                            final Number n3 = (Number) value;
                            if (columnType.wouldCastOverflow(n3))
                            {
                                throw new VoltTypeException("Cast to " +
                                        columnType.toString() +
                                        " would overflow");
                            }
                            m_buffer.putInt(n3.intValue());
                            break;
                        case BIGINT:
                            if (value instanceof BigDecimal)
                                throw new ClassCastException();
                            final Number n4 = (Number) value;
                            if (columnType.wouldCastOverflow(n4))
                            {
                                throw new VoltTypeException("Cast to " +
                                        columnType.toString() +
                                        " would overflow");
                            }
                            m_buffer.putLong(n4.longValue());
                            break;

                        case FLOAT:
                            if (value instanceof BigDecimal)
                                throw new ClassCastException();
                            final Number n5 = (Number) value;
                            if (columnType.wouldCastOverflow(n5))
                            {
                                throw new VoltTypeException("Cast to " +
                                        columnType.toString() +
                                        " would overflow");
                            }
                            m_buffer.putDouble(n5.doubleValue());
                            break;

                        case STRING: {
                            // Accept byte[] and String
                            if (value instanceof byte[]) {
                                if (((byte[]) value).length > maxColSize)
                                    throw new VoltOverflowException(
                                            "Value in VoltTable.addRow(...) larger than allowed max " +
                                                    VoltType.humanReadableSize(maxColSize));

                                // bytes MUST be a UTF-8 encoded string.
                                assert(testForUTF8Encoding((byte[]) value));
                                writeStringOrVarbinaryToBuffer((byte[]) value, m_buffer);
                            }
                            else {
                                if (((String) value).length() > maxColSize)
                                    throw new VoltOverflowException(
                                            "Value in VoltTable.addRow(...) larger than allowed max " +
                                                    VoltType.humanReadableSize(maxColSize));

                                writeStringToBuffer((String) value, ROWDATA_ENCODING, m_buffer);
                            }
                            break;
                        }

                        case VARBINARY: {
                            // Accept byte[] and String (hex-encoded)
                            if (value instanceof String) {
                                value = Encoder.hexDecode((String) value);
                            }
                            if (value instanceof byte[]) {
                                if (((byte[]) value).length > maxColSize)
                                    throw new VoltOverflowException(
                                            "Value in VoltTable.addRow(...) larger than allowed max " +
                                                    VoltType.humanReadableSize(maxColSize));
                                writeStringOrVarbinaryToBuffer((byte[]) value, m_buffer);
                            }
                            break;
                        }

                        case TIMESTAMP: {
                            if (value instanceof BigDecimal) {
                                throw new ClassCastException();
                            }
                            long micros;
                            // Accept long and TimestampType and any kind of Date
                            if (value instanceof java.util.Date ||
                                    value instanceof TimestampType) {
                                 micros = ParameterSet.timestampToMicroseconds(value);
                            } else {
                                micros = ((Number) value).longValue();
                            }
                            m_buffer.putLong(micros);
                            break;
                        }

                        case DECIMAL: {
                            // Only accept BigDecimal; rely on class cast exception for error path
                            VoltDecimalHelper.serializeBigDecimal( (BigDecimal)value, m_buffer);
                            break;
                        }

                        default:
                            throw new VoltTypeException("Unsupported type: " + columnType);
                        }
                    }
                }
                catch (VoltTypeException vte)
                {
                    // revert the row size advance and any other
                    // buffer additions
                    m_buffer.position(pos);
                    throw vte;
                }
                catch (ClassCastException cce) {
                    // revert any added tuples and strings
                    m_buffer.position(pos);
                    throw new VoltTypeException("Value for column " + col + " (" +
                            getColumnName(col) + ") is type " +
                            value.getClass().getSimpleName() + " when type " + columnType +
                            " was expected.");
                }
            }

            m_rowCount++;
            m_buffer.putInt(m_rowStart, m_rowCount);
            final int rowsize = m_buffer.position() - pos - 4;

            // check for too big rows
            if (rowsize > VoltTableRow.MAX_TUPLE_LENGTH) {
                throw new VoltOverflowException(
                        "Table row total length larger than allowed max " + VoltTableRow.MAX_TUPLE_LENGTH_STR);
            }

            // constrain buffer limit back to the new position
            m_buffer.limit(m_buffer.position());
            assert(rowsize >= 0);
            // buffer overflow is caught and handled below.
            m_buffer.putInt(pos, rowsize);
        }
        catch (BufferOverflowException e) {
            m_buffer.position(pos);
            expandBuffer();
            addRow(values);
        }
        catch (IllegalArgumentException e) {
            // if this was thrown because of a lack of space
            // then grow the buffer
            // the number 32 was picked out of a hat ( maybe a bug if str > 32 )
            if (m_buffer.limit() - m_buffer.position() < 32) {
                m_buffer.position(pos);
                expandBuffer();
                addRow(values);
            }
            else throw e;
        }

        assert(verifyTableInvariants());
    }

    private final void expandBuffer() {
        final int end = m_buffer.position();
        assert(end > m_rowStart);
        final ByteBuffer buf2 = ByteBuffer.allocate(m_buffer.capacity() * 2);
        m_buffer.limit(end);
        m_buffer.position(0);
        buf2.put(m_buffer);
        m_buffer = buf2;
    }

    /**
     * Tables containing a single row and a single integer column can be read using this convenience
     * method.
     * @return The integer row value.
     */
    public final long asScalarLong() {
        verifyTableInvariants();

        if (m_rowCount != 1) {
            throw new IllegalStateException(
                    "table must contain exactly 1 tuple; tuples = " + m_rowCount);
        }
        if (m_colCount != 1) {
            throw new IllegalStateException(
                    "table must contain exactly 1 column; columns = " + m_colCount);
        }
        final VoltType colType = getColumnType(0);
        switch (colType) {
        case TINYINT:
            return m_buffer.get(m_rowStart + 8);
        case SMALLINT:
            return m_buffer.getShort(m_rowStart + 8);
        case INTEGER:
            return m_buffer.getInt(m_rowStart + 8);
        case BIGINT:
            return m_buffer.getLong(m_rowStart + 8);
        default:
            throw new IllegalStateException(
                    "table must contain exactly 1 integral value; column 1 is type = " + colType.name());
        }
    }

    /**
     * Returns a {@link java.lang.String String} representation of this table.
     * Resulting string will contain schema and all data and will be formatted.
     * @return a {@link java.lang.String String} representation of this table.
     */
    @Override
    public String toString() {
        assert(verifyTableInvariants());
        StringBuilder buffer = new StringBuilder();

        // commented out code to print byte by byte content
        /*for (int i = 0; i < m_buffer.limit(); i++) {
            byte b = m_buffer.get(i);
            char c = (char) b;
            if (Character.isLetterOrDigit(c))
                buffer.append(c);
            else
                buffer.append("[").append(b).append("]");
            buffer.append(" ");
        }
        buffer.append("\n");*/

        buffer.append(" header size: ").append(m_buffer.getInt(0)).append("\n");

        byte statusCode = m_buffer.get(4);
        buffer.append(" status code: ").append(statusCode);

        short colCount = m_buffer.getShort(5);
        buffer.append(" column count: ").append(colCount).append("\n");
        assert(colCount == m_colCount);

        buffer.append(" cols ");
        for (int i = 0; i < colCount; i++)
            buffer.append("(").append(getColumnName(i)).append(":").append(getColumnType(i).name()).append("), ");
        buffer.append("\n");

        buffer.append(" rows -\n");

        VoltTableRow r = cloneRow();
        r.resetRowPosition();
        while (r.advanceRow()) {
            buffer.append("  ");
            for (int i = 0; i < m_colCount; i++) {
                switch(getColumnType(i)) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    long lval = r.getLong(i);
                    if (r.wasNull())
                        buffer.append("NULL");
                    else
                        buffer.append(lval);
                    break;
                case FLOAT:
                    double dval = r.getDouble(i);
                    if (r.wasNull())
                        buffer.append("NULL");
                    else
                        buffer.append(dval);
                    break;
                case TIMESTAMP:
                    TimestampType tstamp = r.getTimestampAsTimestamp(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (tstamp == null);
                    } else {
                        buffer.append(tstamp);
                    }
                    break;
                case STRING:
                    String string = r.getString(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (string == null);
                    } else {
                        buffer.append(string);
                    }
                    break;
                case VARBINARY:
                    byte[] bin = r.getVarbinary(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (bin == null);
                    } else {
                        buffer.append(VoltType.varbinaryToPrintableString(bin));
                    }
                    break;
                case DECIMAL:
                    BigDecimal bd = r.getDecimalAsBigDecimal(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (bd == null);
                    } else {
                        buffer.append(bd.toString());
                    }
                    break;
                default:
                    // should not get here ever
                    throw new IllegalStateException("Table column had unexpected type.");
                }
                if (i < m_colCount - 1) {
                    buffer.append(",");
                }
            }
            buffer.append("\n");
        }

        assert(verifyTableInvariants());
        return buffer.toString();
    }

    /**
     * Return a "pretty print" representation of this table.  Output will be formatted
     * in a tabular textual format suitable for display.
     * @return A string containing a pretty-print formatted representation of this table.
     */
    public String toFormattedString() {

        final int MAX_PRINTABLE_CHARS = 30;
        final String ELIPSIS = "...";

        StringBuffer sb = new StringBuffer();

        int columnCount = this.getColumnCount();
        int[] padding = new int[columnCount];
        String[] fmt = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            padding[i] = this.getColumnName(i).length(); // min value to be increased later
        }
        this.resetRowPosition();

        // Compute the padding needed for each column of the table (note must
        // visit every row)
        while (this.advanceRow()) {
            for (int i = 0; i < columnCount; i++) {
                Object v = this.get(i, this.getColumnType(i));
                if (this.wasNull()) {
                    v = "NULL";
                }
                int len = 0; // length
                if (this.getColumnType(i) == VoltType.VARBINARY && !this.wasNull()) {
                    len = ((byte[]) v).length * 2;
                } else {
                    len = v.toString().length();
                }
                // crop long strings and such
                if (len > MAX_PRINTABLE_CHARS) {
                    len = MAX_PRINTABLE_CHARS;
                }

                // compute the max for each column
                if (len > padding[i]) {
                    padding[i] = len;
                }
            }
        }

        // Determine the formatting string for each column
        for (int i = 0; i < columnCount; i++) {
            padding[i] += 1;
            fmt[i] = "%1$"
                    + ((this.getColumnType(i) == VoltType.STRING
                            || this.getColumnType(i) == VoltType.TIMESTAMP || this
                            .getColumnType(i) == VoltType.VARBINARY) ? "-" : "")
                    + padding[i] + "s";
        }

        // Create the column headers
        for (int i = 0; i < columnCount; i++) {
            sb.append(String.format("%1$-" + padding[i] + "s",
                    this.getColumnName(i)));
            if (i < columnCount - 1) {
                sb.append(" ");
            }
        }
        sb.append("\n");

        // Create the separator between the column headers and the rows of data
        for (int i = 0; i < columnCount; i++) {
            char[] underline_array = new char[padding[i]];
            Arrays.fill(underline_array, '-');
            sb.append(new String(underline_array));
            if (i < columnCount - 1) {
                sb.append(" ");
            }
        }
        sb.append("\n");

        // Now display each row of data.
        this.resetRowPosition();
        while (this.advanceRow()) {
            for (int i = 0; i < columnCount; i++) {
                Object value = this.get(i, this.getColumnType(i));
                String valueStr;
                if (this.wasNull()) {
                    valueStr = "NULL";
                }
                else if (this.getColumnType(i) == VoltType.VARBINARY) {
                    valueStr = Encoder.hexEncode((byte[]) value);
                }
                else {
                    valueStr = value.toString();
                }
                // truncate long values
                if ((this.getColumnType(i) == VoltType.VARBINARY) && (valueStr.length() > MAX_PRINTABLE_CHARS)) {
                    valueStr = valueStr.substring(0, MAX_PRINTABLE_CHARS - ELIPSIS.length()) + ELIPSIS;
                }
                sb.append(String.format(fmt[i], valueStr));
                if (i < columnCount - 1) {
                    sb.append(" ");
                }
            }
            sb.append("\n");
        }

        // Idempotent. Reset the row position for the next guy...
        this.resetRowPosition();

        return sb.toString();
    }

    /**
     * Get a JSON representation of this table.
     * @return A string containing a JSON representation of this table.
     */
    @Override
    public String toJSONString() {
        JSONStringer js = new JSONStringer();
        try {

            js.object();

            // status code (1 byte)
            js.key(JSON_STATUS_KEY).value(getStatusCode());

            // column schema
            js.key(JSON_SCHEMA_KEY).array();
            for (int i = 0; i < getColumnCount(); i++) {
                js.object();
                js.key(JSON_NAME_KEY).value(getColumnName(i));
                js.key(JSON_TYPE_KEY).value(getColumnType(i).getValue());
                js.endObject();
            }
            js.endArray();

            // row data
            js.key(JSON_DATA_KEY).array();
            VoltTableRow row = cloneRow();
            row.resetRowPosition();
            while (row.advanceRow()) {
                js.array();
                for (int i = 0; i < getColumnCount(); i++) {
                    row.putJSONRep(i, js);
                }
                js.endArray();
            }
            js.endArray();

            js.endObject();
        }
        catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialized a table to JSON.", e);
        }
        return js.toString();
    }

    /**
     * Construct a table from a JSON string. Only parses VoltDB VoltTable JSON format.
     *
     * @param json String containing JSON-formatted table data.
     * @return Constructed <code>VoltTable</code> instance.
     * @throws JSONException on JSON-related error.
     * @throws IOException if thrown by our JSON library.
     */
    public static VoltTable fromJSONString(String json) throws JSONException, IOException {
        JSONObject jsonObj = new JSONObject(json);
        return fromJSONObject(jsonObj);
    }

    /**
     * <p>Construct a table from a JSON object. Only parses VoltDB VoltTable JSON format.</p>
     *
     * @param json String containing JSON-formatted table data.
     * @return Constructed <code>VoltTable</code> instance.
     * @throws JSONException on JSON-related error.
     * @throws IOException if thrown by our JSON library.
     */
    public static VoltTable fromJSONObject(JSONObject json) throws JSONException, IOException {
        // extract the schema and creat an empty table
        JSONArray jsonCols = json.getJSONArray(JSON_SCHEMA_KEY);
        ColumnInfo[] columns = new ColumnInfo[jsonCols.length()];
        for (int i = 0; i < jsonCols.length(); i++) {
            JSONObject jsonCol = jsonCols.getJSONObject(i);
            String name = jsonCol.getString(JSON_NAME_KEY);
            VoltType type = VoltType.get((byte) jsonCol.getInt(JSON_TYPE_KEY));
            columns[i] = new ColumnInfo(name, type);
        }
        VoltTable t = new VoltTable(columns);

        // set the status byte
        byte status = (byte) json.getInt(JSON_STATUS_KEY);
        t.setStatusCode(status);

        // load the row data
        JSONArray data = json.getJSONArray(JSON_DATA_KEY);
        for (int i = 0; i < data.length(); i++) {
            JSONArray jsonRow = data.getJSONArray(i);
            assert(jsonRow.length() == jsonCols.length());
            Object[] row = new Object[jsonRow.length()];
            for (int j = 0; j < jsonRow.length(); j++) {
                row[j] = jsonRow.get(j);
                if (row[j] == JSONObject.NULL)
                    row[j] = null;
                VoltType type = columns[j].type;

                // convert strings to numbers
                if (row[j] != null) {
                    switch (type) {
                    case BIGINT:
                    case INTEGER:
                    case SMALLINT:
                    case TINYINT:
                    case TIMESTAMP:
                        if (row[j] instanceof String) {
                            row[j] = Long.parseLong((String) row[j]);
                        }
                        assert(row[j] instanceof Number);
                        break;
                    case DECIMAL:
                        String decVal;
                        if (row[j] instanceof String)
                            decVal = (String) row[j];
                        else
                            decVal = row[j].toString();
                        if (decVal.compareToIgnoreCase("NULL") == 0)
                            row[j] = null;
                        else
                            row[j] = VoltDecimalHelper.deserializeBigDecimalFromString(decVal);
                        break;
                    default:
                        // empty fallthrough to make the warning go away
                    }
                }
            }
            t.addRow(row);
        }

        return t;
    }

    /**
     * Check to see if this table has the same contents as the provided table.
     * This is not {@link java.lang.Object#equals(Object)} because we don't
     * want to provide all of the additional contractual requirements that go
     * along with it, such as implementing {@link java.lang.Object#hashCode()}.
     *
     * @param other Table to compare to.
     * @return Whether the tables have the same contents.
     */
    public boolean hasSameContents(VoltTable other) {
        assert(verifyTableInvariants());
        if (this == other) return true;

        int mypos = m_buffer.position();
        int theirpos = other.m_buffer.position();
        if (mypos != theirpos) {
            return false;
        }
        long checksum1 = ClientUtils.cheesyBufferCheckSum(m_buffer);
        long checksum2 = ClientUtils.cheesyBufferCheckSum(other.m_buffer);
        boolean checksum = (checksum1 == checksum2);
        assert(verifyTableInvariants());
        return checksum;
    }

    /**
     *  An unreliable version of {@link java.lang.Object#equals(Object)} that should not be used. Only
     *  present for unit testing.
     *
     *  @deprecated Exists for unit testing, but probably shouldn't be called.
     */
    @Deprecated
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VoltTable)) return false;
        return hasSameContents((VoltTable) o);
    }

    /**
     * Also overrides {@link java.lang.Object#hashCode()}  since we are overriding {@link java.lang.Object#equals(Object)}.
     * Throws an {@link java.lang.UnsupportedOperationException}.
     *
     * @deprecated This only throws. Doesn't do anything.
     * @throws UnsupportedOperationException if called.
     */
    @Deprecated
    @Override
    public int hashCode() {
        // TODO(evanj): When overriding equals, we should also override hashCode. I don't want to
        // implement this right now, since VoltTables should never be used as hash keys anyway.
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * <p>Generates a duplicate of a table including the column schema. Only works
     * on tables that have no rows, have columns defined, and will not have columns added/deleted/modified
     * later. Useful as way of creating template tables that can be cloned and then populated with
     * {@link VoltTableRow rows} repeatedly.</p>
     *
     * @param extraBytes The number of extra bytes to leave for to-be-added rows beyond the header.
     * @return An <tt>VoltTable</tt> with the same column schema as the original and enough space
     *         for the specified number of {@link VoltTableRow rows} and strings.
     */
    public final VoltTable clone(int extraBytes) {
        assert(verifyTableInvariants());
        // share the immutable metadata if it's present for tests
        final VoltTable cloned = new VoltTable(m_extraMetadata);
        cloned.m_colCount = m_colCount;
        cloned.m_rowCount = 0;
        cloned.m_rowStart = m_rowStart;

        final int pos = m_buffer.position();
        m_buffer.position(0);
        m_buffer.limit(m_rowStart);
        // the 100 is for extra safety
        cloned.m_buffer = ByteBuffer.allocate(m_rowStart + extraBytes + 100);
        cloned.m_buffer.put(m_buffer);
        m_buffer.limit(m_buffer.capacity());
        m_buffer.position(pos);

        cloned.m_buffer.putInt(0);
        assert(verifyTableInvariants());
        assert(cloned.verifyTableInvariants());
        return cloned;
    }

    boolean testForUTF8Encoding(byte strbytes[]) {
        try {
            // this doesn't prove definitively that the string is UTF-8
            // but will find many cases...
            new String(strbytes, "UTF-8");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return true;
    }

    private final void writeStringToBuffer(String s, Charset encoding, ByteBuffer b) {
        if (s == null) {
            b.putInt(NULL_STRING_INDICATOR);
            return;
        }

        int len = 0;
        byte[] strbytes = s.getBytes(encoding);
        assert (strbytes != null);
        len = strbytes.length;
        b.putInt(len);
        b.put(strbytes);
    }

    private final void writeStringOrVarbinaryToBuffer(byte[] s, ByteBuffer b) {
        if (s == null) {
            b.putInt(NULL_STRING_INDICATOR);
            return;
        }

        int len = s.length;
        b.putInt(len);
        b.put(s);
    }

    @Override
    public final int getRowCount() {
        return m_rowCount;
    }

    @Override
    protected final int getRowStart() {
        return m_rowStart;
    }

    @Override
    public final int getColumnCount() {
        return m_colCount;
    }

    private final boolean verifyTableInvariants() {
        assert(m_buffer != null);
        assert(m_rowCount >= 0);
        assert(m_colCount > 0);
        assert(m_rowStart > 0);
        // minimum reasonable table size
        final int minsize = 4 + 1 + 2 + 1 + 4 + 4;
        assert(m_buffer.capacity() >= minsize);
        assert(m_buffer.limit() >= minsize);
        if (m_buffer.position() < minsize) {
            System.err.printf("Buffer position %d is smaller than it should be.\n", m_buffer.position());
            return false;
        }

        int rowStart = m_buffer.getInt(0) + 4;
        if (rowStart < (4 + 1 + 2 + 1 + 4)) {
            System.err.printf("rowStart with value %d is smaller than it should be.\n", rowStart);
            return false;
        }
        assert(m_buffer.position() > m_rowStart);

        int colCount = m_buffer.getShort(5);
        assert(colCount > 0);

        return true;
    }

    /**
     * Set the status code associated with this table. Default value is 0.
     * @return Status code
     */
    public byte getStatusCode() {
        return m_buffer.get(4);
    }

    /**
     * Set the status code associated with this table. Default value if not set is 0
     * @param code Status code to set
     */
    public void setStatusCode(byte code) {
        m_buffer.put( 4, code);
    }

    /**
     * Get the serialized size in bytes of this table. This is used mostly internally by
     * VoltDB for table serialization purposes.
     *
     * @return The size in bytes.
     */
    public int getSerializedSize() {
        return m_buffer.limit() + 4;
    }

    /**
     * <p>Serialize this table to a given ByteBuffer. Used mostly internally by VoltDB for
     * moving tables over the network.</p>
     *
     * @param buf Buffer to serialize table to.
     */
    public void flattenToBuffer(ByteBuffer buf) {
        ByteBuffer dup = m_buffer.duplicate();
        buf.putInt(dup.limit());
        dup.position(0);
        buf.put(dup);
    }

    void initFromBuffer(ByteBuffer buf) {
        // Note: some of the snapshot and save/restore code makes assumptions
        // about the binary layout of tables.

        final int len = buf.getInt();
        // smallest table is 4-bytes with zero value
        // indicating rowcount is 0
        assert(len >= 4);

        int startLimit = buf.limit();
        buf.limit(buf.position() + len);
        m_buffer = buf.slice().asReadOnlyBuffer();
        buf.limit(startLimit);
        buf.position(buf.position() + len);

        m_buffer.position(m_buffer.limit());

        // rowstart represents and offset to the start of row data,
        //  but the serialization is the non-inclusive length of the header,
        //  so add two bytes.
        m_rowStart = m_buffer.getInt(0) + 4;

        m_colCount = m_buffer.getShort(5);
        m_rowCount = m_buffer.getInt(m_rowStart);

        assert(verifyTableInvariants());
    }

    /**
     * Directly access the table's underlying {@link ByteBuffer}. This should be avoided if
     * possible by end users, as there is potential to really mess stuff up. VoltDB mostly
     * uses it to compute various checksums quickly.
     *
     * @return The underlying {@link ByteBuffer} instance.
     */
    public ByteBuffer getBuffer() {
        ByteBuffer buf = m_buffer.asReadOnlyBuffer();
        buf.position(0);
        return buf;
    }

    /**
     * Get the schema of the table. Can be fed into another table's constructor.
     *
     * @return An ordered array of {@link ColumnInfo} instances for each table column.
     */
    public ColumnInfo[] getTableSchema()
    {
        ColumnInfo[] schema = new ColumnInfo[m_colCount];
        for (int i = 0; i < m_colCount; i++) {
            ColumnInfo col = new ColumnInfo(getColumnName(i), getColumnType(i));
            schema[i] = col;
        }
        return schema;
    }
}
