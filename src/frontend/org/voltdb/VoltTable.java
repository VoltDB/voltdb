/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.zip.CRC32;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.common.Constants;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
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
[byte: table status code]
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

    boolean m_readOnly = false;
    int m_rowStart = -1; // the beginning of the row data (points to before the row count int)
    int m_rowCount = -1;
    int m_colCount = -1;

    // non-positive value that probably shouldn't be -1 to avoid off-by-one errors
    private static final int NO_MEMOIZED_ROW_OFFSET = Integer.MIN_VALUE;

    // memoized offsets used when users iterate rows with fetchrow()
    private int m_memoizedRowOffset = NO_MEMOIZED_ROW_OFFSET;
    private int m_memoizedBufferOffset;

    // cache column indexes for getColumnIndex()
    private HashMap<String,Integer> m_columnNamesMap;

    // cache previous index and position for getColumnName()
    private int m_lastColumnNamePosition;
    private int m_lastColumnNameIndex = 0;


    // JSON KEYS FOR SERIALIZATION
    static final String JSON_NAME_KEY = "name";
    static final String JSON_TYPE_KEY = "type";
    static final String JSON_SCHEMA_KEY = "schema";
    static final String JSON_DATA_KEY = "data";
    static final String JSON_STATUS_KEY = "status";

    // Positions of data in header
    private static final int POS_COL_COUNT = Integer.SIZE / Byte.SIZE + 1;
    private static final int POS_COL_TYPES = POS_COL_COUNT + Short.SIZE / Byte.SIZE;

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
                   String defaultValue) {
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
            }
            catch (CloneNotSupportedException e) {
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

            if (nullable != other.nullable) {
                return false;
            }
            if (unique != other.unique) {
                return false;
            }
            if (defaultValue != other.defaultValue) {
                return false;
            }
            if (size != other.size) {
                return false;
            }
            if (type != other.type) {
                return false;
            }
            return name.equals(other.name);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{ name=" + name + ", type=" + type + " }";
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
                      ColumnInfo... originalColumnInfos) {
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

        m_colCount = m_buffer.getShort(POS_COL_COUNT);
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
     * get a not fully constructed and validated VoltTable.
     * DO NOT use it for production
     * @param buf The buffer containing VoltTable buffer.
     * @return VoltTable instance
     */
    public static VoltTable getInstanceForTest(ByteBuffer buf) {
        return new VoltTable(buf);
    }

    /**
     * This VoltTable is not fully instantiated/validated and is for test only.
     */
    private VoltTable(ByteBuffer backing) {
        // no test metadata when creating tables from buffers
        m_extraMetadata = null;
        m_buffer = backing;
        m_rowStart = m_buffer.getInt(0) + 4;
        m_colCount = m_buffer.getShort(POS_COL_COUNT);
        m_buffer.position(m_buffer.limit());
        m_readOnly = true;
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

                for (int i = 0; i < columnCount; i++) {
                    m_buffer.put(columns[i].type.getValue());
                }
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
        m_activeRowIndex = INVALID_ROW_INDEX;
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
        if (m_offsets != null) {
            for (int i = 0; i < m_colCount; i++) {
                retval.m_offsets[i] = m_offsets[i];
            }
        }
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
        public int getColumnCount() {
            return VoltTable.this.getColumnCount();
        }

        @Override
        public int getColumnIndex(String columnName) {
            return VoltTable.this.getColumnIndex(columnName);
        }

        @Override
        public VoltType getColumnType(int columnIndex) {
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
            for (int i = 0; i < m_colCount; i++) {
                retval.m_offsets[i] = m_offsets[i];
            }
            retval.m_activeRowIndex = m_activeRowIndex;
            return retval;
        }

        @Override
        byte[] getSchemaString() {
            // just get the schema string from the table for this row
            return VoltTable.this.getSchemaString();
        }
    }

    // memo-ize response
    byte[] m_schemaString = null;
    @Override
    byte[] getSchemaString() {
        // return memo-ized response
        if (m_schemaString != null) {
            return m_schemaString;
        }

        // just read the bytes for column types from the buffer into an array
        m_schemaString = new byte[m_colCount];
        int pos = m_buffer.position();
        m_buffer.position(POS_COL_TYPES);
        m_buffer.get(m_schemaString);
        m_buffer.position(pos);
        return m_schemaString;
    }

    /**
     * Return the name of the column with the specified index.
     * @param index Index of the column
     * @return Name of the column with the specified index.
     */
    public final String getColumnName(int index) {
        assert(verifyTableInvariants());
        if ((index < 0) || (index >= m_colCount)) {
            throw new IllegalArgumentException("Not a valid column index.");
        }

        // move to the start of the list of column names
        int pos = POS_COL_TYPES + m_colCount;
        int startIndex = 0;

        if (index >= m_lastColumnNameIndex && m_lastColumnNameIndex > 0) {
            startIndex = m_lastColumnNameIndex;
            pos = m_lastColumnNamePosition;
        }

        for (int i = startIndex; i < index; i++) {
            pos += m_buffer.getInt(pos) + 4;
        }

        // read column name string
        String name = readString(pos, METADATA_ENCODING);
        assert(name != null);

        assert(verifyTableInvariants());

        m_lastColumnNamePosition = pos;
        m_lastColumnNameIndex = index;

        return name;
    }

    @Override
    public final VoltType getColumnType(int index) {
        assert(verifyTableInvariants());
        if (index < 0 || index >= m_colCount) {
            throw new IllegalArgumentException("Not a valid column index: " + index);
        }
        // move to the right place
        VoltType retval = VoltType.get(m_buffer.get(POS_COL_TYPES + index));

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

        if (m_columnNamesMap == null) {
            m_columnNamesMap = new HashMap<>(m_colCount,1.0F);
        }
        String upperName = name.toUpperCase();
        Integer columnIndex = m_columnNamesMap.get(upperName);
        if (columnIndex == null) { // name not in map
            for (int i = m_columnNamesMap.size(); i < m_colCount; i++) {
                String n = getColumnName(i).toUpperCase();
                m_columnNamesMap.put(n, i);
                if (n.equals(upperName)) { // stop when requested column found
                    columnIndex = i;
                    break;
                }
            }
        }
        if (columnIndex == null) {
            String msg = "No Column named '" + name + "'. Existing columns are:";
            for (int i = 0; i < m_colCount; i++) {
                msg += "[" + i + "]" + getColumnName(i) + ",";
            }
            throw new IllegalArgumentException(msg);
        }
        return columnIndex;
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

        // check bounds
        if ((index < 0) || (index >= m_rowCount)) {
            throw new IndexOutOfBoundsException("index = " + index + "; rows = " + m_rowCount);
        }

        // if no memoized value or looking in front of the memoized value, reset
        if ((m_memoizedRowOffset == NO_MEMOIZED_ROW_OFFSET) || (index < m_memoizedRowOffset)) {
            m_memoizedRowOffset = 0; m_memoizedBufferOffset = m_rowStart + ROW_COUNT_SIZE;
        }

        while (m_memoizedRowOffset < index) {
            // add 4 bytes as the row size is non-inclusive
            m_memoizedBufferOffset += m_buffer.getInt(m_memoizedBufferOffset) + ROW_HEADER_SIZE;
            m_memoizedRowOffset++;
        }
        Row retval = new Row(m_memoizedBufferOffset + ROW_HEADER_SIZE);
        retval.m_activeRowIndex = index;
        return retval;
    }

    /**
     * Internal method to add a single value to a table, shared by the multiple
     * ways to add rows.
     */
    private void addColumnValue(Object value, VoltType columnType, int col) {
        // schema checking code that is used for some tests
        boolean allowNulls = true;
        int maxColSize = VoltType.MAX_VALUE_LENGTH;
        if (m_extraMetadata != null) {
            allowNulls = m_extraMetadata.originalColumnInfos[col].nullable;
            maxColSize = m_extraMetadata.originalColumnInfos[col].size;
        }

        if (VoltType.isVoltNullValue(value)) {
            // schema checking code that is used for some tests
            // allowNulls should always be true in production
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
            case GEOGRAPHY:
            case VARBINARY:
                m_buffer.putInt(NULL_STRING_INDICATOR);
                break;
            case GEOGRAPHY_POINT:
                GeographyPointValue.serializeNull(m_buffer);
                break;
            case DECIMAL:
                VoltDecimalHelper.serializeNull(m_buffer);
                break;

            default:
                throw new VoltTypeException("Unsupported type: " +
                        columnType);
            }
        }
        else {

            // Allow implicit conversions across all numeric types
            // except BigDecimal and anything else. Require BigDecimal
            // and reject Long128. Convert byte[] to VoltType.STRING.
            // Allow longs to be converted to VoltType.TIMESTAMPS.

            // In all error paths, catch ClassCastException
            // and VoltTypeException to restore
            // the correct table state.
            // XXX consider adding a fast path that checks for
            // equivalent types of input and column

            try {
                switch (columnType) {
                case TINYINT:
                    if (value instanceof BigDecimal) {
                        throw new ClassCastException();
                    }
                    final Number n1 = (Number) value;
                    if (columnType.wouldCastOverflow(n1)) {
                        throw new VoltTypeException("Cast of " +
                                n1.doubleValue() +
                                " to " +
                                columnType.toString() +
                                " would overflow");
                    }
                    m_buffer.put(n1.byteValue());
                    break;
                case SMALLINT:
                    if (value instanceof BigDecimal) {
                        throw new ClassCastException();
                    }
                    final Number n2 = (Number) value;
                    if (columnType.wouldCastOverflow(n2)) {
                        throw new VoltTypeException("Cast to " +
                                columnType.toString() +
                                " would overflow");
                    }
                    m_buffer.putShort(n2.shortValue());
                    break;
                case INTEGER:
                    if (value instanceof BigDecimal) {
                        throw new ClassCastException();
                    }
                    final Number n3 = (Number) value;
                    if (columnType.wouldCastOverflow(n3)) {
                        throw new VoltTypeException("Cast to " +
                                columnType.toString() +
                                " would overflow");
                    }
                    m_buffer.putInt(n3.intValue());
                    break;
                case BIGINT:
                    if (value instanceof BigDecimal) {
                        throw new ClassCastException();
                    }
                    final Number n4 = (Number) value;
                    if (columnType.wouldCastOverflow(n4)) {
                        throw new VoltTypeException("Cast to " +
                                columnType.toString() +
                                " would overflow");
                    }
                    m_buffer.putLong(n4.longValue());
                    break;

                case FLOAT:
                    if (value instanceof BigDecimal) {
                        throw new ClassCastException();
                    }
                    final Number n5 = (Number) value;
                    if (columnType.wouldCastOverflow(n5)) {
                        throw new VoltTypeException("Cast to " +
                                columnType.toString() +
                                " would overflow");
                    }
                    m_buffer.putDouble(n5.doubleValue());
                    break;

                case STRING: {
                    // Accept byte[] and String
                    if (value instanceof byte[]) {
                        if (((byte[]) value).length > maxColSize) {
                            throw new VoltOverflowException(
                                    "Value in VoltTable.addRow(...) larger than allowed max " +
                                            VoltType.humanReadableSize(maxColSize));
                        }

                        // bytes MUST be a UTF-8 encoded string.
                        assert(testForUTF8Encoding((byte[]) value));
                        writeStringOrVarbinaryToBuffer((byte[]) value, m_buffer);
                    }
                    else {
                        if (((String) value).length() > maxColSize) {
                            throw new VoltOverflowException(
                                    "Value in VoltTable.addRow(...) larger than allowed max " +
                                            VoltType.humanReadableSize(maxColSize));
                        }

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
                        if (((byte[]) value).length > maxColSize) {
                            throw new VoltOverflowException(
                                    "Value in VoltTable.addRow(...) larger than allowed max " +
                                            VoltType.humanReadableSize(maxColSize));
                        }
                        writeStringOrVarbinaryToBuffer((byte[]) value, m_buffer);
                    }
                    else {
                        throw new ClassCastException();
                    }
                    break;
                }

                case GEOGRAPHY: {
                    GeographyValue gv = (GeographyValue)value;
                    m_buffer.putInt(gv.getLengthInBytes());
                    gv.flattenToBuffer(m_buffer);
                    break;
                }

                case GEOGRAPHY_POINT: {
                    GeographyPointValue pt = (GeographyPointValue)value;
                    pt.flattenToBuffer(m_buffer);
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
                    }
                    else {
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
            catch (ClassCastException cce) {
                throw new VoltTypeException("Value for column " + col + " (" +
                        getColumnName(col) + ") is type " +
                        value.getClass().getSimpleName() + " when type " + columnType +
                        " was expected.");
            }
        }
    }

    /**
     * Append a {@link VoltTableRow row} from another <tt>VoltTable</tt>
     * to this VoltTable instance. Technically, it could be from the same
     * table, but this isn't the common usage.
     * @param row {@link VoltTableRow Row} to add.
     */
    public final void add(VoltTableRow row) {
        assert(verifyTableInvariants());
        if (m_readOnly) {
            throw new IllegalStateException("Table is read-only. Make a copy before changing.");
        }
        if (m_colCount == 0) {
            throw new IllegalStateException("Table has no columns defined");
        }
        if (row.getColumnCount() != m_colCount) {
            throw new IllegalArgumentException(row.getColumnCount() + " arguments but table has " + m_colCount + " columns");
        }

        // memoize the start of this row in case we roll back
        final int pos = m_buffer.position();

        try {
            // Allow the buffer to grow to max capacity
            m_buffer.limit(m_buffer.capacity());

            byte[] inboundSchemaString = row.getSchemaString();
            byte[] mySchemaString = getSchemaString();

            // The way this works is that when two schema strings are found to have
            // the same value, the target table's reference is pointed at the source
            // table's reference. This allows the copying of multiple rows from one
            // table to another to only do a deep comparison once, and to do reference
            // equivalence checks for subsequent rows.
            boolean canDoRawCopy = (inboundSchemaString == mySchemaString) ||
                    Arrays.equals(inboundSchemaString, mySchemaString);

            if (canDoRawCopy) {
                // make them the same object if equal for faster comparison next time
                m_schemaString = inboundSchemaString;

                // raw blit the row (assume the row is valid with proper length)
                ByteBuffer rawRow = row.getRawRow();
                m_buffer.put(rawRow);
            }
            else {
                // advance the row size value
                m_buffer.position(pos + 4);

                for (int i = 0; i < m_colCount; i++) {
                    VoltType inboundType = row.getColumnType(i);
                    VoltType outboundType = getColumnType(i);

                    if (inboundType == outboundType) {
                        byte[] raw = row.getRaw(i);
                        m_buffer.put(raw);
                    }
                    else {
                        Object inboundValue = row.get(i, inboundType);
                        addColumnValue(inboundValue, outboundType, i);
                    }
                }

                final int rowsize = m_buffer.position() - pos - 4;
                assert(rowsize >= 0);

                // check for too big rows
                if (rowsize > VoltTableRow.MAX_TUPLE_LENGTH) {
                    throw new VoltOverflowException(
                            "Table row total length larger than allowed max " + VoltTableRow.MAX_TUPLE_LENGTH_STR);
                }

                // buffer overflow is caught and handled below.
                m_buffer.putInt(pos, rowsize);
            }

            // constrain buffer limit back to the new position
            m_buffer.limit(m_buffer.position());

            // increment the rowcount in the member var and in the buffer
            m_rowCount++;
            m_buffer.putInt(m_rowStart, m_rowCount);
        }
        catch (VoltTypeException vte) {
            // revert the row size advance and any other
            // buffer additions
            m_buffer.position(pos);
            throw vte;
        }
        catch (BufferOverflowException e) {
            m_buffer.position(pos);
            expandBuffer();
            add(row);
        }
        // row was too big, reset and rethrow
        catch (VoltOverflowException e) {
            m_buffer.position(pos);
            throw e;
        }
        catch (IllegalArgumentException e) {
            // if this was thrown because of a lack of space
            // then grow the buffer
            // the number 32 was picked out of a hat ( maybe a bug if str > 32 )
            if (m_buffer.limit() - m_buffer.position() < 32) {
                m_buffer.position(pos);
                expandBuffer();
                add(row);
            } else {
                throw e;
            }
        }
        assert(verifyTableInvariants());
    }

    /**
     * Append a new row to the table using the supplied column values.
     * @param values Values of each column in the row.
     * @throws VoltTypeException  when there are casting/type failures
     *         between an input value and the corresponding column
     */
    public final void addRow(Object... values) {
        assert(verifyTableInvariants());
        if (m_readOnly) {
            throw new IllegalStateException("Table is read-only. Make a copy before changing.");
        }
        if (m_colCount == 0) {
            throw new IllegalStateException("Table has no columns defined");
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
            int typePos = POS_COL_TYPES;

            for (int col = 0; col < m_colCount; col++) {
                Object value = values[col];
                VoltType columnType = VoltType.get(m_buffer.get(typePos + col));

                addColumnValue(value, columnType, col);
            }

            //
            // Note, there is some near-identical code in both row add methods.
            // [ add(..) and addRow(..) ]
            // If you change code below here, change it in the other method too.
            // (It would be nice to re-factor, but I couldn't make a clean go at
            //  it quickly - Hugg)
            //

            final int rowsize = m_buffer.position() - pos - 4;
            assert(rowsize >= 0);

            // check for too big rows
            if (rowsize > VoltTableRow.MAX_TUPLE_LENGTH) {
                throw new VoltOverflowException(
                        "Table row total length larger than allowed max " + VoltTableRow.MAX_TUPLE_LENGTH_STR);
            }

            // buffer overflow is caught and handled below.
            m_buffer.putInt(pos, rowsize);

            m_rowCount++;
            m_buffer.putInt(m_rowStart, m_rowCount);
        }
        catch (VoltTypeException vte) {
            // revert the row size advance and any other
            // buffer additions
            m_buffer.position(pos);
            throw vte;
        }
        catch (BufferOverflowException e) {
            m_buffer.position(pos);
            expandBuffer();
            addRow(values);
        }
        // row was too big, reset and rethrow
        catch (VoltOverflowException e) {
            m_buffer.position(pos);
            throw e;
        }
        catch (IllegalArgumentException e) {
            m_buffer.position(pos);
            // if this was thrown because of a lack of space
            // then grow the buffer
            // the number 32 was picked out of a hat ( maybe a bug if str > 32 )
            if (m_buffer.limit() - m_buffer.position() < 32) {
                expandBuffer();
                addRow(values);
            } else {
                throw e;
            }
        }
        finally {
            // constrain buffer limit back to the new position
            m_buffer.limit(m_buffer.position());
        }

        assert(verifyTableInvariants());
    }

    private final void expandBuffer() {
        expandBuffer(0);
    }

    private final void expandBuffer(int minSize) {
        int newSize = m_buffer.capacity();
        do {
            // keep doubling the newSize until it is larger than minSize
            newSize <<= 1;
        } while (newSize < minSize);

        expandBufferTo(newSize);
    }

    private void expandBufferTo(int newSize) {
        final int end = m_buffer.position();
        assert(end > m_rowStart);
        final ByteBuffer buf2 = ByteBuffer.allocate(newSize);
        m_buffer.limit(end);
        m_buffer.position(0);
        buf2.put(m_buffer);
        m_buffer = buf2;
    }

    /**
     * Tables containing a single row and a single integer column can be read using this convenience
     * method.
     * Looking at the return value is not a reliable way to check if the value
     * is <tt>null</tt>. Use {@link #wasNull()} instead.
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
            final byte tinyInt = m_buffer.get(m_rowStart + 8);
            m_wasNull = (tinyInt == VoltType.NULL_TINYINT);
            return tinyInt;
        case SMALLINT:
            final short smallInt = m_buffer.getShort(m_rowStart + 8);
            m_wasNull = (smallInt == VoltType.NULL_SMALLINT);
            return smallInt;
        case INTEGER:
            final int integer = m_buffer.getInt(m_rowStart + 8);
            m_wasNull = (integer == VoltType.NULL_INTEGER);
            return integer;
        case BIGINT:
            final long bigInt = m_buffer.getLong(m_rowStart + 8);
            m_wasNull = (bigInt == VoltType.NULL_BIGINT);
            return bigInt;
        default:
            throw new IllegalStateException(
                    "table must contain exactly 1 integral value; column 1 is type = " + colType.name());
        }
    }

    /**
     * Add all rows from {@code other} into this VoltTable. Both tables must have the exact same schema including column
     * names.
     *
     * @param other {@link VoltTable} to add to this table
     * @throws IllegalArgumentException if {@code other} is not compatible with this table
     */
    public void addTable(VoltTable other) {
        if (m_readOnly) {
            throw new IllegalStateException("Table is read-only. Make a copy before changing.");
        }

        checkHasExactSchema(other);

        // Allow the buffer to grow to max capacity
        m_buffer.limit(m_buffer.capacity());

        ByteBuffer rawRows = other.getAllRowsRaw();
        if (m_buffer.remaining() < rawRows.remaining()) {
            expandBuffer(m_buffer.capacity() - m_buffer.remaining() + rawRows.remaining());
        }
        m_buffer.put(rawRows);

        // constrain buffer limit back to the new position
        m_buffer.limit(m_buffer.position());

        // increment the rowcount in the member var and in the buffer
        m_rowCount += other.getRowCount();
        m_buffer.putInt(m_rowStart, m_rowCount);
    }

    /**
     * Add all rows from {@code tables} into this VoltTable. All tables must have the exact same schema including column
     * names.
     *
     * @param tables {@link Collection} of {@link VoltTable}s to add to this table
     * @throws IllegalArgumentException if any table in {@code tables} is not compatible with this table
     */
    public void addTables(Collection<VoltTable> tables) {
        if (m_readOnly) {
            throw new IllegalStateException("Table is read-only. Make a copy before changing.");
        }

        try {
            // Allow the buffer to grow to max capacity
            m_buffer.limit(m_buffer.capacity());

            Collection<ByteBuffer> rawTables = new ArrayList<>(tables.size());
            int totalRawSize = 0;
            int newRowCounts = 0;
            for (VoltTable table : tables) {
                if (table == null) {
                    continue;
                }
                checkHasExactSchema(table);
                ByteBuffer rawTable = table.getAllRowsRaw();
                rawTables.add(rawTable);
                totalRawSize += rawTable.remaining();
                newRowCounts += table.getRowCount();
            }

            if (m_buffer.remaining() < totalRawSize) {
                expandBufferTo(m_buffer.position() + totalRawSize);
            }

            for (ByteBuffer rawTable : rawTables) {
                m_buffer.put(rawTable);
            }

            // increment the rowcount in the member var and in the buffer
            m_rowCount += newRowCounts;
            m_buffer.putInt(m_rowStart, m_rowCount);
        } finally {
            // constrain buffer limit back to the new position
            m_buffer.limit(m_buffer.position());
        }
    }

    private ByteBuffer getAllRowsRaw() {
        int origPosition = m_buffer.position();
        int start = m_rowStart + ROW_COUNT_SIZE;
        try {
            m_buffer.position(start);
            return m_buffer.slice();
        } finally {
            m_buffer.position(origPosition);
        }
    }

    private void checkHasExactSchema(VoltTable other) {
        if (!hasExactSchema(other)) {
            StringBuilder sb = new StringBuilder("Could not merge table with schema ");
            other.getColumnString(sb).append(" into ");
            throw new IllegalArgumentException(getColumnString(sb).toString());
        }
    }

    private boolean hasExactSchema(VoltTable other) {
        if (getColumnCount() != other.getColumnCount() || m_rowStart != other.m_rowStart) {
            return false;
        }

        // Compare header metadata to make sure column types and names are the same
        ByteBuffer myDup = m_buffer.duplicate();
        myDup.position(POS_COL_TYPES).limit(m_rowStart);
        ByteBuffer otherDup = other.m_buffer.duplicate();
        otherDup.position(POS_COL_TYPES).limit(m_rowStart);

        return myDup.equals(otherDup);
    }

    private StringBuilder getColumnString(StringBuilder buffer) {
        short colCount = m_buffer.getShort(5);
        buffer.append(" column count: ").append(colCount).append('\n');
        assert(colCount == m_colCount);

        for (int i = 0; i < m_colCount; i++) {
            buffer.append('(').append(getColumnName(i)).append(':').append(getColumnType(i).name()).append("), ");
        }

        return buffer;
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

        getColumnString(buffer);

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
                    if (r.wasNull()) {
                        buffer.append("NULL");
                    } else {
                        buffer.append(lval);
                    }
                    break;
                case FLOAT:
                    double dval = r.getDouble(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                    } else {
                        buffer.append(dval);
                    }
                    break;
                case TIMESTAMP:
                    TimestampType tstamp = r.getTimestampAsTimestamp(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (tstamp == null);
                    }
                    else {
                        buffer.append(tstamp);
                    }
                    break;
                case STRING:
                    String string = r.getString(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (string == null);
                    }
                    else {
                        buffer.append(string);
                    }
                    break;
                case VARBINARY:
                    byte[] bin = r.getVarbinary(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (bin == null);
                    }
                    else {
                        buffer.append(varbinaryToPrintableString(bin));
                    }
                    break;
                case DECIMAL:
                    BigDecimal bd = r.getDecimalAsBigDecimal(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                        assert (bd == null);
                    }
                    else {
                        buffer.append(bd.toString());
                    }
                    break;
                case GEOGRAPHY_POINT:
                    GeographyPointValue pt = r.getGeographyPointValue(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                    }
                    else {
                        buffer.append(pt.toString());
                    }
                    break;
                case GEOGRAPHY:
                    GeographyValue gv = r.getGeographyValue(i);
                    if (r.wasNull()) {
                        buffer.append("NULL");
                    }
                    else {
                        buffer.append(gv.toString());
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
     * Make a printable, short string for a varbinary.
     * String includes a CRC and the contents of the varbinary in hex.
     * Contents longer than 13 chars are truncated and elipsized.
     * Yes, "elipsized" is totally a word.
     *
     * Example: "bin[crc:1298399436,value:0xABCDEF12345...]"
     *
     * @param bin The bytes to print out.
     * @return A string representation that is printable and short.
     */
    public static String varbinaryToPrintableString(byte[] bin) {
        PureJavaCrc32 crc = new PureJavaCrc32();
        StringBuilder sb = new StringBuilder();
        sb.append("bin[crc:");
        crc.update(bin);
        sb.append(crc.getValue());
        sb.append(",value:0x");
        String hex = Encoder.hexEncode(bin);
        if (hex.length() > 13) {
            sb.append(hex.substring(0, 10));
            sb.append("...");
        }
        else {
            sb.append(hex);
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Return a "pretty print" representation of this table with column names.  Output will
     * be formatted in a tabular textual format suitable for display.
     * @return A string containing a pretty-print formatted representation of this table.
     */
    public String toFormattedString() {
        return toFormattedString(true);
    }

    /**
     * Return a "pretty print" representation of this table with or without column names.
     * Output will be formatted in a tabular textual format suitable for display.
     * @param includeColumnNames Flag to control if column names should be included or not.
     * @return A string containing a pretty-print formatted representation of this table.
     */
    public String toFormattedString(boolean includeColumnNames) {

        final int MAX_PRINTABLE_CHARS = 90;
        // chose print width for geography column such that it can print polygon in
        // aligned manner with geography column for a polygon up to:
        // a polygon composed of 4 vertices + 1 repeat vertex,
        // one ring, each coordinate of vertex having 5 digits space including the sign of lng/lat
        final int MAX_PRINTABLE_CHARS_GEOGRAPHY = 74;

        final String ELLIPSIS = "...";
        final String DECIMAL_FORMAT = "%01.12f";

        StringBuffer sb = new StringBuffer();

        int columnCount = getColumnCount();
        int[] padding = new int[columnCount];
        String[] fmt = new String[columnCount];
        // start with minimum padding based on length of column names. this gets
        // increased later as needed
        for (int i = 0; i < columnCount; i++) {
            padding[i] = getColumnName(i).length(); // min value to be increased later
        }
        resetRowPosition();

        // Compute the padding needed for each column of the table (note: must
        // visit every row)
        while (advanceRow()) {
            for (int i = 0; i < columnCount; i++) {
                VoltType colType = getColumnType(i);
                Object value = get(i, colType);
                int width;
                if (wasNull()) {
                    width = 4;
                }
                else if (colType == VoltType.DECIMAL) {
                    BigDecimal bd = (BigDecimal) value;
                    String valueStr = String.format(DECIMAL_FORMAT, bd.doubleValue());
                    width = valueStr.length();
                }
                // crop long strings and such
                else {
                    if (colType == VoltType.VARBINARY) {
                        width = ((byte[]) value).length * 2;
                    }
                    else {
                        width = value.toString().length();
                    }
                    if ( ((colType == VoltType.GEOGRAPHY) && (width > MAX_PRINTABLE_CHARS_GEOGRAPHY)) ||
                         ((colType != VoltType.GEOGRAPHY) && (width > MAX_PRINTABLE_CHARS)) ) {
                        width = (colType == VoltType.GEOGRAPHY) ? MAX_PRINTABLE_CHARS_GEOGRAPHY : MAX_PRINTABLE_CHARS;
                    }
                }

                // Adjust the max width for each column
                if (width > padding[i]) {
                    padding[i] = width;
                }
            }
        }

        String pad = ""; // no pad before first column header.
        // calculate formating space based on columns.
        // Append column names and separator line to buffer
        for (int i = 0; i < columnCount; i++) {
            padding[i] += 1;
            // Determine the formatting string for each column
            VoltType colType = getColumnType(i);
            String justification = (colType.isVariableLength() ||
                    colType == VoltType.TIMESTAMP ||
                    colType == VoltType.GEOGRAPHY_POINT) ? "-" : "";
            fmt[i] = "%1$" + justification + padding[i] + "s";

            if (includeColumnNames) {
                // Serialize the column headers
                sb.append(pad).append(String.format("%1$-" + padding[i] + "s",
                        getColumnName(i)));
                pad = " ";
            }
        }

        if (includeColumnNames) {
            // construct separator to be used between column name header and table values
            sb.append("\n");

            // Serialize the separator between the column headers and the rows of data
            pad = "";
            for (int i = 0; i < columnCount; i++) {
                char[] underline_array = new char[padding[i]];
                Arrays.fill(underline_array, '-');
                sb.append(pad).append(new String(underline_array));
                pad = " ";
            }
            sb.append("\n");
        }

        // Serialize each formatted row of data.
        resetRowPosition();
        while (advanceRow()) {
            pad = "";
            for (int i = 0; i < columnCount; i++) {
                VoltType colType = getColumnType(i);
                Object value = get(i, colType);
                String valueStr;
                if (wasNull()) {
                    valueStr = "NULL";
                }
                else if (colType == VoltType.DECIMAL) {
                    BigDecimal bd = (BigDecimal) value;
                    valueStr = String.format(DECIMAL_FORMAT, bd.doubleValue());
                }
                else {
                    if (colType == VoltType.VARBINARY) {
                        valueStr = Encoder.hexEncode((byte[]) value);
                        // crop long varbinaries
                        if (valueStr.length() > MAX_PRINTABLE_CHARS) {
                            valueStr = valueStr.substring(0, MAX_PRINTABLE_CHARS - ELLIPSIS.length()) + ELLIPSIS;
                        }
                    }
                    else {
                        valueStr = value.toString();
                    }
                }
                sb.append(pad).append(String.format(fmt[i], valueStr));
                pad = " ";
            }
            sb.append("\n");
        }

        // Idempotent. Reset the row position for the next guy...
        resetRowPosition();

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
            js.keySymbolValuePair(JSON_STATUS_KEY, getStatusCode());

            // column schema
            js.key(JSON_SCHEMA_KEY).array();
            for (int i = 0; i < getColumnCount(); i++) {
                js.object();
                js.keySymbolValuePair(JSON_NAME_KEY, getColumnName(i));
                js.keySymbolValuePair(JSON_TYPE_KEY, getColumnType(i).getValue());
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
     * Get a JSON /api/2.0/ representation of this table.
     *
     * @param js {@link JSONStringer} instance to add this table to
     * @return A JSONStringer containing representation of this table.
     * @throws JSONException If there was an error generating the JSON
     */
    public JSONStringer toJSONStringerV2(JSONStringer js) throws JSONException {
        // array of row data
        VoltTableRow row = cloneRow();
        row.resetRowPosition();
        js.array();
        while (row.advanceRow()) {
            js.object();
            for (int i = 0; i < getColumnCount(); i++) {

                js.key(getColumnName(i));
                row.putJSONRep(i, js);
            }
            js.endObject();
        }
        js.endArray();
        return js;
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
                if (row[j] == JSONObject.NULL) {
                    row[j] = null;
                }
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
                        if (row[j] instanceof String) {
                            decVal = (String) row[j];
                        } else {
                            decVal = row[j].toString();
                        }
                        if (decVal.compareToIgnoreCase("NULL") == 0) {
                            row[j] = null;
                        } else {
                            row[j] = VoltDecimalHelper.deserializeBigDecimalFromString(decVal);
                        }
                        break;
                    case FLOAT:
                        if (row[j] instanceof String) {
                            row[j] = Double.parseDouble((String) row[j]);
                        }
                        assert(row[j] instanceof Number);
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
        return hasSameContents(other, true);
    }

    public boolean hasSameContents(VoltTable other, boolean ignoreOrder) {
        assert(verifyTableInvariants());
        if (this == other) {
            return true;
        }

        if (m_buffer.position() != other.m_buffer.position()) {
            return false;
        }
        if (ignoreOrder) {
            return getTableCheckSum(true) == other.getTableCheckSum(true);
        } else {
            ByteBuffer thisBuffer = m_buffer.duplicate();
            thisBuffer.limit(thisBuffer.position()).rewind();
            ByteBuffer otherBuffer = other.m_buffer.duplicate();
            otherBuffer.limit(otherBuffer.position()).rewind();
            return thisBuffer.equals(otherBuffer);
        }
    }

    /**
     * Calculate a rudimentary checksum of the table. The result of this method will be the same for two tables with the
     * same rows but not necessarily in the same order.
     * <p>
     * When {@code includeHeader} is {@code false} the result of this checksum can be added to the result from another
     * table to affectively concatenate the hash from this table with another to compare across tables.
     *
     * @param includeHeader If {@code true} the table header will be included in the checksum
     * @return checksum of table
     */
    public long getTableCheckSum(boolean includeHeader) {
        ByteBuffer buffer = m_buffer.asReadOnlyBuffer();
        int limit = buffer.position();
        long hash = 0;
        // Better off using native since the most common user uses direct byte buffers
        CRC32 crc = new CRC32();

        if (includeHeader) {
            buffer.rewind().limit(m_rowStart + Integer.BYTES);
            crc.update(buffer);
            hash = crc.getValue();
        } else {
            buffer.position(m_rowStart + Integer.BYTES);
        }

        int position = buffer.position();
        for (int i = 0; i < m_rowCount; ++i) {
            buffer.limit(limit);
            int length = buffer.getInt(position) + Integer.BYTES;
            buffer.limit(position + length);
            crc.reset();
            crc.update(buffer);
            hash += crc.getValue();
            position += length;
            assert position == buffer.position();
        }

        assert limit == position && buffer.limit() == limit;
        return hash;
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
        if (!(o instanceof VoltTable)) {
            return false;
        }
        return hasSameContents((VoltTable) o);
    }

    /**
     * Also overrides {@link java.lang.Object#hashCode()} since we are
     * overriding {@link java.lang.Object#equals(Object)}.
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
     * Non-public method to duplicate a table.
     * It's possible this might be useful to end-users of VoltDB, but we should
     * talk about naming and semantics first, don't just make this public.
     */
    VoltTable semiDeepCopy() {
        assert(verifyTableInvariants());
        // share the immutable metadata if it's present for tests
        final VoltTable cloned = new VoltTable(m_extraMetadata);
        cloned.m_colCount = m_colCount;
        cloned.m_rowCount = m_rowCount;
        cloned.m_rowStart = m_rowStart;

        cloned.m_buffer = m_buffer.duplicate();
        cloned.m_activeRowIndex = m_activeRowIndex;
        cloned.m_hasCalculatedOffsets = m_hasCalculatedOffsets;
        cloned.m_memoizedBufferOffset = m_memoizedBufferOffset;
        cloned.m_memoizedRowOffset = m_memoizedRowOffset;
        cloned.m_offsets = m_offsets == null ? null : m_offsets.clone();
        cloned.m_position = m_position;
        cloned.m_schemaString = m_schemaString == null ? null : m_schemaString.clone();
        cloned.m_wasNull = m_wasNull;

        // make the new table read only
        cloned.m_readOnly = true;

        assert(verifyTableInvariants());
        assert(cloned.verifyTableInvariants());
        return cloned;
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
        }
        catch (Exception ex) {
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
        final int minsize = POS_COL_TYPES + 1 + 4 + 4;
        assert(m_buffer.capacity() >= minsize);
        assert(m_buffer.limit() >= minsize);
        if (m_buffer.position() < minsize) {
            System.err.printf("Buffer position %d is smaller than it should be.\n", m_buffer.position());
            return false;
        }

        int rowStart = m_buffer.getInt(0) + 4;
        if (rowStart < (POS_COL_TYPES + 1 + 4)) {
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

    public byte[] buildReusableDependenyResult() {
        ByteBuffer dup = m_buffer.duplicate();
        ByteBuffer responseBuf = ByteBuffer.allocate(dup.limit());
        dup.position(0);
        responseBuf.put(dup);
        return responseBuf.array();
    }

    private void initFromRawBuffer() {
        m_buffer.position(m_buffer.limit());

        // m_rowStart represents an offset to the start of row data,
        // but the serialization is the non-inclusive length of the header,
        // so add 4 bytes.
        m_rowStart = m_buffer.getInt(0) + 4;

        m_colCount = m_buffer.getShort(5);
        m_rowCount = m_buffer.getInt(m_rowStart);

        assert(verifyTableInvariants());
    }

    void initFromByteArray(byte[] byteArray, int position, int len) {
        m_buffer = ByteBuffer.wrap(byteArray, position, len).asReadOnlyBuffer();
        initFromRawBuffer();
    }

    public final void convertToHeapBuffer() {
        if (m_buffer.isDirect()) {
            // Either this was allocated by the stored procedure as a direct buffer or this
            // is cached from the EE. If the second, we need to make a copy so the EE can
            // reuse the buffer for the next stored procedure.
            ByteBuffer heapBuffer = ByteBuffer.allocate(m_buffer.limit());
            m_buffer.position(0);
            heapBuffer.put(m_buffer);
            m_buffer = heapBuffer;
        }
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
        initFromRawBuffer();
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
    public ColumnInfo[] getTableSchema() {
        ColumnInfo[] schema = new ColumnInfo[m_colCount];
        for (int i = 0; i < m_colCount; i++) {
            ColumnInfo col = new ColumnInfo(getColumnName(i), getColumnType(i));
            schema[i] = col;
        }
        return schema;
    }
}
