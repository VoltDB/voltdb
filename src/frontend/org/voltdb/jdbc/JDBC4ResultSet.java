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

package org.voltdb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

// TODO: NString, (N)Clob, AsciiStream, (N)CharacterStream all feel dubious to me.  VoltDB stores data in UTF-8 - somewhere along the lines there should be a conversion.
// TODO: Blob, Bytes, BinaryStream are debatable.  Here as well, I merely grab a varchar field's binary data.  With the internal conversion to UTF-8, this isn't going to work very well... Maybe embed Base64 encoding until binary field support is available?

// The ResultSet 'cursorPosition' is made consistent with the enum 'Position' defined in this class
// Initially the cursor points to beforeFirst position (activeRowIndex = -1) and hence cursorPosition is initialized with the beforeFirst enum value
// beforeFirst method sets the cursorPosition to beforeFirst enum value and Resets the activeRowIndex value to -1
// afterLast method sets the cursorPosition to afterLast enum value BUT does not change the activeRowIndex value
// calling previous at first row sets the cursorPosition to beforeFirst enum value and activeRowIndex value to -1
// calling next at last row sets the cursorPosition to afterLast enum value and sets the activeRowIndex value equal to table 'rowCount'

public class JDBC4ResultSet implements java.sql.ResultSet {
    private final Statement statement;
    protected VoltTable table;
    protected int columnCount;
    private int fetchDirection = FETCH_FORWARD;
    private int fetchSize = 0;
    private int rowCount;
    private Position cursorPosition = Position.beforeFirst;
    private enum Position {
                beforeFirst, middle, afterLast
    }

    public JDBC4ResultSet(Statement sourceStatement, VoltTable sourceTable)
            throws SQLException {
        statement = sourceStatement;
        table = sourceTable;
        rowCount = table.getRowCount();

        try {
            columnCount = table.getColumnCount();
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    protected final void checkClosed() throws SQLException {
        if (this.isClosed())
            throw SQLError.get(SQLError.CONNECTION_CLOSED);
    }

    protected synchronized final void checkColumnBounds(int columnIndex)
            throws SQLException {
        checkClosed();
        if ((columnIndex < 1) || (columnIndex > columnCount))
            throw SQLError.get(SQLError.COLUMN_NOT_FOUND, columnIndex,
                    columnCount);
    }

    // Moves the cursor to the given row number in this ResultSet object.
    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if (rowCount == 0) {
            if (row == 0) {
               return true;
            }
            return false;
        }
        if (row == 0) {
            beforeFirst();
            return true;
        }
        if (rowCount + row < 0) {
            beforeFirst();
            return false;
        }
        if (row > rowCount) {
            cursorPosition = Position.afterLast;
            if(row == rowCount+1) {
                return true;
            }
            else {
                return false;
            }
        }
        try {
            // for negative row numbers or row numbers lesser then activeRowIndex, resetRowPosition
            // method is called and the cursor advances to the desired row from top of the table
            if(row < 0) {
                row += rowCount;
                row++;
            }
            if(table.getActiveRowIndex() > row || cursorPosition != Position.middle) {
                table.resetRowPosition();
                table.advanceToRow(0);
            }
            cursorPosition = Position.middle;
            return table.advanceToRow(row-1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Moves the cursor to the end of this ResultSet object, just after the last
    // row.
    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        cursorPosition = Position.afterLast;
    }

    // Moves the cursor to the front of this ResultSet object, just before the
    // first row.
    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        cursorPosition = Position.beforeFirst;
        table.resetRowPosition();
    }

    // Cancels the updates made to the current row in this ResultSet object.
    @Override
    public void cancelRowUpdates() throws SQLException {
        throw SQLError.noSupport();
    }

    // Clears all warnings reported on this ResultSet object.
    @Override
    public void clearWarnings() throws SQLException {
        throw SQLError.noSupport();
    }

    // Releases this ResultSet object's database and JDBC resources immediately
    // instead of waiting for this to happen when it is automatically closed.
    @Override
    public void close() throws SQLException {
        table = null;
    }

    // Deletes the current row from this ResultSet object and from the
    // underlying database.
    @Override
    public void deleteRow() throws SQLException {
        throw SQLError.noSupport();
    }

    // Maps the given ResultSet column label to its ResultSet column index.
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        try {
            return table.getColumnIndex(columnLabel) + 1;
        } catch (IllegalArgumentException iax) {
            throw SQLError.get(iax, SQLError.COLUMN_NOT_FOUND, columnLabel);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Moves the cursor to the first row in this ResultSet object.
    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (rowCount == 0) {
            return false;
        }
        if (cursorPosition != Position.middle) {
            cursorPosition = Position.middle;
        }
        try {
            if (table.getActiveRowIndex() != 0){
                table.resetRowPosition();
            }
            return table.advanceToRow(0);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as an Array object in the Java programming language.
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as an Array object in the Java programming language.
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a stream of ASCII characters.
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return new ByteArrayInputStream(
                    table.getStringAsBytes(columnIndex - 1));
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a stream of ASCII characters.
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.math.BigDecimal with full precision.
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return table.getDecimalAsBigDecimal(columnIndex - 1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Deprecated.
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.math.BigDecimal with full precision.
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    // Deprecated.
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a stream of uninterpreted bytes.
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return new ByteArrayInputStream(
                    table.getStringAsBytes(columnIndex - 1));
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a stream of uninterpreted bytes.
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a Blob object in the Java programming language.
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return new SerialBlob(table.getStringAsBytes(columnIndex - 1));
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a Blob object in the Java programming language.
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a boolean in the Java programming language.
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        // TODO: Tempting to apply a != 0 operation on numbers and
        // .equals("true") on strings, but... hacky
        try {
            return (new Long(table.getLong(columnIndex - 1))).intValue() == 1;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a boolean in the Java programming language.
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a byte in the Java programming language.
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return (new Long(table.getLong(columnIndex - 1))).byteValue();
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a byte in the Java programming language.
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a byte array in the Java programming language.
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            if (table.getColumnType(columnIndex - 1) == VoltType.STRING)
                return table.getStringAsBytes(columnIndex - 1);
            else if (table.getColumnType(columnIndex - 1) == VoltType.VARBINARY)
                return table.getVarbinary(columnIndex - 1);
            else
                throw SQLError.get(SQLError.CONVERSION_NOT_FOUND,
                        table.getColumnType(columnIndex - 1), "byte[]");
        } catch (SQLException x) {
            throw x;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a byte array in the Java programming language.
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.io.Reader object.
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            String value = table.getString(columnIndex - 1);
            if (!wasNull())
                return new StringReader(value);
            return null;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.io.Reader object.
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a Clob object in the Java programming language.
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return new SerialClob(table.getString(columnIndex - 1)
                    .toCharArray());
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a Clob object in the Java programming language.
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    // Retrieves the concurrency mode of this ResultSet object.
    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    // Retrieves the name of the SQL cursor used by this ResultSet object.
    @Override
    public String getCursorName() throws SQLException {
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Date object in the Java programming
    // language.
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            Timestamp ts = table.getTimestampAsSqlTimestamp(columnIndex - 1);
            Date result = null;
            if (ts != null) {
                result = new Date(ts.getTime());
            }
            return result;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Date object in the Java programming
    // language.
    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO: Not sure what to do here... Dates are stored however given.
        // Need to try different TZs to see what happens when retrieving the
        // data
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Date object in the Java programming
    // language.
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Date object in the Java programming
    // language.
    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a double in the Java programming language.
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return table.getDouble(columnIndex - 1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a double in the Java programming language.
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    // Retrieves the fetch direction for this ResultSet object.
    @Override
    public int getFetchDirection() throws SQLException {
        return this.fetchDirection;
    }

    // Retrieves the fetch size for this ResultSet object.
    @Override
    public int getFetchSize() throws SQLException {
        return this.fetchSize;
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a float in the Java programming language.
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return (new Double(table.getDouble(columnIndex - 1))).floatValue();
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a float in the Java programming language.
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    // Retrieves the holdability of this ResultSet object
    @Override
    public int getHoldability() throws SQLException {
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as an int in the Java programming language.
    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return (new Long(table.getLong(columnIndex - 1))).intValue();
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as an int in the Java programming language.
    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a long in the Java programming language.
    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return table.getLong(columnIndex - 1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a long in the Java programming language.
    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    // Retrieves the number, types and properties of this ResultSet object's
    // columns.
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new JDBC4ResultSetMetaData(this);
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.io.Reader object.
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            String value = table.getString(columnIndex - 1);
            if (!wasNull())
                return new StringReader(value);
            return null;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.io.Reader object.
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a NClob object in the Java programming language.
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return new JDBC4NClob(table.getString(columnIndex - 1)
                    .toCharArray());
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a NClob object in the Java programming language.
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a String in the Java programming language.
    @Override
    public String getNString(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return table.getString(columnIndex - 1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a String in the Java programming language.
    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    // Gets the value of the designated column in the current row of this
    // ResultSet object as an Object in the Java programming language.
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            VoltType type = table.getColumnType(columnIndex - 1);
            if (type == VoltType.TIMESTAMP)
                return getTimestamp(columnIndex);
            else
                return table.get(columnIndex - 1, type);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as an Object in the Java programming language.
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException {
        // TODO: bypass high-level type casting, just like the MySQL driver.
        // Could use for extended type deserialization though - to be
        // investigated
        checkColumnBounds(columnIndex);
        return getObject(columnIndex);
    }

    // Gets the value of the designated column in the current row of this
    // ResultSet object as an Object in the Java programming language.
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as an Object in the Java programming language.
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a Ref object in the Java programming language.
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a Ref object in the Java programming language.
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    // Retrieves the current row number.
    @Override
    public int getRow() throws SQLException {
        checkClosed();
        if (cursorPosition != Position.middle) {
            return 0;
        }
        try {
            return table.getActiveRowIndex() + 1;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.RowId object in the Java programming
    // language.
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.RowId object in the Java programming
    // language.
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a short in the Java programming language.
    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return (new Long(table.getLong(columnIndex - 1))).shortValue();
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a short in the Java programming language.
    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet as a java.sql.SQLXML object in the Java programming language.
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet as a java.sql.SQLXML object in the Java programming language.
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    // Retrieves the Statement object that produced this ResultSet object.
    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a String in the Java programming language.
    @Override
    public String getString(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return table.getString(columnIndex - 1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a String in the Java programming language.
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Time object in the Java programming
    // language.
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            Timestamp ts = table.getTimestampAsSqlTimestamp(columnIndex - 1);
            Time result = null;
            if (ts != null) {
                result = new Time(ts.getTime());
            }
            return result;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Time object in the Java programming
    // language.
    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Time object in the Java programming
    // language.
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Time object in the Java programming
    // language.
    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Timestamp object in the Java programming
    // language.
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            Timestamp result = table
                    .getTimestampAsSqlTimestamp(columnIndex - 1);
            return result;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Timestamp object in the Java programming
    // language.
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Timestamp object in the Java programming
    // language.
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.sql.Timestamp object in the Java programming
    // language.
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    // Retrieves the type of this ResultSet object.
    @Override
    public int getType() throws SQLException {
        return TYPE_SCROLL_INSENSITIVE;
    }

    // Deprecated. use getCharacterStream in place of getUnicodeStream
    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        throw SQLError.noSupport();
    }

    // Deprecated. use getCharacterStream instead
    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.net.URL object in the Java programming
    // language.
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        checkColumnBounds(columnIndex);
        try {
            return new URL(table.getString(columnIndex - 1));
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves the value of the designated column in the current row of this
    // ResultSet object as a java.net.URL object in the Java programming
    // language.
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    // Retrieves the first warning reported by calls on this ResultSet object.
    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw SQLError.noSupport();
    }

    // Inserts the contents of the insert row into this ResultSet object and
    // into the database.
    @Override
    public void insertRow() throws SQLException {
        throw SQLError.noSupport();
    }

    // Retrieves whether the cursor is after the last row in this ResultSet
    // object.
    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        if(cursorPosition == Position.afterLast) {
            return true;
        }
        return false;
    }

    // Retrieves whether the cursor is before the first row in this ResultSet
    // object.
    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        if(cursorPosition == Position.beforeFirst) {
            return true;
        }
        return false;
    }

    // Retrieves whether this ResultSet object has been closed.
    @Override
    public boolean isClosed() throws SQLException {
        return table == null;
    }

    // Retrieves whether the cursor is on the first row of this ResultSet
    // object.
    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        if(cursorPosition != Position.middle) {
            return false;
        }
        try {
            return table.getActiveRowIndex() == 0;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves whether the cursor is on the last row of this ResultSet object.
    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        if(cursorPosition != Position.middle) {
            return false;
        }
        try {
            return table.getActiveRowIndex() == rowCount - 1;
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Moves the cursor to the last row in this ResultSet object.
    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (rowCount == 0) {
            return false;
        }
        try {
            if (cursorPosition != Position.middle) {
                cursorPosition = Position.middle;
                table.resetRowPosition();
                table.advanceToRow(0);
            }
            return table.advanceToRow(rowCount - 1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Moves the cursor to the remembered cursor position, usually the current
    // row.
    @Override
    public void moveToCurrentRow() throws SQLException {
        checkClosed();
        // No-op - this isn't a cursor since the resultset is entirely local
    }

    // Moves the cursor to the insert row.
    @Override
    public void moveToInsertRow() throws SQLException {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Moves the cursor forward one row from its current position.
    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (cursorPosition == Position.afterLast || table.getActiveRowIndex() == rowCount - 1) {
            cursorPosition = Position.afterLast;
            return false;
        }
        if (cursorPosition == Position.beforeFirst) {
            cursorPosition = Position.middle;
        }
        try {
            return table.advanceRow();
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Moves the cursor to the previous row in this ResultSet object.
    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        if (cursorPosition == Position.afterLast) {
            return last();
        }
        if (cursorPosition == Position.beforeFirst || table.getActiveRowIndex() <= 0) {
            beforeFirst();
            return false;
        }
        try {
            int tempRowIndex = table.getActiveRowIndex();
            table.resetRowPosition();
            table.advanceToRow(0);
            return table.advanceToRow(tempRowIndex - 1);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Refreshes the current row with its most recent value in the database.
    @Override
    public void refreshRow() throws SQLException {
        throw SQLError.noSupport();
    }

    // Moves the cursor a relative number of rows, either positive or negative.
    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();

        if (rowCount == 0) {
            return false;
        }
        if (cursorPosition == Position.afterLast && rows > 0) {
            return false;
        }
        if (cursorPosition == Position.beforeFirst && rows <= 0) {
            return false;
        }
        if (table.getActiveRowIndex() + rows >= rowCount) {
            cursorPosition = Position.afterLast;
            if (table.getActiveRowIndex() + rows == rowCount) {
                return true;
            }
            return false;
        }
        try {
            // for negative row numbers, resetRowPosition method is called
            // and the cursor advances to the desired row from top of the table
            int rowsToMove = table.getActiveRowIndex() + rows;
            if (cursorPosition == Position.beforeFirst || rows < 0) {
                if(cursorPosition == Position.afterLast) {
                    rowsToMove = rowCount + rows;
                }
                else if(cursorPosition == Position.beforeFirst) {
                    rowsToMove = rows - 1;
                }
                else {
                    rowsToMove = table.getActiveRowIndex() + rows;
                }
                if(rowsToMove < 0){
                    beforeFirst();
                    return false;
                }
                table.resetRowPosition();
                table.advanceToRow(0);
            }
            cursorPosition = Position.middle;
            return table.advanceToRow(rowsToMove);
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Retrieves whether a row has been deleted.
    @Override
    public boolean rowDeleted() throws SQLException {
        throw SQLError.noSupport();
    }

    // Retrieves whether the current row has had an insertion.
    @Override
    public boolean rowInserted() throws SQLException {
        throw SQLError.noSupport();
    }

    // Retrieves whether the current row has been updated.
    @Override
    public boolean rowUpdated() throws SQLException {
        throw SQLError.noSupport();
    }

    // Gives a hint as to the direction in which the rows in this ResultSet
    // object will be processed.
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if ((direction != FETCH_FORWARD) && (direction != FETCH_REVERSE)
                && (direction != FETCH_UNKNOWN))
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, direction);
        this.fetchDirection = direction;
    }

    // Gives the JDBC driver a hint as to the number of rows that should be
    // fetched from the database when more rows are needed for this ResultSet
    // object.
    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0)
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, rows);
        this.fetchSize = rows;
    }

    // Updates the designated column with a java.sql.Array value.
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Array value.
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an ascii stream value.
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an ascii stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an ascii stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an ascii stream value.
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an ascii stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an ascii stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.math.BigDecimal value.
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.BigDecimal value.
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a binary stream value.
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a binary stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a binary stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a binary stream value.
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a binary stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a binary stream value, which will have
    // the specified number of bytes.
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x,
            long length) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Blob value.
    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given input stream.
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given input stream, which will
    // have the specified number of bytes.
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Blob value.
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given input stream.
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given input stream, which will
    // have the specified number of bytes.
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream,
            long length) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a boolean value.
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a boolean value.
    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a byte value.
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a byte value.
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a byte array value.
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a byte array value.
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value.
    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value, which will
    // have the specified number of bytes.
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value, which will
    // have the specified number of bytes.
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value.
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value, which will
    // have the specified number of bytes.
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader,
            int length) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value, which will
    // have the specified number of bytes.
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Clob value.
    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader object.
    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader object, which is the
    // given number of characters long.
    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Clob value.
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader object.
    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader object, which is the
    // given number of characters long.
    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Date value.
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Date value.
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a double value.
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a double value.
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a float value.
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a float value.
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an int value.
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an int value.
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a long value.
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a long value.
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value.
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value, which will
    // have the specified number of bytes.
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value.
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a character stream value, which will
    // have the specified number of bytes.
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader,
            long length) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.NClob value.
    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader The data will be
    // read from the stream as needed until end-of-stream is reached.
    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader object, which is the
    // given number of characters long.
    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.NClob value.
    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader object.
    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column using the given Reader object, which is the
    // given number of characters long.
    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a String value.
    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a String value.
    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a null value.
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a null value.
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an Object value.
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an Object value.
    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an Object value.
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with an Object value.
    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Ref value.
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Ref value.
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the underlying database with the new contents of the current row
    // of this ResultSet object.
    @Override
    public void updateRow() throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a RowId value.
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a RowId value.
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a short value.
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a short value.
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.SQLXML value.
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.SQLXML value.
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a String value.
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a String value.
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Time value.
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Time value.
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Timestamp value.
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Updates the designated column with a java.sql.Timestamp value.
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException {
        throw SQLError.noSupport();
    }

    // Reports whether the last column read had a value of SQL NULL.
    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        try {
            return table.wasNull();
        } catch (Exception x) {
            throw SQLError.get(x);
        }
    }

    // Returns true if this either implements the interface argument or is
    // directly or indirectly a wrapper for an object that does.
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    // Returns an object that implements the given interface to allow access to
    // non-standard methods, or standard methods not exposed by the proxy.
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, iface.toString());
        }
    }

    // Returns the underlying VoltTable
    public VoltTable getVoltTable() {
        return table;
    }

    // Retrieve the raw row data as an array
    public Object[] getRowData() throws SQLException {
        Object[] row = new Object[columnCount];
        for (int i = 1; i < columnCount + 1; i++) {
            row[i - 1] = getObject(i);
        }
        return row;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw SQLError.noSupport();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException {
        throw SQLError.noSupport();
    }
}
