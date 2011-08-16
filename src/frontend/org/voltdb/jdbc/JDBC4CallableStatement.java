/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.jdbc;

import java.sql.*;
import java.lang.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Map;
import java.net.URL;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.math.BigDecimal;
import org.voltdb.*;
import org.voltdb.client.*;

public class JDBC4CallableStatement extends JDBC4PreparedStatement implements java.sql.CallableStatement
{
    JDBC4CallableStatement(JDBC4Connection connection, String sql) throws SQLException
    {
        super(connection, sql, true);
    }

    // Retrieves the value of the designated JDBC ARRAY parameter as an Array object in the Java programming language.
    public Array getArray(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC ARRAY parameter as an Array object in the Java programming language.
    public Array getArray(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC NUMERIC parameter as a java.math.BigDecimal object with as many digits to the right of the decimal point as the value contains.
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Deprecated. use getBigDecimal(int parameterIndex) or getBigDecimal(String parameterName)
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC NUMERIC parameter as a java.math.BigDecimal object with as many digits to the right of the decimal point as the value contains.
    public BigDecimal getBigDecimal(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC BLOB parameter as a Blob object in the Java programming language.
    public Blob getBlob(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC BLOB parameter as a Blob object in the Java programming language.
    public Blob getBlob(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC BIT or BOOLEAN parameter as a boolean in the Java programming language.
    public boolean getBoolean(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC BIT or BOOLEAN parameter as a boolean in the Java programming language.
    public boolean getBoolean(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC TINYINT parameter as a byte in the Java programming language.
    public byte getByte(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC TINYINT parameter as a byte in the Java programming language.
    public byte getByte(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC BINARY or VARBINARY parameter as an array of byte values in the Java programming language.
    public byte[] getBytes(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC BINARY or VARBINARY parameter as an array of byte values in the Java programming language.
    public byte[] getBytes(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated parameter as a java.io.Reader object in the Java programming language.
    public Reader getCharacterStream(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated parameter as a java.io.Reader object in the Java programming language.
    public Reader getCharacterStream(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC CLOB parameter as a java.sql.Clob object in the Java programming language.
    public Clob getClob(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC CLOB parameter as a java.sql.Clob object in the Java programming language.
    public Clob getClob(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC DATE parameter as a java.sql.Date object.
    public Date getDate(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC DATE parameter as a java.sql.Date object, using the given Calendar object to construct the date.
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC DATE parameter as a java.sql.Date object.
    public Date getDate(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC DATE parameter as a java.sql.Date object, using the given Calendar object to construct the date.
    public Date getDate(String parameterName, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC DOUBLE parameter as a double in the Java programming language.
    public double getDouble(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC DOUBLE parameter as a double in the Java programming language.
    public double getDouble(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC FLOAT parameter as a float in the Java programming language.
    public float getFloat(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC FLOAT parameter as a float in the Java programming language.
    public float getFloat(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC INTEGER parameter as an int in the Java programming language.
    public int getInt(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC INTEGER parameter as an int in the Java programming language.
    public int getInt(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC BIGINT parameter as a long in the Java programming language.
    public long getLong(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC BIGINT parameter as a long in the Java programming language.
    public long getLong(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated parameter as a java.io.Reader object in the Java programming language.
    public Reader getNCharacterStream(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated parameter as a java.io.Reader object in the Java programming language.
    public Reader getNCharacterStream(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC NCLOB parameter as a java.sql.NClob object in the Java programming language.
    public NClob getNClob(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC NCLOB parameter as a java.sql.NClob object in the Java programming language.
    public NClob getNClob(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated NCHAR, NVARCHAR or LONGNVARCHAR parameter as a String in the Java programming language.
    public String getNString(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated NCHAR, NVARCHAR or LONGNVARCHAR parameter as a String in the Java programming language.
    public String getNString(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated parameter as an Object in the Java programming language.
    public Object getObject(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Returns an object representing the value of OUT parameter parameterIndex and uses map for the custom mapping of the parameter value.
    public Object getObject(int parameterIndex, Map<String,Class<?>> map) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a parameter as an Object in the Java programming language.
    public Object getObject(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Returns an object representing the value of OUT parameter parameterName and uses map for the custom mapping of the parameter value.
    public Object getObject(String parameterName, Map<String,Class<?>> map) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC REF(<structured-type>) parameter as a Ref object in the Java programming language.
    public Ref getRef(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC REF(<structured-type>) parameter as a Ref object in the Java programming language.
    public Ref getRef(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC ROWID parameter as a java.sql.RowId object.
    public RowId getRowId(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC ROWID parameter as a java.sql.RowId object.
    public RowId getRowId(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC SMALLINT parameter as a short in the Java programming language.
    public short getShort(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC SMALLINT parameter as a short in the Java programming language.
    public short getShort(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated SQL XML parameter as a java.sql.SQLXML object in the Java programming language.
    public SQLXML getSQLXML(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated SQL XML parameter as a java.sql.SQLXML object in the Java programming language.
    public SQLXML getSQLXML(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC CHAR, VARCHAR, or LONGVARCHAR parameter as a String in the Java programming language.
    public String getString(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC CHAR, VARCHAR, or LONGVARCHAR parameter as a String in the Java programming language.
    public String getString(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC TIME parameter as a java.sql.Time object.
    public Time getTime(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC TIME parameter as a java.sql.Time object, using the given Calendar object to construct the time.
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC TIME parameter as a java.sql.Time object.
    public Time getTime(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC TIME parameter as a java.sql.Time object, using the given Calendar object to construct the time.
    public Time getTime(String parameterName, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC TIMESTAMP parameter as a java.sql.Timestamp object.
    public Timestamp getTimestamp(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC TIMESTAMP parameter as a java.sql.Timestamp object, using the given Calendar object to construct the Timestamp object.
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC TIMESTAMP parameter as a java.sql.Timestamp object.
    public Timestamp getTimestamp(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC TIMESTAMP parameter as a java.sql.Timestamp object, using the given Calendar object to construct the Timestamp object.
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of the designated JDBC DATALINK parameter as a java.net.URL object.
    public URL getURL(int parameterIndex) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the value of a JDBC DATALINK parameter as a java.net.URL object.
    public URL getURL(String parameterName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Registers the OUT parameter in ordinal position parameterIndex to the JDBC type sqlType.
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Registers the parameter in ordinal position parameterIndex to be of JDBC type sqlType.
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Registers the designated output parameter.
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Registers the OUT parameter named parameterName to the JDBC type sqlType.
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Registers the parameter named parameterName to be of JDBC type sqlType.
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Registers the designated output parameter.
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given input stream.
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given input stream, which will have the specified number of bytes.
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given input stream, which will have the specified number of bytes.
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.math.BigDecimal value.
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given input stream.
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given input stream, which will have the specified number of bytes.
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given input stream, which will have the specified number of bytes.
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Blob object.
    public void setBlob(String parameterName, Blob x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a InputStream object.
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a InputStream object.
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java boolean value.
    public void setBoolean(String parameterName, boolean x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java byte value.
    public void setByte(String parameterName, byte x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java array of bytes.
    public void setBytes(String parameterName, byte[] x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Reader object.
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Reader object, which is the given number of characters long.
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Reader object, which is the given number of characters long.
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Clob object.
    public void setClob(String parameterName, Clob x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a Reader object.
    public void setClob(String parameterName, Reader reader) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a Reader object.
    public void setClob(String parameterName, Reader reader, long length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Date value using the default time zone of the virtual machine that is running the application.
    public void setDate(String parameterName, Date x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Date value, using the given Calendar object.
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java double value.
    public void setDouble(String parameterName, double x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java float value.
    public void setFloat(String parameterName, float x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java int value.
    public void setInt(String parameterName, int x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java long value.
    public void setLong(String parameterName, long x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a Reader object.
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a Reader object.
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a java.sql.NClob object.
    public void setNClob(String parameterName, NClob value) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a Reader object.
    public void setNClob(String parameterName, Reader reader) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to a Reader object.
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given String object.
    public void setNString(String parameterName, String value) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to SQL NULL.
    public void setNull(String parameterName, int sqlType) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to SQL NULL.
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the value of the designated parameter with the given object.
    public void setObject(String parameterName, Object x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the value of the designated parameter with the given object.
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the value of the designated parameter with the given object.
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.RowId object.
    public void setRowId(String parameterName, RowId x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java short value.
    public void setShort(String parameterName, short x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.SQLXML object.
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given Java String value.
    public void setString(String parameterName, String x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Time value.
    public void setTime(String parameterName, Time x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Time value, using the given Calendar object.
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Timestamp value.
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.sql.Timestamp value, using the given Calendar object.
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the designated parameter to the given java.net.URL object.
    public void setURL(String parameterName, URL val) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves whether the last OUT parameter read had the value of SQL NULL.
    public boolean wasNull() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

}
