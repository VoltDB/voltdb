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
import org.voltdb.*;

public class JDBC4ResultSetMetaData implements java.sql.ResultSetMetaData
{
    private JDBC4ResultSet sourceResultSet;
    public JDBC4ResultSetMetaData(JDBC4ResultSet resultSet)
    {
        sourceResultSet = resultSet;
    }

    // Gets the designated column's table's catalog name.
    private static final String catalogName = "";
    public String getCatalogName(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return catalogName;
    }

    // Returns the fully-qualified name of the Java class whose instances are manufactured if the method ResultSet.getObject is called to retrieve a value from the column.
    public String getColumnClassName(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case TINYINT:
                return "java.lang.Byte";
            case SMALLINT:
                return "java.lang.Short";
            case INTEGER:
                return "java.lang.Integer";
            case BIGINT:
                return "java.lang.Long";
            case FLOAT:
                return "java.lang.Double";
            case DECIMAL:
                return "java.math.BigDecimal";
            case TIMESTAMP:
                return "java.sql.Timestamp";
            case STRING:
                return "java.lang.String";
            default:
                throw SQLError.get(SQLError.TRANSLATION_NOT_FOUND, type);
        }
    }
    // Returns the number of columns in this ResultSet object.
    public int getColumnCount() throws SQLException
    {
        sourceResultSet.checkClosed();
        return sourceResultSet.columnCount;
    }

    // Indicates the designated column's normal maximum width in characters.
    public int getColumnDisplaySize(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case TINYINT:
                return (new Byte(Byte.MIN_VALUE)).toString().length();
            case SMALLINT:
                return (new Short(Short.MIN_VALUE)).toString().length();
            case INTEGER:
                return (new Integer(Integer.MIN_VALUE)).toString().length();
            case BIGINT:
                return (new Long(Long.MIN_VALUE)).toString().length();
            case FLOAT:
                return (new Double(Double.MIN_VALUE)).toString().length();
            case DECIMAL:
                return 40;
            case TIMESTAMP:
                return 32;
            case STRING:
                return 128; // That is wrong: should be length in bytes / 3 (max bytes per char for UTF8), but we don't receive the length!
            default:
                throw SQLError.get(SQLError.TRANSLATION_NOT_FOUND, type);
        }
    }

    // Gets the designated column's suggested title for use in printouts and displays.
    public String getColumnLabel(int column) throws SQLException
    {
        return getColumnName(column);
    }

    // Get the designated column's name.
    public String getColumnName(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return sourceResultSet.table.getColumnName(column - 1);
    }

    // Retrieves the designated column's SQL type.
    public int getColumnType(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case TINYINT:
                return Types.TINYINT;
            case SMALLINT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case BIGINT:
                return Types.BIGINT;
            case FLOAT:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case STRING:
                return Types.VARCHAR;
            default:
                throw SQLError.get(SQLError.TRANSLATION_NOT_FOUND, type);
        }
    }

    // Retrieves the designated column's database-specific type name.
    public String getColumnTypeName(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DECIMAL:
                return "DECIMAL";
            case TIMESTAMP:
                return "TIMESTAMP";
            case STRING:
                return "VARCHAR";
            default:
                return "UNKNOWN";
        }
    }

    // Get the designated column's specified column size.
    public int getPrecision(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
                return 4;
            case BIGINT:
                return 8;
            case FLOAT:
                return 8;
            case DECIMAL:
                return 38;
            case TIMESTAMP:
                return 32;
            case STRING:
                return 128; // That is wrong: should be length in bytes / 3 (max bytes per char for UTF8), but we don't receive the length!
            default:
                throw SQLError.get(SQLError.TRANSLATION_NOT_FOUND, type);
        }
    }

    // Gets the designated column's number of digits to right of the decimal point.
    public int getScale(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case DECIMAL:
                return 12;
            default:
                return 0;
        }
    }

    // Get the designated column's table's schema.
    private static final String schemaName = "";
    public String getSchemaName(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return schemaName; // No such notion with engine
    }

    // Gets the designated column's table name.
    private static final String tableName = "Resultset";
    public String getTableName(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return tableName;  // No way to know without parsing the source SQL for joins or the detai;s of the procedure itself - this information is not in the metadata returned!
    }

    // Indicates whether the designated column is automatically numbered.
    public boolean isAutoIncrement(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return false; // Not supported by DB engine.
    }

    // Indicates whether a column's case matters.
    public boolean isCaseSensitive(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
                return false;
            case STRING:
                return true;
            default:
                return true;
        }
    }

    // Indicates whether the designated column is a cash value.
    public boolean isCurrency(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return false;
    }

    // Indicates whether a write on the designated column will definitely succeed.
    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        return isWritable(column);
    }

    // Indicates the nullability of values in the designated column.
    public int isNullable(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return ResultSetMetaData.columnNullableUnknown;  // Not known from metadata - have to pick "unknown"
    }

    // Indicates whether the designated column is definitely not writable.
    public boolean isReadOnly(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return true; // Always: resultSet cannot be updated and post back to DB.
    }

    // Indicates whether the designated column can be used in a where clause.
    public boolean isSearchable(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        return true;
    }

    // Indicates whether values in the designated column are signed numbers.
    public boolean isSigned(int column) throws SQLException
    {
        sourceResultSet.checkColumnBounds(column);
        VoltType type = sourceResultSet.table.getColumnType(column - 1);
        switch(type)
        {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    // Indicates whether it is possible for a write on the designated column to succeed.
    public boolean isWritable(int column) throws SQLException
    {
        return !isReadOnly(column);
    }

    // Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an object that does.
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.isInstance(this);
    }

    // Returns an object that implements the given interface to allow access to non-standard methods, or standard methods not exposed by the proxy.
    public <T> T unwrap(Class<T> iface)    throws SQLException
    {
        try
        {
            return iface.cast(this);
        }
         catch (ClassCastException cce)
         {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, iface.toString());
        }
    }
}

