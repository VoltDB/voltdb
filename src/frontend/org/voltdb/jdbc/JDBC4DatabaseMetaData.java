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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

public class JDBC4DatabaseMetaData implements java.sql.DatabaseMetaData
{
    /*
     * Two types of tables the metadata generator generates. It has to match the
     * table types generated in JdbcDatabaseMetaDataGenerator.
     */
    static final String tableTypes[] = new String[] {"TABLE",
                                                     "VIEW"};

    private final CallableStatement sysInfo;
    private final CallableStatement sysCatalog;
    private final JDBC4Connection sourceConnection;
    JDBC4DatabaseMetaData(JDBC4Connection connection) throws SQLException
    {
        this.sourceConnection = connection;
        this.sysInfo = connection.prepareCall("{call @SystemInformation}");
        this.sysCatalog = connection.prepareCall("{call @SystemCatalog(?)}");

        // Initialize system information
        loadSystemInformation();
    }

    private String buildString = null;
    private String versionString = null;
    private String catalogString = null;

    private void loadSystemInformation() throws SQLException
    {
        ResultSet res = this.sysInfo.executeQuery();
        while (res.next())
        {
            if (res.getString(2).equals("BUILDSTRING"))
                buildString = res.getString(3);
            else if (res.getString(2).equals("VERSION"))
                versionString = res.getString(3);
            else if (res.getString(2).equals("CATALOG"))
                catalogString = res.getString(3);
        }
        res.close();

    }

    private void checkClosed() throws SQLException
    {
        if (this.sourceConnection.isClosed())
            throw SQLError.get(SQLError.CONNECTION_CLOSED);
    }

    // Retrieves whether the current user can call all the procedures returned by the method getProcedures.
    @Override
    public boolean allProceduresAreCallable() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether the current user can use all the tables returned by the method getTables in a SELECT statement.
    @Override
    public boolean allTablesAreSelectable() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether a SQLException while autoCommit is true indicates that all open ResultSets are closed, even ones that are holdable.
    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether a data definition statement within a transaction forces the transaction to commit.
    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database ignores a data definition statement within a transaction.
    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether or not a visible row delete can be detected by calling the method ResultSet.rowDeleted.
    @Override
    public boolean deletesAreDetected(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether the return value for the method getMaxRowSize includes the SQL data types LONGVARCHAR and LONGVARBINARY.
    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves a description of the given attribute of the given type for a user-defined type (UDT) that is available in the given schema and catalog.
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a description of a table's optimal set of columns that uniquely identifies a row.
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the catalog names available in this database.
    @Override
    public ResultSet getCatalogs() throws SQLException
    {
        checkClosed();
        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("TABLE_CAT",VoltType.STRING));
        result.addRow(new Object[] { catalogString });
        return new JDBC4ResultSet(null, result);
    }

    // Retrieves the String that this database uses as the separator between a catalog and table name.
    @Override
    public String getCatalogSeparator() throws SQLException
    {
        return ".";
    }

    // Retrieves the database vendor's preferred term for "catalog".
    @Override
    public String getCatalogTerm() throws SQLException
    {
        checkClosed();
        return "catalog";
    }

    // Retrieves a list of the client info properties that the driver supports.
    @Override
    public ResultSet getClientInfoProperties() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a description of the access rights for a table's columns.
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a description of table columns available in the specified catalog.
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException
    {
        checkClosed();
        this.sysCatalog.setString(1, "COLUMNS");
        JDBC4ResultSet res = (JDBC4ResultSet) this.sysCatalog.executeQuery();
        VoltTable vtable = res.getVoltTable().clone(0);

        // If no pattern is specified, default to matching any/all.
        if (tableNamePattern == null || tableNamePattern.length() == 0)
        {
            tableNamePattern = "%";
        }
        Pattern table_pattern = computeJavaPattern(tableNamePattern);

        if (columnNamePattern == null || columnNamePattern.length() == 0)
        {
            columnNamePattern = "%";
        }
        Pattern column_pattern = computeJavaPattern(columnNamePattern);

        // Filter columns based on table name and column name
        while (res.next()) {
            Matcher table_matcher = table_pattern.matcher(res.getString("TABLE_NAME"));
            if (table_matcher.matches())
            {
                Matcher column_matcher = column_pattern.matcher(res.getString("COLUMN_NAME"));
                if (column_matcher.matches())
                {
                    vtable.addRow(res.getRowData());
                }
            }
        }

        return new JDBC4ResultSet(this.sysCatalog, vtable);
    }

    // Retrieves the connection that produced this metadata object.
    @Override
    public Connection getConnection() throws SQLException
    {
        checkClosed();
        return sourceConnection;
    }

    // Retrieves a description of the foreign key columns in the given foreign key table that reference the primary key or the columns representing a unique constraint of the parent table (could be the same or a different table).
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the major version number of the underlying database.
    @Override
    public int getDatabaseMajorVersion() throws SQLException
    {
        checkClosed();
        System.out.println("\n\n\nVERSION: " + versionString);
        return Integer.valueOf(versionString.split("\\.")[0]);
    }

    // Retrieves the minor version number of the underlying database.
    @Override
    public int getDatabaseMinorVersion() throws SQLException
    {
        checkClosed();
        return Integer.valueOf(versionString.split("\\.")[1]);
    }

    // Retrieves the name of this database product.
    @Override
    public String getDatabaseProductName() throws SQLException
    {
        checkClosed();
        return "VoltDB";
    }

    // Retrieves the version number of this database product.
    @Override
    public String getDatabaseProductVersion() throws SQLException
    {
        checkClosed();
        return buildString;
    }

    // Retrieves this database's default transaction isolation level.
    @Override
    public int getDefaultTransactionIsolation() throws SQLException
    {
        checkClosed();
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    // Retrieves this JDBC driver's major version number.
    @Override
    public int getDriverMajorVersion()
    {
        return 1;
    }

    // Retrieves this JDBC driver's minor version number.
    @Override
    public int getDriverMinorVersion()
    {
        return 1;
    }

    // Retrieves the name of this JDBC driver.
    @Override
    public String getDriverName() throws SQLException
    {
        checkClosed();
        return "voltdb";
    }

    // Retrieves the version number of this JDBC driver as a String.
    @Override
    public String getDriverVersion() throws SQLException
    {
        checkClosed();
        return new String(getDriverMajorVersion() + "." + getDriverMinorVersion());
    }

    /**
     * Retrieves a description of the foreign key columns that reference the
     * given table's primary key columns (the foreign keys exported by a table).
     */
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException
    {
        checkClosed();
        VoltTable vtable = new VoltTable(
                new ColumnInfo("PKTABLE_CAT", VoltType.STRING),
                new ColumnInfo("PKTABLE_SCHEM", VoltType.STRING),
                new ColumnInfo("PKTABLE_NAME", VoltType.STRING),
                new ColumnInfo("PKCOLUMN_NAME", VoltType.STRING),
                new ColumnInfo("FKTABLE_CAT", VoltType.STRING),
                new ColumnInfo("FKTABLE_SCHEM", VoltType.STRING),
                new ColumnInfo("FKTABLE_NAME", VoltType.STRING),
                new ColumnInfo("FKCOLUMN_NAME", VoltType.STRING),
                new ColumnInfo("KEY_SEQ", VoltType.SMALLINT),
                new ColumnInfo("UPDATE_RULE", VoltType.SMALLINT),
                new ColumnInfo("DELETE_RULE", VoltType.SMALLINT),
                new ColumnInfo("FK_NAME", VoltType.STRING),
                new ColumnInfo("PK_NAME", VoltType.STRING),
                new ColumnInfo("DEFERRABILITY", VoltType.SMALLINT)
        );
        //NB: @SystemCatalog(?) will need additional support if we want to
        // populate the table.
        JDBC4ResultSet res = new JDBC4ResultSet(this.sysCatalog, vtable);
        return res;
    }

    /**
     * Retrieves all the "extra" characters that can be used in unquoted identifier
     * names (those beyond a-z, A-Z, 0-9 and _).
     */
    @Override
    public String getExtraNameCharacters() throws SQLException
    {
        checkClosed();
        return "";
    }

    // Retrieves a description of the given catalog's system or user function parameters and return type.
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a description of the system and user functions available in the given catalog.
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the string used to quote SQL identifiers.
    @Override
    public String getIdentifierQuoteString() throws SQLException
    {
        checkClosed();
        return "\"";
    }

    // Retrieves a description of the primary key columns that are referenced by the given table's foreign key columns (the primary keys imported by a table) throws SQLException.
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException
    {
        checkClosed();
        VoltTable vtable = new VoltTable(
                new ColumnInfo("PKTABLE_CAT", VoltType.STRING),
                new ColumnInfo("PKTABLE_SCHEM", VoltType.STRING),
                new ColumnInfo("PKTABLE_NAME", VoltType.STRING),
                new ColumnInfo("PKCOLUMN_NAME", VoltType.STRING),
                new ColumnInfo("FKTABLE_CAT", VoltType.STRING),
                new ColumnInfo("FKTABLE_SCHEM", VoltType.STRING),
                new ColumnInfo("FKTABLE_NAME", VoltType.STRING),
                new ColumnInfo("FKCOLUMN_NAME", VoltType.STRING),
                new ColumnInfo("KEY_SEQ", VoltType.SMALLINT),
                new ColumnInfo("UPDATE_RULE", VoltType.SMALLINT),
                new ColumnInfo("DELETE_RULE", VoltType.SMALLINT),
                new ColumnInfo("FK_NAME", VoltType.STRING),
                new ColumnInfo("PK_NAME", VoltType.STRING),
                new ColumnInfo("DEFERRABILITY", VoltType.SMALLINT)
        );
        //NB: @SystemCatalog(?) will need additional support if we want to
        // populate the table.
        JDBC4ResultSet res = new JDBC4ResultSet(this.sysCatalog, vtable);
        return res;
    }

    // Retrieves a description of the given table's indices and statistics.
    // NOTE: currently returns the NON_UNIQUE column as a TINYINT due
    // to lack of boolean support in VoltTable schemas.
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException
    {
        assert(table != null && !table.isEmpty());
        checkClosed();
        this.sysCatalog.setString(1, "INDEXINFO");
        JDBC4ResultSet res = (JDBC4ResultSet) this.sysCatalog.executeQuery();
        VoltTable vtable = res.getVoltTable().clone(0);

        // Filter the indexes by table name
        while (res.next()) {
            if (res.getString("TABLE_NAME").equals(table)) {
                if (!unique || res.getShort("NON_UNIQUE") == 0) {
                    vtable.addRow(res.getRowData());
                }
            }
        }

        return new JDBC4ResultSet(sysCatalog, vtable);
    }

    // Retrieves the major JDBC version number for this driver.
    @Override
    public int getJDBCMajorVersion() throws SQLException
    {
        checkClosed();
        return 4;
    }

    // Retrieves the minor JDBC version number for this driver.
    @Override
    public int getJDBCMinorVersion() throws SQLException
    {
        checkClosed();
        return 0;
    }

    // Retrieves the maximum number of hex characters this database allows in an inline binary literal.
    @Override
    public int getMaxBinaryLiteralLength() throws SQLException
    {
        checkClosed();
        return VoltType.MAX_VALUE_LENGTH;
    }

    // Retrieves the maximum number of characters that this database allows in a catalog name.
    @Override
    public int getMaxCatalogNameLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of characters this database allows for a character literal.
    @Override
    public int getMaxCharLiteralLength() throws SQLException
    {
        checkClosed();
        return VoltType.MAX_VALUE_LENGTH;
    }

    // Retrieves the maximum number of characters this database allows for a column name.
    @Override
    public int getMaxColumnNameLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of columns this database allows in a GROUP BY clause.
    @Override
    public int getMaxColumnsInGroupBy() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of columns this database allows in an index.
    @Override
    public int getMaxColumnsInIndex() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of columns this database allows in an ORDER BY clause.
    @Override
    public int getMaxColumnsInOrderBy() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of columns this database allows in a SELECT list.
    @Override
    public int getMaxColumnsInSelect() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of columns this database allows in a table.
    @Override
    public int getMaxColumnsInTable() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of concurrent connections to this database that are possible.
    @Override
    public int getMaxConnections() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of characters that this database allows in a cursor name.
    @Override
    public int getMaxCursorNameLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of bytes this database allows for an index, including all of the parts of the index.
    @Override
    public int getMaxIndexLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of characters that this database allows in a procedure name.
    @Override
    public int getMaxProcedureNameLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of bytes this database allows in a single row.
    @Override
    public int getMaxRowSize() throws SQLException
    {
        checkClosed();
        return 2 * 1024 * 1024;  // 2 MB
    }

    // Retrieves the maximum number of characters that this database allows in a schema name.
    @Override
    public int getMaxSchemaNameLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of characters this database allows in an SQL statement.
    @Override
    public int getMaxStatementLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of active statements to this database that can be open at the same time.
    @Override
    public int getMaxStatements() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of characters this database allows in a table name.
    @Override
    public int getMaxTableNameLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of tables this database allows in a SELECT statement.
    @Override
    public int getMaxTablesInSelect() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of characters this database allows in a user name.
    @Override
    public int getMaxUserNameLength() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a comma-separated list of math functions available with this database.
    @Override
    public String getNumericFunctions() throws SQLException
    {
        checkClosed();
        return "ABS,CEILING,EXP,FLOOR,POWER,SQRT";
    }

    // Retrieves a description of the given table's primary key columns.
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException
    {
        assert(table != null && !table.isEmpty());
        checkClosed();
        this.sysCatalog.setString(1, "PRIMARYKEYS");
        JDBC4ResultSet res = (JDBC4ResultSet) this.sysCatalog.executeQuery();
        VoltTable vtable = res.getVoltTable().clone(0);

        // Filter the primary keys based on table name
        while (res.next()) {
            if (res.getString("TABLE_NAME").equals(table)) {
                vtable.addRow(res.getRowData());
            }
        }
        return new JDBC4ResultSet(sysCatalog, vtable);
    }

    // Retrieves a description of the given catalog's stored procedure parameter and result columns.
    // TODO: implement pattern filtering somewhere (preferably server-side)
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException
    {
        assert(procedureNamePattern != null && !procedureNamePattern.isEmpty());
        checkClosed();
        this.sysCatalog.setString(1, "PROCEDURECOLUMNS");
        JDBC4ResultSet res = (JDBC4ResultSet) this.sysCatalog.executeQuery();
        VoltTable vtable = res.getVoltTable().clone(0);

        // Filter the results based on procedure name and column name
        while (res.next()) {
            if (res.getString("PROCEDURE_NAME").equals(procedureNamePattern)) {
                if (columnNamePattern == null || columnNamePattern.equals("%") ||
                    res.getString("COLUMN_NAME").equals(columnNamePattern)) {
                    vtable.addRow(res.getRowData());
                }
            }
        }

        return new JDBC4ResultSet(sysCatalog, vtable);
    }

    // Retrieves a description of the stored procedures available in the given catalog.
    // TODO: implement pattern filtering somewhere (preferably server-side)
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException
    {
        if (procedureNamePattern != null && !procedureNamePattern.equals("%"))
            throw new SQLException(String.format("getProcedures('%s','%s','%s') does not support pattern filtering", catalog, schemaPattern, procedureNamePattern));

        checkClosed();
        this.sysCatalog.setString(1, "PROCEDURES");
        ResultSet res = this.sysCatalog.executeQuery();
        return res;
    }

    // Retrieves the database vendor's preferred term for "procedure".
    @Override
    public String getProcedureTerm() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves this database's default holdability for ResultSet objects.
    @Override
    public int getResultSetHoldability() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Indicates whether or not this data source supports the SQL ROWID type, and if so the lifetime for which a RowId object remains valid.
    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the schema names available in this database.
    @Override
    public ResultSet getSchemas() throws SQLException
    {
        checkClosed();
        VoltTable vtable = new VoltTable(
                new ColumnInfo("TABLE_SCHEM", VoltType.STRING),
                new ColumnInfo("TABLE_CATALOG", VoltType.STRING));
        JDBC4ResultSet res = new JDBC4ResultSet(this.sysCatalog, vtable);
        return res;
    }

    // Retrieves the schema names available in this database.
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException
    {
        return getSchemas();    // empty
    }

    // Retrieves the database vendor's preferred term for "schema".
    @Override
    public String getSchemaTerm() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the string that can be used to escape wildcard characters.
    @Override
    public String getSearchStringEscape() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a comma-separated list of all of this database's SQL keywords that are NOT also SQL:2003 keywords.
    @Override
    public String getSQLKeywords() throws SQLException
    {
        checkClosed();
        return "";
    }

    // Indicates whether the SQLSTATE returned by SQLException.getSQLState is X/Open (now known as Open Group) SQL CLI or SQL:2003.
    @Override
    public int getSQLStateType() throws SQLException
    {
        checkClosed();
        return sqlStateXOpen;
    }

    // Retrieves a comma-separated list of string functions available with this database.
    @Override
    public String getStringFunctions() throws SQLException
    {
        checkClosed();
        // TODO: find a more suitable place for COALESCE
        return "COALESCE,CHAR,CHAR_LENGTH,CONCAT,FORMAT_CURRENCY,INSERT,LCASE,LEFT,LOWER,LTRIM," +
               "OCTET_LENGTH,OVERLAY,POSITION,REPEAT,REPLACE,RIGHT,RTRIM,SPACE,SUBSTRING,SUBSTR,"+
               "TRIM,UCASE,UPPER";
    }

    // Retrieves a description of the table hierarchies defined in a particular schema in this database.
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a description of the user-defined type (UDT) hierarchies defined in a particular schema in this database.
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a comma-separated list of system functions available with this database.
    @Override
    public String getSystemFunctions() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a description of the access rights for each table available in a catalog.
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException
    {
        checkClosed();
        VoltTable vtable = new VoltTable(
                new ColumnInfo("TABLE_CAT", VoltType.STRING),
                new ColumnInfo("TABLE_SCHEM", VoltType.STRING),
                new ColumnInfo("TABLE_NAME", VoltType.STRING),
                new ColumnInfo("GRANTOR", VoltType.STRING),
                new ColumnInfo("GRANTEE", VoltType.STRING),
                new ColumnInfo("PRIVILEGE", VoltType.STRING),
                new ColumnInfo("IS_GRANTABLE", VoltType.STRING)
        );
        //NB: @SystemCatalog(?) will need additional support if we want to
        // populate the table.
        JDBC4ResultSet res = new JDBC4ResultSet(this.sysCatalog, vtable);
        return res;
    }

    // Convert the users VoltDB SQL pattern into a regex pattern
    public static Pattern computeJavaPattern(String sqlPattern)
    {
        StringBuffer pattern_buff = new StringBuffer();
        // Replace "_" with "." (match exactly 1 character)
        // Replace "%" with ".*" (match 0 or more characters)
        for (int i=0; i<sqlPattern.length(); i++)
        {
            char c = sqlPattern.charAt(i);
            if (c == '_')
            {
                pattern_buff.append('.');
            }
            else
            if (c == '%')
            {
                pattern_buff.append(".*");
            }
            else
                pattern_buff.append(c);
        }
        return Pattern.compile(pattern_buff.toString());
    }

    // Retrieves a description of the tables available in the given catalog.
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException
    {
        checkClosed();
        this.sysCatalog.setString(1, "TABLES");
        JDBC4ResultSet res = (JDBC4ResultSet) this.sysCatalog.executeQuery();
        VoltTable vtable = res.getVoltTable().clone(0);

        List<String> typeStrings = null;
        if (types != null) {
            typeStrings = Arrays.asList(types);
        }

        // If no pattern is specified, default to matching any/all.
        if (tableNamePattern == null || tableNamePattern.length() == 0)
        {
            tableNamePattern = "%";
        }

        Pattern table_pattern = computeJavaPattern(tableNamePattern);

        // Filter tables based on type and pattern
        while (res.next()) {
            if (typeStrings == null || typeStrings.contains(res.getString("TABLE_TYPE"))) {
                Matcher table_matcher = table_pattern.matcher(res.getString("TABLE_NAME"));
                if (table_matcher.matches())
                {
                    vtable.addRow(res.getRowData());
                }
            }
        }

        return new JDBC4ResultSet(this.sysCatalog, vtable);
    }

    // Retrieves the table types available in this database.
    @Override
    public ResultSet getTableTypes() throws SQLException
    {
        checkClosed();
        VoltTable vtable = new VoltTable(new ColumnInfo("TABLE_TYPE", VoltType.STRING));
        for (String type : tableTypes) {
            vtable.addRow(type);
        }
        JDBC4ResultSet res = new JDBC4ResultSet(this.sysCatalog, vtable);
        return res;
    }

    // Retrieves a comma-separated list of the time and date functions available with this database.
    @Override
    public String getTimeDateFunctions() throws SQLException
    {
        checkClosed();
        return "CURRENT_TIMESTAMP,DAY,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,EXTRACT,FROM_UNIXTIME,HOUR," +
               "MINUT,MONTH,NOW,QUARTER,SECOND,SINCE_EPOCH,TO_TIMESTAMP,TRUNCATE,WEEK,WEEKOFYEAR,"+
               "WEEKDAY,YEAR";
    }

    // Retrieves a description of all the data types supported by this database.
    @Override
    public ResultSet getTypeInfo() throws SQLException
    {
        checkClosed();
        this.sysCatalog.setString(1, "TYPEINFO");
        ResultSet res = this.sysCatalog.executeQuery();
        return res;
    }

    // Retrieves a description of the user-defined types (UDTs) defined in a particular schema.
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the URL for this DBMS.
    @Override
    public String getURL() throws SQLException
    {
        checkClosed();
        return "http://voltdb.com/";
    }

    // Retrieves the user name as known to this database.
    @Override
    public String getUserName() throws SQLException
    {
        checkClosed();
        return this.sourceConnection.User;
    }

    // Retrieves a description of a table's columns that are automatically updated when any value in a row is updated.
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves whether or not a visible row insert can be detected by calling the method ResultSet.rowInserted.
    @Override
    public boolean insertsAreDetected(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a catalog appears at the start of a fully qualified table name.
    @Override
    public boolean isCatalogAtStart() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database is in read-only mode.
    @Override
    public boolean isReadOnly() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Indicates whether updates made to a LOB are made on a copy or directly to the LOB.
    @Override
    public boolean locatorsUpdateCopy() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports concatenations between NULL and non-NULL values being NULL.
    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether NULL values are sorted at the end regardless of sort order.
    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether NULL values are sorted at the start regardless of sort order.
    @Override
    public boolean nullsAreSortedAtStart() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether NULL values are sorted high.
    @Override
    public boolean nullsAreSortedHigh() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether NULL values are sorted low.
    @Override
    public boolean nullsAreSortedLow() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether deletes made by others are visible.
    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether inserts made by others are visible.
    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether updates made by others are visible.
    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a result set's own deletes are visible.
    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a result set's own inserts are visible.
    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether for the given type of ResultSet object, the result set's own updates are visible.
    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in lower case.
    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException
    {
        checkClosed();
        return false; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether this database treats mixed case quoted SQL identifiers as case insensitive and stores them in lower case.
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
        checkClosed();
        return false; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException
    {
        checkClosed();
        return false; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether this database treats mixed case quoted SQL identifiers as case insensitive and stores them in mixed case.
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
        checkClosed();
        return false; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in upper case.
    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException
    {
        checkClosed();
        return true; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether this database treats mixed case quoted SQL identifiers as case insensitive and stores them in upper case.
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
        checkClosed();
        return true; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether this database supports ALTER TABLE with add column.
    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports ALTER TABLE with drop column.
    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the ANSI92 entry level SQL grammar.
    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the ANSI92 full SQL grammar supported.
    @Override
    public boolean supportsANSI92FullSQL() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the ANSI92 intermediate SQL grammar supported.
    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports batch updates.
    @Override
    public boolean supportsBatchUpdates() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether a catalog name can be used in a data manipulation statement.
    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a catalog name can be used in an index definition statement.
    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a catalog name can be used in a privilege definition statement.
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a catalog name can be used in a procedure call statement.
    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a catalog name can be used in a table definition statement.
    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports column aliasing.
    @Override
    public boolean supportsColumnAliasing() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports the JDBC scalar function CONVERT for the conversion of one JDBC type to another.
    @Override
    public boolean supportsConvert() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the JDBC scalar function CONVERT for conversions between the JDBC types fromType and toType.
    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the ODBC Core SQL grammar.
    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports correlated subqueries.
    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports both data definition and data manipulation statements within a transaction.
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports only data manipulation statements within a transaction.
    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether, when table correlation names are supported, they are restricted to being different from the names of the tables.
    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
        checkClosed();
        return false; // Alias may be same as the table name
    }

    // Retrieves whether this database supports expressions in ORDER BY lists.
    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the ODBC Extended SQL grammar.
    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException
    {
        checkClosed();
        return false;  // Only assuming based on currently limited set
    }

    // Retrieves whether this database supports full nested outer joins.
    @Override
    public boolean supportsFullOuterJoins() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether auto-generated keys can be retrieved after a statement has been executed
    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports some form of GROUP BY clause.
    @Override
    public boolean supportsGroupBy() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports using columns not included in the SELECT statement in a GROUP BY clause provided that all of the columns in the SELECT statement are included in the GROUP BY clause.
    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports using a column that is not in the SELECT statement in a GROUP BY clause.
    @Override
    public boolean supportsGroupByUnrelated() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the SQL Integrity Enhancement Facility.
    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
        checkClosed();
        return false; // Does support DEFAULT, but not CHECK and UNIQUE constraints get violated over partition boundaries!
    }

    // Retrieves whether this database supports specifying a LIKE escape clause.
    @Override
    public boolean supportsLikeEscapeClause() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database provides limited support for outer joins.
    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports the ODBC Minimum SQL grammar.
    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database treats mixed case unquoted SQL identifiers as case sensitive and as a result stores them in mixed case.
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
        checkClosed();
        return false; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether this database treats mixed case quoted SQL identifiers as case sensitive and as a result stores them in mixed case.
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
        checkClosed();
        return false; // Note: Proc names are sensitive, but not tables/columns!
    }

    // Retrieves whether it is possible to have multiple ResultSet objects returned from a CallableStatement object simultaneously.
    @Override
    public boolean supportsMultipleOpenResults() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports getting multiple ResultSet objects from a single call to the method execute.
    @Override
    public boolean supportsMultipleResultSets() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database allows having multiple transactions open at once (on different connections) throws SQLException.
    @Override
    public boolean supportsMultipleTransactions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports named parameters to callable statements.
    @Override
    public boolean supportsNamedParameters() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether columns in this database may be defined as non-nullable.
    @Override
    public boolean supportsNonNullableColumns() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports keeping cursors open across commits.
    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports keeping cursors open across rollbacks.
    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports keeping statements open across commits.
    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports keeping statements open across rollbacks.
    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports using a column that is not in the SELECT statement in an ORDER BY clause.
    @Override
    public boolean supportsOrderByUnrelated() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports some form of outer join.
    @Override
    public boolean supportsOuterJoins() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports positioned DELETE statements.
    @Override
    public boolean supportsPositionedDelete() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports positioned UPDATE statements.
    @Override
    public boolean supportsPositionedUpdate() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the given concurrency type in combination with the given result set type.
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException
    {
        checkClosed();
        if (type == ResultSet.TYPE_SCROLL_INSENSITIVE && concurrency == ResultSet.CONCUR_READ_ONLY)
            return true;
        return false;
    }

    // Retrieves whether this database supports the given result set holdability.
    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports the given result set type.
    @Override
    public boolean supportsResultSetType(int type) throws SQLException
    {
        checkClosed();
        if (type == ResultSet.TYPE_SCROLL_INSENSITIVE)
            return true;
        return false;
    }

    // Retrieves whether this database supports savepoints.
    @Override
    public boolean supportsSavepoints() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a schema name can be used in a data manipulation statement.
    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a schema name can be used in an index definition statement.
    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a schema name can be used in a privilege definition statement.
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a schema name can be used in a procedure call statement.
    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether a schema name can be used in a table definition statement.
    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports SELECT FOR UPDATE statements.
    @Override
    public boolean supportsSelectForUpdate() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports statement pooling.
    @Override
    public boolean supportsStatementPooling() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports invoking user-defined or vendor functions using the stored procedure escape syntax.
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports stored procedure calls that use the stored procedure escape syntax.
    @Override
    public boolean supportsStoredProcedures() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports subqueries in comparison expressions.
    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports subqueries in EXISTS expressions.
    @Override
    public boolean supportsSubqueriesInExists() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports subqueries in IN expressions.
    @Override
    public boolean supportsSubqueriesInIns() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports subqueries in quantified expressions.
    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports table correlation names.
    @Override
    public boolean supportsTableCorrelationNames() throws SQLException
    {
        checkClosed();
        return true;
    }

    // Retrieves whether this database supports the given transaction isolation level.
    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException
    {
        checkClosed();
        if (level == Connection.TRANSACTION_SERIALIZABLE)
            return true;
        return false;
    }

    // Retrieves whether this database supports transactions.
    @Override
    public boolean supportsTransactions() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports SQL UNION.
    @Override
    public boolean supportsUnion() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database supports SQL UNION ALL.
    @Override
    public boolean supportsUnionAll() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether or not a visible row update can be detected by calling the method ResultSet.rowUpdated.
    @Override
    public boolean updatesAreDetected(int type) throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database uses a file for each table.
    @Override
    public boolean usesLocalFilePerTable() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Retrieves whether this database stores tables in a local file.
    @Override
    public boolean usesLocalFiles() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an object that does.
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        checkClosed();
        return iface.isInstance(this);
    }

    // Returns an object that implements the given interface to allow access to non-standard methods, or standard methods not exposed by the proxy.
    @Override
    public <T> T unwrap(Class<T> iface)    throws SQLException
    {
        checkClosed();
        try
        {
            return iface.cast(this);
        }
         catch (ClassCastException cce)
         {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, iface.toString());
        }
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern)
            throws SQLException {
        checkClosed();
        throw SQLError.noSupport();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw SQLError.noSupport();
    }
}
