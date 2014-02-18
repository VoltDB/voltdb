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


package org.hsqldb_voltpatches.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.hsqldb_voltpatches.ColumnBase;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.persist.HsqlProperties;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.CharacterType;

/* $Id: JDBCResultSetMetaData.java 2980 2009-05-02 14:38:05Z fredt $ */

// fredt@users    - 20040412 - removed DITypeInfo dependencies
// boucherb@users - 200404xx - removed unused imports;refinement for better
//                             usability of getColumnDisplaySize;
//                             javadoc updates
// boucherb@users - 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
// boucherb@users - 20060522 - doc   1.9.0 full synch up to Mustang Build 84
// fredt@users - 1.9.0 usage of types and new ResultMetadata and ColumnMetaData
// Revision 1.15  2006/07/12 12:29:42  boucherb
// patch 1.9.0
// - full synch up to Mustang b90
// - minor update to toString

/**
 * <!-- start generic documentation -->
 * An object that can be used to get information about the types
 * and properties of the columns in a <code>ResultSet</code> object.
 * The following code fragment creates the <code>ResultSet</code> object rs,
 * creates the <code>ResultSetMetaData</code> object rsmd, and uses rsmd
 * to find out how many columns rs has and whether the first column in rs
 * can be used in a <code>WHERE</code> clause.
 * <PRE>
 *
 *     ResultSet rs = stmt.executeQuery("SELECT a, b, c FROM TABLE2");
 *     ResultSetMetaData rsmd = rs.getMetaData();
 *     int numberOfColumns = rsmd.getColumnCount();
 *     boolean b = rsmd.isSearchable(1);
 *
 * </PRE>
 * <!-- end generic documentation -->
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * HSQLDB supports a subset of the <code>ResultSetMetaData</code> interface.<p>
 *
 * The JDBC specification for <code>ResultSetMetaData</code> is in part very
 * vague. This causes potential incompatibility between interpretations of the
 * specification as realized in different JDBC driver implementations. As such,
 * deciding to what degree reporting ResultSetMetaData is accurate has been
 * considered very carefully. Hopefully, the design decisions made in light of
 * these considerations have yeilded precisely the subset of full
 * ResultSetMetaData support that is most commonly needed and that is most
 * important, while also providing, under the most common use-cases, the
 * fastest access with the least overhead and the best comprimise between
 * speed, accuracy, jar-footprint and retention of JDBC resources. <p>
 *
 * (fredt@users) <br>
 * (boucherb@users)<p>
 * </div>
 * <!-- end release-specific documentation -->
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @revised JDK 1.6, HSQLDB 1.8.x
 * @see JDBCStatement#executeQuery
 * @see JDBCStatement#getResultSet
 * @see java.sql.ResultSetMetaData
 */
public class JDBCResultSetMetaData implements ResultSetMetaData {

    /**
     * <!-- start generic documentation -->
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * <!-- end generic documentation -->
     * @return the number of columns
     * @exception SQLException if a database access error occurs
     */
    public int getColumnCount() throws SQLException {
        return resultMetaData.getColumnCount();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column is automatically numbered.
     * <p>(JDBC4 deleted:)[, thus read-only.]
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 fully supports SQL Satandard features T174 and T176 that
     * define identity column support.
     *
     * <hr>
     *
     * However, it must be stated here that contrary to the generic
     * documentation previous to the JDBC4 specification, HSQLDB automatically
     * numbered columns (IDENTITY columns, in HSQLDB parlance) are not
     * read-only. <p>
     *
     * In fact, the generic documentation previous to the JDBC4 specification
     * seems to contradict the general definition of what, at minimum,
     * an auto-increment column is: <p>
     *
     * Simply, an auto-increment column is one that guarantees it has a
     * autogenerated value after a successful insert or update operation,
     * even if no value is supplied, or DEFAULT is specified.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isAutoIncrement(int column) throws SQLException {

        checkColumn(column);

        return resultMetaData.columns[--column].isIdentity();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether a column's case matters.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.7.1 did not report this value accurately.  <p>
     *
     * Starting with 1.7.2, this feature is better supported.  <p>
     *
     * This method returns true for any column whose data type is a character
     * type, with the exception of VARCHAR_IGNORECASE for which it returns
     * false. It also returns false for any column whose data type is a
     * not a character data type. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isCaseSensitive(int column) throws SQLException {

        checkColumn(column);

        Type type = resultMetaData.columnTypes[--column];

        if (type.isCharacterType()) {
            return !((CharacterType) type).isCaseInsensitive();
        }

        return false;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column can be used in a where clause.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 handles this differently from previous versions. <p>
     *
     * If the column in question is a database table or view column, and the
     * type of the column allows searching, then returns true, otherwise false.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isSearchable(int column) throws SQLException {

        checkColumn(column);

        return resultMetaData.columns[--column].isSearchable();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column is a cash value.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 fully supports this feature and returns true for
     * NUMERIC and DECIMAL columns. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isCurrency(int column) throws SQLException {

        checkColumn(column);

        Type type = resultMetaData.columnTypes[--column];

        return (type.typeCode == Types.SQL_DECIMAL
                || type.typeCode == Types.SQL_NUMERIC) && type.scale > 0;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates the nullability of values in the designated column.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 fully supports this feature.  <p>
     *
     * <tt>columnNoNulls</tt> is always returned for result set columns
     * that do not directly represent table column values (i.e. are calculated),
     * while the corresponding value in [INFORMATION_SCHEMA.]SYSTEM_COLUMNS.NULLABLE
     * is returned for result set columns that do directly represent table
     * column values. <p>
     *
     * To determine the nullable status of a table column in isolation from
     * ResultSetMetaData and in a DBMS-independent fashion, the
     * DatabaseMetaData.getColumns() method can be invoked with the
     * appropriate filter values and the result should be inspected at the
     * position described in the DatabaseMetaData.getColumns() API
     * documentation.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>,
     *          <code>columnNullable</code> or <code>columnNullableUnknown</code>
     * @exception SQLException if a database access error occurs
     */
    public int isNullable(int column) throws SQLException {

        checkColumn(column);

        return resultMetaData.columns[--column].getNullability();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether values in the designated column are signed numbers.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 fully supports this feature.  <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isSigned(int column) throws SQLException {

        checkColumn(column);

        Type type = resultMetaData.columnTypes[--column];

        return type.isNumberType();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates the designated column's normal maximum width in characters.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 fully supports this feature.  <p>
     *
     * The current calculation follows these rules: <p>
     *
     * <ol>
     * <li>Long character types and datetime types:<p>
     *
     *     The maximum length/precision, repectively.<p>
     *
     * <li>CHAR and VARCHAR types: <p>
     *
     *      <ul>
     *      <li> If the result set column is a direct pass through of a table
     *           column value and column size was declared, then the declared
     *           value is returned. <p>
     *
     *      <li> Otherwise, the computed length according to SQL Standard is
     *           returned. For very large values, the value of the system property
     *           hsqldb.max_xxxchar_display_size or the magic value
     *           32766 (0x7FFE) (tested usable/accepted by most tools and
     *           compatible with assumptions made by java.io read/write
     *           UTF) when the system property is not defined or is not
     *           accessible, due to security constraints. <p>
     *
     *      </ul>
     *
     *      It must be noted that the latter value in no way affects the
     *      ability of the HSQLDB JDBC driver to retrieve longer values
     *      and serves only as the current best effort at providing a
     *      value that maximizes usability across a wide range of tools,
     *      given that the HSQLDB database engine allows very large
     *      lengths to be declared. <p>
     *
     * <li>Number types: <p>
     *
     *     The max precision, plus the length of the negation character (1),
     *     plus (if applicable) the maximum number of characters that may
     *     occupy the exponent character sequence.  Note that some legacy tools
     *     do not correctly handle BIGINT values of greater than 18 digits. <p>
     *
     * <li>BOOLEAN type: <p>
     *
     *     The length of the character sequence "false" (5), the longer of the
     *     two boolean value String representations. <p>
     *
     * <li>Remaining types: <p>
     *
     *     The maximum length/precision, respectively, as reported by
     *     DatabaseMetaData.getTypeInfo(), when applicable.  If the maximum
     *     display size is unknown, unknowable or inapplicable, then zero is
     *     returned. <p>
     *
     * </ol>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width
     *          of the designated column
     * @exception SQLException if a database access error occurs
     */
    public int getColumnDisplaySize(int column) throws SQLException {

        checkColumn(column);

        Type type = resultMetaData.columnTypes[--column];

        return type.displaySize();
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's suggested title for use in printouts and
     * displays. (JDBC4 clarification:) The suggested title is usually specified by the SQL <code>AS</code>
     * clause.  If a SQL <code>AS</code> is not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * In HSQLDB, a <code>ResultSet</code> column label is determined using the
     * following order of precedence:<p>
     *
     * <OL>
     * <LI>The label (alias) specified in the generating query.</LI>
     * <LI>The name of the underlying column, if no label is specified.<br>
     * <L1>C1, C2, etc. for computed columns that have no label.</LI>
     * </OL> <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @exception SQLException if a database access error occurs
     */
    public String getColumnLabel(int column) throws SQLException {

        checkColumn(column--);

        String label = resultMetaData.columnLabels[column];

        return label == null ? resultMetaData.columns[column].getNameString()
                             : label;
    }

    /**
     * <!-- start generic documentation -->
     * Get the designated column's name.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * In HSQLDB, a ResultSet column name is determined using the following
     * order of prcedence:<p>
     *
     * <OL>
     * <LI>The name of the underlying columnm, if the ResultSet column
     *   represents a column in a table.</LI>
     * <LI>The label or alias specified in the generating query.</LI>
     * <L1>C1, C2, etc. for computed columns that have no label.</LI>
     * </OL> <p>
     *
     * If the <code>jdbc.get_column_name</code> property of the JDBC Connection
     * has been set to false, this method returns the same value as
     * {@link #getColumnLabel(int)}.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @exception SQLException if a database access error occurs
     */
    public String getColumnName(int column) throws SQLException {

        checkColumn(column);

        if (useColumnName) {
            String name = resultMetaData.columns[--column].getNameString();

            return name == null ? ""
                                : name;
        }

        return resultMetaData.columnLabels[--column];
    }

    /**
     * <!-- start generic documentation -->
     * Get the designated column's table's schema.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Since 1.8.0.x, HSQLDB implements standard SQL SCHEMA support;
     * this method returns the actual schema of the column's table.
     * Columns generated in queries have no schema name.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public String getSchemaName(int column) throws SQLException {

        checkColumn(column);

        String name = resultMetaData.columns[--column].getSchemaNameString();;

        return name == null ? ""
                            : name;
    }

    /**
     * <!-- start generic documentation -->
     * (JDBC4 clarification:)
     * Get the designated column's specified column size.
     * For numeric data, this is the maximum precision.  For character data, this is the [maximum] length in characters.
     * For datetime datatypes, this is the [maximim] length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the [maximum] length in bytes.  For the ROWID datatype,
     * this is the length in bytes[, as returned by the implementation-specific java.sql.RowId.getBytes() method]. 0 is returned for data types where the
     * column size is not applicable.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB reports the declared length or precision
     * specifiers for table columns, if they are defined.<p>
     *
     * From 1.9.0, HSQLDB, reports the correct length or precision for
     * computed columns according to the SQL Standard.<p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
     * @exception SQLException if a database access error occurs
     */
    public int getPrecision(int column) throws SQLException {

        checkColumn(column);

        // type in columnTypes overrides column type
        Type type      = resultMetaData.columnTypes[--column];
        long precision = type.precision;

        if (type.isDateTimeType() || type.isIntervalType()) {
            precision = type.displaySize();
        }

        if (precision > Integer.MAX_VALUE) {
            precision = Integer.MAX_VALUE;
        }

        return (int) precision;
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB reports the declared
     * scale for table columns.<p>
     *
     * From 1.9.0, HSQLDB, reports the correct scale for
     * computed columns according to the SQL Standard.<p>
     *
     * <pre>
     * sql.enforce_strict_size
     * </pre>
     * For datetime and interval types such as Timestamp or Time, the
     * fractional second precision is reported.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
     * @exception SQLException if a database access error occurs
     */
    public int getScale(int column) throws SQLException {

        Type type = resultMetaData.columnTypes[--column];

        return type.scale;
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's table name.
     *
     * <!-- end generic documentation -->
     * @param column the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public String getTableName(int column) throws SQLException {

        checkColumn(column);

        String name = resultMetaData.columns[--column].getTableNameString();

        return name == null ? ""
                            : name;
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's table's catalog name.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.9.0, HSQLDB returns the name of the catalog. The default name is
     * PUBLIC.
     * This value can be changed for the database using an SQL command.<p>
     *
     * HSQLDB supports use of catalog qualification in DLL or DML when it is
     * allowed by the Standard. <p>
     *
     * However, not all clients respect the SQL Standard and may use a
     * catalog qualifier in a context where it is not suppoted by the Standard.
     * <p>
     *
     * For greater detail, see discussion at:
     * {@link JDBCDatabaseMetaData}. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the name of the catalog for the table in which the given column
     *          appears or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public String getCatalogName(int column) throws SQLException {

        checkColumn(column);

        String name = resultMetaData.columns[--column].getCatalogNameString();

        return name == null ? ""
                            : name;
    }

    /**
     * <!-- start generic documentation -->
     * Retrieves the designated column's SQL type.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * This reports the SQL type code of the column. For time and timestamp
     * types that are WITH TIME ZONE, the values as the SQL Standarc CLI
     * codes.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type from java.sql.Types
     * @exception SQLException if a database access error occurs
     * @see java.sql.Types
     */
    public int getColumnType(int column) throws SQLException {

        checkColumn(column);

        Type type = resultMetaData.columnTypes[--column];

        return type.getJDBCTypeCode();
    }

    /**
     * <!-- start generic documentation -->
     * Retrieves the designated column's database-specific type name.
     * <!-- end generic documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is
     * a user-defined type, then a fully-qualified type name is returned.
     * @exception SQLException if a database access error occurs
     */
    public String getColumnTypeName(int column) throws SQLException {

        checkColumn(column);

        Type type = resultMetaData.columnTypes[--column];

        return type.getNameString();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column is definitely not writable.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.9.0 this method returns true if the ResuleSet is not updatable
     * or the column in question is not updatable.<p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isReadOnly(int column) throws SQLException {

        checkColumn(column);

        return !resultMetaData.columns[--column].isWriteable();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether it is possible for a write on the designated column to
     * succeed.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.9.0 this method returns false if the ResuleSet is not updatable
     * or the column in question is not updatable.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isWritable(int column) throws SQLException {

        checkColumn(column);

        return resultMetaData.colIndexes != null
               && resultMetaData.colIndexes[--column] > -1;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether a write on the designated column will definitely succeed.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.9.0 this method returns false if the ResuleSet is not updatable
     * or the column in question is not updatable.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {

        checkColumn(column);

        return resultMetaData.colIndexes != null
               && resultMetaData.colIndexes[--column] > -1;
    }

    //--------------------------JDBC 2.0-----------------------------------

    /**
     * <!-- start generic documentation -->
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code>
     * is called to retrieve a value
     * from the column.  <code>ResultSet.getObject</code> may return a subclass of the
     * class returned by this method.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 fully supports this feature.<p>
     *
     * For columns of type OTHER, there is no specific class name and
     * java.lang.Object is returned.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming
     *         language that would be used by the method
     * <code>ResultSet.getObject</code> to retrieve the value in the specified
     * column. This is the class name used for custom mapping.
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *      JDBCResultSet)
     */
    public String getColumnClassName(int column) throws SQLException {

        checkColumn(column);

        Type type = resultMetaData.columnTypes[--column];

        return type.getJDBCClassName();
    }

    //----------------------------- JDBC 4.0 -----------------------------------
    // ------------------- java.sql.Wrapper implementation ---------------------

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     *
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return the
     * the result of calling <code>unwrap</code> recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JAVA6
    @SuppressWarnings("unchecked")
    public <T>T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {

        if (isWrapperFor(iface)) {
            return (T) this;
        }

        throw Util.invalidArgument("iface: " + iface);
    }

//#endif JAVA6

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JAVA6
    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//#endif JAVA6
// ------------------------- Internal Implementation ---------------------------
    private ResultMetaData resultMetaData;

    /**
     * Whether to use the underlying column name or label when reporting
     * getColumnName().
     */
    private boolean useColumnName;
    private int     columnCount;

    /**
     * Constructs a new JDBCResultSetMetaData object from the specified
     * JDBCResultSet and HsqlProprties objects.
     *
     * @param meta the ResultMetaData object from which to construct a new
     *        JDBCResultSetMetaData object
     * @param props the HsqlProperties object from which to construct a
     *        new JDBCResultSetMetaData object
     * @throws SQLException if a database access error occurs
     */
    JDBCResultSetMetaData(ResultMetaData meta, boolean isUpdatable,
                          boolean isInsertable,
                          HsqlProperties props) throws SQLException {
        init(meta, props);
    }

    /**
     *  Initializes this JDBCResultSetMetaData object from the specified
     *  Result and HsqlProperties objects.
     *
     *  @param meta the ResultMetaData object from which to initialize this
     *         JDBCResultSetMetaData object
     *  @param props the HsqlProperties object from which to initialize this
     *         JDBCResultSetMetaData object
     *  @throws SQLException if a database access error occurs
     */
    void init(ResultMetaData meta, HsqlProperties props) throws SQLException {

        resultMetaData = meta;
        columnCount    = resultMetaData.getColumnCount();

        // fredt -  props is null for internal connections, so always use the
        //          default behaviour in this case
        // JDBCDriver.getPropertyInfo says
        // default is true
        useColumnName = (props == null) ? true
                                        : props.isPropertyTrue(
                                        "get_column_name", true);
    }

    /**
     * Performs an internal check for column index validity. <p>
     *
     * @param column index of column to check
     * @throws SQLException when this object's parent ResultSet has
     *      no such column
     */
    private void checkColumn(int column) throws SQLException {

        if (column < 1 || column > columnCount) {
            throw Util.sqlException(ErrorCode.JDBC_COLUMN_NOT_FOUND,
                                    String.valueOf(column));
        }
    }

    /**
     * Returns a string representation of the object. <p>
     *
     * The string consists of the name of the class of which the
     * object is an instance, the at-sign character `<code>@</code>',
     * the unsigned hexadecimal representation of the hash code of the
     * object and a comma-delimited list of this object's indexed attributes,
     * enclosed in square brakets.
     *
     * @return  a string representation of the object.
     */
    public String toString() {

        StringBuffer sb = new StringBuffer();

        sb.append(super.toString());

        if (columnCount == 0) {
            sb.append("[columnCount=0]");

            return sb.toString();
        }
        sb.append('[');

        for (int i = 0; i < columnCount; i++) {
            JDBCColumnMetaData meta = getColumnMetaData(i + 1);

            sb.append('\n');
            sb.append("   column_");
            sb.append(i + 1);
            sb.append('=');
            sb.append(meta);

            if (i + 1 < columnCount) {
                sb.append(',');
                sb.append(' ');
            }
        }
        sb.append('\n');
        sb.append(']');

        return sb.toString();
    }

    JDBCColumnMetaData getColumnMetaData(int i) {

        JDBCColumnMetaData meta = new JDBCColumnMetaData();

        try {
            meta.catalogName          = getCatalogName(i);
            meta.columnClassName      = getColumnClassName(i);
            meta.columnDisplaySize    = getColumnDisplaySize(i);
            meta.columnLabel          = getColumnLabel(i);
            meta.columnName           = getColumnName(i);
            meta.columnType           = getColumnType(i);
            meta.isAutoIncrement      = isAutoIncrement(i);
            meta.isCaseSensitive      = isCaseSensitive(i);
            meta.isCurrency           = isCurrency(i);
            meta.isDefinitelyWritable = isDefinitelyWritable(i);
            meta.isNullable           = isNullable(i);
            meta.isReadOnly           = isReadOnly(i);
            meta.isSearchable         = isSearchable(i);
            meta.isSigned             = isSigned(i);
            meta.isWritable           = isWritable(i);
            meta.precision            = getPrecision(i);
            meta.scale                = getScale(i);
            meta.schemaName           = getSchemaName(i);
            meta.tableName            = getTableName(i);
        } catch (SQLException e) {
        }

        return meta;
    }
}
