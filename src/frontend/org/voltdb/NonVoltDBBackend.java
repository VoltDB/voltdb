/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

/**
 * A wrapper around another database server (and JDBC connection), such as
 * HSQLDB or PostgreSQL. This class, with its sub-classes can be used to
 * execute SQL statements instead of the C++ ExecutionEngine. It is currently
 * used only by the SQL Coverage and JUnit regressionsuite tests.
 */
public abstract class NonVoltDBBackend {
    /** java.util.logging loggers. */
    private static final VoltLogger sqlLog = new VoltLogger("SQL");
    protected static final VoltLogger hostLog = new VoltLogger("HOST");
    protected static final Object backendLock = new Object();
    protected static NonVoltDBBackend m_backend = null;

    protected String m_database_type = null;
    protected Connection dbconn;

    protected static final boolean DEBUG = false;
    protected static boolean PRINT_TRANSFORMED_SQL = false;
    protected static FileWriter TRANSFORMED_SQL_FILE_WRITER = null;

    /** Constructor specifying the databaseType (e.g. HSQL or PostgreSQL),
     *  driverClassName, connectionURL, username, and password. */
    public NonVoltDBBackend(String databaseType, String driverClassName,
                            String connectionURL, String username, String password) {
        m_database_type = databaseType;
        try {
            Class.forName(driverClassName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load " + databaseType + " JDBC driver: " + driverClassName, e);
        }

        try {
            dbconn = DriverManager.getConnection(connectionURL, username, password);
            dbconn.setAutoCommit(true);
            dbconn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to open connection to: " + connectionURL, e);
        }

        // If '-Dsqlcoverage.transform.sql.file=...' was specified on the
        // command line, print info when transforming SQL statements into
        // a format that the backend database can understand
        String transformSqlOutputFile = System.getProperty("sqlcoverage.transform.sql.file", null);
        if (transformSqlOutputFile == null) {
            PRINT_TRANSFORMED_SQL = false;
            TRANSFORMED_SQL_FILE_WRITER = null;
        } else {
            PRINT_TRANSFORMED_SQL = true;
            try {
                TRANSFORMED_SQL_FILE_WRITER = new FileWriter(transformSqlOutputFile, true);
            } catch (IOException e) {
                System.out.println("Caught IOException:\n    " + e
                        + "\nSQL transform debug output will go to stdout.");
            }
        }
    }

    /** Creates a new NonVoltDBBackend wrapping dbconn. This is (was?) used for testing only. */
    protected NonVoltDBBackend(Connection dbconn) {
        this.dbconn = dbconn;
    }

    /** Used to specify when queries should only be modified if they apply to
     *  columns of certain types; so this only includes column types that need
     *  special treatment for a non-VoltDB backend database, such as PostgreSQL. */
    protected enum ColumnType {
        /** Any integer column type, including TINYINT, SMALLINT, INTEGER, BIGINT, etc. */
        INTEGER,
        /** Any Geospatial column type, including GEOGRAPHY_POINT or GEOGRAPHY. */
        GEO
    }

    /**
     * A QueryTransformer object is used to specify (using the builder pattern)
     * which options should be used, when transforming SQL statements (DDL or
     * DML) fitting certain patterns, as is often needed in order for the
     * results of a backend database (e.g. PostgreSQL) to match those of VoltDB.
     * This collection of options (the QueryTransformer object) is then passed
     * to the <i>transformQuery</i> method (see below).
     */
    protected static class QueryTransformer {
        // Required parameter
        private Pattern queryPattern;

        // Optional parameters, initialized with default values
        private String initialText = "";
        private String prefix = "";
        private String suffix = "";
        private String altSuffix = null;
        private String useAltSuffixAfter = null;
        private boolean useWholeMatch = false;
        private String replacementText = null;
        private ColumnType columnType = null;
        private Double multiplier = null;
        private Integer minimum = null;
        private List<String> groups = new ArrayList<String>();
        private boolean debugPrint = false;

        /**
         * Constructor for a QueryTransformer object.
         * @param queryPattern - a regex Pattern to be detected and modified,
         * within a SQL statement that may need to be modified, in order for
         * the backend database's results to match those of VoltDB; may not
         * be <b>null</b>.
         */
        protected QueryTransformer(Pattern queryPattern) {
            if (queryPattern == null) {
                throw new IllegalArgumentException("The queryPattern may not be null.");
            }
            this.queryPattern = queryPattern;
        }

        /** Specifies an initial string with which to begin the replacement
         *  text (e.g. "ORDER BY"); default is an empty string; may not be
         *  <b>null</b>. */
        protected QueryTransformer initialText(String text) {
            if (text == null) {
                throw new IllegalArgumentException("The initialText may not be null.");
            }
            this.initialText = text;
            return this;
        }

        /** Specifies a string to appear before each group, or before the whole
         *  <i>queryPattern</i> (e.g. "TRUNC ( "); default is an empty string;
         *  may not be <b>null</b>. */
        protected QueryTransformer prefix(String text) {
            if (text == null) {
                throw new IllegalArgumentException("The prefix may not be null.");
            }
            this.prefix = text;
            return this;
        }

        /** Specifies a string to appear after each group, or after the whole
         *  <i>queryPattern</i> (e.g. " NULLS FIRST"); default is an empty
         *  string; may not be <b>null</b>. */
        protected QueryTransformer suffix(String text) {
            if (text == null) {
                throw new IllegalArgumentException("The suffix may not be null.");
            }
            this.suffix = text;
            return this;
        }

        /**
         * Specifies an alternate string to appear after each group, and under
         * what circumstances.
         * @param useAltSuffixAfter - when a matching group ends with this
         * text (e.g. "DESC"), the <i>altSuffix</i> will be used, instead of
         * <i>suffix</i>; default is <b>null</b>.
         * @param altSuffix - the alternate suffix, to be used when the group
         * ends with <i>altEnding</i> (e.g. " NULLS LAST"); default is <b>null</b>.
         */
        protected QueryTransformer alternateSuffix(String useAltSuffixAfter, String altSuffix) {
            this.useAltSuffixAfter = useAltSuffixAfter;
            this.altSuffix = altSuffix;
            return this;
        }

        /** Specifies whether or not to use the whole matched pattern; when
         *  <b>true</b>, the <i>prefix</i> and <i>suffix</i> will be applied
         *  to the whole <i>queryPattern</i>; when <b>false</b>, they will be
         *  applied to each <i>group</i> within it; default is <b>false</b>. */
        protected QueryTransformer useWholeMatch(boolean useWholeMatch) {
            this.useWholeMatch = useWholeMatch;
            return this;
        }

        /** Specifies to use the whole matched pattern, i.e., the <i>prefix</i>
         *  and <i>suffix</i> will be applied to the whole <i>queryPattern</i>. */
        protected QueryTransformer useWholeMatch() {
            useWholeMatch(true);
            return this;
        }

        /** Specifies a string to replace each group, within the whole match
         *  (e.g. "||", to replace "+"); default is <b>null</b>, in which
         *  case it is ignored. */
        protected QueryTransformer replacementText(String text) {
            this.replacementText = text;
            return this;
        }

        /** Specifies a ColumnType to which this QueryTransformer should be
         *  applied, i.e., a query will only be modified if the <i>group</i>
         *  is a column of the specified type; default is <b>null</b>, in which
         *  case a matching query is always modified, regardless of column type. */
        protected QueryTransformer columnType(ColumnType columnType) {
            this.columnType = columnType;
            return this;
        }

        /** Specifies a value to be multiplied by the (int-valued) group, in
         *  the transformed query (e.g. 8.0, to convert from bytes to bits);
         *  default is <b>null</b>, in which case it is ignored. */
        protected QueryTransformer multiplier(Double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        /** Specifies a minimum value for the result of multiplying the
         *  (int-valued) group by the <i>multiplier</i>; default is <b>null</b>,
         *  in which case it is ignored. */
        protected QueryTransformer minimum(Integer minimum) {
            this.minimum = minimum;
            return this;
        }

        /** Specifies one or more group names found within the <i>queryPattern</i>
         *  (e.g. "column"), the text matching each group will be modified as
         *  dictated by the other options (e.g. by adding a prefix and suffix). */
        protected QueryTransformer groups(String ... groups) {
            this.groups.addAll(Arrays.asList(groups));
            return this;
        }

        /** Specifies whether or not to print debug info; default is <b>false</b>. */
        protected QueryTransformer debugPrint(boolean debugPrint) {
            this.debugPrint = debugPrint;
            return this;
        }

        /** Specifies to print debug info. */
        protected QueryTransformer debugPrint() {
            debugPrint(true);
            return this;
        }

    }

    /** Returns true if the <i>columnName</i> is an integer column (including
     *  types TINYINT, SMALLINT, INTEGER, BIGINT, etc.); false otherwise. */
    static private boolean isIntegerColumn(String columnName, String... tableNames) {
        // TODO: we may want to modify this to actually check the column type,
        // rather than just go by the column name (ENG-9945)
        if (DEBUG) {
            System.out.println("In NonVoltDBBackend.isIntegerColumn, with columnName: '" + columnName + "'");
        }
        if (columnName == null) {
            return false;
        }
        String columnNameUpper = columnName.trim().toUpperCase();
        return  columnNameUpper.equals("ID")   || columnNameUpper.equals("NUM") ||
                columnNameUpper.equals("TINY") || columnNameUpper.equals("SMALL") ||
                columnNameUpper.equals("BIG")  || columnNameUpper.equals("POINTS") ||
                columnNameUpper.equals("WAGE") || columnNameUpper.equals("DEPT") ||
                columnNameUpper.equals("AGE")  || columnNameUpper.equals("RENT") ||
                columnNameUpper.equals("A1")   || columnNameUpper.equals("A2") ||
                columnNameUpper.equals("A3")   || columnNameUpper.equals("A4") ||
                columnNameUpper.equals("V_G1") || columnNameUpper.equals("V_CNT") ||
                columnNameUpper.equals("V_G2") || columnNameUpper.equals("V_SUM_AGE") ||
                columnNameUpper.equals("V_SUM_RENT");
    }

    /** Returns true if the <i>columnName</i> is a Geospatial column type, i.e. a
     *  GEOGRAPHY_POINT (point) or GEOGRAPHY (polygon) column; false otherwise. */
    static private boolean isGeoColumn(String columnName, String... tableNames) {
        // TODO: we may want to modify this to actually check the column type,
        // rather than just go by the column name (ENG-9945)
        if (DEBUG) {
            System.out.println("In NonVoltDBBackend.isGeoColumn, with columnName: '" + columnName + "'");
        }
        if (columnName == null) {
            return false;
        }
        String columnNameUpper = columnName.trim().toUpperCase();
        return columnNameUpper.startsWith("PT") || columnNameUpper.startsWith("POLY");
    }

    /** Returns the number of occurrence of the specified character in the specified String. */
    static private int numOccurencesOfCharIn(String str, char ch) {
        int num = 0;
        for (int i = str.indexOf(ch); i >= 0 ; i = str.indexOf(ch, i+1)) {
            num++;
        }
        return num;
    }

    /** Returns the Nth occurrence of the specified character in the specified String. */
    static private int indexOfNthOccurrenceOfCharIn(String str, char ch, int n) {
        int index = -1;
        for (int i=0; i < n; i++) {
            index = str.indexOf(ch, index+1);
            if (index < 0) {
                return -1;
            }
        }
        return index;
    }

    /** Simply returns a String consisting of the <i>prefix</i>, <i>group</i>,
     *  and <i>suffix</i> concatenated (in that order), but being careful not
     *  to include more close-parentheses than open-parentheses; if the group
     *  does contain more close-parens than open-parens, the <i>suffix</i> is
     *  inserted before the extra close-parens, instead of at the very end. */
    static private String handleParens(String group, String prefix, String suffix) {
        int numOpenParens  = numOccurencesOfCharIn(group, '(');
        int numCloseParens = numOccurencesOfCharIn(group, ')');
        if (numOpenParens >= numCloseParens || suffix.isEmpty()) {
            return (prefix + group + suffix);
        } else {  // numOpenParens < numCloseParens
            int index;
            if (numOpenParens == 0) {
                index = indexOfNthOccurrenceOfCharIn(group, ')', 1) - 1;
            } else {
                index = indexOfNthOccurrenceOfCharIn(group, ')', numOpenParens);
            }
            return (prefix + group.substring(0, index+1) + suffix + group.substring(index+1));
        }
    }

    /**
     * Modifies a <i>query</i> containing the specified <i>queryPattern</i>, in
     * such a way that the backend database (e.g. PostgreSQL) results will match
     * VoltDB results, typically by adding a <i>prefix</i> and/or <i>suffix</i>,
     * either to individual <i>groups</i> within the <i>queryPattern</i>, or to
     * the <i>queryPattern</i> as a whole.
     *
     * @param query - the query text (DDL, DML or DQL) to be transformed.
     * @param qt - a QueryTransformer object, specifying the various options to
     * be used to transform the query, e.g., a <i>queryPattern</i>, <i>prefix</i>,
     * <i>suffix</i>, or one or more <i>groups</i>. For details of all options,
     * see the <i>QueryTransformer</i> JavaDoc.
     *
     * @return the <i>query</i>, transformed in the specified ways (possibly
     * unchanged).
     * @throws NullPointerException if <i>query</i> or <i>qt</i> is <b>null</b>,
     * or if the <i>qt</i>'s <i>queryPattern</i>, <i>initText</i>, <i>prefix</i>,
     * or <i>suffix</i> is <b>null</b>.
     */
    static protected String transformQuery(String query, QueryTransformer qt) {
        StringBuffer modified_query = new StringBuffer();
        Matcher matcher = qt.queryPattern.matcher(query);
        int count = 0;
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer(qt.initialText);
            String wholeMatch = matcher.group();
            String group = wholeMatch;
            if (qt.debugPrint) {
                if (count < 1) {
                    System.out.println("In NonVoltDBBackend.transformQuery,\n  with query    : " + query);
                    System.out.println("  queryPattern: " + qt.queryPattern);
                    System.out.println("  initialText, prefix, suffix, useAltSuffixAfter, altSuffix, replacementText:\n    '"
                            + qt.initialText + "', '" + qt.prefix + "', '" + qt.suffix + "', '"
                            + qt.useAltSuffixAfter + "', '" + qt.altSuffix + "', '" + qt.replacementText
                            + "'\n  useWholeMatch, columnType, multiplier, minimum, debugPrint; groups:\n    "
                            + qt.useWholeMatch + ", " + qt.columnType + ", " + qt.multiplier + ", "
                            + qt.minimum + ", " + qt.debugPrint + "\n    " + qt.groups);
                }
                System.out.println("  " + ++count + ".wholeMatch: " + wholeMatch);
            }
            for (String groupName : qt.groups) {
                group = matcher.group(groupName);
                if (qt.debugPrint) {
                    System.out.println("    group     : " + group);
                }
                if (group == null) {
                    break;
                } else if (!qt.useWholeMatch) {
                    String groupValue = group, suffixValue = qt.suffix;
                    // Check for the case where a multiplier & minimum are used
                    if (qt.multiplier != null && qt.minimum != null) {
                        groupValue = Long.toString(Math.round(Math.max(Integer.parseInt(group) * qt.multiplier, qt.minimum)));
                    }
                    // Check for the ending that indicates to use the alternate suffix
                    if (qt.altSuffix != null && group.toUpperCase().endsWith(qt.useAltSuffixAfter)) {
                        suffixValue = qt.altSuffix;
                    }
                    // Make sure not to swallow up extra ')', in this group
                    replaceText.append(handleParens(groupValue, qt.prefix, suffixValue));
                }
            }
            if (qt.useWholeMatch) {
                if (qt.columnType != null && (
                           (qt.columnType == ColumnType.INTEGER && !isIntegerColumn(group))
                        || (qt.columnType == ColumnType.GEO     && !isGeoColumn(group)) )) {
                    // Make no changes to query (when columnType is specified,
                    // but does not match the column type found in this query)
                    replaceText.append(wholeMatch);
                } else {
                    // Check for the case where the group is to be replaced with replacementText
                    if (qt.replacementText != null) {
                        wholeMatch = wholeMatch.replace(group, qt.replacementText);
                    }
                    // Make sure not to swallow up extra ')', in whole match
                    replaceText.append(handleParens(wholeMatch, qt.prefix, qt.suffix));
                }
            }
            if (qt.debugPrint) {
                System.out.println("  replaceText : " + replaceText);
            }
            matcher.appendReplacement(modified_query, replaceText.toString());
        }
        matcher.appendTail(modified_query);
        if ((DEBUG || qt.debugPrint) && !query.equalsIgnoreCase(modified_query.toString())) {
            System.out.println("In NonVoltDBBackend.transformQuery,\n  with query    : " + query);
            System.out.println("  modified_query: " + modified_query);
        }
        return modified_query.toString();
    }

    /** Calls the transformQuery method above multiple times, for each
     *  specified QueryTransformer. */
    static protected String transformQuery(String query, QueryTransformer ... qts) {
        String result = query;
        for (QueryTransformer qt : qts) {
            result = transformQuery(result, qt);
        }
        return result;
    }

    /** Optionally (only if both the PRINT_TRANSFORMED_SQL constant and the
     *  <i>print</i> argument are true), prints the original and modified
     *  SQL statements, to a "Transformed SQL" output file, for the current
     *  SQLCoverage test suite (if any). */
    static protected void printTransformedSql(String originalSql, String modifiedSql, boolean print) {
        if (PRINT_TRANSFORMED_SQL && print) {
            try {
                if (TRANSFORMED_SQL_FILE_WRITER != null) {
                    TRANSFORMED_SQL_FILE_WRITER.write("original SQL: " + originalSql + "\n");
                    TRANSFORMED_SQL_FILE_WRITER.write("modified SQL: " + modifiedSql + "\n");
                } else {
                    System.out.println("original SQL: " + originalSql);
                    System.out.println("modified SQL: " + modifiedSql);
                }
            } catch (IOException e) {
                System.out.println("Caught IOException:\n    " + e);
                System.out.println("original SQL: " + originalSql);
                System.out.println("modified SQL: " + modifiedSql);
            }
        }
    }

    protected abstract void shutdown();

    public void shutdownInstance()
    {
        synchronized(backendLock) {
            if (m_backend != null) {
                m_backend.shutdown();
                m_backend = null;
            }
        }
    }

    public void runDDL(String ddl) {
        try {
            //LOG.info("Executing " + ddl);
            Statement stmt = dbconn.createStatement();
            /*boolean success =*/ stmt.execute(ddl);
            SQLWarning warn = stmt.getWarnings();
            if (warn != null)
                sqlLog.warn(warn.getMessage());
            //LOG.info("SQL DDL execute result: " + (success ? "true" : "false"));
        } catch (SQLException e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_Backend_RunDDLFailed.name(), new Object[] { ddl }, e);
        }

    }

    /**
     * Returns a VoltTable.ColumnInfo of appropriate type, based on a
     * <i>typeName</i> and <i>colName</i> (both Strings).
     * This version checks for standard column types used by most databases
     * (and by VoltDB and HSQL, in particular); sub-classes can override it,
     * to handle column types particular to that database.
     */
    protected VoltTable.ColumnInfo getColumnInfo(String typeName, String colName) {
        if (typeName.equalsIgnoreCase("VARCHAR"))
            return new VoltTable.ColumnInfo(colName, VoltType.STRING);
        else if (typeName.equalsIgnoreCase("TINYINT"))
            return new VoltTable.ColumnInfo(colName, VoltType.TINYINT);
        else if (typeName.equalsIgnoreCase("SMALLINT"))
            return new VoltTable.ColumnInfo(colName, VoltType.SMALLINT);
        else if (typeName.equalsIgnoreCase("INTEGER"))
            return new VoltTable.ColumnInfo(colName, VoltType.INTEGER);
        else if (typeName.equalsIgnoreCase("BIGINT"))
            return new VoltTable.ColumnInfo(colName, VoltType.BIGINT);
        else if (typeName.equalsIgnoreCase("DECIMAL"))
            return new VoltTable.ColumnInfo(colName, VoltType.DECIMAL);
        else if (typeName.equalsIgnoreCase("FLOAT"))
            return new VoltTable.ColumnInfo(colName, VoltType.FLOAT);
        else if (typeName.equalsIgnoreCase("TIMESTAMP"))
            return new VoltTable.ColumnInfo(colName, VoltType.TIMESTAMP);
        else if (typeName.equalsIgnoreCase("VARBINARY"))
            return new VoltTable.ColumnInfo(colName, VoltType.VARBINARY);
        else if (typeName.equalsIgnoreCase("CHARACTER"))
            return new VoltTable.ColumnInfo(colName, VoltType.STRING);
        else
            throw new ExpectedProcedureException("Trying to create a column in " + m_database_type
                    + " Backend with a (currently) unsupported type: " + typeName);
    }

    public VoltTable runDML(String dml) {
        dml = dml.trim();
        String indicator = dml.substring(0, 1).toLowerCase();
        if (indicator.equals("s") || // "s" is for "select ..."
            indicator.equals("(")) { // "(" is for "(select ... UNION ...)" et. al.
            try {
                Statement stmt = dbconn.createStatement();
                sqlLog.l7dlog( Level.DEBUG, LogKeys.sql_Backend_ExecutingDML.name(), new Object[] { dml }, null);
                sqlLog.debug("Executing " + dml);
                ResultSet rs = stmt.executeQuery(dml);
                ResultSetMetaData rsmd = rs.getMetaData();

                // note the index values here carefully
                VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[rsmd.getColumnCount()];
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String colname = rsmd.getColumnLabel(i);
                    String type = rsmd.getColumnTypeName(i);
                    //LOG.fine("Column type: " + type);
                    columns[i-1] = getColumnInfo(type, colname);
                }
                VoltTable table = new VoltTable(columns);
                while (rs.next()) {
                    Object[] row = new Object[table.getColumnCount()];
                    for (int i = 0; i < table.getColumnCount(); i++) {
                        if (table.getColumnType(i) == VoltType.STRING)
                            row[i] = rs.getString(i + 1);
                        else if (table.getColumnType(i) == VoltType.TINYINT)
                            row[i] = rs.getByte(i + 1);
                        else if (table.getColumnType(i) == VoltType.SMALLINT)
                            row[i] = rs.getShort(i + 1);
                        else if (table.getColumnType(i) == VoltType.INTEGER)
                            row[i] = rs.getInt(i + 1);
                        else if (table.getColumnType(i) == VoltType.BIGINT)
                            row[i] = rs.getLong(i + 1);
                        else if (table.getColumnType(i) == VoltType.DECIMAL)
                            row[i] = rs.getBigDecimal(i + 1);
                        else if (table.getColumnType(i) == VoltType.FLOAT)
                            row[i] = rs.getDouble(i + 1);
                        else if (table.getColumnType(i) == VoltType.VARBINARY)
                            row[i] = rs.getBytes(i + 1);
                        else if (table.getColumnType(i) == VoltType.TIMESTAMP) {
                            Timestamp t = rs.getTimestamp(i + 1);
                            if (t == null) {
                                row[i] = null;
                            } else {
                                // convert from millisecond to microsecond granularity
                                row[i] = new org.voltdb.types.TimestampType(t.getTime() * 1000);
                            }
                        } else {
                            throw new ExpectedProcedureException("Trying to read a (currently) unsupported type from a JDBC resultset.");
                        }
                        if (rs.wasNull()) {
                            // JDBC returns 0/0.0 instead of null. Put null into the row.
                            row[i] = null;
                        }
                    }

                    table.addRow(row);
                }
                stmt.close();
                rs.close();
                return table;
            } catch (Exception e) {
                if (e instanceof ExpectedProcedureException) {
                    throw (ExpectedProcedureException)e;
                }
                sqlLog.l7dlog( Level.TRACE, LogKeys.sql_Backend_DmlError.name(), e);
                throw new ExpectedProcedureException(m_database_type + " Backend DML Error ", e);
            }
        }
        else {
            try {
                Statement stmt = dbconn.createStatement();
                sqlLog.debug("Executing: " + dml);
                long ucount = stmt.executeUpdate(dml);
                sqlLog.debug("  result: " + String.valueOf(ucount));
                VoltTable table = new VoltTable(new VoltTable.ColumnInfo("", VoltType.BIGINT));
                table.addRow(ucount);
                return table;
            } catch(SQLException e) {
                // glorious hack to determine if the error is a constraint failure
                if (e.getMessage().contains("constraint")) {
                    sqlLog.l7dlog( Level.TRACE, LogKeys.sql_Backend_ConvertingHSQLExtoCFEx.name(), e);
                    final byte messageBytes[] = e.getMessage().getBytes();
                    ByteBuffer b = ByteBuffer.allocate(25 + messageBytes.length);
                    b.putInt(messageBytes.length);
                    b.put(messageBytes);
                    b.put(e.getSQLState().getBytes());
                    b.putInt(0); // ConstraintFailure.type
                    try {
                        FastSerializer.writeString(m_database_type, b);
                    }
                    catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    b.putInt(0);//Table size is 0
                    b.rewind();
                    throw new ConstraintFailureException(b);
                }
                else {
                    sqlLog.l7dlog( Level.TRACE, LogKeys.sql_Backend_DmlError.name(), e);
                    throw new ExpectedProcedureException(m_database_type + " Backend DML Error ", e);
                }

            } catch (Exception e) {
                // rethrow an expected exception
                sqlLog.l7dlog( Level.TRACE, LogKeys.sql_Backend_DmlError.name(), e);
                throw new ExpectedProcedureException(m_database_type + " Backend DML Error ", e);
            }
        }
    }

    VoltTable runSQLWithSubstitutions(final SQLStmt stmt, ParameterSet params, byte[] paramJavaTypes) {
        //HSQLProcedureWrapper does nothing smart. it just implements this interface with runStatement()
        StringBuilder sqlOut = new StringBuilder(stmt.getText().length() * 2);

        assert(paramJavaTypes != null);

        int lastIndex = 0;
        String sql = stmt.getText();

        // if there's no ? in the statmemt, then zero out any auto-parameterization
        int paramCount = StringUtils.countMatches(sql, "?");
        if (paramCount == 0) {
            params = ParameterSet.emptyParameterSet();
            paramJavaTypes = new byte[0];
        }

        Object[] paramObjs = params.toArray();
        for (int i = 0; i < paramObjs.length; i++) {
            int nextIndex = sql.indexOf('?', lastIndex);
            if (nextIndex == -1)
                throw new RuntimeException("SQL Statement has more arguments than params.");
            sqlOut.append(sql, lastIndex, nextIndex);
            lastIndex = nextIndex + 1;

            VoltType type = VoltType.get(paramJavaTypes[i]);

            if (VoltType.isNullVoltType(paramObjs[i])) {
                sqlOut.append("NULL");
            }
            else if (paramObjs[i] instanceof TimestampType) {
                if (type != VoltType.TIMESTAMP)
                    throw new RuntimeException("Inserting date into mismatched column type in HSQL.");
                TimestampType d = (TimestampType) paramObjs[i];
                // convert VoltDB's microsecond granularity to millis.
                Timestamp t = new Timestamp(d.getTime() / 1000);
                sqlOut.append('\'').append(t.toString()).append('\'');
            }
            else if (paramObjs[i] instanceof byte[]) {
                if (type == VoltType.STRING) {
                    // Convert from byte[] -> String; escape single quotes
                    try {
                        sqlOut.append(sqlEscape(new String((byte[]) paramObjs[i], "UTF-8")));
                    } catch (UnsupportedEncodingException e) {
                        // should NEVER HAPPEN
                        System.err.println("FATAL: Your JVM doens't support UTF-8");
                        System.exit(-1);
                    }
                }
                else if (type == VoltType.VARBINARY) {
                    // Convert from byte[] -> String; using hex
                    sqlOut.append(sqlEscape(Encoder.hexEncode((byte[]) paramObjs[i])));
                }
                else {
                    throw new RuntimeException("Inserting string/varbinary (bytes) into mismatched column type in HSQL.");
                }
            }
            else if (paramObjs[i] instanceof String) {
                if (type != VoltType.STRING)
                    throw new RuntimeException("Inserting string into mismatched column type in HSQL.");
                // Escape single quotes
                sqlOut.append(sqlEscape((String) paramObjs[i]));
            }
            else {
                if (type == VoltType.TIMESTAMP) {
                    long t = Long.parseLong(paramObjs[i].toString());
                    TimestampType d = new TimestampType(t);
                    // convert VoltDB's microsecond granularity to millis
                    Timestamp ts = new Timestamp(d.getTime() * 1000);
                    sqlOut.append('\'').append(ts.toString()).append('\'');
                }
                else
                    sqlOut.append(paramObjs[i].toString());
            }
        }
        sqlOut.append(sql, lastIndex, sql.length());

        return runDML(sqlOut.toString());
    }

    private static String sqlEscape(String input) {
        return "\'" + input.replace("'", "''") + "\'";
    }
}
