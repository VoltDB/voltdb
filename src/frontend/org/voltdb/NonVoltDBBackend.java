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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
    protected static FileWriter transformedSqlFileWriter;
    protected static final boolean DEBUG = false;

    /** Pattern used to recognize "variables", such as {table} or {column:pk},
     *  in a QueryTransformer's prefix, suffix or (group) replacement text, for
     *  which an appropriate group value will be substituted. */
    protected static final Pattern groupNameVariables = Pattern.compile(
            "\\{(?<groupName>\\w+)(:(?<columnType>\\w+))?\\}");

    // Used below, to define SELECT_TABLE_NAMES
    private static final String TABLE_REFERENCE = "(?<table1>\\w+)(\\s+(AS\\s+)?\\w+)?";
    private static final String COMMA_OR_JOIN_CLAUSE = "(,|\\s+((INNER|CROSS|((LEFT|RIGHT|FULL)\\s+)?OUTER)\\s+)?JOIN\\s)\\s*";
    private static final String COMPARISON_OP = "\\s*(=|!=|<>|<|>|<=|>=|IS\\s+(NOT\\s+)?DISTINCT\\s+FROM)\\s+";
    private static final String ON_OR_USING_CLAUSE = "\\s+((ON\\s+(\\w+\\.)?\\w+"+COMPARISON_OP+"(\\w+\\.)?\\w+"
            + "|USING\\s+\\(\\w+(,\\s*\\w+\\s*)*\\)))?\\s*";

    /** Pattern used to recognize the table names in a SELECT statement; will
     *  recognize up to 4 table names. */
    private static final Pattern SELECT_TABLE_NAMES = Pattern.compile(
            "(?<!DISTINCT)\\s+FROM\\s+"+TABLE_REFERENCE+"\\s*"
            + "(" + COMMA_OR_JOIN_CLAUSE + TABLE_REFERENCE.replace('1', '2') + ON_OR_USING_CLAUSE + ")?"
            + "(" + COMMA_OR_JOIN_CLAUSE + TABLE_REFERENCE.replace('1', '3') + ON_OR_USING_CLAUSE + ")?"
            + "(" + COMMA_OR_JOIN_CLAUSE + TABLE_REFERENCE.replace('1', '4') + ON_OR_USING_CLAUSE + ")?",
            Pattern.CASE_INSENSITIVE);
    private static final int MAX_NUM_TABLE_NAMES = 4;

    /** Pattern used to recognize the table name in a SQL statement that is not
     *  a SELECT statement, i.e., in an UPDATE, INSERT, UPSERT, or DELETE
     *  statement (TRUNCATE and all DDL statements are omitted because they are
     *  not relevant where this gets used, in determining column data types). */
    private static final Pattern NON_SELECT_TABLE_NAME = Pattern.compile(
            "\\*(UPDATE|(IN|UP)SERT\\s+INTO\\s+|DELETE\\s+FROM)\\s+(?<table1>\\w+)",
            Pattern.CASE_INSENSITIVE);


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
            DatabaseMetaData meta = dbconn.getMetaData();
            System.out.println("Using database: " + meta.getDatabaseProductName()+" "+meta.getDatabaseProductVersion());
            System.out.println(" & JDBC driver: " + meta.getDriverName()+", "+meta.getDriverVersion());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to open connection to: " + connectionURL, e);
        }

        // If '-Dsqlcoverage.transform.sql.file=...' was specified on the
        // command line, print info when transforming SQL statements into
        // a format that the backend database can understand
        String transformedSqlOutputFileName = System.getProperty("sqlcoverage.transform.sql.file", null);
        if (transformedSqlOutputFileName == null) {
            transformedSqlFileWriter = null;
        } else {
            try {
                transformedSqlFileWriter = new FileWriter(transformedSqlOutputFileName, true);
            } catch (IOException e) {
                transformedSqlFileWriter = null;
                System.out.println("Caught IOException:\n    " + e
                        + "\nTransformed SQL output will not be printed.");
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
        /** Only a BIGINT column type, not of any of the smaller integer types. */
        BIGINT,
        /** Any Geospatial column type, including GEOGRAPHY_POINT or GEOGRAPHY. */
        GEO
    }

    /**
     * A QueryTransformer object is used to specify (using the builder pattern)
     * which options should be used, when transforming SQL statements (DDL, DML
     * or DQL) fitting certain patterns, as is often needed in order for the
     * results of a backend database (e.g. PostgreSQL) to match those of VoltDB.
     * This collection of options (the QueryTransformer object) is then passed
     * to the <i>transformQuery</i> method (see below).
     */
    protected static class QueryTransformer {
        // Required parameter
        private Pattern m_queryPattern;

        // Optional parameters, initialized with default values
        private String m_initialText = "";
        private String m_prefix = "";
        private String m_suffix = "";
        private String m_altSuffix = null;
        private String m_useAltSuffixAfter = null;
        private boolean m_useWholeMatch = false;
        private String m_replacementText = null;
        private ColumnType m_columnType = null;
        private Double m_multiplier = null;
        private Integer m_minimum = null;
        private List<String> m_groups = new ArrayList<String>();
        private List<String> m_groupReplacementTexts = new ArrayList<String>();
        private List<String> m_exclude = new ArrayList<String>();
        private boolean m_debugPrint = false;

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
            this.m_queryPattern = queryPattern;
        }

        /** Specifies an initial string with which to begin the replacement
         *  text (e.g. "ORDER BY"); default is an empty string; may not be
         *  <b>null</b>. */
        protected QueryTransformer initialText(String text) {
            if (text == null) {
                throw new IllegalArgumentException("The initialText may not be null.");
            }
            this.m_initialText = text;
            return this;
        }

        /** Specifies a string to appear before each group, or before the whole
         *  <i>queryPattern</i> (e.g. "TRUNC ( "); default is an empty string;
         *  may not be <b>null</b>. */
        protected QueryTransformer prefix(String text) {
            if (text == null) {
                throw new IllegalArgumentException("The prefix may not be null.");
            }
            this.m_prefix = text;
            return this;
        }

        /** Specifies a string to appear after each group, or after the whole
         *  <i>queryPattern</i> (e.g. " NULLS FIRST"); default is an empty
         *  string; may not be <b>null</b>. */
        protected QueryTransformer suffix(String text) {
            if (text == null) {
                throw new IllegalArgumentException("The suffix may not be null.");
            }
            this.m_suffix = text;
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
            this.m_useAltSuffixAfter = useAltSuffixAfter;
            this.m_altSuffix = altSuffix;
            return this;
        }

        /** Specifies whether or not to use the whole matched pattern; when
         *  <b>true</b>, the <i>prefix</i> and <i>suffix</i> will be applied
         *  to the whole <i>queryPattern</i>; when <b>false</b>, they will be
         *  applied to each <i>group</i> within it; default is <b>false</b>. */
        protected QueryTransformer useWholeMatch(boolean useWholeMatch) {
            this.m_useWholeMatch = useWholeMatch;
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
            this.m_replacementText = text;
            return this;
        }

        /** Specifies one or more strings to replace each group, within the
         *  whole match (e.g. "INSERT", to replace "UPSERT"); default is an
         *  empty list, in which case it is ignored. */
        protected QueryTransformer groupReplacementText(String ... text) {
            this.m_groupReplacementTexts = new ArrayList<String>();
            for (String t : text) {
                this.m_groupReplacementTexts.add(t);
            }
            return this;
        }

        /** Specifies a ColumnType to which this QueryTransformer should be
         *  applied, i.e., a query will only be modified if the <i>group</i>
         *  is a column of the specified type; default is <b>null</b>, in which
         *  case a matching query is always modified, regardless of column type. */
        protected QueryTransformer columnType(ColumnType columnType) {
            this.m_columnType = columnType;
            return this;
        }

        /** Specifies a value to be multiplied by the (int-valued) group, in
         *  the transformed query (e.g. 8.0, to convert from bytes to bits);
         *  default is <b>null</b>, in which case it is ignored. */
        protected QueryTransformer multiplier(Double multiplier) {
            this.m_multiplier = multiplier;
            return this;
        }

        /** Specifies a minimum value for the result of multiplying the
         *  (int-valued) group by the <i>multiplier</i>; default is <b>null</b>,
         *  in which case it is ignored. */
        protected QueryTransformer minimum(Integer minimum) {
            this.m_minimum = minimum;
            return this;
        }

        /** Specifies one or more group names found within the <i>queryPattern</i>
         *  (e.g. "column"), the text matching each group will be modified as
         *  dictated by the other options (e.g. by adding a prefix and suffix). */
        protected QueryTransformer groups(String ... groups) {
            this.m_groups.addAll(Arrays.asList(groups));
            return this;
        }

        /** Specifies one or more strings whose appearance in the group means
         *  that this group should not be changed (e.g. "TRUNC", so as not to
         *  wrap TRUNC around the same group twice); default is default is an
         *  empty list of strings, in which case it is ignored. */
        protected QueryTransformer exclude(String ... texts) {
            this.m_exclude.addAll(Arrays.asList(texts));
            return this;
        }

        /** Specifies whether or not to print debug info; default is <b>false</b>. */
        protected QueryTransformer debugPrint(boolean debugPrint) {
            this.m_debugPrint = debugPrint;
            return this;
        }

        /** Specifies to print debug info. */
        protected QueryTransformer debugPrint() {
            debugPrint(true);
            return this;
        }

        // Used by the toString() method.
        private String getNonEmptyValue(String name, Object value) {
            if (value == null) {
                return "";
            } else if (value instanceof Collection && ((Collection<?>)value).isEmpty()) {
                return "";
            } else if (value.toString().isEmpty()) {
                return "";
            } else {
                return "\n    " + name + ": " + value;
            }
        }

        // Useful for debugging
        @Override
        public String toString() {
            String result = "Pattern: " + m_queryPattern
                    + getNonEmptyValue("initialText", m_initialText)
                    + getNonEmptyValue("prefix",      m_prefix)
                    + getNonEmptyValue("suffix",      m_suffix)
                    + getNonEmptyValue("altSuffix",   m_altSuffix)
                    + getNonEmptyValue("useAltSuffixAfter", m_useAltSuffixAfter)
                    + getNonEmptyValue("replacementText",   m_replacementText)
                    + getNonEmptyValue("columnType", m_columnType)
                    + getNonEmptyValue("multiplier", m_multiplier)
                    + getNonEmptyValue("minimum", m_minimum)
                    + getNonEmptyValue("exclude",  m_exclude)
                    + getNonEmptyValue("groups",  m_groups)
                    + getNonEmptyValue("groupReplacementTexts", m_groupReplacementTexts)
                    + getNonEmptyValue("useWholeMatch", (m_useWholeMatch ? "true" : ""))
                    + getNonEmptyValue("debugPrint",    (m_debugPrint    ? "true" : ""));
            return result;
        }

    }

    /** Returns all column names for the specified table, in the order defined
     *  in the DDL. */
    protected List<String> getAllColumns(String tableName) {
        List<String> columns = new ArrayList<String>();
        try {
            // Lower-case table names are required for PostgreSQL; we might need to
            // alter this if we use another comparison database (besides HSQL) someday
            ResultSet rs = dbconn.getMetaData().getColumns(null, null, tableName.toLowerCase(), null);
            while (rs.next()) {
                columns.add(rs.getString(4));
            }
        } catch (SQLException e) {
            System.out.println("In NonVoltDBBackend.getAllColumns, caught SQLException: " + e);
        }
        return columns;
    }

    /** Returns all primary key column names for the specified table, in the
     *  order defined in the DDL. */
    protected List<String> getPrimaryKeys(String tableName) {
        List<String> pkCols = new ArrayList<String>();
        try {
            // Lower-case table names are required for PostgreSQL; we might need to
            // alter this if we use another comparison database (besides HSQL) someday
            ResultSet rs = dbconn.getMetaData().getPrimaryKeys(null, null, tableName.toLowerCase());
            while (rs.next()) {
                pkCols.add(rs.getString(4));
            }
        } catch (SQLException e) {
            System.out.println("In NonVoltDBBackend.getPrimaryKeys, caught SQLException: " + e);
        }
        return pkCols;
    }

    /** Returns all non-primary-key column names for the specified table, in the
     *  order defined in the DDL. */
    protected List<String> getNonPrimaryKeyColumns(String tableName) {
        List<String> columns = getAllColumns(tableName);
        columns.removeAll(getPrimaryKeys(tableName));
        return columns;
    }

    /** Returns the table (or view) names used in the specified SQL statement,
     *  whether it is a SELECT query or something else, i.e., an UPDATE, INSERT,
     *  UPSERT, or DELETE statement.<br>
     *  Note: TRUNCATE and all DDL statements are omitted because they are
     *  not relevant where this gets used, in determining column data types. */
    protected List<String> getTableNames(String sql) {
        Pattern[] patterns = {SELECT_TABLE_NAMES, NON_SELECT_TABLE_NAME};
        List<String> result = new ArrayList<String>();
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(sql);
            while (matcher.find()) {
                for (int i=1; i <= MAX_NUM_TABLE_NAMES; i++) {
                    String group = null;
                    try {
                        group = matcher.group("table"+i);
                    } catch (IllegalArgumentException e) {
                        // The two patterns have different numbers of groups
                        // (table1, table2, etc.), so this may well happen
                        break;
                    }
                    if (group != null) {
                        result.add(group);
                    }
                }
            }
        }
        return result;
    }

    /** Returns true if the <i>columnName</i> is one of the specified
     *  <i>columnTypes</i>, e.g., one of the integer column types, or one of
     *  the Geospatial column types - for one or more of the <i>tableNames</i>,
     *  if specified; otherwise, for any table in the database schema. */
    private boolean isColumnType(List<String> columnTypes, String columnName,
                                 List<String> tableNames) {
        if (tableNames == null || tableNames.size() == 0) {
            tableNames = Arrays.asList((String)null);
        }
        for (String tn : tableNames) {
            // Lower-case table and column names are required for PostgreSQL;
            // we might need to alter this if we use another comparison
            // database (besides HSQL) someday
            String tableName = (tn == null) ? tn : tn.trim().toLowerCase();
            try {
                ResultSet rs = dbconn.getMetaData().getColumns(null, null,
                        tableName, columnName.trim().toLowerCase());
                while (rs.next()) {
                    String columnType = getVoltColumnTypeName(rs.getString(6));
                    if (columnTypes.contains(columnType)) {
                        return true;
                    }
                }
            } catch (SQLException e) {
                System.out.println("In NonVoltDBBackend.isColumnType, with tableName "+tableName+", columnName "
                        + columnName+", columnTypes "+columnTypes+", caught SQLException:\n  " + e);
            }
        }
        return false;
    }

    /** Returns true if the <i>columnName</i> is a Geospatial column type, i.e.,
     *  a GEOGRAPHY_POINT (point) or GEOGRAPHY (polygon) column, or equivalents
     *  in a comparison, non-VoltDB database; false otherwise. */
    private boolean isGeoColumn(String columnName, List<String> tableNames) {
        List<String> geoColumnTypes = Arrays.asList("GEOGRAPHY", "GEOGRAPHY_POINT");
        return isColumnType(geoColumnTypes, columnName, tableNames);
    }

    /** Returns true if the <i>columnName</i> is of column type BIGINT, or
     *  equivalents in a comparison, non-VoltDB database; false otherwise. */
    private boolean isBigintColumn(String columnName, List<String> tableNames) {
        List<String> bigintColumnTypes = Arrays.asList("BIGINT");
        return isColumnType(bigintColumnTypes, columnName, tableNames);
    }

    /** Returns true if the <i>columnName</i> is an integer column (including
     *  types TINYINT, SMALLINT, INTEGER, BIGINT, or equivalents in a
     *  comparison, non-VoltDB database); false otherwise. */
    private boolean isIntegerColumn(String columnName, List<String> tableNames) {
        List<String> intColumnTypes = Arrays.asList("TINYINT", "SMALLINT", "INTEGER", "BIGINT");
        return isColumnType(intColumnTypes, columnName, tableNames);
    }

    /** Returns true if the <i>columnOrConstant</i> is an integer constant;
     *  false otherwise. */
    private boolean isIntegerConstant(String columnOrConstant) {
        try {
            Integer.parseInt(columnOrConstant.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** Returns true if the <i>columnOrConstant</i> is either an integer
     *  constant or an integer column (including types TINYINT, SMALLINT,
     *  INTEGER, BIGINT, or equivalents in a comparison, non-VoltDB database);
     *  false otherwise. */
    private boolean isInteger(String columnOrConstant, List<String> tableNames) {
        return isIntegerConstant(columnOrConstant) || isIntegerColumn(columnOrConstant, tableNames);
    }

    /** Returns the column type name, in VoltDB, corresponding to the specified
     *  column type name in the comparison non-VoltDB backend database. This
     *  base version merely passes back the identical column type name, but it
     *  may be overridden by sub-classes, to return the appropriate values for
     *  that database. */
    protected String getVoltColumnTypeName(String columnTypeName) {
        return columnTypeName;
    }

    /** This base version simply returns a String consisting of the <i>prefix</i>,
     *  <i>group</i>, and <i>suffix</i> concatenated (in that order); however,
     *  it may be overridden to do something more complicated, to make sure that
     *  the prefix and suffix go in the right place, relative to any parentheses
     *  found in the group. */
    protected String handleParens(String group, String prefix, String suffix) {
        return prefix + group + suffix;
    }

    /** Potentially returns the specified String, after replacing certain
     *  "variables", such as {table} or {column:pk}, in a QueryTransformer's
     *  prefix, suffix or (group) replacement text, for which a corresponding
     *  group value will be substituted. However, this base version just
     *  returns the original String unchanged; it may be overridden by
     *  sub-classes, to determine appropriate changes for that non-VoltDB
     *  backend database. */
    protected String replaceGroupNameVariables(String str,
            List<String> groupNames, List<String> groupValues) {
        return str;
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
    protected String transformQuery(String query, QueryTransformer qt) {
        StringBuffer modified_query = new StringBuffer();
        Matcher matcher = qt.m_queryPattern.matcher(query);
        int count = 0;
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer(qt.m_initialText);
            String wholeMatch = matcher.group();
            String lastGroup = wholeMatch;
            List<String> groups = new ArrayList<String>();
            if (qt.m_debugPrint) {
                if (count < 1) {
                    System.out.println("In NonVoltDBBackend.transformQuery,\n  with query    : " + query);
                    System.out.println("  QueryTransformer:\n    " + qt);
                }
                System.out.println("  " + ++count + ".wholeMatch: " + wholeMatch);
            }
            for (String groupName : qt.m_groups) {
                String group = matcher.group(groupName);
                groups.add(group);
                if (qt.m_debugPrint) {
                    System.out.println("    group     : " + group);
                }
                if (group == null) {
                    continue;
                } else if (!qt.m_useWholeMatch) {
                    String groupValue = group, suffixValue = qt.m_suffix;
                    // Check for the case where a multiplier & minimum are used
                    if (qt.m_multiplier != null && qt.m_minimum != null) {
                        groupValue = Long.toString(Math.round(Math.max(Integer.parseInt(group) * qt.m_multiplier, qt.m_minimum)));
                    }
                    // Check for the ending that indicates to use the alternate suffix
                    if (qt.m_altSuffix != null && group.toUpperCase().endsWith(qt.m_useAltSuffixAfter)) {
                        suffixValue = qt.m_altSuffix;
                    }
                    // Make sure not to swallow up extra ')', in this group
                    replaceText.append(handleParens(groupValue, qt.m_prefix, suffixValue));
                }
                lastGroup = group;
            }
            if (qt.m_debugPrint) {
                System.out.println("    lastGroup : " + lastGroup);
            }
            if (qt.m_useWholeMatch) {
                boolean noChangesNeeded = false;
                // If the matched string contains one of the strings in the
                // (possibly empty) list of excluded strings, then no changes
                // are needed
                if (qt.m_exclude != null) {
                    for (String excl : qt.m_exclude) {
                        if (wholeMatch.contains(excl)) {
                            noChangesNeeded = true;
                        }
                    }
                }
                // When columnType is specified, it means only modify queries
                // that use that type; so if the relevant column(s) are not of
                // the specified type, no changes are needed
                if (!noChangesNeeded && qt.m_columnType != null) {
                    // When columnType is GEO, check whether the last, and
                    // presumably only, column is not of that type, in which
                    // case no changes are needed
                    if (qt.m_columnType == ColumnType.GEO && !isGeoColumn(lastGroup, null)) {
                        noChangesNeeded = true;
                    // When columnType is BIGINT, check whether any of the columns
                    // are of BIGINT type, in which case changes *are* needed
                    } else if (qt.m_columnType == ColumnType.BIGINT) {
                        noChangesNeeded = true;
                        List<String> tableNames = getTableNames(query);
                        for (int i=0; i < groups.size(); i++) {
                            String group = groups.get(i);
                            if (group != null && isBigintColumn(group, tableNames)) {
                                noChangesNeeded = false;
                                break;
                            }
                        }
                    // When columnType is INTEGER, check whether any of the
                    // columns (or constants) are non-integer, in which case
                    // no changes are needed
                    } else if (qt.m_columnType == ColumnType.INTEGER) {
                        for (int i=0; i < groups.size(); i++) {
                            String group = groups.get(i);
                            // Not specifying the table name(s) here (i.e., the
                            // null second argument to isInteger) is deliberately
                            // saying to treat anything that "looks" like an integer
                            // (i.e., the column name is one that is normally used
                            // for an integer column) like an integer; this solves
                            // certain odd materialized view cases where PostgreSQL
                            // decides that the SUM of BIGINT is a DECIMAL, but
                            // VoltDB treats it as BIGINT
                            if (group != null && !isInteger(group, null)) {
                                noChangesNeeded = true;
                                break;
                            }
                        }
                    }
                }
                if (noChangesNeeded) {
                    // Make no changes to the query, if one of the excluded
                    // strings was found, or when the columnType is specified,
                    // but does not match the column type(s) found in this query
                    replaceText.append(wholeMatch);
                } else {
                    // Check for the case where the group (or the whole text) is to be replaced with replacementText
                    if (qt.m_replacementText != null) {
                        wholeMatch = wholeMatch.replace(lastGroup, qt.m_replacementText);
                    }
                    // Check for the case where each group is to be replaced using groupReplacementTexts
                    if (qt.m_groupReplacementTexts != null && !qt.m_groupReplacementTexts.isEmpty()) {
                        for (int i=0; i < Math.min(groups.size(), qt.m_groupReplacementTexts.size()); i++) {
                            if (groups.get(i) != null && qt.m_groupReplacementTexts.get(i) != null) {
                                wholeMatch = wholeMatch.replaceFirst(groups.get(i), qt.m_groupReplacementTexts.get(i));
                            }
                        }
                    }
                    // Make sure not to swallow up extra ')', in whole match; and
                    // replace symbols like {foo} with the appropriate group values
                    replaceText.append(replaceGroupNameVariables(
                            handleParens(wholeMatch, qt.m_prefix, qt.m_suffix),
                            qt.m_groups, groups));
                }
            }
            if (qt.m_debugPrint) {
                System.out.println("  replaceText : " + replaceText);
            }
            matcher.appendReplacement(modified_query, replaceText.toString());
        }
        matcher.appendTail(modified_query);
        if ((DEBUG || qt.m_debugPrint) && !query.equalsIgnoreCase(modified_query.toString())) {
            System.out.println("In NonVoltDBBackend.transformQuery,\n  with query    : " + query);
            System.out.println("  modified_query: " + modified_query);
        }
        return modified_query.toString();
    }

    /** Calls the transformQuery method above multiple times, for each
     *  specified QueryTransformer. */
    protected String transformQuery(String query, QueryTransformer ... qts) {
        String result = query;
        for (QueryTransformer qt : qts) {
            result = transformQuery(result, qt);
        }
        return result;
    }

    /** Prints the original and modified SQL statements, to the "Transformed
     *  SQL" output file, assuming that that file is defined; and only if
     *  the original and modified SQL are not the same, i.e., only if some
     *  transformation has indeed taken place. */
    static protected void printTransformedSql(String originalSql, String modifiedSql) {
        if (transformedSqlFileWriter != null && !originalSql.equals(modifiedSql)) {
            try {
                transformedSqlFileWriter.write("original SQL: " + originalSql + "\n");
                transformedSqlFileWriter.write("modified SQL: " + modifiedSql + "\n");
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
                    ByteBuffer b = ByteBuffer.allocate(100 + messageBytes.length);
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

            if (VoltType.isVoltNullValue(paramObjs[i])) {
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
