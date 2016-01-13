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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

/**
 * A wrapper around a PostgreSQL database server, and JDBC connection. This
 * class can be used to execute SQL statements instead of the C++ ExecutionEngine.
 * It is currently used only by the SQL Coverage tests (and perhaps, someday,
 * the JUnit regressionsuite tests).
 */
public class PostgreSQLBackend extends NonVoltDBBackend {
    /** java.util.logging logger. */
    @SuppressWarnings("unused")
    private static final VoltLogger log = new VoltLogger(PostgreSQLBackend.class.getName());

    protected static final String m_default_username = "postgres";
    protected static final String m_default_password = "voltdb";
    protected static final String m_permanent_database_name = "postgres";
    protected static final String m_database_name = "sqlcoveragetest";
    protected static PostgreSQLBackend m_permanent_db_backend = null;
    // Used to specify when queries should only be modified when they apply
    // to columns of certain types, so this only includes column types that
    // need special treatment for PostgreSQL
    protected enum ColumnType {
        INTEGER, GEO
    }
    // PostgreSQL column type names that are not found in VoltDB or HSQL,
    // mapped to their VoltDB/HSQL equivalents
    private static final Map<String,String> m_PostgreSQLTypeNames;
    static {
        m_PostgreSQLTypeNames = new HashMap<String,String>();
        m_PostgreSQLTypeNames.put("int2", "SMALLINT");
        m_PostgreSQLTypeNames.put("int4", "INTEGER");
        m_PostgreSQLTypeNames.put("int8", "BIGINT");
        m_PostgreSQLTypeNames.put("float8", "FLOAT");
        m_PostgreSQLTypeNames.put("numeric", "DECIMAL");
        m_PostgreSQLTypeNames.put("varbit", "VARBINARY");
        m_PostgreSQLTypeNames.put("char", "CHARACTER");
        m_PostgreSQLTypeNames.put("text", "VARCHAR");
    }
    // Captures up to 6 order-by columns; beyond those will be ignored
    // (similar to tests/scripts/examples/sql_coverage/StandardNormalzer.py)
    private static final Pattern orderByQuery = Pattern.compile(
            "ORDER BY(?<column1>\\s+(\\w*\\s*\\(\\s*)*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?)"
            + "((?<column2>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column3>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column4>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column5>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column6>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of EXTRACT(DAY_OF_WEEK FROM ...), which PostgreSQL
    // does not support
    private static final Pattern dayOfWeekQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_WEEK\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of EXTRACT(DAY_OF_YEAR FROM ...), which PostgreSQL
    // does not support
    private static final Pattern dayOfYearQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_YEAR\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of AVG(columnName), which PostgreSQL handles
    // differently, when the columnName is of one of the integer types
    private static final Pattern avgQuery = Pattern.compile(
            "AVG\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of CEILING(columnName) or FLOOR(columnName), which PostgreSQL
    // handles differently, when the columnName is of one of the integer types
    private static final Pattern ceilingOrFloorQuery = Pattern.compile(
            "(CEILING|FLOOR)\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures up to 6 table names, for each FROM clause used in the query
    // TODO: fix/finish this! (Use for AVG, CEILING, FLOOR queries)
//    private static final Pattern tableNames = Pattern.compile(
//              "FROM\\s*\\(?<table1>\\w+)\\s*"
//            + "(\\s*,s*\\(?<table2>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table3>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table4>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table5>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table6>\\w+)\\s*)?",
//            Pattern.CASE_INSENSITIVE);
    // Captures the use of string concatenation using 'str' + ..., which
    // PostgreSQL does not support
    private static final Pattern stringConcatQuery = Pattern.compile(
            "'\\w+'\\s*(?<plus>\\+)", Pattern.CASE_INSENSITIVE);
    // Captures the use of VARCHAR(n BYTES), which PostgreSQL does not support
    private static final Pattern varcharBytesDdl = Pattern.compile(
            "VARCHAR\\s*\\(\\s*(?<numBytes>\\w+)\\s+BYTES\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of VARBINARY(n), which PostgreSQL does not support
    private static final Pattern varbinaryDdl = Pattern.compile(
            "VARBINARY\\s*\\(\\s*(?<numBytes>\\d+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final boolean DEBUG = false;

    static public PostgreSQLBackend initializePostgreSQLBackend(CatalogContext context)
    {
        synchronized(backendLock) {
            if (m_backend == null) {
                try {
                    if (m_permanent_db_backend == null) {
                        m_permanent_db_backend = new PostgreSQLBackend();
                    }
                    Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                    stmt.execute("drop database if exists " + m_database_name + ";");
                    stmt.execute("create database " + m_database_name + ";");
                    m_backend = new PostgreSQLBackend(m_database_name);
                    final String binDDL = context.database.getSchema();
                    final String ddl = Encoder.decodeBase64AndDecompress(binDDL);
                    final String[] commands = ddl.split("\n");
                    for (String command : commands) {
                        String decoded_cmd = Encoder.hexDecodeToString(command);
                        decoded_cmd = decoded_cmd.trim();
                        if (decoded_cmd.length() == 0) {
                            continue;
                        }
                        m_backend.runDDL(decoded_cmd);
                    }
                }
                catch (final Exception e) {
                    hostLog.fatal("Unable to construct PostgreSQL backend");
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
            }
            return (PostgreSQLBackend) m_backend;
        }
    }

    /** Constructor specifying a (PostgreSQL) 'database', username and password. */
    public PostgreSQLBackend(String databaseName, String username, String password) {
        super("PostgreSQL", "org.postgresql.Driver",
                "jdbc:postgresql:" + databaseName,
                username, password);
    }

    /** Constructor specifying a username and password, with default (PostgreSQL) 'database'. */
    public PostgreSQLBackend(String username, String password) {
        this(m_permanent_database_name, username, password);
    }

    /** Constructor specifying a (PostgreSQL) 'database', with default username and password. */
    public PostgreSQLBackend(String databaseName) {
        this(databaseName, m_default_username, m_default_password);
    }

    /** Constructor using the default (PostgreSQL) 'database', username, and password. */
    public PostgreSQLBackend() {
        this(m_permanent_database_name);
    }

    /** Creates a new PostgreSQLBackend wrapping dbconn. This is (was?) used for testing only. */
    protected PostgreSQLBackend(Connection dbconn) {
        super(dbconn);
    }

    /** Returns true if the input string is an integer constant; false otherwise. */
    static private boolean isIntegerConstant(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** Returns true if the <i>columnName</i> is an integer column; false otherwise. */
    static private boolean isIntegerColumn(String columnName, String... tableNames) {
        // TODO: Temporary method, which will mostly work, for now:
        if (DEBUG) {
            System.out.println("In PostgreSQLBackend.isIntegerColumn, with columnName: '" + columnName + "'");
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

    /** Returns true if the <i>columnName</i> is a Geography Point or a
     *  Geography (polygon) column; false otherwise. */
    static private boolean isGeoColumn(String columnName, String... tableNames) {
        // TODO: Temporary method, which will mostly work, for now:
        if (DEBUG) {
            System.out.println("In PostgreSQLBackend.isGeoColumn, with columnName: '" + columnName + "'");
        }
        if (columnName == null) {
            return false;
        }
        String columnNameUpper = columnName.trim().toUpperCase();
        return columnNameUpper.startsWith("PT") || columnNameUpper.startsWith("POLY");
    }

    /** Returns true if the <i>columnName</i> is an integer constant or column; false otherwise. */
    @SuppressWarnings("unused")
    static private boolean isInteger(String columnName, String... tableNames) {
        return isIntegerConstant(columnName) || isIntegerColumn(columnName, tableNames);
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
        if (numOpenParens >= numCloseParens) {
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
     * Modify a <i>query</i> containing the specified <i>queryPattern</i>, in
     * such a way that PostgreSQL results will match VoltDB results, generally
     * by adding a <i>prefix</i> and/or <i>suffix</i>, either to individual
     * <i>groups</i> within the <i>queryPattern</i>, or to the <i>queryPattern</i>
     * as a whole.
     *
     * @param query - the query text (DDL, DML or DQL) to be transformed.
     * @param queryPattern - the Pattern to be detected and modified, within
     * the query.
     * @param initText - an initial string with which to begin the replacement
     * text (e.g. "ORDER BY"); may be an empty string, but not <b>null</b>.
     * @param prefix - a string to appear before each group, or before the
     * whole <i>queryPattern</i> (e.g. "TRUNC ( "); may be an empty string,
     * but not <b>null</b>.
     * @param suffix - a string to appear after each group, or after the
     * whole <i>queryPattern</i> (e.g. " NULLS FIRST"); may be an empty string,
     * but not <b>null</b>.
     * @param altEnding - when a matching group ends with this text (e.g. "DESC"),
     * the <i>altText</i> will be used, instead of <i>suffix</i>; may be <b>null</b>.
     * @param altText - when <i>altEnding</i> is not null, the alternate suffix,
     * to be used when the group ends with <i>altEnding</i> (e.g. " NULLS LAST");
     * when <i>altEnding</i> is null, the text to be used to replace each group,
     * within the whole match (e.g. "||"); may be <b>null</b>, in which case the
     * group is not replaced.
     * @param useWhole - when <b>true</b>, the <i>prefix</i> and <i>suffix</i>
     * will be applied to the whole <i>queryPattern</i>; when <b>false</b>,
     * they will be applied to each group.
     * @param columnType - when specified, the <i>query</i> will only be modified
     * if the group is a column of the specified type; may be <b>null</b>, in
     * which case a matching query is always modified, regardless of column type.
     * @param multiplier - a value to be multiplied by the (int-valued) group,
     * in the transformed query (e.g. 8.0, to convert from bytes to bits); may
     * be <b>null</b>, in which case it is ignored.
     * @param minimum - a minimum value for the result of multiplying the
     * (int-valued) group by the <i>multiplier</i>; may be <b>null</b>, in
     * which case it is ignored.
     * @param groups - zero or more groups found within the <i>queryPattern</i>
     * (e.g. "column").
     * @return the <i>query</i>, transformed in the specified ways (possibly
     * unchanged).
     * @throws NullPointerException if <i>query</i>, <i>queryPattern</i>,
     * <i>initText</i>, <i>prefix</i>, or <i>suffix</i> is <b>null</b>.
     */
    @SuppressWarnings("unused")
    // TODO: improve this method (& overloaded versions), and its large,
    // confusing argument list, probably using the builder pattern
    static protected String transformQuery(String query, Pattern queryPattern, String initText,
            String prefix, String suffix, String altEnding, String altText,
            boolean useWhole, ColumnType columnType, Double multiplier, Integer minimum,
            String ... groups) {
        StringBuffer modified_query = new StringBuffer();
        Matcher matcher = queryPattern.matcher(query);
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer(initText);
            String wholeMatch = null, group = null;
            wholeMatch = matcher.group();
            for (String groupName : groups) {
                group = null;
                try {
                    group = matcher.group(groupName);
                } catch (IllegalArgumentException e) {
                    // do nothing: group remains null
                }
                if (DEBUG) {
                    System.out.println("In PostgreSQLBackend.transformQuery,\n  with query    : " + query);
                    System.out.println("  queryPattern: " + queryPattern);
                    System.out.println("  initText, prefix, suffix, altEnding, altText:\n    '"
                            + initText + "', '" + prefix + "', '" + suffix + "', '" + altEnding + "', '" + altText
                            + "'\n  useWhole, columnType, multiplier, minimum; groups:\n    '"
                            + useWhole + ", " + columnType + ", " + multiplier + ", " + minimum + "\n" + groups);
                    System.out.println("  wholeMatch: " + wholeMatch);
                    System.out.println("  group     : " + group);
                }
                if (group == null) {
                    break;
                } else if (!useWhole) {
                    String groupValue = group, suffixValue = suffix;
                    // Check for the case where a multiplier & minimum are used
                    if (multiplier != null && minimum != null) {
                        groupValue = Long.toString(Math.round(Math.max(Integer.parseInt(group) * multiplier, minimum)));
                    }
                    // Check for the ending that indicates to use the alternate suffix
                    if (altText != null && group.toUpperCase().endsWith(altEnding)) {
                        suffixValue = altText;
                    }
                    // Make sure not to swallow up extra ')', in this group
                    replaceText.append(handleParens(groupValue, prefix, suffixValue));
                }
            }
            if (useWhole) {
                if (columnType != null && (
                           (columnType == ColumnType.INTEGER && !isIntegerColumn(group))
                        || (columnType == ColumnType.GEO     && !isGeoColumn(group)) )) {
                    // Make no changes to query
                    replaceText.append(wholeMatch);
                } else {
                    // Check for the case where the group is to be replaced with altText
                    if (altText != null && altEnding == null) {
                        wholeMatch = wholeMatch.replace(group, altText);
                    }
                    // Make sure not to swallow up extra ')', in whole match
                    replaceText.append(handleParens(wholeMatch, prefix, suffix));
                }
            }
            matcher.appendReplacement(modified_query, replaceText.toString());
        }
        matcher.appendTail(modified_query);
        if (DEBUG && !query.equalsIgnoreCase(modified_query.toString())) {
            System.out.println("In PostgreSQLBackend.transformQuery,\n  with query    : " + query);
            System.out.println("  modified_query: " + modified_query);
        }
        return modified_query.toString();
    }

    /**
     * Modify a <i>query</i> containing the specified <i>queryPattern</i>, in
     * such a way that PostgreSQL results will match VoltDB results, generally
     * by adding a <i>prefix</i> and/or <i>suffix</i>, either to individual
     * <i>groups</i> within the <i>queryPattern</i>, or to the <i>queryPattern</i>
     * as a whole.<p>
     * This simpler version leaves out the arguments that are usually not
     * needed, and just calls the complete version with those arguments set
     * to <b>null</b>.
     *
     * @param query - the query text (DDL, DML or DQL) to be transformed.
     * @param queryPattern - the Pattern to be detected and modified, within
     * the query.
     * @param initText - an initial string with which to begin the replacement
     * text (e.g. "ORDER BY"); may be an empty string, but not <b>null</b>.
     * @param prefix - a string to appear before each group, or before the
     * whole <i>queryPattern</i> (e.g. "TRUNC ( "); may be an empty string,
     * but not <b>null</b>.
     * @param suffix - a string to appear after each group, or after the
     * whole <i>queryPattern</i> (e.g. " NULLS FIRST"); may be an empty string,
     * but not <b>null</b>.
     * @param altText - the text to be used to replace each group, within the
     * whole match (e.g. "||"); may be <b>null</b>, in which case the group is
     * not replaced.
     * @param useWhole - when <b>true</b>, the <i>prefix</i> and <i>suffix</i>
     * will be applied to the whole <i>queryPattern</i>; when <b>false</b>,
     * they will be applied to each group.
     * @param columnType - when specified, the <i>query</i> will only be modified
     * if the group is a column of the specified type; may be <b>null</b>, in
     * which case a matching query is always modified, regardless of column type.
     * @param groups - zero or more groups found within the <i>queryPattern</i>
     * (e.g. "column").
     * @return the <i>query</i>, transformed in the specified ways (possibly
     * unchanged).
     * @throws NullPointerException if <i>query</i>, <i>queryPattern</i>,
     * <i>initText</i>, <i>prefix</i>, or <i>suffix</i> is <b>null</b>.
     */
    static protected String transformQuery(String query, Pattern queryPattern,
            String initText, String prefix, String suffix, String altText,
            boolean useWhole, ColumnType columnType, String ... groups) {
        return transformQuery(query, queryPattern, initText,
                prefix, suffix, null, altText,
                useWhole, columnType, null, null,
                groups);
    }

    /**
     * Modify a <i>query</i> containing the specified <i>queryPattern</i>, in
     * such a way that PostgreSQL results will match VoltDB results, generally
     * by adding a <i>prefix</i> and/or <i>suffix</i>, either to individual
     * <i>groups</i> within the <i>queryPattern</i>, or to the <i>queryPattern</i>
     * as a whole.<p>
     * This version includes the <i>multiplier</i> and <i>minimum</i> arguments,
     * but leaves out the ones that are usually not needed with those, and then
     * calls the complete version with the latter set to <b>null</b>.
     *
     * @param query - the query text (DDL, DML or DQL) to be transformed.
     * @param queryPattern - the Pattern to be detected and modified, within
     * the query.
     * @param initText - an initial string with which to begin the replacement
     * text (e.g. "ORDER BY"); may be an empty string, but not <b>null</b>.
     * @param prefix - a string to appear before each group, or before the
     * whole <i>queryPattern</i> (e.g. "TRUNC ( "); may be an empty string,
     * but not <b>null</b>.
     * @param suffix - a string to appear after each group, or after the
     * whole <i>queryPattern</i> (e.g. " NULLS FIRST"); may be an empty string,
     * but not <b>null</b>.
     * @param useWhole - when <b>true</b>, the <i>prefix</i> and <i>suffix</i>
     * will be applied to the whole <i>queryPattern</i>; when <b>false</b>,
     * they will be applied to each group.
     * @param multiplier - a value to be multiplied by the (int-valued) group,
     * in the transformed query (e.g. 8.0, to convert from bytes to bits); may
     * be <b>null</b>, in which case it is ignored.
     * @param minimum - a minimum value for the result of multiplying the
     * (int-valued) group by the <i>multiplier</i>; may be <b>null</b>, in
     * which case it is ignored.
     * @param groups - zero or more groups found within the <i>queryPattern</i>
     * (e.g. "column").
     * @return the <i>query</i>, transformed in the specified ways (possibly
     * unchanged).
     * @throws NullPointerException if <i>query</i>, <i>queryPattern</i>,
     * <i>initText</i>, <i>prefix</i>, or <i>suffix</i> is <b>null</b>.
     */
    static protected String transformQuery(String query, Pattern queryPattern,
            String initText, String prefix, String suffix,
            boolean useWhole, Double multiplier, Integer minimum,
            String ... groups) {
        return transformQuery(query, queryPattern,
                initText, prefix, suffix, null, null,
                useWhole, null, multiplier, minimum,
                groups);
    }

    /** Modify a query containing an ORDER BY clause, in such a way that
     *  PostgreSQL results will match VoltDB results, generally by adding
     *  NULLS FIRST or (after "DESC") NULLS LAST. */
    static private String transformOrderByQuery(String dml) {
        return transformQuery(dml, orderByQuery, "ORDER BY",
                "", " NULLS FIRST", "DESC", " NULLS LAST", false, null, null, null,
                "column1", "column2", "column3", "column4", "column5", "column6");
    }

    /** Modify a query containing an EXTRACT(DAY_OF_WEEK FROM ...) function,
     *  which PostgreSQL does not support, and replace it with
     *  EXTRACT(DOW FROM ...)+1, which is an equivalent that PostgreSQL
     *  does support. (The '+1' is because PostgreSQL counts Sunday as 0 and
     *  Saturday as 6, etc., whereas VoltDB counts Sunday as 1 and Saturday
     *  as 7, etc.) */
    static private String transformDayOfWeekQuery(String dml) {
        return transformQuery(dml, dayOfWeekQuery, "EXTRACT ( ",
                "DOW FROM", ")+1", null, false, null, "column");
    }

    /** Modify a query containing an EXTRACT(DAY_OF_YEAR FROM ...) function,
     *  which PostgreSQL does not support, and replace it with
     *  EXTRACT(DOY FROM ...), which is an equivalent that PostgreSQL
     *  does support. */
    static private String transformDayOfYearQuery(String dml) {
        return transformQuery(dml, dayOfYearQuery, "EXTRACT ( ",
                "DOY FROM", ")", null, false, null, "column");
    }

    /** Modify a query containing an AVG(columnName), where <i>columnName</i>
     *  is of an integer type, for which PostgreSQL returns a numeric
     *  (non-integer) value, unlike VoltDB, which returns an integer;
     *  so change it to: TRUNC ( AVG(columnName) ). */
    static private String transformAvgOfIntegerQuery(String dml) {
        return transformQuery(dml, avgQuery, "",
                "TRUNC ( ", " )", null, true, ColumnType.INTEGER, "column");
    }

    /** Modify a query containing a CEILING(columnName) or FLOOR(columnName),
     *  where <i>columnName</i> is of an integer type, for which PostgreSQL
     *  returns a numeric (non-integer) value, unlike VoltDB, which returns
     *  an integer; so change it to: CAST ( CEILING(columnName) as INTEGER ),
     *  or CAST ( FLOOR(columnName) as INTEGER ), respectively. */
    static private String transformCeilingOrFloorOfIntegerQuery(String dml) {
        return transformQuery(dml, ceilingOrFloorQuery, "",
                "CAST ( ", " as INTEGER )", null, true, ColumnType.INTEGER, "column");
    }

    /** Modify a query containing 'FOO' + ..., which PostgreSQL does not
     *  support, and replace it with 'FOO' || ..., which is an equivalent
     *  that PostgreSQL does support. */
    static private String transformStringConcatQuery(String dml) {
        return transformQuery(dml, stringConcatQuery, "",
                "", "", "||", true, null, "plus");
    }

    /** Modify DDL containing VARCHAR(n BYTES), which PostgreSQL does not
     *  support, and replace it with VARCHAR(m), where m = n / 4 (but m is
     *  always at least 14, since many SQLCoverage tests use strings of that
     *  length), which it does support. */
    static private String transformVarcharOfBytes(String ddl) {
        return transformQuery(ddl, varcharBytesDdl, "",
                "VARCHAR(", ")", false, 0.25, 14, "numBytes");
    }

    /** Modify DDL containing VARBINARY(n), which PostgreSQL does not support,
     *  and replace it with BIT VARYING(m), where m = n * 8 (i.e., converting
     *  from bytes to bits), which it does support. */
    static private String transformVarbinary(String ddl) {
        return transformQuery(ddl, varbinaryDdl, "",
                "BIT VARYING(", ")", false, 8.0, 8, "numBytes");
    }

    /** For a SQL DDL statement, replace (VoltDB) keywords not supported by
     *  PostgreSQL with other, similar terms. */
    static public String transformDDL(String ddl) {
        return transformVarcharOfBytes(transformVarbinary(ddl))
                .replace("TINYINT", "SMALLINT")
                .replace("ASSUMEUNIQUE", "UNIQUE");
    }

    /** For a SQL query, replace keywords not supported by PostgreSQL, or
     *  which behave differently in PostgreSQL than in VoltDB, with other,
     *  similar terms, so that the results will match. */
    static public String transformDML(String dml) {
        return transformStringConcatQuery(
                transformDayOfWeekQuery(
                    transformDayOfYearQuery(
                        transformCeilingOrFloorOfIntegerQuery(
                            transformAvgOfIntegerQuery(
                                transformOrderByQuery(dml) )))));
    }

    /** Optionally, modifies DDL statements in such a way that PostgreSQL
     *  results will match VoltDB results; and then passes the remaining
     *  work to the base class version. */
    protected void runDDL(String ddl, boolean transformDdl) {
        super.runDDL((transformDdl ? transformDDL(ddl) : ddl));
    }

    /** Modifies DDL statements in such a way that PostgreSQL results will
     *  match VoltDB results, and then passes the remaining work to the base
     *  class version. */
    @Override
    public void runDDL(String ddl) {
        runDDL(ddl, true);
    }

    /**
     * Returns a VoltTable.ColumnInfo of appropriate type, based on a
     * <i>typeName</i> and <i>colName</i> (both Strings).
     * This version checks for column types used only by PostgreSQL,
     * and then passes the remaining work to the base class version.
     */
    @Override
    protected VoltTable.ColumnInfo getColumnInfo(String typeName, String colName) {
        String equivalentTypeName = m_PostgreSQLTypeNames.get(typeName);
        equivalentTypeName = (equivalentTypeName == null) ? typeName : equivalentTypeName;
        return super.getColumnInfo(equivalentTypeName, colName);
    }

    /** Optionally, modifies queries in such a way that PostgreSQL results will
     *  match VoltDB results; and then passes the remaining work to the base
     *  class version. */
    protected VoltTable runDML(String dml, boolean transformDml) {
        return super.runDML((transformDml ? transformDML(dml) : dml));
    }

    /** Modifies queries in such a way that PostgreSQL results will match VoltDB
     *  results, and then passes the remaining work to the base class version. */
    @Override
    public VoltTable runDML(String dml) {
        return runDML(dml, true);
    }

    protected Connection getConnection() {
        return dbconn;
    }

    @Override
    protected void shutdown() {
        try {
            dbconn.close();
            dbconn = null;
            System.gc();
            try {
                Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                stmt.execute("drop database if exists " + m_database_name + ";");
            } catch (SQLException ex) {
                System.err.println("In PostgreSQLBackend.shutdown(), caught exception: " + ex);
                ex.printStackTrace();
            }
        } catch (Exception e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_Backend_ErrorOnShutdown.name(), e);
        }
    }

}
