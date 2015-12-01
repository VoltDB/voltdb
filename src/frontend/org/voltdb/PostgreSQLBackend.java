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

    // TODO: these will need to match the values used on our Jenkins machines,
    // once a PostgreSQL server is set up.
    private static final String m_default_username = "postgres";
    private static final String m_default_password = "voltdb";
    private static final String m_permanent_database_name = "postgres";
    private static final String m_database_name = "sqlcoveragetest";
    private static PostgreSQLBackend m_permanent_db_backend = null;
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
        m_PostgreSQLTypeNames.put("bytea", "VARBINARY");
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
    private PostgreSQLBackend(Connection dbconn) {
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

    /** Returns true if the <i>columnName</i> is an integer constant or column; false otherwise. */
    @SuppressWarnings("unused")
    static private boolean isInteger(String columnName, String... tableNames) {
        return isIntegerConstant(columnName) || isIntegerColumn(columnName, tableNames);
    }

    /** TODO */
    static private int numOccurencesOfCharIn(String str, char ch) {
        int num = 0;
        for (int i = str.indexOf(ch); i >= 0 ; i = str.indexOf(ch, i+1)) {
            num++;
        }
        return num;
    }

    /** TODO */
    static private int indexOfNthOccurenceOfCharIn(String str, char ch, int n) {
        int index = -1;
        for (int i=0; i < n; i++) {
            index = str.indexOf(ch, index+1);
            if (index < 0) {
                return -1;
            }
        }
        return index;
    }

    /** TODO */
    static private String handleParens(String group, String preGroup, String postGroup) {
        int numOpenParens  = numOccurencesOfCharIn(group, '(');
        int numCloseParens = numOccurencesOfCharIn(group, ')');
//        System.out.println("  numOpenParens, numCloseParens: " + numOpenParens + ", " + numCloseParens);
        if (numOpenParens == numCloseParens) {
//            System.out.println("  returning (preGroup + group + postGroup):\n    " + preGroup + group + postGroup);
            return (preGroup + group + postGroup);
        } else if (numOpenParens < numCloseParens) {
            int index;
            if (numOpenParens == 0) {
                index = indexOfNthOccurenceOfCharIn(group, ')', 1) - 1;
            } else {
                index = indexOfNthOccurenceOfCharIn(group, ')', numOpenParens);
            }
//            System.out.println("  index, substr: " + index + ", '" + group.substring(0, index+1) + "'");
//            System.out.println("  returning (preGroup + substr + postGroup + group.substring(index+1)):\n    "
//                    + preGroup + group.substring(0, index+1) + postGroup + group.substring(index+1));
            return (preGroup + group.substring(0, index+1) + postGroup + group.substring(index+1));
        } else {
            // numOpenParens >= numCloseParens: give up, leave group unchanged
//            System.out.println("  gave up; returning (group):\n    " + group);
            return group;
        }
    }

    /** TODO: Modify queries containing an ORDER BY clause, in such a way that
     *  PostgreSQL results will match VoltDB results, generally by adding
     *  NULLS FIRST or NULLS LAST. */
    @SuppressWarnings("unused")
    static private String transformQuery(String dml, Pattern queryPattern, String initText,
            String prefix, String suffix, String altEnding, String altSuffix,
            boolean useWhole, boolean intOnly, String ... groups) {
        StringBuffer modified_dml = new StringBuffer();
        Matcher matcher = queryPattern.matcher(dml);
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer(initText);
            String wholeMatch = null, group = null;
            wholeMatch = matcher.group();
            for (String groupName : groups) {
//                System.out.println("  groupName : " + groupName);
                group = null;
                try {
                    group = matcher.group(groupName);
                } catch (IllegalArgumentException e) {
                    // do nothing: group remains null
                }
                if (DEBUG) {
                    System.out.println("In PostgreSQLBackend.transformQuery,\n  with dml    : " + dml);
                    System.out.println("  queryPattern: " + queryPattern);
                    System.out.println("  initText, intOnly, prefix, suffix:\n    '"
                            + initText + "', '" + intOnly + "', '" + prefix + "', '" + suffix
                            + "'\n  preGroup, postGroup, altEnding, altSuffix; groups:\n    '"
//                            + preGroup + "', '" + postGroup + "', '" + altEnding + "', '" + altPostGroup + "';\n    "
                            + prefix + "', '" + suffix + "', '" + altEnding + "', '" + altSuffix + "';\n    "
                            + groups);
                    System.out.println("  wholeMatch: " + wholeMatch);
                    System.out.println("  group     : " + group);
                }
                if (group == null) {
                    break;
                } else if (!useWhole) {
                    // Check for the ending that indicates to use the alternate suffix
                    if (altEnding != null && group.toUpperCase().endsWith(altEnding)) {
                        suffix = altSuffix;
                    }
                    // Make sure not to swallow up extra ')', in this group
                    replaceText.append(handleParens(group, prefix, suffix));
                }
            }
            if (useWhole) {
                if (DEBUG) {
                    System.out.println("In PostgreSQLBackend.transformQuery,\n  with dml    : " + dml);
                    System.out.println("  queryPattern: " + queryPattern);
                    System.out.println("  initText, intOnly, prefix, suffix:\n    '"
                            + initText + "', '" + intOnly + "', '" + prefix + "', '" + suffix
                            + "'\n  preGroup, postGroup, altEnding, altSuffix; groups:\n    '"
                            + prefix + "', '" + suffix + "', '" + altEnding + "', '" + altSuffix + "';\n    "
                            + groups);
                    System.out.println("  wholeMatch: " + wholeMatch);
                    System.out.println("  group     : " + group);
                }
                if (intOnly && !isIntegerColumn(group)) {
                    // Make no changes to query
                    replaceText.append(wholeMatch);
                } else {
                    // Make sure not to swallow up extra ')', in whole match
                    replaceText.append(handleParens(wholeMatch, prefix, suffix));
                }
            }
            matcher.appendReplacement(modified_dml, replaceText.toString());
        }
        matcher.appendTail(modified_dml);
        if (DEBUG && !dml.equalsIgnoreCase(modified_dml.toString())) {
            System.out.println("In PostgreSQLBackend.transformQuery,\n  with dml    : " + dml);
            System.out.println("  modified_dml: " + modified_dml);
        }
        return modified_dml.toString();
    }

    /** Modify queries containing an ORDER BY clause, in such a way that
     *  PostgreSQL results will match VoltDB results, generally by adding
     *  NULLS FIRST or NULLS LAST. */
    static private String transformOrderByQuery(String dml) {
        return transformQuery(dml, orderByQuery, "ORDER BY",
                "", " NULLS FIRST", "DESC", " NULLS LAST", false, false,
                "column1", "column2", "column3", "column4", "column5", "column6");
    }

    /** Modify queries containing an EXTRACT(DAY_OF_WEEK FROM ...) or
     *  EXTRACT(DAY_OF_YEAR FROM ...) function, which PostgreSQL does not
     *  support, and replace it with EXTRACT(DOW FROM ...)+1 or
     *  DATE_PART('DOY', ...), respectively, which is an equivalent that
     *  PostgreSQL does support. (The '+1' for DOW is because PostgreSQL
     *  counts Sunday as 0 and Saturday as 6, etc., whereas VoltDB counts
     *  Sunday as 1 and Saturday as 7, etc.) */
    static private String transformDayOfWeekQuery(String dml) {
        return transformQuery(dml, dayOfWeekQuery, "EXTRACT ( ",
                "DOW FROM", ")+1", null, null, true, false, "column");
    }

    /** Modify queries containing an EXTRACT(DAY_OF_WEEK FROM ...) or
     *  EXTRACT(DAY_OF_YEAR FROM ...) function, which PostgreSQL does not
     *  support, and replace it with EXTRACT(DOW FROM ...)+1 or
     *  DATE_PART('DOY', ...), respectively, which is an equivalent that
     *  PostgreSQL does support. (The '+1' for DOW is because PostgreSQL
     *  counts Sunday as 0 and Saturday as 6, etc., whereas VoltDB counts
     *  Sunday as 1 and Saturday as 7, etc.) */
    static private String transformDayOfYearQuery(String dml) {
        return transformQuery(dml, dayOfYearQuery, "EXTRACT ( ",
                "DOY FROM", ")", null, null, true, false, "column");
    }

    /** Modify queries containing an AVG(columnName), where <i>columnName</i>
     *  is of an integer type, for which PostgreSQL returns a numeric
     *  (non-integer) value, unlike VoltDB, which returns an integer;
     *  so change it to: TRUNC ( AVG(columnName) ). */
    static private String transformAvgOfIntegerQuery(String dml) {
        return transformQuery(dml, avgQuery, "",
                "TRUNC ( ", " )", null, null, true, true, "column");
    }

    /** Modify queries containing a CEILING(columnName) or FLOOR() where
     *  <i>columnName</i> is of an integer type, for which PostgreSQL returns
     *  a numeric (non-integer) value, unlike VoltDB, which returns an integer;
     *  so change it to: CAST ( CEILING(columnName) as INTEGER ). */
    static private String transformCeilingOrFloorOfIntegerQuery(String dml) {
        return transformQuery(dml, ceilingOrFloorQuery, "",
                "CAST ( ", " as INTEGER )", null, null, true, true, "column");
    }

    /** TODO: Modify queries containing an AVG(columnName) where <i>columnName</i>
     *  is of an integer type, for which PostgreSQL returns a numeric
     *  (non-integer) value, unlike VoltDB, which returns an integer;
     *  so change it to: TRUNC ( AVG(columnName) ). */
    static private String transformStringConcatQuery(String dml) {
        return transformQuery(dml, stringConcatQuery, "",
                "||", "", null, null, true, false, "plus");
    }

    /** Modify DDL containing VARCHAR(n BYTES), which PostgreSQL does not
     *  support, and replace it with VARCHAR(m), where m = n / 4 (but m is
     *  always at least 14, since many SQLCoverage tests use strings of that
     *  length). */
    @SuppressWarnings("unused")
    static private String transformVarcharOfBytes(String ddl) {
        StringBuffer modified_ddl = new StringBuffer();
        Matcher matcher = varcharBytesDdl.matcher(ddl);
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer();
            String numBytesStr = null;
            int numBytes = -1;
            try {
                numBytesStr = matcher.group("numBytes");
                numBytes = Integer.parseInt(numBytesStr);
            } catch (IllegalArgumentException e) {
                // do nothing: numBytes remains -1
                break;
            }
            replaceText.append("VARCHAR(" + Math.max(numBytes / 4, 14) + ")");
            matcher.appendReplacement(modified_ddl, replaceText.toString());
        }
        matcher.appendTail(modified_ddl);
        if (DEBUG && !ddl.equalsIgnoreCase(modified_ddl.toString())) {
            System.out.println("In PostgreSQLBackend.transformVarcharOfBytes,\n  with dml    : " + ddl);
            System.out.println("  modified_dml: " + modified_ddl);
        }
        return modified_ddl.toString();
    }

    /** Modify DDL containing VARBINARY(n), which PostgreSQL does not
     *  support, and replace it with BYTEA (which it does). */
    @SuppressWarnings("unused")
    static private String transformVarbinary(String ddl) {
        StringBuffer modified_ddl = new StringBuffer();
        Matcher matcher = varbinaryDdl.matcher(ddl);
        while (matcher.find()) {
            String varbinary = null;
            try {
                varbinary = matcher.group();
            } catch (IllegalArgumentException e) {
                // do nothing: varbinary remains null
            }
            matcher.appendReplacement(modified_ddl, "BYTEA");
        }
        matcher.appendTail(modified_ddl);
        if (DEBUG && !ddl.equalsIgnoreCase(modified_ddl.toString())) {
            System.out.println("In PostgreSQLBackend.transformVarbinary,\n  with ddl    : " + ddl);
            System.out.println("  modified_dml: " + modified_ddl);
        }
        return modified_ddl.toString();
    }

    /** For a SQL DDL statement, replace keywords not supported by PostgreSQL
     *  with other, similar terms. */
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

    /** Modifies DDL statements in such a way that PostgreSQL results will
     *  match VoltDB results, and then passes the remaining work to the base
     *  class version. */
    @Override
    public void runDDL(String ddl) {
        super.runDDL(transformDDL(ddl));
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

    /** Modifies queries in such a way that PostgreSQL results will match VoltDB
     *  results, and then passes the remaining work to the base class version. */
    @Override
    public VoltTable runDML(String dml) {
        return super.runDML(transformDML(dml));
    }

    private Connection getConnection() {
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
