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
            "ORDER BY(?<column1>\\s+(\\w*\\s*\\(\\s*)*(\\w+\\.)?\\w+(\\s*\\))*(\\s+(ASC|DESC))?)"
            + "((?<column2>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+(\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column3>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+(\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column4>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+(\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column5>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+(\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column6>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+(\\s*\\))*(\\s+(ASC|DESC))?))?",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of EXTRACT(DAY_OF_WEEK FROM ...) or
    // EXTRACT(DAY_OF_YEAR FROM ...), which PostgreSQL does not support
    private static final Pattern dayOfWeekOrYearQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*(?<weekOrYear>DAY_OF_WEEK|DAY_OF_YEAR)\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of AVG(columnName), which PostgreSQL handles
    // differently, when the columnName is of one of the integer types
    private static final Pattern avgQuery = Pattern.compile(
            "AVG\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\))*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures up to 6 table names, for each FROM clause used in the query
    // TODO: fix/finish this!
//    private static final Pattern tableNames = Pattern.compile(
//              "FROM\\s*\\(?<table1>\\w+)\\s*"
//            + "(\\s*,s*\\(?<table2>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table3>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table4>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table5>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table6>\\w+)\\s*)?",
//            Pattern.CASE_INSENSITIVE);
    // Captures the use of VARCHAR(n BYTES), which PostgreSQL does not support
    private static final Pattern varcharBytesDdl = Pattern.compile(
            "VARCHAR\\s*\\(\\s*(?<numBytes>\\w+)\\s+BYTES\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of VARBINARY(n), which PostgreSQL does not support
    private static final Pattern varbinaryDdl = Pattern.compile(
            "VARBINARY\\s*\\(\\s*\\d+\\s*\\)",
            Pattern.CASE_INSENSITIVE);

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

    /** Modify queries containing an ORDER BY clause, in such a way that
     *  PostgreSQL results will match VoltDB results, generally by adding
     *  NULLS FIRST or NULLS LAST. */
    static private String transformOrderByQueries(String dml) {
        // TODO: should we only add "NULLS FIRST|LAST" when we find "LIMIT" and/or "OFFSET"??
        StringBuffer modified_dml = new StringBuffer();
        Matcher matcher = orderByQuery.matcher(dml);
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer("ORDER BY");
            // 100 is arbitrary; the real limitation is in the orderByQuery Pattern
            for (int i=1; i < 100; i++) {
                String column_i = null;
                try {
                    column_i = matcher.group("column" + i);
                } catch (IllegalArgumentException e) {
                    //System.out.println("In PostgreSQLBackend.transformOrderByQueries, caught:\n" + e);
                    // do nothing: column_i remains null
                }
                if (column_i == null) {
                    break;
                } else if (column_i.toUpperCase().endsWith("DESC")) {
                    replaceText.append(column_i + " NULLS LAST");
                } else {
                    replaceText.append(column_i + " NULLS FIRST");
                }
            }
            matcher.appendReplacement(modified_dml, replaceText.toString());
        }
        matcher.appendTail(modified_dml);
//        // TODO: temp debug:
//        if (!dml.equalsIgnoreCase(modified_dml.toString())) {
//            System.out.println("In PostgreSQLBackend.transformOrderByQueries,\n  with dml    : " + dml);
//            System.out.println("  modified_dml: " + modified_dml);
//        }
        return modified_dml.toString();
    }

    /** Modify queries containing an EXTRACT(DAY_OF_WEEK FROM ...) or
     *  EXTRACT(DAY_OF_YEAR FROM ...) function, which PostgreSQL does not
     *  support, and replace it with EXTRACT(DOW FROM ...)+1 or
     *  DATE_PART('DOY', ...), respectively, which is an equivalent that
     *  PostgreSQL does support. (The '+1' for DOW is because PostgreSQL
     *  counts Sunday as 0 and Saturday as 6, etc., whereas VoltDB counts
     *  Sunday as 1 and Saturday as 7, etc.) */
    static private String transformDayOfWeekOrYearQueries(String dml) {
        // TODO: temp debug:
//        System.out.println("Entered PostgreSQLBackend.transformDayOfWeekOrYearQueries,\n  with dml       : " + dml);
        StringBuffer modified_dml = new StringBuffer();
        Matcher matcher = dayOfWeekOrYearQuery.matcher(dml);
        // TODO: temp debug:
//        System.out.println("  matcher: " + matcher);
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer("EXTRACT ( ");
            String weekOrYear = null, column = null;
            try {
                weekOrYear = matcher.group("weekOrYear");
                column     = matcher.group("column");
                // TODO: temp debug:
//                System.out.println("  weekOrYear: " + weekOrYear);
//                System.out.println("  column    : " + column);
            } catch (IllegalArgumentException e) {
                // TODO: temp debug:
//                System.out.println("In PostgreSQLBackend.transformDayOfWeekOrYearQueries, caught:\n" + e);
                // do nothing: weekOrYear remains null
                break;
            }
            if (weekOrYear == null) {
                throw new ExpectedProcedureException("Programming error: weekOrYear should be 'DAY_OF_WEEK' " +
                        "or 'DAY_OF_YEAR', but is null (" + weekOrYear + "), for SQL statement:\n" + dml);
            } else if (weekOrYear.equalsIgnoreCase("DAY_OF_WEEK")) {
                // Off-by-one mismatch: VoltDB counts Sunday as 1; PostgreSQL
                // counts it as 0, so we need to always add one to 'DOW'
                replaceText.append("DOW FROM" + column + ")+1");
            } else if (weekOrYear.equalsIgnoreCase("DAY_OF_YEAR")) {
                replaceText.append("DOY FROM" + column + ")");
            } else {
                throw new ExpectedProcedureException("Programming error: weekOrYear should be 'DAY_OF_WEEK' " +
                        "or 'DAY_OF_YEAR', but is '" + weekOrYear + "', for SQL statement:\n" + dml);
            }
            // TODO: temp debug:
//            System.out.println("  replaceText: " + replaceText);
            matcher.appendReplacement(modified_dml, replaceText.toString());
            // TODO: temp debug:
//            System.out.println("  modified_dml(1): " + modified_dml);
        }
        matcher.appendTail(modified_dml);
        // TODO: temp debug:
//        System.out.println("  modified_dml(2): " + modified_dml);
//        // TODO: temp debug:
//        if (!dml.equalsIgnoreCase(modified_dml.toString())) {
//            System.out.println("In PostgreSQLBackend.transformDayOfWeekOrYearQueries,\n  with dml    : " + dml);
//            System.out.println("  modified_dml: " + modified_dml);
//        }
        return modified_dml.toString();
    }

    /** TODO */
    static private boolean isIntegerColumn(String columnName, String... tableNames) {
        // TODO: Temporary method, which will mostly work, for now:
        String columnNameUpper = columnName.toUpperCase();
        return  columnNameUpper.endsWith("ID") ||
                columnNameUpper.endsWith("NUM") ||
                columnNameUpper.endsWith("TINY") ||
                columnNameUpper.endsWith("SMALL") ||
                columnNameUpper.endsWith("BIG");
    }

    /** Modify queries containing an AVG(columnName) where <i>columnName</i>
     *  is of an integer type, for which PostgreSQL returns a numeric
     *  (non-integer) value, unlike VoltDB, which returns an integer. */
    static private String transformAvgOfIntegerQueries(String dml) {
        // TODO: temp debug:
//        System.out.println("Entered PostgreSQLBackend.transformAvgOfIntegerQueries,\n  with dml       : " + dml);
        StringBuffer modified_dml = new StringBuffer();
        Matcher matcher = avgQuery.matcher(dml);
        // TODO: temp debug:
//        System.out.println("  matcher: " + matcher);
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer();
            String column = null, avgFunc = null;
            try {
                column  = matcher.group("column");
                avgFunc = matcher.group(0);
                // TODO: temp debug:
//                System.out.println("  avgFunc: " + avgFunc);
//                System.out.println("  column : " + column);
            } catch (IllegalArgumentException e) {
                // TODO: temp debug:
//                System.out.println("In PostgreSQLBackend.transformAvgOfIntegerQueries, caught:\n" + e);
                // do nothing: group remains null
                break;
            }
            if (column == null) {
                throw new ExpectedProcedureException("Programming error: column should not "
                        + "be null, but is (" + column + "), for SQL statement:\n" + dml);
            } else if (isIntegerColumn(column)) {
                replaceText.append("TRUNC ( " + avgFunc + " )");
            } else {
                replaceText.append(avgFunc);
            }
            // TODO: temp debug:
//            System.out.println("  replaceText: " + replaceText);
            matcher.appendReplacement(modified_dml, replaceText.toString());
            // TODO: temp debug:
//            System.out.println("  modified_dml(1): " + modified_dml);
        }
        matcher.appendTail(modified_dml);
        // TODO: temp debug:
//        System.out.println("  modified_dml(2): " + modified_dml);
//        // TODO: temp debug:
//        if (!dml.equalsIgnoreCase(modified_dml.toString())) {
//            System.out.println("In PostgreSQLBackend.transformAvgOfIntegerQueries,\n  with dml    : " + dml);
//            System.out.println("  modified_dml: " + modified_dml);
//        }
        return modified_dml.toString();
    }

    /** Modify DDL containing VARCHAR(n BYTES), which PostgreSQL does not
     *  support, and replace it with VARCHAR(m), where m = n / 4 (but m is
     *  always at least 14, since many SQLCoverage tests use strings of that
     *  length). */
    static private String transformVarcharOfBytes(String ddl) {
        // TODO: temp debug:
//        System.out.println("Entered PostgreSQLBackend.transformVarcharOfBytes,\n  with ddl       : " + ddl);
        StringBuffer modified_ddl = new StringBuffer();
        Matcher matcher = varcharBytesDdl.matcher(ddl);
        // TODO: temp debug:
//        System.out.println("  matcher: " + matcher);
        while (matcher.find()) {
            StringBuffer replaceText = new StringBuffer();
            String numBytesStr = null;
            int numBytes = -1;
            try {
                numBytesStr = matcher.group("numBytes");
                numBytes = Integer.parseInt(numBytesStr);
                // TODO: temp debug:
//                System.out.println("  numBytesStr: " + numBytesStr);
//                System.out.println("  numBytes   : " + numBytes);
            } catch (IllegalArgumentException e) {
                // TODO: temp debug:
//                System.out.println("In PostgreSQLBackend.transformVarcharOfBytes, caught:\n" + e);
                // do nothing: numBytes remains -1
                break;
            }
            replaceText.append("VARCHAR(" + Math.max(numBytes / 4, 14) + ")");
            // TODO: temp debug:
//            System.out.println("  replaceText: " + replaceText);
            matcher.appendReplacement(modified_ddl, replaceText.toString());
            // TODO: temp debug:
//            System.out.println("  modified_ddl(1): " + modified_ddl);
        }
        matcher.appendTail(modified_ddl);
        // TODO: temp debug:
//        System.out.println("  modified_ddl(2): " + modified_ddl);
//        // TODO: temp debug:
//        if (!ddl.equalsIgnoreCase(modified_ddl.toString())) {
//            System.out.println("In PostgreSQLBackend.transformVarcharOfBytes,\n  with dml    : " + ddl);
//            System.out.println("  modified_dml: " + modified_ddl);
//        }
        return modified_ddl.toString();
    }

    /** Modify DDL containing VARBINARY(n), which PostgreSQL does not
     *  support, and replace it with BYTEA (which it does). */
    static private String transformVarbinary(String ddl) {
        // TODO: temp debug:
        System.out.println("Entered PostgreSQLBackend.transformVarbinary,\n  with ddl       : " + ddl);
        StringBuffer modified_ddl = new StringBuffer();
        Matcher matcher = varbinaryDdl.matcher(ddl);
        // TODO: temp debug:
//        System.out.println("  matcher: " + matcher);
        while (matcher.find()) {
            String varbinary = null;
            try {
                varbinary = matcher.group();
                // TODO: temp debug:
                System.out.println("  varbinary: " + varbinary);
            } catch (IllegalArgumentException e) {
                // TODO: temp debug:
                System.out.println("In PostgreSQLBackend.transformVarbinary, caught:\n" + e);
                // do nothing: numBytes remains -1
                break;
            }
            matcher.appendReplacement(modified_ddl, "BYTEA");
            // TODO: temp debug:
            System.out.println("  modified_ddl(1): " + modified_ddl);
        }
        matcher.appendTail(modified_ddl);
        // TODO: temp debug:
        System.out.println("  modified_ddl(2): " + modified_ddl);
        // TODO: temp debug:
        if (!ddl.equalsIgnoreCase(modified_ddl.toString())) {
            System.out.println("In PostgreSQLBackend.transformVarbinary,\n  with dml    : " + ddl);
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
        return  transformOrderByQueries(
                transformDayOfWeekOrYearQueries(
                transformAvgOfIntegerQueries(dml) ));
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
