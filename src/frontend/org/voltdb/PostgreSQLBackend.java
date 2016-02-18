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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
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

    // Captures the use of ORDER BY, with up to 6 order-by columns; beyond
    // those will be ignored (similar to
    // voltdb/tests/scripts/examples/sql_coverage/StandardNormalzer.py)
    private static final Pattern orderByQuery = Pattern.compile(
            "ORDER BY(?<column1>\\s+(\\w*\\s*\\(\\s*)*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?)"
            + "((?<column2>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column3>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column4>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column5>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column6>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an ORDER BY clause, by adding (for each
    // order-by column) either NULLS FIRST or (after "DESC") NULL LAST, so
    // that PostgreSQL results will match VoltDB results
    private static final QueryTransformer orderByQueryTransformer
            = new QueryTransformer(orderByQuery)
            .initialText("ORDER BY").suffix(" NULLS FIRST")
            .alternateSuffix("DESC", " NULLS LAST")
            .groups("column1", "column2", "column3", "column4", "column5", "column6");

    // Captures the use of EXTRACT(DAY_OF_WEEK FROM ...)
    private static final Pattern dayOfWeekQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_WEEK\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an EXTRACT(DAY_OF_WEEK FROM ...)
    // function, which PostgreSQL does not support, and replaces it with
    // EXTRACT(DOW FROM ...)+1, which is an equivalent that PostgreSQL does
    // support. (The '+1' is because PostgreSQL counts Sunday as 0 and Saturday
    // as 6, etc., whereas VoltDB counts Sunday as 1 and Saturday as 7, etc.)
    private static final QueryTransformer dayOfWeekQueryTransformer
            = new QueryTransformer(dayOfWeekQuery)
            .initialText("EXTRACT ( ").prefix("DOW FROM").suffix(")+1").groups("column");

    // Captures the use of EXTRACT(DAY_OF_YEAR FROM ...)
    private static final Pattern dayOfYearQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_YEAR\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an EXTRACT(DAY_OF_YEAR FROM ...)
    // function, which PostgreSQL does not support, and replaces it with
    // EXTRACT(DOY FROM ...), which is an equivalent that PostgreSQL does
    // support
    private static final QueryTransformer dayOfYearQueryTransformer
            = new QueryTransformer(dayOfYearQuery)
            .initialText("EXTRACT ( ").prefix("DOY FROM").suffix(")").groups("column");

    // Captures the use of AVG(columnName), which PostgreSQL handles
    // differently, when the columnName is of one of the integer types
    private static final Pattern avgQuery = Pattern.compile(
            "AVG\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an AVG(columnName) function, where
    // <i>columnName</i> is of an integer type, for which PostgreSQL returns
    // a numeric (non-integer) value, unlike VoltDB, which returns an integer;
    // so change it to: TRUNC ( AVG(columnName) )
    private static final QueryTransformer avgQueryTransformer
            = new QueryTransformer(avgQuery)
            .prefix("TRUNC ( ").suffix(" )").groups("column")
            .useWholeMatch().columnType(ColumnType.INTEGER);

    // Captures the use of CEILING(columnName) or FLOOR(columnName)
    private static final Pattern ceilingOrFloorQuery = Pattern.compile(
            "(CEILING|FLOOR)\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a CEILING(columnName) or FLOOR(columnName)
    // function, where <i>columnName</i> is of an integer type, for which
    // PostgreSQL returns a numeric (non-integer) value, unlike VoltDB, which
    // returns an integer; so change it to:
    // CAST ( CEILING(columnName) as INTEGER ), or
    // CAST ( FLOOR(columnName) as INTEGER ), respectively.
    private static final QueryTransformer ceilingOrFloorQueryTransformer
            = new QueryTransformer(ceilingOrFloorQuery)
            .prefix("CAST ( ").suffix(" as INTEGER )").groups("column")
            .useWholeMatch().columnType(ColumnType.INTEGER);

    // Captures the use of string concatenation using 'str' + ...
    private static final Pattern stringConcatQuery = Pattern.compile(
            "'\\w+'\\s*(?<plus>\\+)", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing 'FOO' + ..., which PostgreSQL does not
    // support, and replaces it with 'FOO' || ..., which is an equivalent
    // that PostgreSQL does support
    private static final QueryTransformer stringConcatQueryTransformer
            = new QueryTransformer(stringConcatQuery)
            .replacementText("||").useWholeMatch().groups("plus");

    // Captures the use of VARCHAR(n BYTES) (in DDL)
    private static final Pattern varcharBytesDdl = Pattern.compile(
            "VARCHAR\\s*\\(\\s*(?<numBytes>\\w+)\\s+BYTES\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing VARCHAR(n BYTES), which PostgreSQL
    // does not support, and replaces it with VARCHAR(m), where m = n / 4
    // (but m is always at least 14, since many SQLCoverage tests use strings
    // of that length), which it does support
    private static final QueryTransformer varcharBytesDdlTransformer
            = new QueryTransformer(varcharBytesDdl)
            .prefix("VARCHAR(").suffix(")").multiplier(0.25).minimum(14)
            .groups("numBytes");

    // Captures the use of VARBINARY(n)
    private static final Pattern varbinaryDdl = Pattern.compile(
            "VARBINARY\\s*\\(\\s*(?<numBytes>\\d+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing VARBINARY(n), which PostgreSQL does
    // not support, and replaces it with BIT VARYING(m), where m = n * 8
    // (i.e., converting from bytes to bits), which it does support
    private static final QueryTransformer varbinaryDdlTransformer
            = new QueryTransformer(varbinaryDdl)
            .prefix("BIT VARYING(").suffix(")").multiplier(8.0).minimum(8)
            .groups("numBytes");

    // Captures the use of TINYINT (in DDL)
    private static final Pattern tinyintDdl = Pattern.compile(
            "TINYINT", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing TINYINT, which PostgreSQL does not
    // support, and replaces it with SMALLINT, which is an equivalent that
    // PostGIS does support
    private static final QueryTransformer tinyintDdlTransformer
            = new QueryTransformer(tinyintDdl)
            .replacementText("SMALLINT").useWholeMatch();

    // Captures the use of ASSUMEUNIQUE (in DDL)
    private static final Pattern assumeUniqueDdl = Pattern.compile(
            "ASSUMEUNIQUE", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing ASSUMEUNIQUE, which PostgreSQL does
    // not support, and replaces it with UNIQUE, which is an equivalent that
    // PostGIS does support
    private static final QueryTransformer assumeUniqueDdlTransformer
            = new QueryTransformer(assumeUniqueDdl)
            .replacementText("UNIQUE").useWholeMatch();

    // Captures up to 6 table names, for each FROM clause used in the query
    // TODO: we may want to fix & finish this, in order to actually check the
    // column types, rather than just go by the column names (ENG-9945); this
    // would be used for for AVG, CEILING, FLOOR, and CAST queries
//    private static final Pattern tableNames = Pattern.compile(
//              "FROM\\s*\\(?<table1>\\w+)\\s*"
//            + "(\\s*,s*\\(?<table2>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table3>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table4>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table5>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table6>\\w+)\\s*)?",
//            Pattern.CASE_INSENSITIVE);

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

    /** For a SQL DDL statement, replace (VoltDB) keywords not supported by
     *  PostgreSQL with other, similar terms. */
    static public String transformDDL(String ddl) {
        return transformQuery(ddl, tinyintDdlTransformer,
                varcharBytesDdlTransformer, varbinaryDdlTransformer,
                assumeUniqueDdlTransformer);
    }

    /** For a SQL query, replace (VoltDB) keywords not supported by PostgreSQL,
     *  or which behave differently in PostgreSQL than in VoltDB, with other,
     *  similar terms, so that the results will match. */
    static public String transformDML(String dml) {
        return transformQuery(dml, orderByQueryTransformer,
                avgQueryTransformer, ceilingOrFloorQueryTransformer,
                dayOfWeekQueryTransformer, dayOfYearQueryTransformer,
                stringConcatQueryTransformer);
    }

    /** Optionally, modifies DDL statements in such a way that PostgreSQL
     *  results will match VoltDB results; and then passes the remaining
     *  work to the base class version. */
    protected void runDDL(String ddl, boolean transformDdl) {
        String modifiedDdl = (transformDdl ? transformDDL(ddl) : ddl);
        printTransformedSql(ddl, modifiedDdl);
        super.runDDL(modifiedDdl);
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
        String modifiedDml = (transformDml ? transformDML(dml) : dml);
        printTransformedSql(dml, modifiedDml);
        return super.runDML(modifiedDml);
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
            if (transformedSqlFileWriter != null) {
                transformedSqlFileWriter.close();
                transformedSqlFileWriter = null;
            }
        } catch (Exception e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_Backend_ErrorOnShutdown.name(), e);
        }
    }

}
