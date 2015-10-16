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

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

/**
 * A wrapper around a PostgreSQL database server, and JDBC connection. This
 * class can be used to execute SQL statements instead of the C++ ExecutionEngine.
 * It is currently used only by the SQL Coverage tests.
 */
public class PostgreSQLBackend extends GenericBackend {
    /** java.util.logging logger. */
    @SuppressWarnings("unused")
    private static final VoltLogger log = new VoltLogger(PostgreSQLBackend.class.getName());

    private static final Object backendLock = new Object();
    private static final String m_default_username = "postgres";
    private static final String m_default_password = "voltdb";
    private static final String m_permanent_database_name = "postgres";
    private static final String m_database_name = "sqlcoveragetest";
    private static PostgreSQLBackend m_permanent_db_backend = null;
    private static PostgreSQLBackend m_backend = null;

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
            return m_backend;
        }
    }

    static public void shutdownInstance()
    {
        synchronized(backendLock) {
            if (m_backend != null) {
                m_backend.shutdown();
                m_backend = null;
            }
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

    /** Before running the specified SQL DDL, replace keywords not supported
     *  by PostgreSQL with similar terms. */
    @Override
    public void runDDL(String ddl) {
        String modified_ddl = ddl.replace("TINYINT", "SMALLINT")
                                 .replace("ASSUMEUNIQUE", "UNIQUE");
        super.runDDL(modified_ddl);
    }

    /**
     * Returns a VoltTable.ColumnInfo of appropriate type, based on a
     * <i>typeName</i> and <i>colName</i> (both Strings).
     * This version checks for special column types used by PostgreSQL,
     * and then passes the remaining work to the base class version.
     */
    @Override
    protected VoltTable.ColumnInfo getColumnInfo(String typeName, String colName) {
        if (typeName.equalsIgnoreCase("TINYINT"))
            return new VoltTable.ColumnInfo(colName, VoltType.SMALLINT);
        else if (typeName.equals("int2"))
            return new VoltTable.ColumnInfo(colName, VoltType.SMALLINT);
        else if (typeName.equals("int4"))
            return new VoltTable.ColumnInfo(colName, VoltType.INTEGER);
        else if (typeName.equals("int8"))
            return new VoltTable.ColumnInfo(colName, VoltType.BIGINT);
        else if (typeName.equalsIgnoreCase("NUMERIC"))
            return new VoltTable.ColumnInfo(colName, VoltType.DECIMAL);
        else if (typeName.equals("float8"))
            return new VoltTable.ColumnInfo(colName, VoltType.FLOAT);
        else if (typeName.equals("bytea"))
            return new VoltTable.ColumnInfo(colName, VoltType.VARBINARY);
        else if (typeName.equals("char") || typeName.equals("text"))
            return new VoltTable.ColumnInfo(colName, VoltType.STRING);
        else
            return super.getColumnInfo(typeName, colName);
    }

    private Connection getConnection() {
        return dbconn;
    }

    private void shutdown() {
        try {
            dbconn.close();
            dbconn = null;
            System.gc();
            try {
                Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                stmt.execute("drop database if exists " + m_database_name + ";");
            } catch (SQLException ex) {
                System.out.println("In PostgreSQLBackend.shutdown(), caught exception: " + ex);
                ex.printStackTrace();
            }
        } catch (Exception e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_Backend_ErrorOnShutdown.name(), e);
        }
    }

}
