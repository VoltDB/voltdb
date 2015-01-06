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

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

/**
 * A wrapper around the HSQLDB engine. This class can be used to execute SQL
 * statements instead of the C++ ExecutionEngine. It is currently not used.
 *
 */
public class HsqlBackend {
    /** java.util.logging logger. */
    @SuppressWarnings("unused")
    private static final VoltLogger log = new VoltLogger(HsqlBackend.class.getName());
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger sqlLog = new VoltLogger("SQL");

    private final static Object backendLock = new Object();
    private static HsqlBackend m_backend = null;

    static public HsqlBackend initializeHSQLBackend(long siteId,
                                                    CatalogContext context)
    {
        synchronized(backendLock) {
            if (m_backend == null) {
                try {
                    m_backend = new HsqlBackend(siteId);
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
                catch (final Exception ex) {
                    hostLog.fatal("Unable to construct HSQL backend");
                    VoltDB.crashLocalVoltDB(ex.getMessage(), true, ex);
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

    Connection dbconn;

    public HsqlBackend(long siteId) {
        try {
            Class.forName("org.hsqldb_voltpatches.jdbcDriver" );
        } catch (Exception e) {
            throw new RuntimeException("Failed to load HSQLDB JDBC driver.", e);
        }

        try {
            final String connectionURL = "jdbc:hsqldb:mem:x" + String.valueOf(siteId);
            dbconn = DriverManager.getConnection(connectionURL, "sa", "");
            dbconn.setAutoCommit(true);
            dbconn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to open connection to " + "jdbc:hsqldb:mem:x" + String.valueOf(siteId), e);
        }
    }

    /** Creates a new backend wrapping dbconn. This is used for testing only. */
    private HsqlBackend(Connection dbconn) {
        this.dbconn = dbconn;
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
                    if (type.equals("VARCHAR"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.STRING);
                    else if (type.equals("TINYINT"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.TINYINT);
                    else if (type.equals("SMALLINT"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.SMALLINT);
                    else if (type.equals("INTEGER"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.INTEGER);
                    else if (type.equals("BIGINT"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.BIGINT);
                    else if (type.equals("DECIMAL"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.DECIMAL);
                    else if (type.equals("FLOAT"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.FLOAT);
                    else if (type.equals("TIMESTAMP"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.TIMESTAMP);
                    else if (type.equals("VARBINARY"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.VARBINARY);
                    else if (type.equals("CHARACTER"))
                        columns[i-1] = new VoltTable.ColumnInfo(colname, VoltType.STRING);
                    else
                        throw new ExpectedProcedureException("Trying to create a column in Backend with a (currently) unsupported type: " + type);
                }
                VoltTable table = new VoltTable(columns);
                while (rs.next()) {
                    Object[] row = new Object[table.getColumnCount()];
                    for (int i = 0; i < table.getColumnCount(); i++) {
                        // TODO(evanj): JDBC returns 0 instead of null. Put null into the row?
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
                throw new ExpectedProcedureException("HSQLDB Backend DML Error ", e);
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
                        FastSerializer.writeString("HSQL", b);
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
                    throw new ExpectedProcedureException("HSQLDB Backend DML Error ", e);
                }

            } catch (Exception e) {
                // rethrow an expected exception
                sqlLog.l7dlog( Level.TRACE, LogKeys.sql_Backend_DmlError.name(), e);
                throw new ExpectedProcedureException("HSQLDB Backend DML Error ", e);
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
                        System.err.println("FATAL: Your JVM doens't support UTF-&");
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

    private void shutdown() {
        try {
            try {
                Statement stmt = dbconn.createStatement();
                stmt.execute("SHUTDOWN;");
            } catch (Exception e) {};
            dbconn.close();
            dbconn = null;
            System.gc();
        } catch (Exception e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_Backend_ErrorOnShutdown.name(), e);
        }
    }
}
