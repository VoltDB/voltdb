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

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;

public class JDBC4Connection implements java.sql.Connection, IVoltDBConnection
{
    public static final String COMMIT_THROW_EXCEPTION = "jdbc.committhrowexception";
    public static final String ROLLBACK_THROW_EXCEPTION = "jdbc.rollbackthrowexception";

    protected final JDBC4ClientConnection NativeConnection;
    protected final String User;
    private boolean isClosed = false;
    private Properties props;
    private boolean autoCommit = true;

    public JDBC4Connection(JDBC4ClientConnection connection, Properties props)
    {
        this.NativeConnection = connection;
        this.props = props;
        this.User = this.props.getProperty("user", "");
    }

    private void checkClosed() throws SQLException
    {
        if (this.isClosed())
            throw SQLError.get(SQLError.CONNECTION_CLOSED);
    }

    // Clears all warnings reported for this Connection object.
    @Override
    public void clearWarnings() throws SQLException
    {
        checkClosed();
    }

    // Releases this Connection object's database and JDBC resources immediately instead of waiting for them to be automatically released.
    @Override
    public void close() throws SQLException
    {
        try
        {
            isClosed = true;
            JDBC4ClientConnectionPool.dispose(NativeConnection);
        }
        catch(Exception x)
        {
            throw SQLError.get(x);
        }
    }

    // Makes all changes made since the previous commit/rollback permanent and releases any database locks currently held by this Connection object.
    @Override
    public void commit() throws SQLException
    {
        checkClosed();
        if (props.getProperty(COMMIT_THROW_EXCEPTION, "true").equalsIgnoreCase("true")) {
            throw SQLError.noSupport();
        }
    }

    // Factory method for creating Array objects.
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the Blob interface.
    @Override
    public Blob createBlob() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the Clob interface.
    @Override
    public Clob createClob() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the NClob interface.
    @Override
    public NClob createNClob() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the SQLXML interface.
    @Override
    public SQLXML createSQLXML() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a Statement object for sending SQL statements to the database.
    @Override
    public Statement createStatement() throws SQLException
    {
        checkClosed();
        try
        {
            return new JDBC4Statement(this);
        }
        catch(Exception x)
        {
            throw SQLError.get(x);
        }
    }

    /**
     * Check if the createStatement() options are supported
     *
     * See http://docs.oracle.com/javase/7/docs/api/index.html?java/sql/DatabaseMetaData.html
     *
     * The following flags are supported:
     *  - The type must either be TYPE_SCROLL_INSENSITIVE or TYPE_FORWARD_ONLY.
     *  - The concurrency must be CONCUR_READ_ONLY.
     *  - The holdability must be CLOSE_CURSORS_AT_COMMIT.
     *
     * @param resultSetType  JDBC result set type option
     * @param resultSetConcurrency  JDBC result set concurrency option
     * @param resultSetHoldability  JDBC result set holdability option
     * @throws SQLException  if not supported
     */
    private static void checkCreateStatementSupported(
            int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                    throws SQLException
    {
        if (   (   (resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE
                &&  resultSetType != ResultSet.TYPE_FORWARD_ONLY))
            || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
            || resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw SQLError.noSupport();
        }
    }

    /**
     * Check if the createStatement() options are supported
     *
     * The following flags are supported:
     *  - The type must either be TYPE_SCROLL_INSENSITIVE or TYPE_FORWARD_ONLY.
     *  - The concurrency must be CONCUR_READ_ONLY.
     *
     * @param resultSetType  JDBC result set type option
     * @param resultSetConcurrency  JDBC result set concurrency option
     * @throws SQLException  if not supported
     */
    private static void checkCreateStatementSupported(
            int resultSetType, int resultSetConcurrency)
                    throws SQLException
    {
        checkCreateStatementSupported(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    // Creates a Statement object that will generate ResultSet objects with the given type and concurrency.
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        checkClosed();
        // Reject options that don't coincide with normal VoltDB behavior.
        checkCreateStatementSupported(resultSetType, resultSetConcurrency);
        return createStatement();
    }

    // Creates a Statement object that will generate ResultSet objects with the given type, concurrency, and holdability.
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkClosed();
        // Reject options that don't coincide with normal VoltDB behavior.
        checkCreateStatementSupported(resultSetType, resultSetConcurrency, resultSetHoldability);
        return createStatement();
    }

    // Factory method for creating Struct objects.
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the current auto-commit mode for this Connection object.
    // We are always auto-committing, but if let's be consistent with the lying.
    @Override
    public boolean getAutoCommit() throws SQLException
    {
        checkClosed();
        return autoCommit;
    }

    // Retrieves this Connection object's current catalog name.
    @Override
    public String getCatalog() throws SQLException
    {
        checkClosed();
        return "";
    }

    // Returns a list containing the name and current value of each client info property supported by the driver.
    @Override
    public Properties getClientInfo() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Returns the value of the client info property specified by name.
    @Override
    public String getClientInfo(String name) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the current holdability of ResultSet objects created using this Connection object.
    @Override
    public int getHoldability() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a DatabaseMetaData object that contains metadata about the database to which this Connection object represents a connection.
    @Override
    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkClosed();
        return new JDBC4DatabaseMetaData(this);
    }

    // Retrieves this Connection object's current transaction isolation level.
    @Override
    public int getTransactionIsolation() throws SQLException
    {
        checkClosed();
        return TRANSACTION_SERIALIZABLE;
    }

    // Retrieves the Map object associated with this Connection object.
    @Override
    public Map<String,Class<?>> getTypeMap() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the first warning reported by calls on this Connection object.
    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        checkClosed();
        return null;
    }

    // Retrieves whether this Connection object has been closed.
    @Override
    public boolean isClosed() throws SQLException
    {
        return isClosed;
    }

    // Retrieves whether this Connection object is in read-only mode.
    @Override
    public boolean isReadOnly() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Returns true if the connection has not been closed and is still valid.
    @Override
    public boolean isValid(int timeout) throws SQLException
    {
        return !isClosed;
    }

    // Converts the given SQL statement into the system's native SQL grammar.
    @Override
    public String nativeSQL(String sql) throws SQLException
    {
        checkClosed();
        return sql; // Well...
    }

    // Creates a CallableStatement object for calling database stored procedures.
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        checkClosed();
        return new JDBC4CallableStatement(this, sql);
    }

    // Creates a CallableStatement object that will generate ResultSet objects with the given type and concurrency.
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)
            return prepareCall(sql);
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a CallableStatement object that will generate ResultSet objects with the given type and concurrency.
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a PreparedStatement object for sending parameterized SQL statements to the database.
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        checkClosed();
        return new JDBC4PreparedStatement(this, sql);
    }

    // Creates a default PreparedStatement object that has the capability to retrieve auto-generated keys.
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a default PreparedStatement object capable of returning the auto-generated keys designated by the given array.
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a PreparedStatement object that will generate ResultSet objects with the given type and concurrency.
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)
            return prepareStatement(sql);
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a PreparedStatement object that will generate ResultSet objects with the given type, concurrency, and holdability.
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a default PreparedStatement object capable of returning the auto-generated keys designated by the given array.
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Removes the specified Savepoint and subsequent Savepoint objects from the current transaction.
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Undoes all changes made in the current transaction and releases any database locks currently held by this Connection object.
    @Override
    public void rollback() throws SQLException
    {
        checkClosed();
        if (props.getProperty(ROLLBACK_THROW_EXCEPTION, "true").equalsIgnoreCase("true")) {
            throw SQLError.noSupport();
        }
    }

    // Undoes all changes made after the given Savepoint object was set.
    @Override
    public void rollback(Savepoint savepoint) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets this connection's auto-commit mode to the given state.
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkClosed();
        // Always true - error out only if the client is trying to set somethign else
        if (!autoCommit && (props.getProperty(COMMIT_THROW_EXCEPTION, "true").equalsIgnoreCase("true"))) {
            throw SQLError.noSupport();
        }
        else {
            this.autoCommit = autoCommit;
        }
    }

    // Sets the given catalog name in order to select a subspace of this Connection object's database in which to work.
    @Override
    public void setCatalog(String catalog) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the value of the connection's client info properties.
    @Override
    public void setClientInfo(Properties properties)
    {
        // No-op (key client properties cannot be changed after the connection has been opened anyways!)
    }

    // Sets the value of the client info property specified by name to the value specified by value.
    @Override
    public void setClientInfo(String name, String value)
    {
        // No-op (key client properties cannot be changed after the connection has been opened anyways!)
    }

    // Changes the default holdability of ResultSet objects created using this Connection object to the given holdability.
    @Override
    public void setHoldability(int holdability) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Puts this connection in read-only mode as a hint to the driver to enable database optimizations.
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates an unnamed savepoint in the current transaction and returns the new Savepoint object that represents it.
    @Override
    public Savepoint setSavepoint() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a savepoint with the given name in the current transaction and returns the new Savepoint object that represents it.
    @Override
    public Savepoint setSavepoint(String name) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Attempts to change the transaction isolation level for this Connection object to the one given.
    @Override
    public void setTransactionIsolation(int level) throws SQLException
    {
        checkClosed();
        if (level == TRANSACTION_SERIALIZABLE)
            return;
        throw SQLError.noSupport();
    }

    // Installs the given TypeMap object as the type map for this Connection object.
    @Override
    public void setTypeMap(Map<String,Class<?>> map) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an object that does.
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.isInstance(this);
    }

    // Returns an object that implements the given interface to allow access to non-standard methods, or standard methods not exposed by the proxy.
    @Override
    public <T> T unwrap(Class<T> iface)    throws SQLException
    {
        try
        {
            return iface.cast(this);
        }
         catch (ClassCastException cce)
         {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, iface.toString());
        }
    }

    /**
     * Gets the new version of the performance statistics for this connection only.
     * @return A {@link ClientStatsContext} that correctly represents the client statistics.
     */
    @Override
    public ClientStatsContext createStatsContext() {
        return this.NativeConnection.getClientStatsContext();
    }

    // Save statistics to a file
    @Override
    public void saveStatistics(ClientStats stats, String file) throws IOException
    {
        this.NativeConnection.saveStatistics(stats, file);
    }

    public void setSchema(String schema) throws SQLException {
        throw SQLError.noSupport();
    }

    public String getSchema() throws SQLException {
        throw SQLError.noSupport();
    }

    public void abort(Executor executor) throws SQLException {
        throw SQLError.noSupport();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        throw SQLError.noSupport();
    }

    public int getNetworkTimeout() throws SQLException {
        throw SQLError.noSupport();
    }

    @Override
    public void writeSummaryCSV(ClientStats stats, String path)
            throws IOException {
        this.NativeConnection.writeSummaryCSV(stats, path);

    }

}
