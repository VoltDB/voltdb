/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.jdbc;

import java.sql.*;
import java.util.*;
import java.util.regex.*;
import org.voltdb.*;
import org.voltdb.client.*;
import org.voltdb.client.exampleutils.*;

public class JDBC4Connection implements java.sql.Connection, IVoltDBConnection
{
    protected final ClientConnection NativeConnection;
    protected final String User;
    private boolean isClosed = false;

    public JDBC4Connection(ClientConnection connection, String user)
    {
        this.NativeConnection = connection;
        this.User = user;
    }

    private void checkClosed() throws SQLException
    {
        if (this.isClosed())
            throw SQLError.get(SQLError.CONNECTION_CLOSED);
    }

    // Clears all warnings reported for this Connection object.
    public void clearWarnings() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Releases this Connection object's database and JDBC resources immediately instead of waiting for them to be automatically released.
    public void close() throws SQLException
    {
        try
        {
            isClosed = true;
            ClientConnectionPool.dispose(NativeConnection);
        }
        catch(Exception x)
        {
            throw SQLError.get(x);
        }
    }

    // Makes all changes made since the previous commit/rollback permanent and releases any database locks currently held by this Connection object.
    public void commit() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Factory method for creating Array objects.
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the Blob interface.
    public Blob createBlob() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the Clob interface.
    public Clob createClob() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the NClob interface.
    public NClob createNClob() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Constructs an object that implements the SQLXML interface.
    public SQLXML createSQLXML() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a Statement object for sending SQL statements to the database.
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

    // Creates a Statement object that will generate ResultSet objects with the given type and concurrency.
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)
            return createStatement();
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a Statement object that will generate ResultSet objects with the given type, concurrency, and holdability.
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Factory method for creating Struct objects.
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the current auto-commit mode for this Connection object.
    public boolean getAutoCommit() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport(); // return true always instead?
    }

    // Retrieves this Connection object's current catalog name.
    public String getCatalog() throws SQLException
    {
        checkClosed();
        return "";
    }

    // Returns a list containing the name and current value of each client info property supported by the driver.
    public Properties getClientInfo() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Returns the value of the client info property specified by name.
    public String getClientInfo(String name) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the current holdability of ResultSet objects created using this Connection object.
    public int getHoldability() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves a DatabaseMetaData object that contains metadata about the database to which this Connection object represents a connection.
    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkClosed();
        return new JDBC4DatabaseMetaData(this);
    }

    // Retrieves this Connection object's current transaction isolation level.
    public int getTransactionIsolation() throws SQLException
    {
        checkClosed();
        return TRANSACTION_SERIALIZABLE;
    }

    // Retrieves the Map object associated with this Connection object.
    public Map<String,Class<?>> getTypeMap() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the first warning reported by calls on this Connection object.
    public SQLWarning getWarnings() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves whether this Connection object has been closed.
    public boolean isClosed() throws SQLException
    {
        return isClosed; // TODO: This is retarded: the native VoltDB.Client does not have such a status - we should have this so we can appropriately deal with connection failures!
    }

    // Retrieves whether this Connection object is in read-only mode.
    public boolean isReadOnly() throws SQLException
    {
        checkClosed();
        return false;
    }

    // Returns true if the connection has not been closed and is still valid.
    public boolean isValid(int timeout) throws SQLException
    {
        return isClosed; // TODO: This is retarded: the native VoltDB.Client does not have such a status - we should have this so we can appropriately deal with connection failures!
    }

    // Converts the given SQL statement into the system's native SQL grammar.
    public String nativeSQL(String sql) throws SQLException
    {
        checkClosed();
        return sql; // Well...
    }

    // Creates a CallableStatement object for calling database stored procedures.
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        checkClosed();
        return new JDBC4CallableStatement(this, sql);
    }

    // Creates a CallableStatement object that will generate ResultSet objects with the given type and concurrency.
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)
            return prepareCall(sql);
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a CallableStatement object that will generate ResultSet objects with the given type and concurrency.
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a PreparedStatement object for sending parameterized SQL statements to the database.
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        checkClosed();
        return new JDBC4PreparedStatement(this, sql);
    }

    // Creates a default PreparedStatement object that has the capability to retrieve auto-generated keys.
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a default PreparedStatement object capable of returning the auto-generated keys designated by the given array.
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a PreparedStatement object that will generate ResultSet objects with the given type and concurrency.
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY)
            return prepareStatement(sql);
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a PreparedStatement object that will generate ResultSet objects with the given type, concurrency, and holdability.
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a default PreparedStatement object capable of returning the auto-generated keys designated by the given array.
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Removes the specified Savepoint and subsequent Savepoint objects from the current transaction.
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Undoes all changes made in the current transaction and releases any database locks currently held by this Connection object.
    public void rollback() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Undoes all changes made after the given Savepoint object was set.
    public void rollback(Savepoint savepoint) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets this connection's auto-commit mode to the given state.
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkClosed();
    if (!autoCommit) // Always true - error out only if the client is trying to set somethign else
            throw SQLError.noSupport();
    }

    // Sets the given catalog name in order to select a subspace of this Connection object's database in which to work.
    public void setCatalog(String catalog) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets the value of the connection's client info properties.
    public void setClientInfo(Properties properties)
    {
        // No-op (key client properties cannot be changed after the connection has been opened anyways!)
    }

    // Sets the value of the client info property specified by name to the value specified by value.
    public void setClientInfo(String name, String value)
    {
        // No-op (key client properties cannot be changed after the connection has been opened anyways!)
    }

    // Changes the default holdability of ResultSet objects created using this Connection object to the given holdability.
    public void setHoldability(int holdability) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Puts this connection in read-only mode as a hint to the driver to enable database optimizations.
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates an unnamed savepoint in the current transaction and returns the new Savepoint object that represents it.
    public Savepoint setSavepoint() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Creates a savepoint with the given name in the current transaction and returns the new Savepoint object that represents it.
    public Savepoint setSavepoint(String name) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Attempts to change the transaction isolation level for this Connection object to the one given.
    public void setTransactionIsolation(int level) throws SQLException
    {
        checkClosed();
        if (level == TRANSACTION_SERIALIZABLE)
            return;
        throw SQLError.noSupport();
    }

    // Installs the given TypeMap object as the type map for this Connection object.
    public void setTypeMap(Map<String,Class<?>> map) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an object that does.
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.isInstance(this);
    }

    // Returns an object that implements the given interface to allow access to non-standard methods, or standard methods not exposed by the proxy.
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

    // IVoltDBConnection extended method
    // Return global performance statistics for the underlying connection (pooled information)
    public PerfCounterMap getStatistics()
    {
        return this.NativeConnection.getStatistics();
    }

    // Return performance statistics for a specific procedure, for the underlying connection (pooled information)
    public PerfCounter getStatistics(String procedure)
    {
        return this.NativeConnection.getStatistics(procedure);
    }

    // Return performance statistics for a list of procedures, for the underlying connection (pooled information)
    public PerfCounter getStatistics(String... procedures)
    {
        return this.NativeConnection.getStatistics(procedures);
    }
}

