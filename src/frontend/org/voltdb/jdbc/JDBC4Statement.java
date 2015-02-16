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
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.parser.JDBCParser;
import org.voltdb.parser.SQLLexer;
import org.voltdb.parser.JDBCParser.ParsedCall;

public class JDBC4Statement implements java.sql.Statement
{

    //Timeout for statement. This is used for execute* methods. batch add dont have timeout.
    private int m_timeout = 0;
    static class VoltSQL
    {
        public static final byte TYPE_SELECT = 1;
        public static final byte TYPE_UPDATE = 2;
        public static final byte TYPE_EXEC = 3;

        private final String[] sql;
        private final int parameterCount;
        private final byte type;
        private final byte queryType;   // Type of query EXEC'd by @AdHoc
        private final Object[] parameters;

        private VoltSQL(String[] sql, int parameterCount, byte type)
        {
            this.sql = sql;
            this.parameterCount = parameterCount;
            this.type = this.queryType = type;
            this.parameters = null;
        }

        private VoltSQL(String[] sql, int parameterCount, byte type, Object[] parameters)
        {
            this(sql, parameterCount, type, type, parameters);
        }

        private VoltSQL(String[] sql, int parameterCount, byte type, byte queryType, Object[] parameters)
        {
            this.sql = sql;
            this.parameterCount = parameterCount;
            this.type = type;
            this.queryType = queryType;
            this.parameters = parameters;
        }

        public boolean hasParameters()
        {
            return this.parameterCount > 0;
        }

        public Object[] getParameterArray() throws SQLException
        {
            return new Object[this.parameterCount];
        }

        public int getParameterCount()
        {
            return this.parameterCount;
        }

        public boolean isOfType(int... types)
        {
            for(int i=0;i<types.length;i++) {
                if (this.type == types[i]) {
                    return true;
                }
            }
            return false;
        }

        public boolean isQueryOfType(int... types)
        {
            for(int i=0;i<types.length;i++) {
                if (this.queryType == types[i]) {
                    return true;
                }
            }
            return false;
        }

        protected VoltTable[] execute(JDBC4ClientConnection connection, long timeout) throws SQLException {
            try
            {
                if (this.type == TYPE_EXEC) {
                    return connection.execute(this.sql[0], timeout, this.parameters).getResults();
                } else {
                    return connection.execute("@AdHoc", timeout, this.sql[0]).getResults();
                }
            }
            catch(ProcCallException e)
            {
                ClientResponse response = e.getClientResponse();
                if (response != null) {
                    // Map response status to specific JDBC exception, mostly GENERAL_ERROR except
                    // for connection problems.
                    switch (response.getStatus()) {
                    case ClientResponse.CONNECTION_LOST:
                        throw SQLError.get(e, SQLError.CONNECTION_CLOSED, "CONNECTION_LOST", e.getMessage());
                    case ClientResponse.CONNECTION_TIMEOUT:
                        throw SQLError.get(e, SQLError.CONNECTION_FAILURE, "CONNECTION_TIMEOUT", e.getMessage());
                    case ClientResponse.SERVER_UNAVAILABLE:
                        throw SQLError.get(e, SQLError.CONNECTION_FAILURE, "CONNECTION_UNAVAILABLE", e.getMessage());
                    case ClientResponse.USER_ABORT:
                        throw SQLError.get(e, SQLError.GENERAL_ERROR, "USER_ABORT", e.getMessage());
                    case ClientResponse.UNEXPECTED_FAILURE:
                        throw SQLError.get(e, SQLError.GENERAL_ERROR, "UNEXPECTED_FAILURE", e.getMessage());
                    case ClientResponse.GRACEFUL_FAILURE:
                        throw SQLError.get(e, SQLError.GENERAL_ERROR, "GRACEFUL_FAILURE", e.getMessage());
                    default:
                        throw SQLError.get(e, SQLError.GENERAL_ERROR, String.format("status=%d", (int)response.getStatus()), e.getMessage());
                    }
                } else {
                    throw SQLError.get(e, SQLError.GENERAL_ERROR, e.getMessage());
                }
            }
            catch(IOException e)
            {
                throw SQLError.get(e, SQLError.CONNECTION_FAILURE, e.getMessage());
            }
        }

        public static boolean isUpdateResult(VoltTable table)
        {
            return ((table.getColumnName(0).length() == 0 || table.getColumnName(0).equals("modified_tuples"))&& table.getRowCount() == 1 && table.getColumnCount() == 1 && table.getColumnType(0) == VoltType.BIGINT);
        }

        public String toSqlString()
        {
            return this.sql[0];
        }

        public VoltSQL getExecutableQuery(Object... params) throws SQLException
        {
            if (params.length != this.parameterCount) {
                throw SQLError.get(SQLError.ILLEGAL_ARGUMENT);
            }

            if (this.type == TYPE_EXEC) {
                return new VoltSQL(this.sql, this.parameterCount, this.type, params);
            } else
            {
                Object[] paramsOut = new Object[params.length+1];
                paramsOut[0] = this.sql[0];
                for (int i = 0; i < params.length; ++i) {
                    paramsOut[i+1] = params[i];
                }
                return new VoltSQL(new String[] {"@AdHoc"}, this.parameterCount, TYPE_EXEC, this.type, paramsOut);
            }
        }

        // SQL Parsing
        public static VoltSQL parseCall(String jdbcCall) throws SQLException
        {
            ParsedCall parsedCall = JDBCParser.parseJDBCCall(jdbcCall);
            if (parsedCall == null) {
                throw SQLError.get(SQLError.ILLEGAL_STATEMENT);
            }
            return new VoltSQL(new String[]{parsedCall.sql}, parsedCall.parameterCount, TYPE_EXEC);
        }

        public static VoltSQL parseSQL(String queryIn) throws SQLException
        {
            if (queryIn == null || queryIn.length() == 0) {
                throw SQLError.get(SQLError.ILLEGAL_STATEMENT);
            }

            String query = queryIn.trim();
            if (query.length() == 0) {
                throw SQLError.get(SQLError.ILLEGAL_STATEMENT);
            }

            // Assume the type is an update.  We'll look for SELECT explicitly.
            // We need to know the type to validate the exec() statements.  Either it is a query or DDL/update.
            byte type = TYPE_UPDATE;
            if (SQLLexer.isSelect(query)) {
                type = TYPE_SELECT;
            }

            // Make sure there's a ';' terminator;
            if (!query.endsWith(";")) {
                query += ";";
            }
            // Count substitution parameters.
            int parameterCount = 0;
            if(query.indexOf("?") > -1)
            {
                String[] queryParts = (query + ";").split("\\?");
                parameterCount = queryParts.length-1;
            }

            return new VoltSQL(new String[] {query}, parameterCount, type);
        }
    }

    private ArrayList<VoltSQL> batch = null;
    protected boolean isClosed = false;
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    private int fetchSize = 0;
    private final int maxFieldSize = VoltType.MAX_VALUE_LENGTH;
    private int maxRows = VoltTable.MAX_SERIALIZED_TABLE_LENGTH/2; // Not exactly true, but best type of estimate we can give...
    protected JDBC4Connection sourceConnection;
    private boolean isPoolable = false;

    protected VoltTable[] tableResults = null;
    protected int tableResultIndex = -1;
    protected int lastUpdateCount = -1;
    protected Set<JDBC4ResultSet> openResults = new HashSet<JDBC4ResultSet>();
    protected JDBC4ResultSet result = null;

    public JDBC4Statement(JDBC4Connection connection)
    {
        sourceConnection = connection;
    }

    protected void checkClosed() throws SQLException
    {
        if (this.isClosed()) {
            throw SQLError.get(SQLError.CONNECTION_CLOSED);
        }
    }

    private void closeCurrentResult() throws SQLException
    {
        this.lastUpdateCount = -1;
        if (this.result != null) {
            this.result.close();
        }
        this.result = null;
    }

    private JDBC4ResultSet createTrimmedResultSet(VoltTable input) throws SQLException
    {
        VoltTable result = input;
        if (maxRows > 0 && input.getRowCount() > maxRows) {
            VoltTable trimmed = new VoltTable(input.getTableSchema());
            input.resetRowPosition();
            for (int i = 0; i < maxRows; i++) {
                input.advanceRow();
                trimmed.add(input.cloneRow());
            }
            result = trimmed;
        }
        return new JDBC4ResultSet(this, result);
    }

    private void setCurrentResult(VoltTable[] tables, int updateCount) throws SQLException
    {
        this.tableResults = tables;
        this.tableResultIndex = -1;
        this.lastUpdateCount = updateCount;
        if (this.result != null) {
            this.result.close();
        }
        if (this.tableResults == null || this.tableResults.length == 0) {
            return;
        }
        this.tableResultIndex = 0;
        this.result = createTrimmedResultSet(this.tableResults[this.tableResultIndex]);
    }

    private void closeAllOpenResults() throws SQLException
    {
        if (this.openResults != null)
        {
            for (Iterator<JDBC4ResultSet> iter = this.openResults.iterator(); iter.hasNext();)
            {
                JDBC4ResultSet element = iter.next();

                try
                {
                    element.close();
                }
                catch (SQLException x) {} // Will simply never happen, by design
            }
            this.openResults.clear();
        }
    }

    protected void addBatch(VoltSQL query) throws SQLException
    {
        if (batch == null) {
            batch = new ArrayList<VoltSQL>();
        }
        batch.add(query);
    }

    // Adds the given SQL command to the current list of commands for this Statement object.
    @Override
    public void addBatch(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        // Reject SELECT statements.
        if (query.isOfType(VoltSQL.TYPE_SELECT)) {
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, sql);
        }
        this.addBatch(query);
    }

    // Cancels this Statement object if both the DBMS and driver support aborting an SQL statement.
    @Override
    public void cancel() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Empties this Statement object's current list of SQL commands.
    @Override
    public void clearBatch() throws SQLException
    {
        checkClosed();
        batch = null;
    }

    // Clears all the warnings reported on this Statement object.
    @Override
    public void clearWarnings() throws SQLException
    {
        checkClosed();
    }

    // Releases this Statement object's database and JDBC resources immediately instead of waiting for this to happen when it is automatically closed.
    @Override
    public void close() throws SQLException
    {
        // close resultset too
        this.isClosed = true;
    }

    protected boolean execute(VoltSQL query) throws SQLException
    {
        checkClosed();
        if (query.isQueryOfType(VoltSQL.TYPE_SELECT,VoltSQL.TYPE_EXEC))
        {
            setCurrentResult(query.execute(this.sourceConnection.NativeConnection, this.m_timeout), -1);
            return true;
        }
        else
        {
            setCurrentResult(null, (int) query.execute(this.sourceConnection.NativeConnection, this.m_timeout)[0].fetchRow(0).getLong(0));
            return false;
        }
    }

    // Executes the given SQL statement, which may return multiple results.
    @Override
    public boolean execute(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        return this.execute(query);
    }

    // Executes the given SQL statement, which may return multiple results, and signals the driver that any auto-generated keys should be made available for retrieval.
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Executes the given SQL statement, which may return multiple results, and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Executes the given SQL statement, which may return multiple results, and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Submits a batch of commands to the database for execution and if all commands execute successfully, returns an array of update counts.
    @Override
    public int[] executeBatch() throws SQLException
    {
        checkClosed();
        closeCurrentResult();
        if (batch == null || batch.size() == 0) {
            return new int[0];
        }

        int[] updateCounts = new int[batch.size()];
        // keep a running total of update counts
        int runningUpdateCount = 0;

        for(int i = 0; i < batch.size(); i++)
        {
            try
            {
                setCurrentResult(null, (int) batch.get(i).execute(sourceConnection.NativeConnection,
                        this.m_timeout)[0].fetchRow(0).getLong(0));
                updateCounts[i] = this.lastUpdateCount;
                runningUpdateCount += this.lastUpdateCount;
            }
            catch(SQLException x)
            {
                updateCounts[i] = EXECUTE_FAILED;
                throw new BatchUpdateException(Arrays.copyOf(updateCounts, i+1), x);
            }
        }

        // replace the update count from the last statement with the update count
        // from the last batch.
        this.lastUpdateCount = runningUpdateCount;

        return updateCounts;
    }

    protected ResultSet executeQuery(VoltSQL query) throws SQLException
    {
        setCurrentResult(query.execute(this.sourceConnection.NativeConnection, this.m_timeout), -1);
        return this.result;
    }

    // Executes the given SQL statement, which returns a single ResultSet object.
    @Override
    public ResultSet executeQuery(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        if (!query.isOfType(VoltSQL.TYPE_SELECT)) {
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, sql);
        }
        return this.executeQuery(query);
    }

    protected int executeUpdate(VoltSQL query) throws SQLException
    {
        setCurrentResult(null, (int) query.execute(this.sourceConnection.NativeConnection, this.m_timeout)[0].fetchRow(0).getLong(0));
        return this.lastUpdateCount;
    }

    // Executes the given SQL statement, which may be an INSERT, UPDATE, or DELETE statement or an SQL statement that returns nothing, such as an SQL DDL statement.
    @Override
    public int executeUpdate(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        // Reject SELECT statements.
        if (query.isOfType(VoltSQL.TYPE_SELECT)) {
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, sql);
        }
        return this.executeUpdate(query);
    }

    // Executes the given SQL statement and signals the driver with the given flag about whether the auto-generated keys produced by this Statement object should be made available for retrieval.
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport(); // AutoGeneratedKeys not supported by provider
    }

    // Executes the given SQL statement and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport(); // AutoGeneratedKeys not supported by provider
    }

    // Executes the given SQL statement and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport(); // AutoGeneratedKeys not supported by provider
    }

    // Retrieves the Connection object that produced this Statement object.
    @Override
    public Connection getConnection() throws SQLException
    {
        checkClosed();
        return sourceConnection;
    }

    // Retrieves the direction for fetching rows from database tables that is the default for result sets generated from this Statement object.
    @Override
    public int getFetchDirection() throws SQLException
    {
        checkClosed();
        return this.fetchDirection;
    }

    // Retrieves the number of result set rows that is the default fetch size for ResultSet objects generated from this Statement object.
    @Override
    public int getFetchSize() throws SQLException
    {
        checkClosed();
        return this.fetchSize;
    }

    // Retrieves any auto-generated keys created as a result of executing this Statement object.
    @Override
    public ResultSet getGeneratedKeys() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of bytes that can be returned for character and binary column values in a ResultSet object produced by this Statement object.
    @Override
    public int getMaxFieldSize() throws SQLException
    {
        checkClosed();
        return this.maxFieldSize;
    }

    // Retrieves the maximum number of rows that a ResultSet object produced by this Statement object can contain.
    @Override
    public int getMaxRows() throws SQLException
    {
        checkClosed();
        return this.maxRows;
    }

    // Moves to this Statement object's next result, returns true if it is a ResultSet object, and implicitly closes any current ResultSet object(s) throws SQLException { throw SQLError.noSupport(); } obtained with the method getResultSet.
    @Override
    public boolean getMoreResults() throws SQLException
    {
        return this.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    // Moves to this Statement object's next result, deals with any current ResultSet object(s) throws SQLException { throw SQLError.noSupport(); } according to the instructions specified by the given flag, and returns true if the next result is a ResultSet object.
    @Override
    public boolean getMoreResults(int current) throws SQLException
    {
        checkClosed();
        switch(current)
        {
            case Statement.KEEP_CURRENT_RESULT:
                this.openResults.add(this.result);
                this.result = null;
                this.lastUpdateCount = -1;
                break;
            case Statement.CLOSE_CURRENT_RESULT:
                closeCurrentResult();
                break;
            case Statement.CLOSE_ALL_RESULTS:
                closeCurrentResult();
                closeAllOpenResults();
                break;
            default:
                throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, current);
        }
        if (current != Statement.CLOSE_ALL_RESULTS)
        {
            this.tableResultIndex++;
            if (this.tableResultIndex < this.tableResults.length)
            {
                VoltTable table = this.tableResults[this.tableResultIndex];
                if (VoltSQL.isUpdateResult(table)) {
                    this.lastUpdateCount = (int)table.fetchRow(0).getLong(0);
                } else
                {
                    this.result = createTrimmedResultSet(table);
                    return true;
                }
            }
        }
        return false;
    }

    // Retrieves the number of seconds the driver will wait for a Statement object to execute.
    @Override
    public int getQueryTimeout() throws SQLException
    {
        checkClosed();
        return this.m_timeout;
    }

    // Retrieves the current result as a ResultSet object.
    @Override
    public ResultSet getResultSet() throws SQLException
    {
        checkClosed();
        return this.result;
    }

    // Retrieves the result set concurrency for ResultSet objects generated by this Statement object.
    @Override
    public int getResultSetConcurrency() throws SQLException
    {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    // Retrieves the result set holdability for ResultSet objects generated by this Statement object.
    @Override
    public int getResultSetHoldability() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the result set type for ResultSet objects generated by this Statement object.
    @Override
    public int getResultSetType() throws SQLException
    {
        checkClosed();
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    // Retrieves the current result as an update count; if the result is a ResultSet object or there are no more results, -1 is returned.
    @Override
    public int getUpdateCount() throws SQLException
    {
        checkClosed();
        return this.lastUpdateCount;
    }

    // Retrieves the first warning reported by calls on this Statement object.
    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves whether this Statement object has been closed.
    @Override
    public boolean isClosed() throws SQLException
    {
        return this.isClosed;
    }

    // Returns a value indicating whether the Statement is poolable or not.
    @Override
    public boolean isPoolable() throws SQLException
    {
        checkClosed();
        return this.isPoolable;
    }

    // Sets the SQL cursor name to the given String, which will be used by subsequent Statement object execute methods.
    @Override
    public void setCursorName(String name) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets escape processing on or off.
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        checkClosed();
        // Do nothing / not applicable?
    }

    // Gives the driver a hint as to the direction in which rows will be processed in ResultSet objects created using this Statement object.
    @Override
    public void setFetchDirection(int direction) throws SQLException
    {
        checkClosed();
        if ((direction != ResultSet.FETCH_FORWARD) && (direction != ResultSet.FETCH_REVERSE) && (direction != ResultSet.FETCH_UNKNOWN)) {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, direction);
        }
        this.fetchDirection = direction;
    }

    // Gives the JDBC driver a hint as to the number of rows that should be fetched from the database when more rows are needed for ResultSet objects genrated by this Statement.
    @Override
    public void setFetchSize(int rows) throws SQLException
    {
        checkClosed();
        if (rows < 0) {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, rows);
        }
        this.fetchSize = rows;
    }

    // Sets the limit for the maximum number of bytes that can be returned for character and binary column values in a ResultSet object produced by this Statement object.
    @Override
    public void setMaxFieldSize(int max) throws SQLException
    {
        checkClosed();
        if (max < 0) {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, max);
        }
        throw SQLError.noSupport(); // Not supported by provider - no point trashing data we received from the server just to simulate the feature while not getting any gains!
    }

    // Sets the limit for the maximum number of rows that any ResultSet object generated by this Statement object can contain to the given number.
    @Override
    public void setMaxRows(int max) throws SQLException
    {
        checkClosed();
        if (max < 0) {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, max);
        }
        this.maxRows = max;
    }

    // Requests that a Statement be pooled or not pooled.
    @Override
    public void setPoolable(boolean poolable) throws SQLException
    {
        checkClosed();
        this.isPoolable = poolable;
    }

    // Sets the number of seconds the driver will wait for a Statement object to execute to the given number of seconds.
    @Override
    public void setQueryTimeout(int seconds) throws SQLException
    {
        checkClosed();
        if (seconds < 0) {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, seconds);
        }
        this.m_timeout = seconds;
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

    @Override
    public void closeOnCompletion() throws SQLException {
        throw SQLError.noSupport();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw SQLError.noSupport();
    }
}

