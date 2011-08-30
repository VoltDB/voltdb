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
import org.voltdb.client.exampleutils.ClientConnection;
import java.math.BigDecimal;

public class JDBC4Statement implements java.sql.Statement
{
    static class VoltSQL
    {
        public static final byte TYPE_SELECT = 1;
        public static final byte TYPE_INSERT = 2;
        public static final byte TYPE_UPDATE = 3;
        public static final byte TYPE_DELETE = 4;
        public static final byte TYPE_EXEC = 5;

        private final String[] sql;
        private final int parameterCount;
        private final byte type;
        private final Object[] parameters;
        private VoltSQL(String[] sql, int parameterCount, byte type)
        {
            this.sql = sql;
            this.parameterCount = parameterCount;
            this.type = type;
            this.parameters = null;
        }

        private VoltSQL(String[] sql, int parameterCount, byte type, Object[] parameters)
        {
            this.sql = sql;
            this.parameterCount = parameterCount;
            this.type = type;
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
            for(int i=0;i<types.length;i++)
                if (this.type == types[i])
                    return true;
            return false;
        }

        protected VoltTable[] execute(ClientConnection connection) throws SQLException
        {
            try
            {
                if (this.type == TYPE_EXEC)
                    return connection.execute(this.sql[0], this.parameters).getResults();
                else
                    return connection.execute("@AdHoc", this.sql[0]).getResults();
            }
            catch(Exception x)
            {
                throw SQLError.get(x, SQLError.GENERAL_ERROR);
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
            if (params.length != this.parameterCount)
                throw SQLError.get(SQLError.ILLEGAL_ARGUMENT);

            if (this.type == TYPE_EXEC)
                return new VoltSQL( this.sql, this.parameterCount, this.type, params );
            else if (this.parameterCount == 0)
                return new VoltSQL( this.sql, 0, this.type );
            else
            {
                StringBuilder query = new StringBuilder();
                for(int i=0;i<params.length;i++)
                {
                    query.append(this.sql[i]);
                    if (params[i] == null)
                        query.append("null");
                    else if (params[i].getClass().equals(Byte.class))
                        query.append(((Byte)params[i]).byteValue());
                    else if (params[i].getClass().equals(Short.class))
                        query.append(((Short)params[i]).shortValue());
                    else if (params[i].getClass().equals(Integer.class))
                        query.append(((Integer)params[i]).intValue());
                    else if (params[i].getClass().equals(Long.class))
                        query.append(((Long)params[i]).longValue());
                    else if (params[i].getClass().equals(Double.class))
                        query.append(((Double)params[i]).doubleValue());
                    else if (params[i] == VoltType.NULL_TIMESTAMP)
                        query.append("null");
                    else if (params[i].getClass().equals(Timestamp.class))
                    {
                        Timestamp timestamp = (Timestamp)params[i];
                        if (timestamp.toString().length() < 26)
                            timestamp.setNanos(timestamp.getNanos()+1);
                        query.append("'" + timestamp.toString().substring(0,26) + "'");
                    }
                    else if (params[i] == VoltType.NULL_DECIMAL)
                        query.append("null");
                    else if (params[i].getClass().equals(BigDecimal.class))
                        query.append("'" + ((BigDecimal)params[i]).toPlainString() + "'");
                    else if (params[i] == VoltType.NULL_STRING_OR_VARBINARY)
                        query.append("null");
                    else if (params[i].getClass().equals(String.class))
                    {
                        if (((String)params[i]).indexOf("\r") > -1 || ((String)params[i]).indexOf("\n") > -1)
                            throw SQLError.get(SQLError.QUERY_PARSING_ERROR); // Sorry pal: no line feeds for ad-hoc queries
                        query.append("'" + SingleQuote.matcher((String)params[i]).replaceAll("''") + "'");
                    }
                    else
                        throw SQLError.get(SQLError.ILLEGAL_ARGUMENT); // Unknown parameter type
                }
                if (this.sql.length > this.parameterCount)
                    query.append(this.sql[this.sql.length-1]);
                return new VoltSQL( new String[] { query.toString() }, 0, this.type);
            }
        }

        // SQL Parsing
        private static final Pattern ExtractParameterizedCall = Pattern.compile("^\\s*\\{\\s*call\\s+([^\\s()]+)\\s*\\(([?,\\s]+)\\)\\s*\\}\\s*$", Pattern.CASE_INSENSITIVE);
        private static final Pattern ExtractNoParameterCall = Pattern.compile("^\\s*\\{\\s*call\\s+([^\\s()]+)\\s*\\}\\s*$", Pattern.CASE_INSENSITIVE);
        private static final Pattern CleanCallParameters = Pattern.compile("[\\s,]+");
        public static VoltSQL parseCall(String jdbcCall) throws SQLException
        {
            Matcher m = ExtractParameterizedCall.matcher(jdbcCall);
            if (m.matches())
                return new VoltSQL(new String[] { m.group(1) }, CleanCallParameters.matcher(m.group(2)).replaceAll("").length(), TYPE_EXEC);
            else
            {
                m = ExtractNoParameterCall.matcher(jdbcCall);
                if (m.matches())
                    return new VoltSQL(new String[] { m.group(1) }, 0, TYPE_EXEC);
            }
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT);
        }

        private static final Pattern SingleQuote = Pattern.compile("'", Pattern.MULTILINE);
        private static final Pattern EscapedSingleQuote = Pattern.compile("''", Pattern.MULTILINE);
        private static final Pattern SingleLineComments = Pattern.compile("^\\s*(\\/\\/|--).*$", Pattern.MULTILINE);
        private static final Pattern Extract = Pattern.compile("'[^']*'", Pattern.MULTILINE);
        private static final Pattern AutoSplit = Pattern.compile("\\s(select|insert|update|delete)\\s", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
        private static final Pattern SpaceCleaner = Pattern.compile("[\\s]+", Pattern.MULTILINE);
        private static final Pattern IsSelect = Pattern.compile("^select\\s.+", Pattern.CASE_INSENSITIVE);
        private static final Pattern IsInsert = Pattern.compile("^insert\\s.+", Pattern.CASE_INSENSITIVE);
        private static final Pattern IsUpdate = Pattern.compile("^update\\s.+", Pattern.CASE_INSENSITIVE);
        private static final Pattern IsDelete = Pattern.compile("^delete\\s.+", Pattern.CASE_INSENSITIVE);
        public static VoltSQL parseSQL(String query) throws SQLException
        {
            if (query == null || query.length() == 0)
                throw SQLError.get(SQLError.ILLEGAL_STATEMENT);

            query = SingleLineComments.matcher(query).replaceAll("");
            query = EscapedSingleQuote.matcher(query).replaceAll("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)");
            Matcher stringFragmentMatcher = Extract.matcher(query);
            ArrayList<String> stringFragments = new ArrayList<String>();
            int i = 0;
            while(stringFragmentMatcher.find())
            {
                stringFragments.add(stringFragmentMatcher.group());
                query = stringFragmentMatcher.replaceFirst("#(SQL_PARSER_STRING_FRAGMENT#" + i + ")");
                stringFragmentMatcher = Extract.matcher(query);
                i++;
            }
            query = AutoSplit.matcher(query.trim()).replaceAll(";$1 ");
            String[] sqlFragments = query.split("\\s*;+\\s*");
            if (sqlFragments.length > 1)
                throw SQLError.get(SQLError.QUERY_PARSING_ERROR);
            query = SpaceCleaner.matcher(sqlFragments[0]).replaceAll(" ").trim();

            if (query.length() == 0)
                throw SQLError.get(SQLError.ILLEGAL_STATEMENT);

            byte type = 0;
            if (IsSelect.matcher(query).matches())
                type = TYPE_SELECT;
            else if (IsInsert.matcher(query).matches())
                type = TYPE_INSERT;
            else if (IsUpdate.matcher(query).matches())
                type = TYPE_UPDATE;
            else if (IsDelete.matcher(query).matches())
                type = TYPE_DELETE;

            if (type == 0)
                throw SQLError.get(SQLError.INVALID_QUERY_TYPE);

            if (query.indexOf("'") > -1)
                throw SQLError.get(SQLError.UNTERMINATED_STRING);

            String[] queryParts = null;
            int parameterCount = 0;
            if(query.indexOf("?") > -1)
            {
                queryParts = (query + ";").split("\\?");
                parameterCount = queryParts.length-1;
            }
            else
                queryParts = new String[] { query };

            if(stringFragments.size() > 0)
            {
                for(int k = 0;k<stringFragments.size();k++)
                    for(int l = 0;l<queryParts.length;l++)
                        queryParts[l] = queryParts[l].replace("#(SQL_PARSER_STRING_FRAGMENT#" + k + ")", stringFragments.get(k));
            }
            for(int l = 0;l<queryParts.length;l++)
                queryParts[l] = queryParts[l].replace("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)", "''");

            // Welcome to volt... cannot accept \r or \n in inline SQL...
            for(int l = 0;l<queryParts.length;l++)
                if (queryParts[l].indexOf("\r") > -1 || queryParts[l].indexOf("\n") > -1)
                    throw SQLError.get(SQLError.QUERY_PARSING_ERROR);

            return new VoltSQL(queryParts, parameterCount, type);
        }
    }

    private ArrayList<VoltSQL> batch = null;
    private ArrayList<String> warnings = null;
    protected boolean isClosed = false;
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    private int fetchSize = 0;
    private final int maxFieldSize = VoltType.MAX_VALUE_LENGTH;
    private final int maxRows = VoltTable.MAX_SERIALIZED_TABLE_LENGTH/2; // Not exactly true, but best type of estimate we can give...
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
        if (this.isClosed())
            throw SQLError.get(SQLError.CONNECTION_CLOSED);
    }

    private void closeCurrentResult() throws SQLException
    {
        this.lastUpdateCount = -1;
        if (this.result != null)
            this.result.close();
        this.result = null;
    }
    private void setCurrentResult(VoltTable[] tables, int updateCount) throws SQLException
    {
        this.tableResults = tables;
        this.tableResultIndex = -1;
        this.lastUpdateCount = updateCount;
        if (this.result != null)
            this.result.close();
        if (this.tableResults == null || this.tableResults.length == 0)
            return;
        this.tableResultIndex = 0;
        this.result = new JDBC4ResultSet(this, this.tableResults[this.tableResultIndex]);
    }

    private void closeAllOpenResults() throws SQLException
    {
        if (this.openResults != null)
        {
            for (Iterator iter = this.openResults.iterator(); iter.hasNext();)
            {
                JDBC4ResultSet element = (JDBC4ResultSet)iter.next();

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
        if (batch == null)
            batch = new ArrayList<VoltSQL>();
        batch.add(query);
    }

    // Adds the given SQL command to the current list of commmands for this Statement object.
    public void addBatch(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        if (query.hasParameters() || !query.isOfType(VoltSQL.TYPE_INSERT,VoltSQL.TYPE_UPDATE,VoltSQL.TYPE_DELETE))
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, sql);
        this.addBatch(query);
    }

    // Cancels this Statement object if both the DBMS and driver support aborting an SQL statement.
    public void cancel() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Empties this Statement object's current list of SQL commands.
    public void clearBatch() throws SQLException
    {
        checkClosed();
        batch = null;
    }

    // Clears all the warnings reported on this Statement object.
    public void clearWarnings() throws SQLException
    {
        checkClosed();
        warnings = null;
    }

    // Releases this Statement object's database and JDBC resources immediately instead of waiting for this to happen when it is automatically closed.
    public void close() throws SQLException
    {
        // close resultset too
        this.isClosed = true;
    }

    protected boolean execute(VoltSQL query) throws SQLException
    {
        checkClosed();
        try
        {
            if (query.isOfType(VoltSQL.TYPE_SELECT,VoltSQL.TYPE_EXEC))
            {
                setCurrentResult(query.execute(sourceConnection.NativeConnection), -1);
                return true;
            }
            else
            {
                setCurrentResult(null, (int)query.execute(sourceConnection.NativeConnection)[0].fetchRow(0).getLong(0));
                return false;
            }
        }
        catch(Exception x)
        {
            throw SQLError.get(x);
        }
    }

    // Executes the given SQL statement, which may return multiple results.
    public boolean execute(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        if (query.hasParameters() || !query.isOfType(VoltSQL.TYPE_SELECT,VoltSQL.TYPE_INSERT,VoltSQL.TYPE_UPDATE,VoltSQL.TYPE_DELETE))
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, sql);

        return this.execute(query);
    }

    // Executes the given SQL statement, which may return multiple results, and signals the driver that any auto-generated keys should be made available for retrieval.
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Executes the given SQL statement, which may return multiple results, and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Executes the given SQL statement, which may return multiple results, and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Submits a batch of commands to the database for execution and if all commands execute successfully, returns an array of update counts.
    public int[] executeBatch() throws SQLException
    {
        checkClosed();
        closeCurrentResult();
        if (batch == null || batch.size() == 0)
            return new int[0];
        int[] updateCounts = new int[batch.size()];
        for(int i=0;i<batch.size();i++)
        {
            try
            {
                setCurrentResult(null, (int)batch.get(i).execute(sourceConnection.NativeConnection)[0].fetchRow(0).getLong(0));
                updateCounts[i] = this.lastUpdateCount;
            }
            catch(Exception x)
            {
                updateCounts[i] = EXECUTE_FAILED;
                throw new BatchUpdateException(Arrays.copyOf(updateCounts, i+1), x);
            }
        }
        return updateCounts;
    }

    protected ResultSet executeQuery(VoltSQL query) throws SQLException
    {
        try
        {
            setCurrentResult(query.execute(sourceConnection.NativeConnection), -1);
            return this.result;
        }
        catch(Exception x)
        {
            throw SQLError.get(x);
        }
    }

    // Executes the given SQL statement, which returns a single ResultSet object.
    public ResultSet executeQuery(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        if (query.hasParameters() || !query.isOfType(VoltSQL.TYPE_SELECT))
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, sql);
        return this.executeQuery(query);
    }

    protected int executeUpdate(VoltSQL query) throws SQLException
    {
        try
        {
            setCurrentResult(null, (int)query.execute(sourceConnection.NativeConnection)[0].fetchRow(0).getLong(0));
            return this.lastUpdateCount;
        }
        catch(Exception x)
        {
            throw SQLError.get(x);
        }
    }

    // Executes the given SQL statement, which may be an INSERT, UPDATE, or DELETE statement or an SQL statement that returns nothing, such as an SQL DDL statement.
    public int executeUpdate(String sql) throws SQLException
    {
        checkClosed();
        VoltSQL query = VoltSQL.parseSQL(sql);
        if (query.hasParameters() || !query.isOfType(VoltSQL.TYPE_INSERT,VoltSQL.TYPE_UPDATE,VoltSQL.TYPE_DELETE))
            throw SQLError.get(SQLError.ILLEGAL_STATEMENT, sql);
        return this.executeUpdate(query);
    }

    // Executes the given SQL statement and signals the driver with the given flag about whether the auto-generated keys produced by this Statement object should be made available for retrieval.
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport(); // AutoGeneratedKeys not supported by provider
    }

    // Executes the given SQL statement and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport(); // AutoGeneratedKeys not supported by provider
    }

    // Executes the given SQL statement and signals the driver that the auto-generated keys indicated in the given array should be made available for retrieval.
    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport(); // AutoGeneratedKeys not supported by provider
    }

    // Retrieves the Connection object that produced this Statement object.
    public Connection getConnection() throws SQLException
    {
        checkClosed();
        return sourceConnection;
    }

    // Retrieves the direction for fetching rows from database tables that is the default for result sets generated from this Statement object.
    public int getFetchDirection() throws SQLException
    {
        checkClosed();
        return this.fetchDirection;
    }

    // Retrieves the number of result set rows that is the default fetch size for ResultSet objects generated from this Statement object.
    public int getFetchSize() throws SQLException
    {
        checkClosed();
        return this.fetchSize;
    }

    // Retrieves any auto-generated keys created as a result of executing this Statement object.
    public ResultSet getGeneratedKeys() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the maximum number of bytes that can be returned for character and binary column values in a ResultSet object produced by this Statement object.
    public int getMaxFieldSize() throws SQLException
    {
        checkClosed();
        return this.maxFieldSize;
    }

    // Retrieves the maximum number of rows that a ResultSet object produced by this Statement object can contain.
    public int getMaxRows() throws SQLException
    {
        checkClosed();
        return this.maxRows;
    }

    // Moves to this Statement object's next result, returns true if it is a ResultSet object, and implicitly closes any current ResultSet object(s) throws SQLException { throw SQLError.noSupport(); } obtained with the method getResultSet.
    public boolean getMoreResults() throws SQLException
    {
        return this.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    // Moves to this Statement object's next result, deals with any current ResultSet object(s) throws SQLException { throw SQLError.noSupport(); } according to the instructions specified by the given flag, and returns true if the next result is a ResultSet object.
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
                if (VoltSQL.isUpdateResult(table))
                    this.lastUpdateCount = (int)table.fetchRow(0).getLong(0);
                else
                {
                    this.result = new JDBC4ResultSet(this, table);
                    return true;
                }
            }
        }
        return false;
    }

    // Retrieves the number of seconds the driver will wait for a Statement object to execute.
    public int getQueryTimeout() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();  // Fake client-side timeout can be done, however true timeouts and the ability to cancel queries is not possible - do not mislead client by implementing this!
    }

    // Retrieves the current result as a ResultSet object.
    public ResultSet getResultSet() throws SQLException
    {
        checkClosed();
        return this.result;
    }

    // Retrieves the result set concurrency for ResultSet objects generated by this Statement object.
    public int getResultSetConcurrency() throws SQLException
    {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    // Retrieves the result set holdability for ResultSet objects generated by this Statement object.
    public int getResultSetHoldability() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the result set type for ResultSet objects generated by this Statement object.
    public int getResultSetType() throws SQLException
    {
        checkClosed();
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    // Retrieves the current result as an update count; if the result is a ResultSet object or there are no more results, -1 is returned.
    public int getUpdateCount() throws SQLException
    {
        checkClosed();
        return this.lastUpdateCount;
    }

    // Retrieves the first warning reported by calls on this Statement object.
    public SQLWarning getWarnings() throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves whether this Statement object has been closed.
    public boolean isClosed() throws SQLException
    {
        return this.isClosed;
    }

    // Returns a value indicating whether the Statement is poolable or not.
    public boolean isPoolable() throws SQLException
    {
        checkClosed();
        return this.isPoolable;
    }

    // Sets the SQL cursor name to the given String, which will be used by subsequent Statement object execute methods.
    public void setCursorName(String name) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Sets escape processing on or off.
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        checkClosed();
        // Do nothing / not applicable?
    }

    // Gives the driver a hint as to the direction in which rows will be processed in ResultSet objects created using this Statement object.
    public void setFetchDirection(int direction) throws SQLException
    {
        checkClosed();
        if ((direction != ResultSet.FETCH_FORWARD) && (direction != ResultSet.FETCH_REVERSE) && (direction != ResultSet.FETCH_UNKNOWN))
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, direction);
        this.fetchDirection = direction;
    }

    // Gives the JDBC driver a hint as to the number of rows that should be fetched from the database when more rows are needed for ResultSet objects genrated by this Statement.
    public void setFetchSize(int rows) throws SQLException
    {
        checkClosed();
        if (rows < 0)
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, rows);
        this.fetchSize = rows;
    }

    // Sets the limit for the maximum number of bytes that can be returned for character and binary column values in a ResultSet object produced by this Statement object.
    public void setMaxFieldSize(int max) throws SQLException
    {
        checkClosed();
        if (max < 0)
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, max);
        throw SQLError.noSupport(); // Not supported by provider - no point trashing data we received from the server just to simulate the feature while not getting any gains!
    }

    // Sets the limit for the maximum number of rows that any ResultSet object generated by this Statement object can contain to the given number.
    public void setMaxRows(int max) throws SQLException
    {
        checkClosed();
        if (max < 0)
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, max);
        throw SQLError.noSupport(); // Not supported by provider - could hack around with limit/top injection, but it gets ugly of the submitted statement also contains this... What we'd need for this feature is cursor/streamed data retrieval from the provider.
    }

    // Requests that a Statement be pooled or not pooled.
    public void setPoolable(boolean poolable) throws SQLException
    {
        checkClosed();
        this.isPoolable = poolable;
    }

    // Sets the number of seconds the driver will wait for a Statement object to execute to the given number of seconds.
    public void setQueryTimeout(int seconds) throws SQLException
    {
        checkClosed();
        if (seconds < 0)
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, seconds);
        throw SQLError.noSupport();  // Fake client-side timeout can be done, however true timeouts and the ability to cancel queries is not possible - do not mislead client by implementing this!
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
}

