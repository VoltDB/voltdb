/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.jdbc.pool;

import org.hsqldb_voltpatches.jdbc.JDBCConnection;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
import org.hsqldb_voltpatches.jdbc.JDBCConnection;

/**
 * Don't do pooling. Only be a factory. Let ManagedPoolDataSource do the pooling.
 * @author Jakob Jenkov
 */
public class JDBCConnectionPoolDataSource implements ConnectionPoolDataSource {

    /**
     * @todo:  Break off code used here and in JDBCXADataSource into an
     *        abstract class, and have these classes extend the abstract
     *        class.
     */
    public static final String   driver = "org.hsqldb_voltpatches.jdbc.JDBCDriver";
    protected String             url                = null;
    protected ConnectionDefaults connectionDefaults = null;
    private int                  loginTimeout       = 0;
    private PrintWriter          logWriter          = null;
    protected Properties         connProperties = new java.util.Properties();

    public JDBCConnectionPoolDataSource() {}

    public JDBCConnectionPoolDataSource(String url, String user,
                                        String password,
                                        ConnectionDefaults connectionDefaults)
                                        throws SQLException {

        this.url                = url;
        this.connectionDefaults = connectionDefaults;

        setUser(user);
        setPassword(password);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return connProperties.getProperty("user");
    }

    public void setUser(String user) {
        connProperties.setProperty("user", user);
    }

    public String getPassword() {
        return connProperties.getProperty("password");
    }

    public void setPassword(String password) {
        connProperties.setProperty("password", password);
    }

    /**
     * @return seconds Time, in seconds.
     * @throws SQLException
     * @see javax.sql.ConnectionPoolDataSource#getLoginTimeout()
     */
    public int getLoginTimeout() throws SQLException {
        return this.loginTimeout;
    }

    /**
     * @param seconds Time, in seconds.
     * @throws SQLException
     * @see javax.sql.ConnectionPoolDataSource#setLoginTimeout(int)
     */
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    /**
     * @throws SQLException
     * @see javax.sql.ConnectionPoolDataSource#getLogWriter()
     */
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    /**
     * @throws SQLException
     * @see javax.sql.ConnectionPoolDataSource#setLogWriter(PrintWriter)
     */
    public void setLogWriter(PrintWriter out) throws SQLException {
        logWriter = out;
    }

    public PooledConnection getPooledConnection() throws SQLException {

        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException e) {
            throw new SQLException("Error opening connection: "
                                   + e.getMessage());
        } catch (IllegalAccessException e) {
            throw new SQLException("Error opening connection: "
                                   + e.getMessage());
        } catch (InstantiationException e) {
            throw new SQLException("Error opening connection: "
                                   + e.getMessage());
        }

        JDBCConnection connection =
            (JDBCConnection) DriverManager.getConnection(url, connProperties);

        return createPooledConnection(connection);
    }

    /**
     * Throws a SQLException if given user name or password are not same
     * as those configured for this object.
     *
     * @throws SQLException if given user name or password is wrong.
     */
    protected void validateSpecifiedUserAndPassword(String user,
            String password) throws SQLException {

        String configuredUser     = connProperties.getProperty("user");
        String configuredPassword = connProperties.getProperty("password");

        if (((user == null && configuredUser != null) || (user != null && configuredUser == null))
                || (user != null && !user.equals(configuredUser))
                || ((password == null && configuredPassword != null) || (password != null && configuredPassword == null))
                || (password != null
                    && !password.equals(configuredPassword))) {
            throw new SQLException("Given user name or password does not "
                                   + "match those configured for this object");
        }
    }

    /**
     * Performs a getPooledConnection() after validating the given username
     * and password.
     *
     * @param user String which must match the 'user' configured for this
     *             JDBCConnectionPoolDataSource.
     * @param password  String which must match the 'password' configured
     *                  for this JDBCConnectionPoolDataSource.
     *
     * @see #getPooledConnection()
     */
    public PooledConnection getPooledConnection(String user,
            String password) throws SQLException {

        validateSpecifiedUserAndPassword(user, password);

        return getPooledConnection();
    }

    public void close() {}

    protected void logInfo(String message) {

        if (logWriter != null) {
            logWriter.write("HSQLDB:Info: " + message + '\n');
            logWriter.flush();
        }
    }

    protected void logInfo(Throwable t) {

        if (logWriter != null) {
            t.printStackTrace(logWriter);
            logWriter.flush();
        }
    }

    protected void logInfo(String message, Throwable t) {

        if (logWriter != null) {
            logWriter.write("HSQLDB:Exception: " + message + '\n');
            logWriter.flush();
            logInfo(t);
        }
    }

    /**
     * Sets JDBC Connection Properties to be used when physical
     * connections are obtained for the pool.
     */
    public Object setConnectionProperty(String name, String value) {
        return connProperties.setProperty(name, value);
    }

    /**
     * Removes JDBC Connection Properties.
     *
     * @see #setConnectionProperty(String, String)
     */
    public Object removeConnectionProperty(String name) {
        return connProperties.remove(name);
    }

    /**
     * @see #setConnectionProperty(String, String)
     *
     * Beware that this property list will normally contain the password.
     * It is under consideration whether the list should be cloned and
     * returned with the password obscured or removed.
     */
    public Properties getConnectionProperties() {
        return connProperties;
    }

    /**
     * Portability wrapper.
     * Many app servers call the URL setting "database".
     */
    public void setDatabase(String url) {
        setUrl(url);
    }

    /**
     * Portability wrapper.
     * Many app servers call the URL setting "database".
     */
    public String getDatabase() {
        return getUrl();
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Retrieves the QueryObjectGenerator for the given JDBC driver.  If the
     * JDBC driver does not provide its own QueryObjectGenerator, NULL is
     * returned.
     * @return The QueryObjectGenerator for this JDBC Driver or NULL if the driver does not provide its own
     * implementation
     * @exception SQLException if a database access error occurs
     * @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JAVA6BETA
/*
    public QueryObjectGenerator getQueryObjectGenerator() throws SQLException {
        return null;
    }
*/

//#endif JAVA6BETA
    // ------------------------ internal implementation ------------------------
    private PooledConnection createPooledConnection(JDBCConnection connection)
    throws SQLException {

        LifeTimeConnectionWrapper connectionWrapper =
            new LifeTimeConnectionWrapper(connection, this.connectionDefaults);
        JDBCPooledConnection pooledConnection =
            new JDBCPooledConnection(connectionWrapper);

        connectionWrapper.setPooledConnection(pooledConnection);

        return pooledConnection;
    }

    /************************* Volt DB Extensions *************************/

    public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
        throw new java.sql.SQLFeatureNotSupportedException();
    }
    /**********************************************************************/
}
