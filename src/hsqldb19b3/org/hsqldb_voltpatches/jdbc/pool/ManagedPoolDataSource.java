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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;

import org.hsqldb_voltpatches.jdbc.Util;

/* $Id: ManagedPoolDataSource.java 2944 2009-03-21 22:53:43Z fredt $ */

// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
// boucherb@users 20060523 - patch 1.9.0 full synch up to Mustang Build 84
// Revision 1.23  2006/07/12 12:45:54  boucherb
// patch 1.9.0
// - full synch up to Mustang b90

/** @todo configurable: rollback() exceptions passed on to the client? */

/**
 * Connection pool manager.
 *
 * @author Jakob Jenkov
 */
public class ManagedPoolDataSource implements javax.sql.DataSource,
        ConnectionEventListener {

    /**
     * The default connection count in the pool.
     * Since HSQLDB has database level locking (only 1 thread in the database at a time) there is
     * no reason to default to a large number of connections in the connection pool. All
     * other threads will be waiting for the other connections to finish anyways. A large number
     * of connections will only be a waste of resources in the HSQLDB server.
     *
     * I'm raising the default max size because
     * (a) HSQLDB will have a multi-threaded core shortly.
     * (b) The number of connections is not determined only by performance
     * considerations, but also by transaction isolation requirements.
     * Web apps often require a separate conn. app per HTTP Session or
     * Request.  It is, indeed, the standard paradigm for J2EE web apps.
     *                                                     - blaine
     */
    private static final int             DEFAULT_MAX_POOL_SIZE    = 8;
    private boolean                      isPoolClosed             = false;
    private int                          sessionTimeout           = 0;
    private JDBCConnectionPoolDataSource connectionPoolDataSource = null;
    private Set                          connectionsInUse = new HashSet();
    private List                         connectionsInactive = new ArrayList();
    private Map sessionConnectionWrappers = new HashMap();
    private int                          maxPoolSize = DEFAULT_MAX_POOL_SIZE;
    private ConnectionDefaults           connectionDefaults       = null;
    private boolean                      initialized              = false;
    private int                          initialSize              = 0;

    // The doReset* settings say whether to automatically apply the
    // relevant setting to physical Connections when they are (re)-established.
    boolean doResetAutoCommit           = false;
    boolean doResetReadOnly             = false;
    boolean doResetTransactionIsolation = false;
    boolean doResetCatalog              = false;

    // The default values below will only be enforced if the user "checks"
    // the values by using a getter().
    // If user uses neither a getter nor a setter, no resetting or
    // enforcement of these settings will be done.
    boolean isAutoCommit         = true;
    boolean isReadOnly           = false;
    int     transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
    String  catalog              = null;

    /** Optional query to validate new Connections before returning them. */
    private String validationQuery = null;

    /**
     * Base no-arg constructor.
     * Useful for JavaBean repositories and IOC frameworks.
     */
    public ManagedPoolDataSource() {
        this.connectionPoolDataSource = new JDBCConnectionPoolDataSource();
    }

    /**
     * Base constructor that handles all parameters.
     */
    public ManagedPoolDataSource(
            String url, String user, String password, int maxPoolSize,
            ConnectionDefaults connectionDefaults) throws SQLException {

        this.connectionPoolDataSource = new JDBCConnectionPoolDataSource(url,
                user, password, connectionDefaults);
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Convience constructor wrapper.
     *
     * @see #ManagedPoolDataSource()
     */
    public ManagedPoolDataSource(String url, String user,
                                 String password) throws SQLException {
        this(url, user, password, DEFAULT_MAX_POOL_SIZE, null);
    }

    /**
     * Convience constructor wrapper.
     *
     * @see #ManagedPoolDataSource()
     */
    public ManagedPoolDataSource(
            String url, String user, String password,
            ConnectionDefaults connectionDefaults) throws SQLException {
        this(url, user, password, DEFAULT_MAX_POOL_SIZE, connectionDefaults);
    }

    /**
     * Convience constructor wrapper.
     *
     * @see #ManagedPoolDataSource()
     */
    public ManagedPoolDataSource(String url, String user, String password,
                                 int maxPoolSize) throws SQLException {
        this(url, user, password, maxPoolSize, null);
    }

    public ConnectionDefaults getConnectionDefaults() {
        return connectionDefaults;
    }

    public synchronized String getUrl() {
        return this.connectionPoolDataSource.getUrl();
    }

    public synchronized void setUrl(String url) {
        this.connectionPoolDataSource.setUrl(url);
    }

    public synchronized String getUser() {
        return this.connectionPoolDataSource.getUser();
    }

    public synchronized void setUser(String user) {
        this.connectionPoolDataSource.setUser(user);
    }

    public synchronized String getPassword() {
        return this.connectionPoolDataSource.getPassword();
    }

    public synchronized void setPassword(String password) {
        this.connectionPoolDataSource.setPassword(password);
    }

    public synchronized int getSessionTimeout() {
        return sessionTimeout;
    }

    public synchronized void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public synchronized int getMaxPoolSize() {
        return maxPoolSize;
    }

    public synchronized void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * @return seconds Time, in seconds.
     * @see JDBCConnectionPoolDataSource#getLoginTimeout()
     */
    public synchronized int getLoginTimeout() throws SQLException {
        return connectionPoolDataSource.getLoginTimeout();
    }

    /**
     * @param seconds Time, in seconds.
     * @see JDBCConnectionPoolDataSource#setLoginTimeout(int)
     */
    public synchronized void setLoginTimeout(int seconds) throws SQLException {
        connectionPoolDataSource.setLoginTimeout(seconds);
    }

    /**
     * @see JDBCConnectionPoolDataSource#getLogWriter()
     */
    public synchronized PrintWriter getLogWriter() throws SQLException {
        return connectionPoolDataSource.getLogWriter();
    }

    /**
     * @see JDBCConnectionPoolDataSource#setLogWriter(PrintWriter)
     */
    public synchronized void setLogWriter(
            PrintWriter out) throws SQLException {
        connectionPoolDataSource.setLogWriter(out);
    }

    /**
     * Performs a getConnection() after validating the given username
     * and password.
     *
     * @param user String which must match the 'user' configured for this
     *             ManagedPoolDataSource.
     * @param password  String which must match the 'password' configured
     *                  for this ManagedPoolDataSource.
     *
     * @see #getConnection()
     */
    public Connection getConnection(String user,
                                    String password) throws SQLException {

        String managedPassword = getPassword();
        String managedUser     = getUsername();

        if (((user == null && managedUser != null)
                || (user != null && managedUser == null)) || (user != null
                   && !user.equals(managedUser)) || ((password == null
                       && managedPassword != null) || (password != null
                           && managedPassword == null)) || (password != null
                               && !password.equals(managedPassword))) {
            throw new SQLException(
                "Connection pool manager user/password validation failed");
        }

        return getConnection();
    }

    /**
     * This method will return a valid connection as fast as possible. Here is the sequence of events:
     *
     * <ol>
     *  <li>First it will try to take a ready connection from the pool.</li>
     *  <li>If the pool doesn't have any ready connections this method will check
     *      if the pool has space for new connections. If yes, a new connection is created and inserted
     *      into the pool.</li>
     * <li>If the pool is already at maximum size this method will check for abandoned
     *     connections among the connections given out earlier.
     *     If any of the connections are abandoned it will be reclaimed, reset and
     *     given out.</li>
     * <li>If there are no abandoned connections this method waits until a connection becomes
     *     available in the pool, or until the login time out time has passed (if login timeout set).</li>
     * </ol>
     *
     * This sequence means that the pool will not grow till max size as long as there are available connections
     * in the pool. Only when no connections are available will it spend time creating new connections.
     * In addition it will not spend
     * time reclaiming abandoned connections until it's absolutely necessary. Only when no new connections
     * can be created. Ultimately it only blocks if there really are no available connections,
     * none can be created and none can be reclaimed. Since the
     * pool is passive this is the sequence of events that should give the highest performance possible
     * for giving out connections from the pool.
     *
     * <br/><br/>
     * Perhaps it is faster to reclaim a connection if possible than to create a new one. If so, it would be faster
     * to reclaim existing connections before creating new ones. It may also preserve resources better.
     * However, assuming that there will not often be abandoned connections, that programs most often
     * remember to close them, on the average the reclaim check is only going to be "wasted overhead".
     * It will only rarely result in a usable connection. In that perspective it should be faster
     * to only do the reclaim check when no connections can be created.
     *
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {

        PooledConnection pooledConnection = null;

        synchronized (this) {
            if (!initialized) {
                if (initialSize > maxPoolSize) {
                    throw new SQLException("Initial size of " + initialSize
                            + " exceeds max. pool size of " + maxPoolSize);
                }
                logInfo("Pre-initializing " + initialSize
                        + " physical connections");

                for (int i = 0; i < initialSize; i++) {
                    connectionsInactive.add(createNewConnection());
                }
                initialized = true;
            }

            long loginTimeoutExpiration = calculateLoginTimeoutExpiration();

            //each waiting thread will spin around in this loop until either
            //
            // a) a connection becomes available
            // b) the login timeout passes. Results in SQLException.
            // c) the thread is interrupted while waiting for an available connection
            // d) the connection pool is closed. Results in SQLException.
            //
            // the reason the threads spin in a loop is that they should always check all options
            // (available + space for new  + abandoned) before waiting. In addition the threads will
            // repeat the close check before spinning a second time. That way all threads waiting
            // for a connection when the connection is closed will get an SQLException.
            while (pooledConnection == null) {
                if (this.isPoolClosed) {
                    throw new SQLException(
                        "The pool is closed. You cannot get anymore connections from it.");
                }

                // 1) Check if any connections available in the pool.
                pooledConnection = dequeueFirstIfAny();

                if (pooledConnection != null) {
                    return wrapConnectionAndMarkAsInUse(pooledConnection);
                }

                // 2) Check if the pool has space for new connections and create one if it does.
                if (poolHasSpaceForNewConnections()) {
                    pooledConnection = createNewConnection();

                    return wrapConnectionAndMarkAsInUse(pooledConnection);
                }

                // 3) Check if connection pool has session timeout. If yes,
                // check if any connections has timed out. Return one if there are any.
                if (this.sessionTimeout > 0) {
                    reclaimAbandonedConnections();

                    //check if any connections were closed during the time out check.
                    pooledConnection = dequeueFirstIfAny();

                    if (pooledConnection != null) {
                        return wrapConnectionAndMarkAsInUse(pooledConnection);
                    }
                }

                // 4) wait until a connection becomes available or the login timeout passes (if set).
                doWait(loginTimeoutExpiration);
            }

            return wrapConnectionAndMarkAsInUse(pooledConnection);
        }
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     *  Creates a concrete implementation of a Query interface using the JDBC drivers <code>QueryObjectGenerator</code>
     *  implementation.
     *  If the JDBC driver does not provide its own <code>QueryObjectGenerator</code>, the <code>QueryObjectGenerator</code>
     *  provided with J2SE will be used.
     * <p>
     *  @param ifc The Query interface that will be created
     *  @return A concrete implementation of a Query interface
     *  @exception SQLException if a database access error occurs.
     *  @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JAVA6BETA
/*
   public <T extends BaseQuery> T createQueryObject(Class<T> ifc) throws SQLException {
        return QueryObjectFactory.createDefaultQueryObject(ifc, this);
   }
*/

//#endif JAVA6BETA

    /**
     * Creates a concrete implementation of a Query interface using the JDBC drivers <code>QueryObjectGenerator</code>
     * implementation.
     * <p>
     * If the JDBC driver does not provide its own <code>QueryObjectGenerator</code>, the <code>QueryObjectGenerator</code>
     * provided with Java SE will be used.
     * <p>
     * This method is primarly for developers of Wrappers to JDBC implementations.
     * Application developers should use <code>createQueryObject(Class&LT;T&GT; ifc).
     * <p>
     * @param ifc The Query interface that will be created
     * @param ds The <code>DataSource</code> that will be used when invoking methods that access
     * the data source. The QueryObjectGenerator implementation will use
     * this <code>DataSource</code> without any unwrapping or modications
     * to create connections to the data source.
     *
     * @return An concrete implementation of a Query interface
     * @exception SQLException if a database access error occurs.
     * @since 1.6
     */
//#ifdef JAVA6BETA
/*
    public <T extends BaseQuery> T createQueryObject(Class<T> ifc, javax.sql.DataSource ds) throws SQLException {
        return QueryObjectFactory.createQueryObject(ifc, ds);
    }
*/

//#endif JAVA6BETA

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

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     * The result may be either the object found to implement the interface or a proxy for that object.
     * If the receiver implements the interface then that is the object. If the receiver is a wrapper
     * and the wrapped object implements the interface then that is the object. Otherwise the object is
     *  the result of calling <code>unwrap</code> recursively on the wrapped object. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JAVA6
    public <T>T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {

        if (isWrapperFor(iface)) {
            return (T) this;
        }

        throw Util.invalidArgument("iface: " + iface);
    }

//#endif JAVA6

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JAVA6
    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//#endif JAVA6
    // ------------------------ internal implementation ------------------------
    private void doWait(long loginTimeoutExpiration) throws SQLException {

        try {
            if (loginTimeoutExpiration > 0) {
                long timeToWait = loginTimeoutExpiration
                                  - System.currentTimeMillis();

                if (timeToWait > 0) {
                    this.wait(timeToWait);
                } else {
                    throw new SQLException(
                        "No connections available within the given login timeout: "
                        + getLoginTimeout());
                }
            } else {
                this.wait();
            }
        } catch (InterruptedException e) {
            throw new SQLException(
                "Thread was interrupted while waiting for available connection");
        }
    }

    private PooledConnection createNewConnection() throws SQLException {

        PooledConnection pooledConnection;

        // I have changed "size() + 1" to "size()".  I don't know why
        // we would want to report 1 more than the actual pool size,
        // so I am assuming that this is a coding error.  (The size
        // method does return the actual size of an array).  -blaine
        logInfo("Connection created since no connections available and "
                + "pool has space for more connections. Pool size: " + size());

        pooledConnection = this.connectionPoolDataSource.getPooledConnection();

        pooledConnection.addConnectionEventListener(this);

        return pooledConnection;
    }

    private void reclaimAbandonedConnections() {

        long     now                  = System.currentTimeMillis();
        long     sessionTimeoutMillis = ((long) sessionTimeout) * 1000L;
        Iterator iterator             = this.connectionsInUse.iterator();
        List     abandonedConnections = new ArrayList();

        while (iterator.hasNext()) {
            PooledConnection connectionInUse =
                (PooledConnection) iterator.next();
            SessionConnectionWrapper sessionWrapper =
                (SessionConnectionWrapper) this.sessionConnectionWrappers.get(
                    connectionInUse);

            if (isSessionTimedOut(now, sessionWrapper, sessionTimeoutMillis)) {
                abandonedConnections.add(sessionWrapper);
            }
        }

        //The timed out sessions are added to a list before being closed to avoid
        //ConcurrentModificationException. When the session wrapper is closed the underlying
        // connection will eventually call connectionClosed() on this class. connectionClosed()
        // will in turn try to remove the pooled connection from the connectionsInUse set. So,
        // if the sessionWrapper is closed while iterating the connectionsInUse it would result
        // in a ConcurrentModificationException.
        iterator = abandonedConnections.iterator();

        while (iterator.hasNext()) {
            SessionConnectionWrapper sessionWrapper =
                (SessionConnectionWrapper) iterator.next();

            closeSessionWrapper(
                sessionWrapper,
                "Error closing abandoned session connection wrapper.");
        }

        //if there are more than one abandoned connection then other waiting threads might
        //now have a chance to get a connections. therefore we notify all waiting threads.
        if (abandonedConnections.size() > 1) {
            abandonedConnections.clear();
            this.notifyAll();
        }
    }

    /**
     * Closes the session wrapper. The call to close() will in turn result in a call to this pools
     * connectionClosed or connectionErrorOccurred method. These methods will remove the PooledConnection
     * and SessionConnectionWrapper instances from the connectionsInUse and sessionConnectionWrappers collections,
     * and do any necessary cleanup afterwards. That is why this method only calls sessionWrapper.close();
     * @param sessionWrapper The session wrapper to close.
     * @param logText The text to write to the log if the close fails.
     */
    private void closeSessionWrapper(SessionConnectionWrapper sessionWrapper,
                                     String logText) {

        try {
            sessionWrapper.close();
        } catch (SQLException e) {

            //ignore exception. The connection will automatically be removed from the pool.
            logInfo(logText, e);
        }
    }

    private long calculateLoginTimeoutExpiration() throws SQLException {

        long loginTimeoutExpiration = 0;

        if (getLoginTimeout() > 0) {
            loginTimeoutExpiration = 1000L * ((long) getLoginTimeout());
        }

        return loginTimeoutExpiration;
    }

    private void enqueue(PooledConnection connection) {
        this.connectionsInactive.add(connection);
        this.notifyAll();
    }

    /**
     * Dequeues first available connection if any. If no available connections it returns null.
     * @return The first available connection if any. Null if no connections are available.
     */
    private PooledConnection dequeueFirstIfAny() {

        if (this.connectionsInactive.size() <= 0) {
            return null;
        }

        return (PooledConnection) this.connectionsInactive.remove(0);
    }

    public synchronized int size() {
        return this.connectionsInUse.size() + this.connectionsInactive.size();
    }

    private Connection wrapConnectionAndMarkAsInUse(
            PooledConnection pooledConnection) throws SQLException {

        pooledConnection = assureValidConnection(pooledConnection);

        Connection conn = pooledConnection.getConnection();

        if (doResetAutoCommit) {
            conn.setAutoCommit(isAutoCommit);
        }

        if (doResetReadOnly) {
            conn.setReadOnly(isReadOnly);
        }

        if (doResetTransactionIsolation) {
            conn.setTransactionIsolation(transactionIsolation);
            /* TESING ONLY!!
            System.err.println("<<<<<<<<< ISO LVL => " + transactionIsolation
            + " >>>>>>>>>>>>");
            */
        }

        if (doResetCatalog) {
            conn.setCatalog(catalog);
            /* TESTING ONLY!
            System.err.println("<<<<<<<<< CAT => " + catalog
            + " >>>>>>>>>>>>");
            */
        }

        if (validationQuery != null) {

            // End-to-end test before return the Connection.
            java.sql.ResultSet rs = null;

            try {
                rs = conn.createStatement().executeQuery(validationQuery);

                if (!rs.next()) {
                    throw new SQLException("0 rows returned");
                }
            } catch (SQLException se) {
                closePhysically(pooledConnection,
                                "Closing non-validating pooledConnection.");

                throw new SQLException("Validation query failed: "
                                       + se.getMessage());
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
        this.connectionsInUse.add(pooledConnection);

        SessionConnectionWrapper sessionWrapper =
            new SessionConnectionWrapper(pooledConnection.getConnection());

        this.sessionConnectionWrappers.put(pooledConnection, sessionWrapper);

        return sessionWrapper;
    }

    private PooledConnection assureValidConnection(
            PooledConnection pooledConnection) throws SQLException {

        if (isInvalid(pooledConnection)) {
            closePhysically(pooledConnection,
                            "closing invalid pooledConnection.");

            return this.connectionPoolDataSource.getPooledConnection();
        }

        return pooledConnection;
    }

    private boolean isInvalid(PooledConnection pooledConnection) {

        /** @todo: add  || pooledConnection.getConnection.isValid()   when JDBC 4.0 arrives. */
        try {
            return pooledConnection.getConnection().isClosed();
        } catch (SQLException e) {
            logInfo(
                "Error calling pooledConnection.getConnection().isClosed(). Connection will be removed from pool.",
                e);

            return false;
        }
    }

    private boolean isSessionTimedOut(long now,
                                      SessionConnectionWrapper sessionWrapper,
                                      long sessionTimeoutMillis) {
        return now - sessionWrapper.getLatestActivityTime()
               >= sessionTimeoutMillis;
    }

    /**
     * Tells if a connection has space for new connections
     * (fx. if a connection leaked from the pool - not properly returned),
     * by comparing the total number of
     * connections supposed to be in the pool with the number of connections currently
     * in the pool + the number of connections currently being used.
     * @return True if the number of connections supposed to be in the pool is higher
     *         than the number of connections in the pool + the number of connections currently
     *         in use. False if not.
     */
    private boolean poolHasSpaceForNewConnections() {
        return this.maxPoolSize > size();
    }

    public synchronized void connectionClosed(ConnectionEvent event) {

        PooledConnection connection = (PooledConnection) event.getSource();

        this.connectionsInUse.remove(connection);
        this.sessionConnectionWrappers.remove(connection);

        if (!this.isPoolClosed) {
            enqueue(connection);
            logInfo("Connection returned to pool.");
        } else {
            closePhysically(connection, "closing returned connection.");
            logInfo(
                "Connection returned to pool was closed because pool is closed.");
            this.notifyAll();    //notifies evt. threads waiting for connection or for the pool to close.
        }
    }

    /**
     *
     * A fatal error has occurred and the connection cannot be used anymore.
     * A close event from such a connection should be ignored. The connection should not be reused.
     * A new connection will be created to replace the invalid connection, when the next client
     * calls getConnection().
     */
    public synchronized void connectionErrorOccurred(ConnectionEvent event) {

        PooledConnection connection = (PooledConnection) event.getSource();

        connection.removeConnectionEventListener(this);
        this.connectionsInUse.remove(connection);
        this.sessionConnectionWrappers.remove(connection);
        logInfo(
            "Fatal exception occurred on pooled connection. Connection is removed from pool: ");
        logInfo(event.getSQLException());
        closePhysically(connection, "closing invalid, removed connection.");

        //notify threads waiting for connections or for the pool to close.
        //one waiting thread can now create a new connection since the pool has space for a new connection.
        //if a thread waits for the pool to close this could be the last unclosed connection in the pool.
        this.notifyAll();
    }

    /**
     * Closes this connection pool. No further connections can be obtained from it after this.
     * All inactive connections are physically closed before the call returns.
     * Active connections are not closed.
     * There may still be active connections in use after this method returns.
     * When these connections are closed and returned to the pool they will be
     * physically closed.
     */
    public synchronized void close() {

        this.isPoolClosed = true;

        while (this.connectionsInactive.size() > 0) {
            PooledConnection connection = dequeueFirstIfAny();

            if (connection != null) {
                closePhysically(
                    connection,
                    "closing inactive connection when connection pool was closed.");
            }
        }
    }

    /**
     * Closes this connection pool. All inactive connections in the pool are closed physically immediatedly.
     * Waits until all active connections are returned to the pool and closed physically before returning.
     * @throws InterruptedException If the thread waiting is interrupted before all connections are returned
     *         to the pool and closed.
     */
    public synchronized void closeAndWait() throws InterruptedException {

        close();

        while (size() > 0) {
            this.wait();
        }
    }

    /**
     * Closes this connection
     */
    public synchronized void closeImmediatedly() {

        close();

        Iterator iterator = this.connectionsInUse.iterator();

        while (iterator.hasNext()) {
            PooledConnection connection = (PooledConnection) iterator.next();
            SessionConnectionWrapper sessionWrapper =
                (SessionConnectionWrapper) this.sessionConnectionWrappers.get(
                    connection);

            closeSessionWrapper(
                sessionWrapper,
                "Error closing session wrapper. Connection pool was shutdown immediatedly.");
        }
    }

    private void closePhysically(PooledConnection source, String logText) {

        try {
            source.close();
        } catch (SQLException e) {
            logInfo("Error " + logText, e);
        }
    }

    /**
     * @see JDBCConnectionPoolDataSource#logInfo(String)
     */
    private void logInfo(String message) {

        /* For external unit tests, temporarily change visibility to public.*/
        connectionPoolDataSource.logInfo(message);
    }

    /**
     * @see JDBCConnectionPoolDataSource#logInfo(Throwable)
     */
    private void logInfo(Throwable t) {

        /* For external unit tests, temporarily change visibility to public.*/
        connectionPoolDataSource.logInfo(t);
    }

    /**
     * @see JDBCConnectionPoolDataSource#logInfo(String, Throwable)
     */
    private void logInfo(String message, Throwable t) {

        /* For external unit tests, temporarily change visibility to public.*/
        connectionPoolDataSource.logInfo(message, t);
    }

    /**
     * Sets auto-commit mode for every new connection that we provide.
     *
     * This is very useful to enforce desired JDBC environments when
     * using containers (app servers being the most popular).
     */
    public void setDefaultAutoCommit(boolean defaultAutoCommit) {
        isAutoCommit      = defaultAutoCommit;
        doResetAutoCommit = true;
    }

    /**
     * Sets read-only mode for every new connection that we provide
     *
     * This is an easy way to ensure a safe test environment.
     * By making one container setting, you can be sure that the
     * DB will not be updated.
     */
    public void setDefaultReadOnly(boolean defaultReadOnly) {
        isReadOnly      = defaultReadOnly;
        doResetReadOnly = true;
    }

    /**
     * Sets transaction level for every new connection that we provide.
     *
     * For portability purposes, this has no effect on HSQLDB right now.
     * We anticipate this working intuitively for release 1.9.0 of HSQLDB.
     */
    public void setDefaultTransactionIsolation(
            int defaultTransactionIsolation) {
        transactionIsolation        = defaultTransactionIsolation;
        doResetTransactionIsolation = true;
    }

    /**
     * Sets catalog for every new connection that we provide.
     *
     * For portability purposes, this has no effect on HSQLDB right now.
     * Don't know yet when HSQLDB will have catalog-switching implemented.
     */
    public void setDefaultCatalog(String defaultCatalog) {
        catalog        = defaultCatalog;
        doResetCatalog = true;
    }

    /**
     * @see #setDefaultAutoCommit(boolean)
     */
    public boolean getDefaultAutoCommit() {

        doResetAutoCommit = true;

        return isAutoCommit;
    }

    /**
     * @see #setDefaultCatalog(String)
     */
    public String getDefaultCatalog() {

        doResetCatalog = true;

        return catalog;
    }

    /**
     * @see #setDefaultReadOnly(boolean)
     */
    public boolean getDefaultReadOnly() {

        doResetReadOnly = true;

        return isReadOnly;
    }

    /**
     * @see #setDefaultTransactionIsolation(int)
     */
    public int getDefaultTransactionIsolation() {

        doResetTransactionIsolation = true;

        return transactionIsolation;
    }

    /**
     * For compatibility.
     *
     * @param driverClassName must be the main JDBC driver class name.
     */
    public void setDriverClassName(String driverClassName) {

        if (driverClassName.equals(JDBCConnectionPoolDataSource.driver)) {
            return;
        }

        /** @todo: Use a HSQLDB RuntimeException subclass */
        throw new RuntimeException("This class only supports JDBC driver '"
                                   + JDBCConnectionPoolDataSource.driver
                                   + "'");
    }

    /**
     * For compatibility.
     *
     * @see #setDriverClassName(String)
     */
    public String getDriverClassName() {
        return JDBCConnectionPoolDataSource.driver;
    }

    /**
     * Call this method to pre-initialize some physical connections.
     *
     * You must call this method before your first getConnection() call,
     * or there will be no effect.
     * N.b. that regardless of the initialSize, no physical connections
     * will be established until the first call to getConnection().
     *
     * @param initialSize  Pre-initialize this number of physical
     *                      connections upon first call to getConnection().
     *                      The default is 0, which means, no pre-allocation.
     * @see #getConnection()
     */
    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    /**
     * This wrapper is to conform with section 11.7 of the JDBC 3.0 Spec.
     *
     * @see #setInitialSize(int)
     */
    public int getInitialPoolSize() {
        return getInitialSize();
    }

    /**
     * This wrapper is to conform with section 11.7 of the JDBC 3.0 Spec.
     *
     * @see #getInitialSize()
     */
    public void setInitialPoolSize(int initialSize) {
        setInitialSize(initialSize);
    }

    /**
     * @see #setInitialPoolSize(int)
     */
    public int getInitialSize() {
        return initialSize;
    }

    /**
     * @todo:  Implement
     * public void setMaxIdle(int maxIdle) {
     * }
     * public int getMaxIdle() {
     *   return maxIdle;
     * }
     */

    /**
     * @todo:  Implement
     * public void setMaxWait(long maxWait) {
     * }
     */

    /**
     * @todo:  Implement
     * public void setMinIdle(int minIdle) {
     * }
     */

    /**
     * The current number of active connections that have been allocated from
     * this data source
     */
    public int getNumActive() {
        return connectionsInUse.size();
    }

    /**
     * The current number of idle connections that are waiting to be allocated
     * from this data source
     */
    public int getNumIdle() {
        return connectionsInactive.size();
    }

    /**
     * Wrapper.
     *
     * @see #setUser(String)
     */
    public void setUsername(String username) {
        setUser(username);
    }

    /**
     * Wrapper.
     *
     * @see #getUser()
     */
    public String getUsername() {
        return getUser();
    }

    /**
     * Wrapper.
     *
     * @see #setMaxPoolSize(int)
     */
    public void setMaxActive(int maxActive) {
        setMaxPoolSize(maxActive);
    }

    /**
     * Wrapper.
     *
     * @see #getMaxPoolSize()
     */
    public int getMaxActive() {
        return getMaxPoolSize();
    }

    /**
     * Set a query that always returns at least one row.
     *
     * This is a standard and important connection pool manager feature.
     * This is used to perform end-to-end validation before returning
     * a Connection to a user.
     */
    public void setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    /**
     * @see #setValidationQuery(String)
     */
    public String getValidationQuery() {
        return validationQuery;
    }

    /**
     * Sets JDBC Connection Properties to be used when physical
     * connections are obtained for the pool.
     */
    public void addConnectionProperty(String name, String value) {
        this.connectionPoolDataSource.setConnectionProperty(name, value);
    }

    /**
     * Removes JDBC Connection Properties.
     *
     * @see #addConnectionProperty(String, String)
     */
    public void removeConnectionProperty(String name) {
        this.connectionPoolDataSource.removeConnectionProperty(name);
    }

    /**
     * @see #addConnectionProperty(String, String)
     */
    public Properties getConnectionProperties() {
        return this.connectionPoolDataSource.getConnectionProperties();
    }

    /**
     * Dumps current state of this Manager.
     */
    public String toString() throws RuntimeException {

        int timeout = 0;

        try {
            timeout = getLoginTimeout();
        } catch (SQLException se) {
            throw new RuntimeException(
                "Failed to retrieve the Login Timeout value");
        }

        StringBuffer sb =
            new StringBuffer(ManagedPoolDataSource.class.getName()
                             + " instance:\n    User:  " + getUsername()
                             + "\n    Url:  " + getUrl()
                             + "\n    Login Timeout:  " + timeout
                             + "\n    Num ACTIVE:  " + getNumActive()
                             + "\n    Num IDLE:  " + getNumIdle());

        if (doResetAutoCommit) {
            sb.append("\n    Default auto-commit: " + getDefaultAutoCommit());
        }

        if (doResetReadOnly) {
            sb.append("\n    Default read-only: " + getDefaultReadOnly());
        }

        if (doResetTransactionIsolation) {
            sb.append("\n    Default trans. lvl.: "
                      + getDefaultTransactionIsolation());
        }

        if (doResetCatalog) {
            sb.append("\n    Default catalog: " + getDefaultCatalog());
        }

        /** @todo:  Add report for max and min settings which aren't implemented yet. */
        return sb.toString() + "\n    Max Active: " + getMaxActive()
               + "\n    Init Size: " + getInitialSize() + "\n    Conn Props: "
               + getConnectionProperties() + "\n    Validation Query: "
               + validationQuery + '\n';
    }

    /************************* Volt DB Extensions *************************/

    public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
        throw new java.sql.SQLFeatureNotSupportedException();
    }
    /**********************************************************************/
}
