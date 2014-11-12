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

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work

/**
 * A simple wrapper around a regular <code>java.sql.Connection</code>.
 * Redirects all calls to the encapsulated connection except <code>close()</code>.
 * Calling <code>close()</code> on this wrapper marks the wrappers as closed,
 * and puts the encapsulated connection back in the queue of idle connections.
 *
 * <br/><br/>
 * This version doesn't wrap Statement, PreparedStatement, CallableStatement
 * and ResultSet instances. In order to behave 100% correctly it probably should.
 * That way this wrapper can close all of these instances when the wrapper is closed,
 * before the connection is given back to the pool. Normally the wrapped connection
 * would do this, but since it's never closed... someone has to do it.
 *
 * @author Jakob Jenkov
 */
public class LifeTimeConnectionWrapper extends BaseConnectionWrapper {

    protected JDBCConnection     connection          = null;
    protected PooledConnection   pooledConnection    = null;
    protected Set                connectionListeners = new HashSet();
    protected ConnectionDefaults connectionDefaults  = null;

    public LifeTimeConnectionWrapper(
            JDBCConnection connection,
            ConnectionDefaults connectionDefaults) throws SQLException {

        this.connection = connection;

        if (connectionDefaults != null) {
            this.connectionDefaults = connectionDefaults;

            this.connectionDefaults.setDefaults(connection);
        } else {
            this.connectionDefaults = new ConnectionDefaults(connection);
        }
    }

    public LifeTimeConnectionWrapper(
            JDBCConnection connection) throws SQLException {
        this(connection, null);
    }

    public void setPooledConnection(JDBCPooledConnection pooledConnection) {
        this.pooledConnection = pooledConnection;
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
        connectionListeners.add(listener);
    }

    public void removeConnectionEventListener(
            ConnectionEventListener listener) {
        connectionListeners.remove(listener);
    }

    protected void finalize() throws Throwable {
        closePhysically();
    }

    protected Connection getConnection() {
        return this.connection;
    }

    /**
     * Rolls the connection back, resets the connection back to defaults, clears warnings,
     * resets the connection on the server side, and returns the connection to the pool.
     * @throws SQLException
     */
    public void close() throws SQLException {

        validate();

        try {
            this.connection.rollback();
            this.connection.clearWarnings();
            this.connectionDefaults.setDefaults(this.connection);
            this.connection.reset();
            fireCloseEvent();
        } catch (SQLException e) {
            fireSqlExceptionEvent(e);

            throw e;
        }
    }

    /**
     * Closes the connection physically. The pool is not notified of this.
     * @throws SQLException If something goes wrong during the closing of the wrapped JDBCConnection.
     */
    public void closePhysically() throws SQLException {

        SQLException exception = null;

        if (!isClosed && this.connection != null
                && !this.connection.isClosed()) {
            try {
                this.connection.close();
            } catch (SQLException e) {

                //catch and hold so that the rest of the finalizer is run too. Throw at the end if present.
                exception = e;
            }
        }
        this.isClosed           = true;
        this.pooledConnection   = null;
        this.connection         = null;
        this.connectionDefaults = null;

        this.connectionListeners.clear();

        this.connectionListeners = null;

        if (exception != null) {
            throw exception;
        }
    }

    protected void fireSqlExceptionEvent(SQLException e) {

        ConnectionEvent event = new ConnectionEvent(this.pooledConnection, e);

        for (Iterator iterator = connectionListeners.iterator();
                iterator.hasNext(); ) {
            ConnectionEventListener connectionEventListener =
                (ConnectionEventListener) iterator.next();

            connectionEventListener.connectionErrorOccurred(event);
        }
    }

    protected void fireCloseEvent() {

        ConnectionEvent connectionEvent =
            new ConnectionEvent(this.pooledConnection);

        for (Iterator iterator = connectionListeners.iterator();
                iterator.hasNext(); ) {
            ConnectionEventListener connectionListener =
                (ConnectionEventListener) iterator.next();

            connectionListener.connectionClosed(connectionEvent);
        }
    }
}
