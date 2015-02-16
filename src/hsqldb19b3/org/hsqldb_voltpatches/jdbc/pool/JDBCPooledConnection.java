/* Copyright (c) 2001-2011, The HSQL Development Group
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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;

//#ifdef JAVA6
import javax.sql.StatementEventListener;

//#endif JAVA6
import org.hsqldb_voltpatches.jdbc.JDBCConnection;
import org.hsqldb_voltpatches.jdbc.JDBCConnectionEventListener;
import org.hsqldb_voltpatches.lib.OrderedHashSet;

/**
 * An implementations of {@link javax.sql.PooledConnection PooledConnection}
 * for use by connection pooling software.<p>
 * The class maintains a lifetime connection to the database. The
 * getConnection() method establishes a lease on the lifetime connection
 * and returns a special JDBCConnection (userConnection) that is
 * valid until it is closed.<p>
 *
 * This class uses a dedicated HyperSQL method to guarantee each lease on the
 * connection starts with the original state of the connection.<p>
 *
 * The ConnectionEventLister objects that have been registered with this
 * PooledConnection are notified when each lease expires, or an unrecoverable
 * error occurs on the connection to the database.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since JDK 1.2, HSQLDB 2.0
 */
public class JDBCPooledConnection
implements PooledConnection, JDBCConnectionEventListener {

    synchronized public Connection getConnection() throws SQLException {

        if (isInUse) {
            throw new SQLException("Connection in use");
        }

        isInUse = true;

        userConnection = new JDBCConnection(connection, this);

        return userConnection;
    }

    public void close() throws SQLException {

        if (connection != null) {
            connection.closeFully();

            this.connection = null;
        }
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
        listeners.add(listener);
    }

    public void removeConnectionEventListener(
            ConnectionEventListener listener) {
        listeners.remove(listener);
    }

//#ifdef JAVA6
    public void addStatementEventListener(StatementEventListener listener) {}

    public void removeStatementEventListener(
            StatementEventListener listener) {}

//#endif JAVA6
    // ------------------------ internal implementation ------------------------
    synchronized public void connectionClosed() {

        ConnectionEvent event = new ConnectionEvent(this);

        userConnection = null;

        reset();

        for (int i = 0; i < listeners.size(); i++) {
            ConnectionEventListener connectionEventListener =
                (ConnectionEventListener) listeners.get(i);

            connectionEventListener.connectionClosed(event);
        }
    }

    synchronized public void connectionErrorOccured(SQLException e) {

        ConnectionEvent event = new ConnectionEvent(this, e);

        reset();

        for (int i = 0; i < listeners.size(); i++) {
            ConnectionEventListener connectionEventListener =
                (ConnectionEventListener) listeners.get(i);

            connectionEventListener.connectionErrorOccurred(event);
        }
    }

    /**
     * Returns true if getConnection() has been called and a leas has been
     * given.
     */
    synchronized public boolean isInUse() {
        return isInUse;
    }

    /**
     * Force close the userConnection, no close event is fired.
     */
    synchronized public void reset() {

        if (userConnection != null) {

            // userConnection is already closed in normal use
            try {
                userConnection.close();
            } catch (SQLException e) {

                // check connection problems
            }
        }

        try {
            connection.reset();
        } catch (SQLException e) {

            // check connection problems
        }

        isInUse = false;
    }

    /**
     * Force close the userConnection, and connection, no close event is fired.
     */
    synchronized public void release() {

        if (userConnection != null) {

            // userConnection is already closed in normal use
            try {
                userConnection.close();
            } catch (SQLException e) {

                // check connection problems
            }
        }

        try {
            connection.close();
        } catch (SQLException e) {

            // check connection problems
        }

        isInUse = false;
    }

    protected OrderedHashSet listeners = new OrderedHashSet();
    protected JDBCConnection connection;
    protected JDBCConnection userConnection;
    protected boolean        isInUse;

    public JDBCPooledConnection(JDBCConnection connection) {
        this.connection = connection;
    }
}
