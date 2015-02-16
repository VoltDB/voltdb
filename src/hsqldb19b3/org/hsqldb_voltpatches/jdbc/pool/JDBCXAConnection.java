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
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.hsqldb_voltpatches.jdbc.JDBCConnection;

// @(#)$Id: JDBCXAConnection.java 5026 2012-07-14 20:02:27Z fredt $

/**
 * Subclass of JDBCPooledConnection implements the XAConneciton interface.
 * For use by global transaction service managers.<p>
 *
 * Each instance has an JDBCXAResource inherits the superclass's two
 * JDBCConnection objects, one for internal access, and one for user access.<P>
 *
 * The getConnection() method returns a user connection and links this with
 * the JDBCXAResource. This puts the object in the inUse state.
 * When the user connection is closed, the object is put in the free state.
 *
 * @version 2.2.9
 * @since HSQLDB 2.0
 * @author Fred Toussi (fredt at users.sourceforge.net)
 * @see javax.sql.XAConnection
 */
public class JDBCXAConnection extends JDBCPooledConnection implements XAConnection {

    JDBCXAResource xaResource;

    public JDBCXAConnection(JDBCXADataSource dataSource, JDBCConnection connection) {

        super(connection);
        xaResource = new JDBCXAResource(dataSource, connection);
    }

    public XAResource getXAResource() throws SQLException {
        return xaResource;
    }

    /**
     * Returns a connection that can be used by the user application.
     *
     * @throws SQLException if a lease has already been given on this connection
     * @return Connection
     */
    synchronized public Connection getConnection() throws SQLException {

        if (isInUse) {
            throw new SQLException("Connection in use");
        }

        isInUse = true;


        return new JDBCXAConnectionWrapper(xaResource, this, connection);
    }

    public void close() throws SQLException {
        super.close();

        // deal with xaResource
    }
}
