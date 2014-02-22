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

import javax.sql.XADataSource;

import java.sql.SQLException;

import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.Iterator;

import javax.transaction.xa.Xid;
import javax.sql.PooledConnection;

import org.hsqldb_voltpatches.jdbc.JDBCConnection;

import java.sql.DriverManager;

import javax.sql.XAConnection;

// @(#)$Id: JDBCXADataSource.java 2944 2009-03-21 22:53:43Z fredt $

/**
 * Connection factory for JDBCXAConnections.
 * For use by XA data source factories, not by end users.
 *
 * @since HSQLDB v. 1.9.0
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @see javax.sql.XADataSource
 * @see org.hsqldb_voltpatches.jdbc.pool.JDBCXAConnection
 */
public class JDBCXADataSource extends JDBCConnectionPoolDataSource implements XADataSource {

    /** @todo:  Break off code used here and in JDBCConnectionPoolDataSource
     *        into an abstract class, and have these classes extend the
     *        abstract class.  This class should NOT extend
     *        JDBCConnectionPoolDataSource (notice the masked
     *        pool-specific methods below).
     */
    private HashMap resources = new HashMap();

    public void addResource(Xid xid, JDBCXAResource xaResource) {
        resources.put(xid, xaResource);
    }

    public JDBCXAResource removeResource(Xid xid) {
        return (JDBCXAResource) resources.remove(xid);
    }

    /**
     * Return the list of transactions currently <I>in prepared or
     * heuristically completed states</I>.
     * Need to find out what non-prepared states they are considering
     * <I>heuristically completed</I>.
     *
     * @see javax.transaction.xa.XAResource#recover(int)
     */
    Xid[] getPreparedXids() {

        Iterator it = resources.keySet().iterator();
        Xid      curXid;
        HashSet  preparedSet = new HashSet();

        while (it.hasNext()) {
            curXid = (Xid) it.next();

            if (((JDBCXAResource) resources.get(curXid)).state
                    == JDBCXAResource.XA_STATE_PREPARED) {
                preparedSet.add(curXid);
            }
        }

        return (Xid[]) preparedSet.toArray(new Xid[0]);
    }

    /**
     * This is needed so that XAResource.commit() and
     * XAResource.rollback() may be applied to the right Connection
     * (which is not necessarily that associated with that XAResource
     * object).
     *
     * @see javax.transaction.xa.XAResource#commit(Xid, boolean)
     * @see javax.transaction.xa.XAResource#rollback(Xid)
     */
    JDBCXAResource getResource(Xid xid) {
        return (JDBCXAResource) resources.get(xid);
    }

    /**
     * Get new PHYSICAL connection, to be managed by a connection manager.
     */
    public XAConnection getXAConnection() throws SQLException {

        // Comment out before public release:
        System.err.print("Executing " + getClass().getName()
                         + ".getXAConnection()...");

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

        // Comment out before public release:
        System.err.print("New phys:  " + connection);

        JDBCXAResource xaResource = new JDBCXAResource(connection, this);
        JDBCXAConnectionWrapper xaWrapper =
            new JDBCXAConnectionWrapper(connection, xaResource,
                                        connectionDefaults);
        JDBCXAConnection xaConnection = new JDBCXAConnection(xaWrapper,
            xaResource);

        xaWrapper.setPooledConnection(xaConnection);

        return xaConnection;
    }

    /**
     * Gets a new physical connection after validating the given username
     * and password.
     *
     * @param user String which must match the 'user' configured for this
     *             JDBCXADataSource.
     * @param password  String which must match the 'password' configured
     *                  for this JDBCXADataSource.
     *
     * @see #getXAConnection()
     */
    public XAConnection getXAConnection(String user,
                                        String password) throws SQLException {

        validateSpecifiedUserAndPassword(user, password);

        return getXAConnection();
    }

    public PooledConnection getPooledConnection() throws SQLException {

        throw new SQLException(
            "Use the getXAConnections to get XA Connections.\n"
            + "Use the class JDBCConnectionPoolDataSource for non-XA data sources.");
    }

    public PooledConnection getPooledConnection(String user,
            String password) throws SQLException {

        throw new SQLException(
            "Use the getXAConnections to get XA Connections.\n"
            + "Use the class JDBCConnectionPoolDataSource for non-XA data sources.");
    }
}
