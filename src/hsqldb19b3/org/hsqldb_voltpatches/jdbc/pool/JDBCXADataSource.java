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

import java.io.Serializable;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

//#ifdef JAVA6
import javax.sql.CommonDataSource;

//#endif JAVA6
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.jdbc.JDBCCommonDataSource;
import org.hsqldb_voltpatches.jdbc.JDBCConnection;
import org.hsqldb_voltpatches.jdbc.JDBCDriver;
import org.hsqldb_voltpatches.jdbc.JDBCUtil;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;

// @(#)$Id: JDBCXADataSource.java 5283 2013-09-29 17:52:44Z unsaved $

/**
 * Connection factory for JDBCXAConnections.
 * For use by XA data source factories, not by end users.<p>
 *
 * The {@link org.hsqldb_voltpatches.jdbc.JDBCDataSourceFactory} can be used to get
 * instances of this class.<p>
 *
 * The methods of the superclass, {@link org.hsqldb_voltpatches.jdbc.JDBCCommonDataSource},
 * are used for settings the HyperSQL server and user.<p>
 *
 * @version 2.2.9
 * @since 2.0.0
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @see javax.sql.XADataSource
 * @see org.hsqldb_voltpatches.jdbc.pool.JDBCXAConnection
 */
public class JDBCXADataSource extends JDBCCommonDataSource
implements XADataSource, Serializable, Referenceable

//#ifdef JAVA6
, CommonDataSource

//#endif JAVA6
{

    /**
     * Get new XAConnection connection, to be managed by a connection manager.
     */
    public XAConnection getXAConnection() throws SQLException {

        // Comment out before public release:
/*
        System.err.print("Executing " + getClass().getName()
                         + ".getXAConnection()...");
*/

        // Use JDBCDriver directly so there is no need to regiser with DriverManager
        JDBCConnection connection =
            (JDBCConnection) JDBCDriver.getConnection(url, connectionProps);
        JDBCXAConnection xaConnection = new JDBCXAConnection(this, connection);

        return xaConnection;
    }

    /**
     * Gets a new XAConnection after validating the given username
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

        if (user == null || password == null) {
            throw JDBCUtil.nullArgument();
        }

        if (user.equals(this.user) && password.equals(this.password)) {
            return getXAConnection();
        }

        throw JDBCUtil.sqlException(Error.error(ErrorCode.X_28000));
    }

    /**
     * Retrieves the Reference of this object.
     *
     * @return The non-null javax.naming.Reference of this object.
     * @exception NamingException If a naming exception was encountered
     *          while retrieving the reference.
     */
    public Reference getReference() throws NamingException {

        String    cname = "org.hsqldb_voltpatches.jdbc.JDBCDataSourceFactory";
        Reference ref   = new Reference(getClass().getName(), cname, null);

        ref.add(new StringRefAddr("database", getDatabase()));
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", password));
        ref.add(new StringRefAddr("loginTimeout",
                                  Integer.toString(loginTimeout)));

        return ref;
    }

    // ------------------------ internal implementation ------------------------
    private HashMap resources = new HashMap();

    public void addResource(Xid xid, JDBCXAResource xaResource) {
        resources.put(xid, xaResource);
    }

    public JDBCXADataSource() throws SQLException {

        //
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

        Xid[] array = new Xid[preparedSet.size()];

        preparedSet.toArray(array);

        return array;
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
}
