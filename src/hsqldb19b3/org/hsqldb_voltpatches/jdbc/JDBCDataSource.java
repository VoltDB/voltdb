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


package org.hsqldb_voltpatches.jdbc;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

import org.hsqldb_voltpatches.jdbc.JDBCDriver;

//#ifdef JAVA6
import java.sql.Wrapper;

import javax.sql.CommonDataSource;

//#endif JAVA6

/* $Id: JDBCDataSource.java 2944 2009-03-21 22:53:43Z fredt $ */

// boucherb@users 20040411 - doc 1.7.2 - javadoc updates toward 1.7.2 final
// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
// boucherb@users 20060522 - patch 1.9.0 full synch up to Mustang Build 84
// Revision 1.12  2006/07/12 12:04:06 boucherb
// - full synch up to Mustang b90
//- support new createQueryObject signature

/**
 * <p>A factory for connections to the physical data source that this
 * <code>DataSource</code> object represents.  An alternative to the
 * <code>DriverManager</code> facility, a <code>DataSource</code> object
 * is the preferred means of getting a connection. An object that implements
 * the <code>DataSource</code> interface will typically be
 * registered with a naming service based on the
 * Java<sup><font size=-2>TM</font></sup> Naming and Directory (JNDI) API.
 * <P>
 * The <code>DataSource</code> interface is implemented by a driver vendor.
 * There are three types of implementations:
 * <OL>
 *   <LI>Basic implementation -- produces a standard <code>Connection</code>
 *       object
 *   <LI>Connection pooling implementation -- produces a <code>Connection</code>
 *       object that will automatically participate in connection pooling.  This
 *       implementation works with a middle-tier connection pooling manager.
 *   <LI>Distributed transaction implementation -- produces a
 *       <code>Connection</code> object that may be used for distributed
 *       transactions and almost always participates in connection pooling.
 *       This implementation works with a middle-tier
 *       transaction manager and almost always with a connection
 *       pooling manager.
 * </OL>
 * <P>
 * A <code>DataSource</code> object has properties that can be modified
 * when necessary.  For example, if the data source is moved to a different
 * server, the property for the server can be changed.  The benefit is that
 * because the data source's properties can be changed, any code accessing
 * that data source does not need to be changed.
 * <P>
 * A driver that is accessed via a <code>DataSource</code> object does not
 * register itself with the <code>DriverManager</code>.  Rather, a
 * <code>DataSource</code> object is retrieved though a lookup operation
 * and then used to create a <code>Connection</code> object.  With a basic
 * implementation, the connection obtained through a <code>DataSource</code>
 * object is identical to a connection obtained through the
 * <code>DriverManager</code> facility.
 *
 * @since JDK 1.4
 * @author deforest@users
 * @author boucherb@users
 * @version 1.9.0
 * @revised JDK 1.6, HSQLDB 1.9.0
 */
//#ifdef JAVA6
@SuppressWarnings("serial")

//#endif JAVA6
public class JDBCDataSource implements Serializable, Referenceable, DataSource

//#ifdef JAVA6
, CommonDataSource, Wrapper

//#endif JAVA6
{

    /**
     * Login timeout
     */
    private int loginTimeout = 0;

    /**
     * Log writer
     */
    private transient PrintWriter logWriter;

    /**
     * Default password to use for connections
     */
    private String password = "";

    /**
     * Default user to use for connections
     */
    private String user = "";

    /**
     * Database location
     */
    private String database = "";

    /**
     * Constructor
     */
    public JDBCDataSource() {
    }

    /**
     * <p>Attempts to establish a connection with the data source that
     * this <code>DataSource</code> object represents.
     *
     * @return  a connection to the data source
     * @exception SQLException if a database access error occurs
     */
    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    /**
     * <p>Attempts to establish a connection with the data source that
     * this <code>DataSource</code> object represents.
     *
     * @param username the database user on whose behalf the connection is
     *  being made
     * @param password the user's password
     * @return  a connection to the data source
     * @exception SQLException if a database access error occurs
     */
    public Connection getConnection(String username,
                                    String password) throws SQLException {

        Properties props = new Properties();

        if (username != null) {
            props.put("user", username);
        }

        if (password != null) {
            props.put("password", password);
        }

        return JDBCDriver.getConnection(database, props);
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Creates a concrete implementation of a Query interface using the JDBC drivers <code>QueryObjectGenerator</code>
     * implementation.
     * <p>
     * If the JDBC driver does not provide its own <code>QueryObjectGenerator</code>, the <code>QueryObjectGenerator</code>
     * provided with Java SE will be used.
     * <p>
     * @param ifc The Query interface that will be created
     * @return A concrete implementation of a Query interface
     * @exception SQLException if a database access error occurs.
     *  @since JDK 1.6, HSQLDB 1.9.0
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
    public <T extends BaseQuery> T createQueryObject(Class<T> ifc, DataSource ds) throws SQLException {
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
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JDBC4BETA
/*
    public QueryObjectGenerator getQueryObjectGenerator() throws SQLException {
        return null;
    }
*/

//#endif JDBC4BETA
    // ------------------- java.sql.Wrapper implementation ---------------------

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     *
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return the
     * the result of calling <code>unwrap</code> recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    @SuppressWarnings("unchecked")
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
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//#endif JAVA6
    // ------------------------ internal implementation ------------------------

    /**
     * Retrieves the jdbc database connection url attribute. <p>
     *
     * @return the jdbc database connection url attribute
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Gets the maximum time in seconds that this data source can wait
     * while attempting to connect to a database.  A value of zero
     * means that the timeout is the default system timeout
     * if there is one; otherwise, it means that there is no timeout.
     * When a <code>DataSource</code> object is created, the login timeout is
     * initially zero.
     *
     * @return the data source login time limit
     * @exception SQLException if a database access error occurs.
     * @see #setLoginTimeout
     * @see #setLoginTimeout
     */
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    /**
     * <p>Retrieves the log writer for this <code>DataSource</code>
     * object.
     *
     * <p>The log writer is a character output stream to which all logging
     * and tracing messages for this data source will be
     * printed.  This includes messages printed by the methods of this
     * object, messages printed by methods of other objects manufactured
     * by this object, and so on.  Messages printed to a data source
     * specific log writer are not printed to the log writer associated
     * with the <code>java.sql.Drivermanager</code> class.  When a
     * <code>DataSource</code> object is
     * created, the log writer is initially null; in other words, the
     * default is for logging to be disabled.
     *
     * @return the log writer for this data source or null if
     *        logging is disabled
     * @exception SQLException if a database access error occurs
     * @see #setLogWriter
     */
    public java.io.PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    // javadoc to be copied from javax.naming.Referenceable.getReference()
    public Reference getReference() throws NamingException {

        String    cname = "org.hsqldb_voltpatches.jdbc.JDBCDataSourceFactory";
        Reference ref   = new Reference(getClass().getName(), cname, null);

        ref.add(new StringRefAddr("database", getDatabase()));
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", password));

        return ref;
    }

    /**
     * Retrieves the user ID for the connection. <p>
     *
     * @return the user ID for the connection
     */
    public String getUser() {
        return user;
    }

    /**
     * Assigns the value of this object's jdbc database connection
     * url attribute. <p>
     *
     * @param database the new value of this object's jdbc database connection
     *      url attribute
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * <p>Sets the maximum time in seconds that this data source will wait
     * while attempting to connect to a database.  A value of zero
     * specifies that the timeout is the default system timeout
     * if there is one; otherwise, it specifies that there is no timeout.
     * When a <code>DataSource</code> object is created, the login timeout is
     * initially zero.
     *
     * @param seconds the data source login time limit
     * @exception SQLException if a database access error occurs.
     * @see #getLoginTimeout
     */
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = 0;
    }

    /**
     * <p>Sets the log writer for this <code>DataSource</code>
     * object to the given <code>java.io.PrintWriter</code> object.
     *
     * <p>The log writer is a character output stream to which all logging
     * and tracing messages for this data source will be
     * printed.  This includes messages printed by the methods of this
     * object, messages printed by methods of other objects manufactured
     * by this object, and so on.  Messages printed to a data source-
     * specific log writer are not printed to the log writer associated
     * with the <code>java.sql.DriverManager</code> class. When a
     * <code>DataSource</code> object is created the log writer is
     * initially null; in other words, the default is for logging to be
     * disabled.
     *
     * @param logWriter the new log writer; to disable logging, set to null
     * @exception SQLException if a database access error occurs
     * @see #getLogWriter
     */
    public void setLogWriter(PrintWriter logWriter) throws SQLException {
        this.logWriter = logWriter;
    }

    /**
     * Sets the password to use for connecting to the database
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Sets the userid
     * @param user the user id
     */
    public void setUser(String user) {
        this.user = user;
    }

    /************************* Volt DB Extensions *************************/

    public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
        throw new java.sql.SQLFeatureNotSupportedException();
    }
    /**********************************************************************/
}
