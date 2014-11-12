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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.hsqldb_voltpatches.DatabaseURL;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.persist.HsqlProperties;

// fredt@users 20011220 - patch 1.7.0 by fredt
// new version numbering scheme
// fredt@users 20020320 - patch 1.7.0 - JDBC 2 support and error trapping
// JDBC 2 methods can now be called from jdk 1.1.x - see javadoc comments
// fredt@users 20030528 - patch 1.7.2 suggested by Gerhard Hiller - support for properties in URL
// boucherb@users 20051207 - patch 1.8.x initial JDBC 4.0 support work

/**
 * Provides the java.sql.Driver interface implementation required by
 * the JDBC specification. <p>
 *
 *  The Java SQL framework allows for multiple database drivers. <p>
 *
 *  The DriverManager will try to load as many drivers as it can find and
 *  then for any given connection request, it will ask each driver in turn
 *  to try to connect to the target URL. <p>
 *
 *  The application developer will normally not need to call any function of
 *  the Driver directly. All required calls are made by the DriverManager. <p>
 *
 * <!-- start release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 *  When the HSQL Database Engine Driver class is loaded, it creates an
 *  instance of itself and register it with the DriverManager. This means
 *  that a user can load and register the HSQL Database Engine driver by
 *  calling:
 *  <pre>
 *  <code>Class.forName("org.hsqldb_voltpatches.jdbc.JDBCDriver")</code>
 *  </pre>
 *
 *  For detailed information about how to obtain HSQLDB JDBC Connections,
 *  please see {@link org.hsqldb_voltpatches.jdbc.JDBCConnection JDBCConnection}.<p>
 *
 * <hr>
 *
 * <b>JRE 1.1.x Notes:</b> <p>
 *
 * In general, JDBC 2 support requires Java 1.2 and above, and JDBC3 requires
 * Java 1.4 and above. In HSQLDB, support for methods introduced in different
 * versions of JDBC depends on the JDK version used for compiling and building
 * HSQLDB.<p>
 *
 * Since 1.7.0, it is possible to build the product so that
 * all JDBC 2 methods can be called while executing under the version 1.1.x
 * <em>Java Runtime Environment</em><sup><font size="-2">TM</font></sup>.
 * However, in addition to this technique requiring explicit casts to the
 * org.hsqldb_voltpatches.jdbc.* classes, some of the method calls also require
 * <code>int</code> values that are defined only in the JDBC 2 or greater
 * version of the {@link java.sql.ResultSet ResultSet} interface.  For this
 * reason, when the product is compiled under JDK 1.1.x, these values are
 * defined in {@link org.hsqldb_voltpatches.jdbc.JDBCResultSet JDBCResultSet}. <p>
 *
 * In a JRE 1.1.x environment, calling JDBC 2 methods that take or return the
 * JDBC2-only <code>ResultSet</code> values can be achieved by referring
 * to them in parameter specifications and return value comparisons,
 * respectively, as follows: <p>
 *
 * <pre class="JavaCodeExample">
 * JDBCResultSet.FETCH_FORWARD
 * JDBCResultSet.TYPE_FORWARD_ONLY
 * JDBCResultSet.TYPE_SCROLL_INSENSITIVE
 * JDBCResultSet.CONCUR_READ_ONLY
 * // etc.
 * </pre>
 *
 * However, please note that code written to use HSQLDB JDBC 2 features under
 * JDK 1.1.x will not be compatible for use with other JDBC 2 drivers. Please
 * also note that this feature is offered solely as a convenience to developers
 * who must work under JDK 1.1.x due to operating constraints, yet wish to
 * use some of the more advanced features available under the JDBC 2
 * specification. <p>
 *
 * <hr>
 *
 * <b>JDBC 4.0 notes:</b><p>
 *
 * Starting with JDBC 4.0 (JDK 1.6), the <code>DriverManager</code> methods
 * <code>getConnection</code> and <code>getDrivers</code> have been
 * enhanced to support the Java Standard Edition Service Provider mechanism.
 * When built under a Java runtime that supports JDBC 4.0, HSQLDB distribution
 * jars containing the Driver implementation also include the file
 * <code>META-INF/services/java.sql.Driver</code>. This file contains the fully
 * qualified class name ('org.hsqldb_voltpatches.jdbc.JDBCDriver') of the HSQLDB implementation
 * of <code>java.sql.Driver</code>. <p>
 *
 * Hence, under JDBC 4.0 or greater, applications no longer need to explictly
 * load the HSQLDB JDBC driver using <code>Class.forName()</code>. Of course,
 * existing programs which do load JDBC drivers using
 * <code>Class.forName()</code> will continue to work without modification. <p>
 *
 * <hr>
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * </div> <!-- end release-specific documentation -->
 *
 * @see org.hsqldb_voltpatches.jdbc.JDBCConnection
 */
public class JDBCDriver implements Driver {

    /**
     * Default constructor
     */
    public JDBCDriver() {
    }

    /**
     * Attempts to make a database connection to the given URL.<p>
     *
     * Returns "null" if this is the wrong kind of driver to connect to the
     * given URL.  This will be common, as when the JDBC driver manager is asked
     * to connect to a given URL it passes the URL to each loaded driver in
     * turn. <p>
     *
     * <P>The driver throws an <code>SQLException</code> if it is the right
     * driver to connect to the given URL but has trouble connecting to
     * the database. <p>
     *
     * <P>The <code>java.util.Properties</code> argument can be used to pass
     * arbitrary string tag/value pairs as connection arguments.
     * Normally at least "user" and "password" properties should be
     * included in the <code>Properties</code> object. <p>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     *  For the HSQL Database Engine, at least "user" and
     *  "password" properties should be included in the Properties.<p>
     *
     *  From version 1.7.1, two optional properties are supported:<p>
     *
     *  <ul>
     *      <li><code>get_column_name</code> (default true) -  if set to false,
     *          a ResultSetMetaData.getColumnName() call will return the user
     *          defined label (getColumnLabel()) instead of the column
     *          name.<br/>
     *
     *          This property is available in order to achieve
     *          compatibility with certain non-HSQLDB JDBC driver
     *          implementations.</li>
     *
     *      <li><code>strict_md</code> if set to true, some ResultSetMetaData
     *          methods return more strict values for compatibility
     *          reasons.</li>
     *  </ul> <p>
     *
     *  From version 1.8.0.x, <code>strict_md</code> is deprecated (ignored)
     *  because metadata reporting is always strict (JDBC-compliant), and
     *  three new optional properties are supported: <p>
     *
     *  <ul>
     *      <li><code>ifexits</code> (default false) - when true, an exception
     *          is raised when attempting to connect to an in-process
     *          file: or mem: scheme database instance if it has not yet been
     *          created.  When false, an in-process file: or mem: scheme
     *          database instance is created automatically if it has not yet
     *          been created. This property does not apply to requests for
     *          network or res: (i.e. files_in_jar) scheme connections. <li>
     *
     *      <li><code>shutdown</code> (default false) - when true, the
     *          the target database mimics the behaviour of 1.7.1 and older
     *          versions. When the last connection to a database is closed,
     *          the database is automatically shut down. The property takes
     *          effect only when the first connection is made to the database.
     *          This means the connection that opens the database. It has no
     *          effect if used with subsequent, simultaneous connections. <br/>
     *
     *          This command has two uses. One is for test suites, where
     *          connections to the database are made from one JVM context,
     *          immediately followed by another context. The other use is for
     *          applications where it is not easy to configure the environment
     *          to shutdown the database. Examples reported by users include
     *          web application servers, where the closing of the last
     *          connection conicides with the web app being shut down.</li>
     *
     *      <li><code>default_schema</code> - backwards compatibility feature.
     *          To be used for clients written before HSQLDB schema support.
     *          Denotes whether to use the default schema when a schema
     *          qualifier is not included in a database object's SQL identifier
     *          character sequence. Also affects the semantics of
     *          DatabaseMetaData calls that supply null-valued schemaNamePattern
     *          parameter values.</li>
     *  </ul>
     *
     *
     * </div> <!-- end release-specific documentation -->
     * @param url the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as connection
     *      arguments. Normally at least a "user" and "password" property
     *      should be included.
     * @return a <code>Connection</code> object that represents a
     *      connection to the URL
     * @exception SQLException if a database access error occurs
     */
    public Connection connect(String url,
                              Properties info) throws SQLException {
        return getConnection(url, info);
    }

    /**
     * The static equivalent of the <code>connect(String,Properties)</code>
     * method. <p>
     *
     * @param url the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as connection
     *      arguments including at least at a "user" and a "password" property
     * @throws java.sql.SQLException if a database access error occurs
     * @return a <code>Connection</code> object that represents a
     *      connection to the URL
     */

//#ifdef JAVA6
    @SuppressWarnings("deprecation")

//#endif JAVA6
    public static Connection getConnection(String url,
            Properties info) throws SQLException {

        final HsqlProperties props = DatabaseURL.parseURL(url, true, false);

        if (props == null) {

            // supposed to be an HSQLDB driver url but has errors
            throw Util.invalidArgument();
        } else if (props.isEmpty()) {

            // is not an HSQLDB driver url
            return null;
        }
        props.addProperties(info);

        long timeout = DriverManager.getLoginTimeout();

        if (timeout == 0) {

            // no timeout restriction
            return new JDBCConnection(props);
        }

        String connType = props.getProperty("connection_type");

        if (DatabaseURL.isInProcessDatabaseType(connType)) {
            return new JDBCConnection(props);
        }

        /** @todo:  Better: ThreadPool? HsqlTimer with callback? */
        final JDBCConnection[] conn = new JDBCConnection[1];
        final SQLException[]   ex   = new SQLException[1];
        Thread                 t    = new Thread() {

            public void run() {

                try {
                    conn[0] = new JDBCConnection(props);
                } catch (SQLException se) {
                    ex[0] = se;
                }
            }
        };

        t.start();

        final long start = System.currentTimeMillis();

        try {
            t.join(1000 * timeout);
        } catch (InterruptedException ie) {
        }

        try {

            // PRE:
            // deprecated, but should be ok, since neither
            // the HSQLClientConnection or the HTTPClientConnection
            // constructor will ever hold monitors on objects in
            // an inconsistent state, such that damaged objects
            // become visible to other threads with the
            // potential of arbitrary behavior.
            t.stop();
        } catch (Exception e) {
        }

        if (ex[0] != null) {
            throw ex[0];
        }

        if (conn[0] != null) {
            return conn[0];
        }

        throw Util.sqlException(ErrorCode.X_08501);
    }

    /**
     *  Returns true if the driver thinks that it can open a connection to
     *  the given URL. Typically drivers will return true if they understand
     *  the subprotocol specified in the URL and false if they don't.
     *
     * @param  url the URL of the database
     * @return  true if this driver can connect to the given URL
     */

    // fredt@users - patch 1.7.0 - allow mixedcase url's
    public boolean acceptsURL(String url) {

        if (url == null) {
            return false;
        }

        return url.regionMatches(true, 0, DatabaseURL.S_URL_PREFIX, 0,
                                 DatabaseURL.S_URL_PREFIX.length());
    }

    /**
     *  Gets information about the possible properties for this driver. <p>
     *
     *  The getPropertyInfo method is intended to allow a generic GUI tool
     *  to discover what properties it should prompt a human for in order to
     *  get enough information to connect to a database. Note that depending
     *  on the values the human has supplied so far, additional values may
     *  become necessary, so it may be necessary to iterate though several
     *  calls to getPropertyInfo.<p>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB uses the values submitted in info to set the value for
     * each DriverPropertyInfo object returned. It does not use the default
     * value that it would use for the property if the value is null. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param  url the URL of the database to which to connect
     * @param  info a proposed list of tag/value pairs that will be sent on
     *      connect open
     * @return  an array of DriverPropertyInfo objects describing possible
     *      properties. This array may be an empty array if no properties
     *      are required.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {

        if (!acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }

        String[]             choices = new String[] {
            "true", "false"
        };
        DriverPropertyInfo[] pinfo   = new DriverPropertyInfo[6];
        DriverPropertyInfo   p;

        if (info == null) {
            info = new Properties();
        }
        p          = new DriverPropertyInfo("user", null);
        p.value    = info.getProperty("user");
        p.required = true;
        pinfo[0]   = p;
        p          = new DriverPropertyInfo("password", null);
        p.value    = info.getProperty("password");
        p.required = true;
        pinfo[1]   = p;
        p          = new DriverPropertyInfo("get_column_name", null);
        p.value    = info.getProperty("get_column_name", "true");
        p.required = false;
        p.choices  = choices;
        pinfo[2]   = p;
        p          = new DriverPropertyInfo("ifexists", null);
        p.value    = info.getProperty("ifexists", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[3]   = p;
        p          = new DriverPropertyInfo("default_schema", null);
        p.value    = info.getProperty("default_schema", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[4]   = p;
        p          = new DriverPropertyInfo("shutdown", null);
        p.value    = info.getProperty("shutdown", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[5]   = p;

        return pinfo;
    }

    /**
     *  Gets the driver's major version number.
     *
     * @return  this driver's major version number
     */
    public int getMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    /**
     *  Gets the driver's minor version number.
     *
     * @return  this driver's minor version number
     */
    public int getMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }

    /**
     * Reports whether this driver is a genuine JDBC Compliant<sup><font
     * size=-2>TM</font></sup> driver. A driver may only report
     * <code>true</code> here if it passes the JDBC compliance tests; otherwise
     * it is required to return <code>false</code>. <p>
     *
     * JDBC compliance requires full support for the JDBC API and full support
     * for SQL 92 Entry Level. <p>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     *  HSQLDB 1.9.0 is aimed to be compliant with JDBC 4.0 specification.
     *  It supports SQL 92 Entry Level and beyond.
     * </div> <!-- end release-specific documentation -->
     *
     * This method is not intended to encourage the development of non-JDBC
     * compliant drivers, but is a recognition of the fact that some vendors
     * are interested in using the JDBC API and framework for lightweight
     * databases that do not support full database functionality, or for
     * special databases such as document information retrieval where a SQL
     * implementation may not be feasible.
     *
     * @return <code>true</code> if this driver is JDBC Compliant;
     *         <code>false</code> otherwise
     */
    public boolean jdbcCompliant() {
        return true;
    }

    static {
        try {
            DriverManager.registerDriver(new JDBCDriver());
        } catch (Exception e) {
        }
    }

    /************************* Volt DB Extensions *************************/

    public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
        throw new java.sql.SQLFeatureNotSupportedException();
    }
    /**********************************************************************/
}
