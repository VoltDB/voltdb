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

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Map;

//#ifdef JAVA4
import java.sql.Savepoint;

//#endif JAVA4
//#ifdef JAVA6
import java.sql.Array;
import java.sql.SQLClientInfoException;
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.Properties;

//#endif JAVA6
import org.hsqldb_voltpatches.DatabaseManager;
import org.hsqldb_voltpatches.DatabaseURL;
// A VoltDB extension to disable a subpackage dependency
/* disable 2 lines ...
import org.hsqldb_voltpatches.ClientConnection;
import org.hsqldb_voltpatches.ClientConnectionHTTP;
... disabled 2 lines */
// End of VoltDB extension
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlDateTime;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.persist.HsqlProperties;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;

import java.sql.SQLData;
import java.sql.SQLOutput;
import java.sql.SQLInput;

/* $Id: JDBCConnection.java 3001 2009-06-04 12:31:11Z fredt $ */

// fredt@users    20020320 - patch 1.7.0 - JDBC 2 support and error trapping
//
// JDBC 2 methods can now be called from jdk 1.1.x - see javadoc comments
//
// boucherb@users 20020509 - added "throws SQLException" to all methods where
//                           it was missing here but specified in the
//                           java.sql.Connection interface,
//                           updated generic documentation to JDK 1.4, and
//                           added JDBC3 methods and docs
// boucherb &
// fredt@users    20020505 - extensive review and update of docs and behaviour
//                           to comply with java.sql specification
// fredt@users    20020830 - patch 487323 by xclayl@users - better synchronization
// fredt@users    20020930 - patch 1.7.1 - support for connection properties
// kneedeepincode@users
//                20021110 - patch 635816 - correction to properties
// unsaved@users  20021113 - patch 1.7.2 - SSL support
// boucherb@users 2003 ??? - patch 1.7.2 - SSL support moved to factory interface
// fredt@users    20030620 - patch 1.7.2 - reworked to use a SessionInterface
// boucherb@users 20030801 - JavaDoc updates to reflect new connection urls
// boucherb@users 20030819 - patch 1.7.2 - partial fix for broken nativeSQL method
// boucherb@users 20030819 - patch 1.7.2 - SQLWarning cases implemented
// boucherb@users 20051207 - 1.9.0       - JDBC 4.0 support - docs and methods
//              - 20060712               - full synch up to Mustang Build 90
// fredt@users    20090810 - 1.9.0       - full review and updates
//
// Revision 1.23  2006/07/12 12:02:43 boucherb
// patch 1.9.0
// - full synch up to Mustang b90

/**
 * <!-- start generic documentation -->
 *
 * <P>A connection (session) with a specific
 * database. SQL statements are executed and results are returned
 * within the context of a connection.
 * <P>
 * A <code>Connection</code> object's database is able to provide information
 * describing its tables, its supported SQL grammar, its stored
 * procedures, the capabilities of this connection, and so on. This
 * information is obtained with the <code>getMetaData</code> method.
 *
 * <P>(JDBC4 clarification:)
 * <P><B>Note:</B> When configuring a <code>Connection</code>, JDBC applications
 *  should use the appropritate <code>Connection</code> method such as
 *  <code>setAutoCommit</code> or <code>setTransactionIsolation</code>.
 *  Applications should not invoke SQL commands directly to change the connection's
 *   configuration when there is a JDBC method available.  By default a <code>Connection</code> object is in
 * auto-commit mode, which means that it automatically commits changes
 * after executing each statement. If auto-commit mode has been
 * disabled, the method <code>commit</code> must be called explicitly in
 * order to commit changes; otherwise, database changes will not be saved.
 * <P>
 * A new <code>Connection</code> object created using the JDBC 2.1 core API
 * has an initially empty type map associated with it. A user may enter a
 * custom mapping for a UDT in this type map.
 * When a UDT is retrieved from a data source with the
 * method <code>ResultSet.getObject</code>, the <code>getObject</code> method
 * will check the connection's type map to see if there is an entry for that
 * UDT.  If so, the <code>getObject</code> method will map the UDT to the
 * class indicated.  If there is no entry, the UDT will be mapped using the
 * standard mapping.
 * <p>
 * A user may create a new type map, which is a <code>java.util.Map</code>
 * object, make an entry in it, and pass it to the <code>java.sql</code>
 * methods that can perform custom mapping.  In this case, the method
 * will use the given type map instead of the one associated with
 * the connection.
 * <p>
 * For example, the following code fragment specifies that the SQL
 * type <code>ATHLETES</code> will be mapped to the class
 * <code>Athletes</code> in the Java programming language.
 * The code fragment retrieves the type map for the <code>Connection
 * </code> object <code>con</code>, inserts the entry into it, and then sets
 * the type map with the new entry as the connection's type map.
 * <pre>
 *      java.util.Map map = con.getTypeMap();
 *      map.put("mySchemaName.ATHLETES", Class.forName("Athletes"));
 *      con.setTypeMap(map);
 * </pre>
 *
 * <!-- end generic documentation -->
 *
 * <!-- start release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * To get a <code>Connection</code> to an HSQLDB database, the
 * following code may be used (updated to reflect the most recent
 * recommendations): <p>
 *
 * <hr>
 *
 * When using HSQLDB, the database connection <b>&lt;url&gt;</b> must start with
 * <b>'jdbc:hsqldb:'</b><p>
 *
 * Since 1.7.2, connection properties (&lt;key-value-pairs&gt;) may be appended
 * to the database connection <b>&lt;url&gt;</b>, using the form: <p>
 *
 * <blockquote>
 *      <b>'&lt;url&gt;[;key=value]*'</b>
 * </blockquote> <p>
 *
 * Also since 1.7.2, the allowable forms of the HSQLDB database connection
 * <b>&lt;url&gt;</b> have been extended.  However, all legacy forms continue
 * to work, with unchanged semantics.  The extensions are as described in the
 * following material. <p>
 *
 * <hr>
 *
 * <b>Network Server Database Connections:</b> <p>
 *
 * The {@link org.hsqldb_voltpatches.server.Server Server} database connection <b>&lt;url&gt;</b>
 * takes one of the two following forms: <p>
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:hsql://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *
 * <li> <b>'jdbc:hsqldb:hsqls://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *         (with TLS).
 * </ol>
 * </div> <p>
 *
 * The {@link org.hsqldb_voltpatches.server.WebServer WebServer} database connection <b>&lt;url&gt;</b>
 * takes one of two following forms: <p>
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:http://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *
 * <li> <b>'jdbc:hsqldb:https://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *      (with TLS).
 * </ol>
 * </div><p>
 *
 * In both network server database connection <b>&lt;url&gt;</b> forms, the
 * optional <b>&lt;alias&gt;</b> component is used to identify one of possibly
 * several database instances available at the indicated host and port.  If the
 * <b>&lt;alias&gt;</b> component is omitted, then a connection is made to the
 * network server's default database instance, if such an instance is
 * available. <p>
 *
 * For more information on server configuration regarding mounting multiple
 * databases and assigning them <b>&lt;alias&gt;</b> values, please read the
 * Java API documentation for {@link org.hsqldb_voltpatches.server.Server Server} and related
 * chapters in the general documentation, especially the <em>Advanced Users
 * Guide</em>. <p>
 *
 * <hr>
 *
 * <b>Transient, In-Process Database Connections:</b> <p>
 *
 * The 100% in-memory (transient, in-process) database connection
 * <b>&lt;url&gt;</b> takes one of the two following forms: <p>
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:.[&lt;key-value-pairs&gt;]'</b>
 *     (the legacy form, extended)
 *
 * <li> <b>'jdbc:hsqldb:mem:&lt;alias&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (the new form)
 * </ol>
 * </div> <p>
 *
 * The driver converts the supplied <b>&lt;alias&gt;</b> component to
 * Local.ENGLISH lower case and uses the resulting character sequence as the
 * key used to look up a <b>mem:</b> protocol database instance amongst the
 * collection of all such instances already in existence within the current
 * class loading context in the current JVM. If no such instance exists, one
 * <em>may</em> be automatically created and mapped to the <b>&lt;alias&gt;</b>,
 * as governed by the <b>'ifexists=true|false'</b> connection property. <p>
 *
 * The rationale for converting the supplied <b>&lt;alias&gt;</b> component to
 * lower case is to provide consistency with the behavior of <b>res:</b>
 * protocol database connection <b>&lt;url&gt;</b>s, explained further on in
 * this overview.
 *
 * <hr>
 *
 * <b>Persistent, In-Process Database Connections:</b> <p>
 *
 * The standalone (persistent, in-process) database connection
 * <b>&lt;url&gt;</b> takes one of the three following forms: <p>
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:&lt;path&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (the legacy form, extended)
 *
 * <li> <b>'jdbc:hsqldb:file:&lt;path&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (same semantics as the legacy form)
 *
 * <li> <b>'jdbc:hsqldb:res:&lt;path&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (new form with 'files_in_jar' semantics)
 * </ol>
 * </div> <p>
 *
 * For the persistent, in-process database connection <b>&lt;url&gt;</b>,
 * the <b>&lt;path&gt;</b> component is the path prefix common to all of
 * the files that compose the database. <p>
 *
 * From 1.7.2, although other files may be involved (such as transient working
 * files and/or TEXT table CSV data source files), the essential set that may,
 * at any particular point in time, compose an HSQLDB database is: <p>
 *
 * <div class="GeneralExample">
 * <ul>
 * <li>&lt;path&gt;.properties
 * <li>&lt;path&gt;.script
 * <li>&lt;path&gt;.log
 * <li>&lt;path&gt;.data
 * <li>&lt;path&gt;.backup
 * <li>&lt;path&gt;.lck
 * </ul>
 * </div> <p>
 *
 * For example: <b>'jdbc:hsqldb:file:test'</b> connects to a database
 * composed of some subset of the files listed above, where the expansion
 * of <b>&lt;path&gt;</b> is <b>'test'</b> prefixed with the canonical path of
 * the JVM's effective working directory at the time the designated database
 * is first opened in-process. <p>
 *
 * Be careful to note that this canonical expansion of <b>&lt;path&gt;</b> is
 * cached by the driver until JVM exit. So, although legacy JVMs tend to fix
 * the reported effective working directory at the one noted upon JVM startup,
 * there is no guarantee that modern JVMs will continue to uphold this
 * behaviour.  What this means is there is effectively no guarantee into the
 * future that a relative <b>file:</b> protocol database connection
 * <b>&lt;url&gt;</b> will connect to the same database instance for the life
 * of the JVM.  To avoid any future ambigutity issues, it is probably a best
 * practice for clients to attempt to pre-canonicalize the <b>&lt;path&gt;</b>
 * component of <b>file:</b> protocol database connection* <b>&lt;url&gt;</b>s.
 * <p>
 *
 * Under <em>Windows</em> <sup><font size="-2">TM</font> </sup>, <b>
 * 'jdbc:hsqldb:file:c:\databases\test'</b> connects to a database located
 * on drive <b>'C:'</b> in the directory <b>'databases'</b>, composed
 * of some subset of the files: <p>
 *
 * <pre class="GeneralExample">
 * C:\
 * +--databases\
 *    +--test.properties
 *    +--test.script
 *    +--test.log
 *    +--test.data
 *    +--test.backup
 *    +--test.lck
 * </pre>
 *
 * Under most variations of UNIX, <b>'jdbc:hsqldb:file:/databases/test'</b>
 * connects to a database located in the directory <b>'databases'</b> directly
 * under root, once again composed of some subset of the files: <p>
 *
 * <pre class="GeneralExample">
 *
 * +--databases
 *    +--test.properties
 *    +--test.script
 *    +--test.log
 *    +--test.data
 *    +--test.backup
 *    +--test.lck
 * </pre>
 *
 * <b>Some Guidelines:</b> <p>
 *
 * <ol>
 * <li> Both relative and absolute database file paths are supported. <p>
 *
 * <li> Relative database file paths can be specified in a platform independent
 *      manner as: <b>'[dir1/dir2/.../dirn/]&lt;file-name-prefix&gt;'</b>. <p>
 *
 * <li> Specification of absolute file paths is operating-system specific.<br>
 *      Please read your OS file system documentation. <p>
 *
 * <li> Specification of network mounts may be operating-system specific.<br>
 *      Please read your OS file system documentation. <p>
 *
 * <li> Special care may be needed w.r.t. file path specifications
 *      containing whitespace, mixed-case, special characters and/or
 *      reserved file names.<br>
 *      Please read your OS file system documentation. <p>
 * </ol> <p>
 *
 * <b>Note:</b> Versions of HSQLDB previous to 1.7.0 did not support creating
 * directories along the file path specified in the persistent, in-process mode
 * database connection <b>&lt;url&gt;</b> form, in the case that they did
 * not already exist.  Starting with HSQLDB 1.7.0, directories <i>will</i>
 * be created if they do not already exist., but only if HSQLDB is built under
 * a version of the compiler greater than JDK 1.1.x. <p>
 *
 * <hr>
 *
 * <b>res: protocol Connections:</b><p>
 *
 * The <b>'jdbc:hsqldb:res:&lt;path&gt;'</b> database connection
 * <b>&lt;url&gt;</b> has different semantics than the
 * <b>'jdbc:hsqldb:file:&lt;path&gt;'</b> form. The semantics are similar to
 * those of a <b>'files_readonly'</b> database, but with some additional
 * points to consider. <p>
 *
 * Specifically, the <b>'&lt;path&gt;'</b> component of a <b>res:</b> protocol
 * database connection <b>&lt;url&gt;</b> is first converted to lower case
 * with <tt>Locale.ENGLISH</tt> and only then used to obtain resource URL
 * objects, which in turn are used to read the database files as resources on
 * the class path. <p>
 *
 * Due to lower case conversion by the driver, <b>res:</b> <b>'&lt;path&gt;'</b>
 * components <em>never</em> find jar resources stored with
 * <tt>Locale.ENGLISH</tt> mixed case paths. The rationale for converting to
 * lower case is that not all pkzip implementations guarantee path case is
 * preserved when archiving resources, and conversion to lower case seems to
 * be the most common occurrence (although there is also no actual guarantee
 * that the conversion is <tt>Locale.ENGLISH</tt>).<p>
 *
 * More importantly, <b>res:</b> <b>'&lt;path&gt;'</b> components <em>must</em>
 * point only to resources contained in one or more jars on the class
 * path. That is, only resources having the jar sub-protocol are considered
 * valid. <p>
 *
 * This restriction is enforced to avoid the unfortunate situation in which,
 * because <b>res:</b> database instances do not create a <b>&lt;path&gt;</b>.lck
 * file (they are strictly files-read-only) and because the <b>&lt;path&gt;</b>
 * components of <b>res:</b> and <b>file:</b> database <tt>URI</tt>s are not
 * checked for file system equivalence, it is possible for the same database
 * files to be accessed concurrently by both <b>file:</b> and <b>res:</b>
 * database instances. That is, without this restriction, it is possible that
 * <b>&lt;path&gt;</b>.data and <b>&lt;path&gt;</b>.properties file content may
 * be written by a <b>file:</b> database instance without the knowlege or
 * cooperation of a <b>res:</b> database instance open on the same files,
 * potentially resulting in unexpected database errors, inconsistent operation
 * and/or data corruption. <p>
 *
 * In short, a <b>res:</b> type database connection <b>&lt;url&gt;</b> is
 * designed specifically to connect to a <b>'files_in_jar'</b> mode database
 * instance, which in turn is designed specifically to operate under
 * <em>Java WebStart</em><sup><font size="-2">TM</font></sup> and
 * <em>Java Applet</em><sup><font size="-2">TM</font></sup>configurations,
 * where co-locating the database files in the jars that make up the
 * <em>WebStart</em> application or Applet avoids the need for special security
 * configuration or code signing. <p>
 *
 * <b>Note:</b> Since it is difficult and often nearly impossible to determine
 * or control at runtime from where all classes are being loaded or which class
 * loader is doing the loading (and hence how relative path specifications
 * are resolved) under <b>'files_in_jar'</b> semantics, the <b>&lt;path&gt;</b>
 * component of the <b>res:</b> database connection <b>&lt;url&gt;</b> is always
 * taken to be relative to the default package and resource URL resolution is
 * always performed using the ClassLoader that loads the
 * org.hsqldb_voltpatches.persist.Logger class. That is, if the <b>&lt;path&gt;</b>
 * component does not start with '/', then'/' is prepended when obtaining the
 * resource URLs used to read the database files, and only the effective class
 * path of org.hsqldb_voltpatches.persist.Logger's ClassLoader is searched. <p>
 *
 * <hr>
 *
 * For more information about HSQLDB file structure, various database modes
 * and other attributes such as those controlled through the HSQLDB properties
 * files, please read the general documentation, especially the Advanced Users
 * Guide. <p>
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
 * defined in {@link JDBCResultSet JDBCResultSet}. <p>
 *
 * In a JRE 1.1.x environment, calling JDBC 2 methods that take or return the
 * JDBC 2+ <code>ResultSet</code> values can be achieved by referring
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
 * <b>JDBC 4.0 Notes:</b><p>
 *
 * Starting with JDBC 4.0 (JDK 1.6), the <code>DriverManager</code> methods
 * <code>getConnection</code> and <code>getDrivers</code> have been
 * enhanced to support the Java Standard Edition Service Provider mechanism.
 * When built under a Java runtime that supports JDBC 4.0, HSQLDB distribution
 * jars containing the Driver implementatiton also include the file
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
 *
 * (fredt@users)<br>
 * (boucherb@users)<p>
 *
 * </div> <!-- end release-specific documentation -->
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @revised JDK 1.6, HSQLDB 1.9.0
 * @see JDBCDriver
 * @see JDBCStatement
 * @see JDBCParameterMetaData
 * @see JDBCCallableStatement
 * @see JDBCResultSet
 * @see JDBCDatabaseMetaData
 * @see java.sql.DriverManager#getConnection
 * @see java.sql.Statement
 * @see java.sql.ResultSet
 * @see java.sql.DatabaseMetaData
 */
public class JDBCConnection implements Connection {

// ----------------------------------- JDBC 1 -------------------------------

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>Statement</code> object for sending
     * SQL statements to the database.
     * SQL statements without parameters are normally
     * executed using <code>Statement</code> objects. If the same SQL statement
     * is executed many times, it may be more efficient to use a
     * <code>PreparedStatement</code> object.
     * <P>
     * Result sets created using the returned <code>Statement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.7.2, support for precompilation at the engine level
     * has been implemented, so it is now much more efficient and performant
     * to use a <code>PreparedStatement</code> object if the same short-running
     * SQL statement is to be executed many times. <p>
     *
     * HSQLDB supports <code>TYPE_FORWARD_ONLY</code>,
     * <code>TYPE_SCROLL_INSENSITIVE</code> and <code>CONCUR_READ_ONLY</code>
     * results. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return a new default <code>Statement</code> object
     * @exception SQLException if a database access error occurs
     * (JDBC4 clarification:)
     * or this method is called on a closed connection
     * @see #createStatement(int,int)
     * @see #createStatement(int,int,int)
     */
    public synchronized Statement createStatement() throws SQLException {

        checkClosed();

        Statement stmt = new JDBCStatement(this,
            JDBCResultSet.TYPE_FORWARD_ONLY, JDBCResultSet.CONCUR_READ_ONLY,
            rsHoldability);

        return stmt;
    }

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>PreparedStatement</code> object for sending
     * parameterized SQL statements to the database.
     * <P>
     * A SQL statement with or without IN parameters can be
     * pre-compiled and stored in a <code>PreparedStatement</code> object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     *
     * <P><B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method <code>prepareStatement</code> will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the <code>PreparedStatement</code>
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain <code>SQLException</code> objects.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.7.2, support for precompilation at the engine level
     * has been implemented, so it is now much more efficient and performant
     * to use a <code>PreparedStatement</code> object if the same short-running
     * SQL statement is to be executed many times. <p>
     *
     * The support for and behaviour of PreparedStatment complies with SQL and
     * JDBC standards.  Please read the introductory section
     * of the documentation for ${link JDBCParameterMetaData}. <P>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     * parameter placeholders
     * @return a new default <code>PreparedStatement</code> object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database access error occurs
     * (JDBC4 clarification:)
     * or this method is called on a closed connection
     * @see #prepareStatement(String,int,int)
     */
    public synchronized PreparedStatement prepareStatement(
            String sql) throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>CallableStatement</code> object for calling
     * database stored procedures.
     * The <code>CallableStatement</code> object provides
     * methods for setting up its IN and OUT parameters, and
     * methods for executing the call to a stored procedure.
     *
     * <P><B>Note:</B> This method is optimized for handling stored
     * procedure call statements. Some drivers may send the call
     * statement to the database when the method <code>prepareCall</code>
     * is done; others
     * may wait until the <code>CallableStatement</code> object
     * is executed. This has no
     * direct effect on users; however, it does affect which method
     * throws certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>CallableStatement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.7.2, the support for and behaviour of
     * CallableStatement has changed.  Please read the introductory section
     * of the documentation for org.hsqldb_voltpatches.jdbc.JDBCCallableStatement.
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?'
     * parameter placeholders. (JDBC4 clarification:) Typically this statement is specified using JDBC
     * call escape syntax.
     * @return a new default <code>CallableStatement</code> object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database access error occurs
     * (JDBC4 clarification:)
     * or this method is called on a closed connection
     * @see #prepareCall(String,int,int)
     */
    public synchronized CallableStatement prepareCall(
            String sql) throws SQLException {

        CallableStatement stmt;

        checkClosed();

        try {
            stmt = new JDBCCallableStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability);

            return stmt;
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Converts the given SQL statement into the system's native SQL grammar.
     * A driver may convert the JDBC SQL grammar into its system's
     * native SQL grammar prior to sending it. This method returns the
     * native form of the statement that the driver would have sent.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB converts the JDBC SQL
     * grammar into the system's native SQL grammar prior to sending
     * it, if escape processing is set true; this method returns the
     * native form of the statement that the driver would send in place
     * of client-specified JDBC SQL grammar. <p>
     *
     * Before 1.7.2, escape processing was incomplete and
     * also broken in terms of support for nested escapes. <p>
     *
     * Starting with 1.7.2, escape processing is complete and handles nesting
     * to arbitrary depth, but enforces a very strict interpretation of the
     * syntax and does not detect or process SQL comments. <p>
     *
     * In essence, the HSQLDB engine directly handles the prescribed syntax
     * and date / time formats specified internal to the JDBC escapes.
     * It also directly offers the XOpen / ODBC extended scalar
     * functions specified available internal to the {fn ...} JDBC escape.
     * As such, the driver simply removes the curly braces and JDBC escape
     * codes in the simplest and fastest fashion possible, by replacing them
     * with whitespace.
     *
     * But to avoid a great deal of complexity, certain forms of input
     * whitespace are currently not recognised.  For instance,
     * the driver handles "{?= call ...}" but not "{ ?= call ...} or
     * "{? = call ...}" <p>
     *
     * Also, comments embedded in SQL are currently not detected or
     * processed and thus may have unexpected effects on the output
     * of this method, for instance causing otherwise valid SQL to become
     * invalid. It is especially important to be aware of this because escape
     * processing is set true by default for Statement objects and is always
     * set true when producing a PreparedStatement from prepareStatement()
     * or CallableStatement from prepareCall().  Currently, it is simply
     * recommended to avoid submitting SQL having comments containing JDBC
     * escape sequence patterns and/or single or double quotation marks,
     * as this will avoid any potential problems.
     *
     * It is intended to implement a less strict handling of whitespace and
     * proper processing of SQL comments at some point in the near future.
     *
     * In any event, 1.7.2 now correctly processes the following JDBC escape
     * forms to arbitrary nesting depth, but only if the exact whitespace
     * layout described below is used: <p>
     *
     * <ol>
     * <li>{call ...}
     * <li>{?= call ...}
     * <li>{fn ...}
     * <li>{oj ...}
     * <li>{d ...}
     * <li>{t ...}
     * <li>{ts ...}
     * </ol> <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?'
     * parameter placeholders
     * @return the native form of this statement
     * @exception SQLException if a database access error occurs
     * (JDBC4 clarification:)
     * or this method is called on a closed connection
     */
    public synchronized String nativeSQL(
            final String sql) throws SQLException {

        // boucherb@users 20030405
        // FIXME: does not work properly for nested escapes
        //       e.g.  {call ...(...,{ts '...'},....)} does not work
        // boucherb@users 20030817
        // TESTME: First kick at the FIXME cat done.  Now lots of testing
        // and refinment
        checkClosed();

        // CHECKME:  Thow or return null if input is null?
        if (sql == null || sql.length() == 0 || sql.indexOf('{') == -1) {
            return sql;
        }

        // boolean   changed = false;
        int          state = 0;
        int          len   = sql.length();
        int          nest  = 0;
        StringBuffer sb    = new StringBuffer(sql.length());    //avoid 16 extra
        String       msg;

        //--
        final int outside_all                         = 0;
        final int outside_escape_inside_single_quotes = 1;
        final int outside_escape_inside_double_quotes = 2;

        //--
        final int inside_escape                      = 3;
        final int inside_escape_inside_single_quotes = 4;
        final int inside_escape_inside_double_quotes = 5;

        /** @todo */

        // final int inside_single_line_comment          = 6;
        // final int inside_multi_line_comment           = 7;
        // Better than old way for large inputs and for avoiding GC overhead;
        // toString() reuses internal char[], reducing memory requirment
        // and garbage items 3:2
        sb.append(sql);

        for (int i = 0; i < len; i++) {
            char c = sb.charAt(i);

            switch (state) {

                case outside_all :    // Not inside an escape or quotes
                    if (c == '\'') {
                        state = outside_escape_inside_single_quotes;
                    } else if (c == '"') {
                        state = outside_escape_inside_double_quotes;
                    } else if (c == '{') {
                        i = onStartEscapeSequence(sql, sb, i);

                        // changed = true;
                        nest++;

                        state = inside_escape;
                    }

                    break;
                case outside_escape_inside_single_quotes :    // inside ' ' only
                case inside_escape_inside_single_quotes :     // inside { } and ' '
                    if (c == '\'') {
                        state -= 1;
                    }

                    break;
                case outside_escape_inside_double_quotes :    // inside " " only
                case inside_escape_inside_double_quotes :     // inside { } and " "
                    if (c == '"') {
                        state -= 2;
                    }

                    break;
                case inside_escape :                          // inside { }
                    if (c == '\'') {
                        state = inside_escape_inside_single_quotes;
                    } else if (c == '"') {
                        state = inside_escape_inside_double_quotes;
                    } else if (c == '}') {
                        sb.setCharAt(i, ' ');

                        // changed = true;
                        nest--;

                        state = (nest == 0) ? outside_all
                                : inside_escape;
                    } else if (c == '{') {
                        i = onStartEscapeSequence(sql, sb, i);

                        // changed = true;
                        nest++;

                        state = inside_escape;
                    }
            }
        }

        return sb.toString();
    }

    /**
     * @todo - semantics of autocommit regarding commit when the ResultSet is closed
     */

    /**
     * <!-- start generic documentation -->
     *
     * Sets this connection's auto-commit mode to the given state.
     * If a connection is in auto-commit mode, then all its SQL
     * statements will be executed and committed as individual
     * transactions.  Otherwise, its SQL statements are grouped into
     * transactions that are terminated by a call to either
     * the method <code>commit</code> or the method <code>rollback</code>.
     * By default, new connections are in auto-commit
     * mode.
     * <P>
     * The commit occurs when the statement completes. The time when the statement
     * completes depends on the type of SQL Statement:
     * <ul>
     * <li>For DML statements, such as Insert, Update or Delete, and DDL statements,
     * the statement is complete as soon as it has finished executing.
     * <li>For Select statements, the statement is complete when the associated result
     * set is closed.
     * <li>For <code>CallableStatement</code> objects or for statements that return
     * multiple results, the statement is complete
     * when all of the associated result sets have been closed, and all update
     * counts and output parameters have been retrieved.
     * </ul>
     * <P>
     * <B>NOTE:</B>  If this method is called during a transaction and the
     * auto-commit mode is changed, the transaction is committed.  If
     * <code>setAutoCommit</code> is called and the auto-commit mode is
     * not changed, the call is a no-op.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including HSQLDB 1.9.0, <p>
     *
     * <ol>
     *   <li> All rows of a result set are retrieved internally <em>
     *   before</em> the first row can actually be fetched.<br>
     *   Therefore, a statement can be considered complete as soon as
     *   any XXXStatement.executeXXX method returns. </li>
     *
     *   <li> Multiple result sets and output parameters are not yet
     *   supported. </li>
     * </ol>
     * <p>
     *
     * Starting with 1.9.0, HSQLDB may not return a result set to the network
     * client as a whole; the
     * generic documentation may apply.
     *
     * (boucherb@users) </div> <!-- end release-specific
     * documentation -->
     *
     * @param autoCommit <code>true</code> to enable auto-commit mode;
     *         <code>false</code> to disable it
     * @exception SQLException if a database access error occurs,
     *  (JDBC4 Clarification:)
     *  setAutoCommit(true) is called while participating in a distributed transaction,
     * or this method is called on a closed connection
     * @see #getAutoCommit
     */
    public synchronized void setAutoCommit(
            boolean autoCommit) throws SQLException {

        checkClosed();

        try {
            sessionProxy.setAutoCommit(autoCommit);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * Retrieves the current auto-commit mode for this <code>Connection</code>
     * object.
     *
     * @return the current state of this <code>Connection</code> object's
     *         auto-commit mode
     * @exception SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     * @see #setAutoCommit
     */
    public synchronized boolean getAutoCommit() throws SQLException {

        checkClosed();

        try {
            return sessionProxy.isAutoCommit();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Makes all changes made since the previous
     * commit/rollback permanent and releases any database locks
     * currently held by this <code>Connection</code> object.
     * This method should be
     * used only when auto-commit mode has been disabled.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     * </div><!-- end release-specific documentation -->
     *
     * @exception SQLException if a database access error occurs,
     * (JDBC4 Clarification:)
     * this method is called while participating in a distributed transaction,
     * if this method is called on a closed conection or this
     *            <code>Connection</code> object is in auto-commit mode
     * @see #setAutoCommit
     */
    public synchronized void commit() throws SQLException {

        checkClosed();

        try {
            sessionProxy.commit(false);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Undoes all changes made in the current transaction
     * and releases any database locks currently held
     * by this <code>Connection</code> object. This method should be
     * used only when auto-commit mode has been disabled.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.7.2, savepoints are fully supported both
     * in SQL and via the JDBC interface. <p>
     *
     * Using SQL, savepoints may be set, released and used in rollback
     * as follows:
     *
     * <pre>
     * SAVEPOINT &lt;savepoint-name&gt;
     * RELEASE SAVEPOINT &lt;savepoint-name&gt;
     * ROLLBACK TO SAVEPOINT &lt;savepoint-name&gt;
     * </pre>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @exception SQLException if a database access error occurs,
     * (JDBC4 Clarification:)
     * this method is called while participating in a distributed transaction,
     * this method is called on a closed connection or this
     *            <code>Connection</code> object is in auto-commit mode
     * @see #setAutoCommit
     */
    public synchronized void rollback() throws SQLException {

        checkClosed();

        try {
            sessionProxy.rollback(false);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Releases this <code>Connection</code> object's database and JDBC resources
     * immediately instead of waiting for them to be automatically released.
     * <P>
     * Calling the method <code>close</code> on a <code>Connection</code>
     * object that is already closed is a no-op.
     * <P>
     * It is <b>strongly recommended</b> that an application explicitly
     * commits or rolls back an active transaction prior to calling the
     * <code>close</code> method.  If the <code>close</code> method is called
     * and there is an active transaction, the results are implementation-defined.
     * <P>
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.7.2, HSQLDB <code>INTERNAL</code> <code>Connection</code>
     * objects are not closable from JDBC client code. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @exception SQLException SQLException if a database access error occurs
     */
    public synchronized void close() throws SQLException {

        // Changed to synchronized above because
        // we would not want a sessionProxy.close()
        // operation to occur concurrently with a
        // statementXXX.executeXXX operation.
        if (isInternal || isClosed) {
            return;
        }
        isClosed = true;

        if (sessionProxy != null) {
            sessionProxy.close();
        }
        sessionProxy   = null;
        rootWarning    = null;
        connProperties = null;
    }

    /**
     * Retrieves whether this <code>Connection</code> object has been
     * closed.  A connection is closed if the method <code>close</code>
     * has been called on it or if certain fatal errors have occurred.
     * This method is guaranteed to return <code>true</code> only when
     * it is called after the method <code>Connection.close</code> has
     * been called.
     * <P>
     * This method generally cannot be called to determine whether a
     * connection to a database is valid or invalid.  A typical client
     * can determine that a connection is invalid by catching any
     * exceptions that might be thrown when an operation is attempted.
     *
     * @return <code>true</code> if this <code>Connection</code> object
     *         is closed; <code>false</code> if it is still open
     * @exception SQLException if a database access error occurs
     */
    public synchronized boolean isClosed() throws SQLException {
        return isClosed;
    }

    //======================================================================
    // Advanced features:

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves a <code>DatabaseMetaData</code> object that contains
     * metadata about the database to which this
     * <code>Connection</code> object represents a connection.
     * The metadata includes information about the database's
     * tables, its supported SQL grammar, its stored
     * procedures, the capabilities of this connection, and so on.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 essentially supports full database metadata. <p>
     *
     * For discussion in greater detail, please follow the link to the
     * overview for JDBCDatabaseMetaData, below.
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return a <code>DatabaseMetaData</code> object for this
     *         <code>Connection</code> object
     * @exception  SQLException if a database access error occurs
     * (JDBC4 Clarification)
     * or this method is called on a closed connection
     * @see JDBCDatabaseMetaData
     */
    public synchronized DatabaseMetaData getMetaData() throws SQLException {

        checkClosed();

        return new JDBCDatabaseMetaData(this);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Puts this connection in read-only mode as a hint to the driver to enable
     * database optimizations.
     *
     * <P><B>Note:</B> This method cannot be called during a transaction.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports the SQL standard, which will not allow calls to
     * this method to succeed during a transaction.<p>
     *
     * Additionally, HSQLDB provides a way to put a whole database in
     * read-only mode. This is done by manually adding the line
     * 'readonly=true' to the database's .properties file while the
     * database is offline. Upon restart, all connections will be
     * readonly, since the entire database will be readonly. To take
     * a database out of readonly mode, simply take the database
     * offline and remove the line 'readonly=true' from the
     * database's .properties file. Upon restart, the database will
     * be in regular (read-write) mode. <p>
     *
     * When a database is put in readonly mode, its files are opened
     * in readonly mode, making it possible to create CD-based
     * readonly databases. To create a CD-based readonly database
     * that has CACHED tables and whose .data file is suspected of
     * being highly fragmented, it is recommended that the database
     * first be SHUTDOWN COMPACTed before copying the database
     * files to CD. This will reduce the space required and may
     * improve access times against the .data file which holds the
     * CACHED table data. <p>
     *
     * Starting with 1.7.2, an alternate approach to opimizing the
     * .data file before creating a CD-based readonly database is to issue
     * the CHECKPOINT DEFRAG command followed by SHUTDOWN to take the
     * database offline in preparation to burn the database files to CD. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param readOnly <code>true</code> enables read-only mode;
     *        <code>false</code> disables it
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     *  method is called on a closed connection or this
     *            method is called during a transaction
     */
    public synchronized void setReadOnly(
            boolean readOnly) throws SQLException {

        checkClosed();

        try {
            sessionProxy.setReadOnlyDefault(readOnly);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * Retrieves whether this <code>Connection</code>
     * object is in read-only mode.
     *
     * @return <code>true</code> if this <code>Connection</code> object
     *         is read-only; <code>false</code> otherwise
     * @exception SQLException SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     */
    public synchronized boolean isReadOnly() throws SQLException {

        checkClosed();

        try {
            return sessionProxy.isReadOnlyDefault();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Sets the given catalog name in order to select
     * a subspace of this <code>Connection</code> object's database
     * in which to work.
     * <P>
     *
     * (JDBC4 Clarification:)<p>
     * If the driver does not support catalogs, it will
     * silently ignore this request.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports a single catalog per database. If the given catalog name
     * is not the same as the database catalog name, this method throws an
     * error. <p>
     * </div> <!-- end release-specific documentation -->
     *
     * @param catalog the name of a catalog (subspace in this
     *        <code>Connection</code> object's database) in which to work
     * @exception SQLException if a database access error occurs
     * (JDBC4 Clarification)
     * or this method is called on a closed connection
     * @see #getCatalog
     */
    public synchronized void setCatalog(String catalog) throws SQLException {

        checkClosed();

        try {
            sessionProxy.setAttribute(SessionInterface.INFO_CATALOG, catalog);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves this <code>Connection</code> object's current catalog name.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports a single catalog per database. This method
     * returns the catalog name for the current database
     * error. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return the current catalog name or <code>null</code> if there is none
     * @exception SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     * @see #setCatalog
     */
    public synchronized String getCatalog() throws SQLException {

        checkClosed();

        try {
            return (String) sessionProxy.getAttribute(
                SessionInterface.INFO_CATALOG);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * <code>Connection</code> object to the one given.
     * The constants defined in the interface <code>Connection</code>
     * are the possible transaction isolation levels.
     * <P>
     * <B>Note:</B> If this method is called during a transaction, the result
     * is implementation-defined.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 2.0 supports all isolation levels. If database transaction
     * control is MVCC, <code>Connection.TRANSACTION_READ_UNCOMMITED</code>
     * is promoted to <code>Connection.TRANSACTION_READ_COMMITED</code>.
     * Calling this method during a transaction always fails.<p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param level one of the following <code>Connection</code> constants:
     *        <code>Connection.TRANSACTION_READ_UNCOMMITTED</code>,
     *        <code>Connection.TRANSACTION_READ_COMMITTED</code>,
     *        <code>Connection.TRANSACTION_REPEATABLE_READ</code>, or
     *        <code>Connection.TRANSACTION_SERIALIZABLE</code>.
     *        (Note that <code>Connection.TRANSACTION_NONE</code> cannot be used
     *        because it specifies that transactions are not supported.)
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     * method is called on a closed connection
     * (:JDBC4 End Clarification)
     *            or the given parameter is not one of the <code>Connection</code>
     *            constants
     * @see JDBCDatabaseMetaData#supportsTransactionIsolationLevel
     * @see #getTransactionIsolation
     */
    public synchronized void setTransactionIsolation(
            int level) throws SQLException {

        checkClosed();

        switch (level) {

            case TRANSACTION_READ_UNCOMMITTED :
            case TRANSACTION_READ_COMMITTED :
            case TRANSACTION_REPEATABLE_READ :
            case TRANSACTION_SERIALIZABLE :
                break;
            default :
                throw Util.invalidArgument();
        }

        try {
            sessionProxy.setIsolationDefault(level);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves this <code>Connection</code> object's current
     * transaction isolation level.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 2.0 supports all isolation levels. If database transaction
     * control is MVCC, <code>Connection.TRANSACTION_READ_UNCOMMITED</code>
     * is promoted to <code>Connection.TRANSACTION_READ_COMMITED</code>.
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return the current transaction isolation level, which will be one
     *         of the following constants:
     *        <code>Connection.TRANSACTION_READ_UNCOMMITTED</code>,
     *        <code>Connection.TRANSACTION_READ_COMMITTED</code>,
     *        <code>Connection.TRANSACTION_REPEATABLE_READ</code>,
     *        <code>Connection.TRANSACTION_SERIALIZABLE</code>, or
     *        <code>Connection.TRANSACTION_NONE</code>.
     * @exception SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     * @see JDBCDatabaseMetaData#supportsTransactionIsolationLevel
     * @see #setTransactionIsolation
     */
    public synchronized int getTransactionIsolation() throws SQLException {

        checkClosed();

        try {
            return sessionProxy.getIsolation();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the first warning reported by calls on this
     * <code>Connection</code> object.  If there is more than one
     * warning, subsequent warnings will be chained to the first one
     * and can be retrieved by calling the method
     * <code>SQLWarning.getNextWarning</code> on the warning
     * that was retrieved previously.
     * <P>
     * This method may not be
     * called on a closed connection; doing so will cause an
     * <code>SQLException</code> to be thrown.
     *
     * <P><B>Note:</B> Subsequent warnings will be chained to this
     * SQLWarning.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB produces warnings whenever a createStatement(),
     * prepareStatement() or prepareCall() invocation requests an unsupported
     * but defined combination of result set type, concurrency and holdability,
     * such that another set is substituted.<p>
     * Other warnings are typically raised during the execution of data change
     * and query statements.<p>
     *
     * </div> <!-- end release-specific documentation -->
     * @return the first <code>SQLWarning</code> object or <code>null</code>
     *         if there are none
     * @exception SQLException if a database access error occurs or
     *            this method is called on a closed connection
     * @see java.sql.SQLWarning
     */
    public synchronized SQLWarning getWarnings() throws SQLException {

        checkClosed();

        synchronized (rootWarning_mutex) {
            return rootWarning;
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Clears all warnings reported for this <code>Connection</code> object.
     * After a call to this method, the method <code>getWarnings</code>
     * returns <code>null</code> until a new warning is
     * reported for this <code>Connection</code> object.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * The standard behaviour is implemented. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @exception SQLException SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     */
    public synchronized void clearWarnings() throws SQLException {

        checkClosed();

        synchronized (rootWarning_mutex) {
            rootWarning = null;
        }
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>Statement</code> object that will generate
     * <code>ResultSet</code> objects with the given type and concurrency.
     * This method is the same as the <code>createStatement</code> method
     * above, but it allows the default result set
     * type and concurrency to be overridden.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports <code>TYPE_FORWARD_ONLY</code>,
     * <code>TYPE_SCROLL_INSENSITIVE</code>,
     * <code>CONCUR_READ_ONLY</code>,
     * <code>CONCUR_UPDATABLE</code>
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param resultSetType a result set type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of
     *        <code>ResultSet.CONCUR_READ_ONLY</code> or
     *        <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new <code>Statement</code> object that will generate
     *         <code>ResultSet</code> objects with the given type and
     *         concurrency
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     * method is called on a closed connection
     * (:JDBC4 Clarification)
     *         or the given parameters are not <code>ResultSet</code>
     *         constants indicating type and concurrency
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type and result set concurrency.
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *  for JDBCConnection)
     */
    public synchronized Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLException {

        checkClosed();

        return new JDBCStatement(this, resultSetType, resultSetConcurrency,
                                 rsHoldability);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>PreparedStatement</code> object that will generate
     * <code>ResultSet</code> objects with the given type and concurrency.
     * This method is the same as the <code>prepareStatement</code> method
     * above, but it allows the default result set
     * type and concurrency to be overridden.
     * (JDBC4 Clarification:)
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports <code>TYPE_FORWARD_ONLY</code>,
     * <code>TYPE_SCROLL_INSENSITIVE</code>,
     * <code>CONCUR_READ_ONLY</code>,
     * <code>CONCUR_UPDATABLE</code>
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql a <code>String</code> object that is the SQL statement to
     *            be sent to the database; may contain one or more '?' IN
     *            parameters
     * @param resultSetType a result set type; one of
     *         <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of
     *         <code>ResultSet.CONCUR_READ_ONLY</code> or
     *         <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new PreparedStatement object containing the
     * pre-compiled SQL statement that will produce <code>ResultSet</code>
     * objects with the given type and concurrency
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     * method is called on a closed connection
     * (:JDBC4 Clarification)
     *         or the given parameters are not <code>ResultSet</code>
     *         constants indicating type and concurrency
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type and result set concurrency.
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *  for JDBCConnection)
     */
    public synchronized PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency) throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(this, sql, resultSetType,
                    resultSetConcurrency, rsHoldability,
                    ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>CallableStatement</code> object that will generate
     * <code>ResultSet</code> objects with the given type and concurrency.
     * This method is the same as the <code>prepareCall</code> method
     * above, but it allows the default result set
     * type and concurrency to be overridden.
     * (JDBC4 Clarification:)
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports <code>TYPE_FORWARD_ONLY</code>,
     * <code>TYPE_SCROLL_INSENSITIVE</code>,
     * <code>CONCUR_READ_ONLY</code>,
     * <code>CONCUR_UPDATABLE</code>
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql a <code>String</code> object that is the SQL statement to
     *            be sent to the database; may contain on or more '?' parameters
     * @param resultSetType a result set type; one of
     *         <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency a concurrency type; one of
     *         <code>ResultSet.CONCUR_READ_ONLY</code> or
     *         <code>ResultSet.CONCUR_UPDATABLE</code>
     * @return a new <code>CallableStatement</code> object containing the
     * pre-compiled SQL statement that will produce <code>ResultSet</code>
     * objects with the given type and concurrency
     * @exception SQLException if a database access error occurs, this method
     * (JDBC4 Clarification:)
     * is called on a closed connection
     * (:JDBC4 Clarification)
     *         or the given parameters are not <code>ResultSet</code>
     *         constants indicating type and concurrency
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type and result set concurrency.
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     * for JDBCConnection)
     */
    public synchronized CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency) throws SQLException {

        checkClosed();

        try {
            return new JDBCCallableStatement(this, sql, resultSetType,
                    resultSetConcurrency, rsHoldability);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the <code>Map</code> object associated with this
     * <code>Connection</code> object.
     * Unless the application has added an entry, the type map returned
     * will be empty.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * For compatibility, HSQLDB returns an empty map. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return the <code>java.util.Map</code> object associated
     *         with this <code>Connection</code> object
     * @exception SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCConnection)
     */

//#ifdef JAVA6
    public synchronized java.util
            .Map<java.lang.String,
                 java.lang.Class<?>> getTypeMap() throws SQLException {

        checkClosed();

        return new java.util.HashMap<java.lang.String, java.lang.Class<?>>();
    }

//#else
/*
    public synchronized Map getTypeMap() throws SQLException {

        checkClosed();

        return new java.util.HashMap();
    }
*/

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Installs the given <code>TypeMap</code> object as the type map for
     * this <code>Connection</code> object.  The type map will be used for the
     * custom mapping of SQL structured types and distinct types.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. Calling this
     * method always throws a <code>SQLException</code>, stating that
     * the function is not supported. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param map the <code>java.util.Map</code> object to install
     *        as the replacement for this <code>Connection</code>
     *        object's default type map
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     * method is called on a closed connection or
     * (:JDBC4 Clarification)
     *        the given parameter is not a <code>java.util.Map</code>
     *        object
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCConnection)
     * @see #getTypeMap
     */
//#ifdef JAVA6
    public synchronized void setTypeMap(Map<String,
            Class<?>> map) throws SQLException {

        checkClosed();

        throw Util.notSupported();
    }

//#else
/*
    public synchronized void setTypeMap(Map map) throws SQLException {

        checkClosed();

        throw Util.notSupported();
    }
*/

//#endif JAVA6
    //--------------------------JDBC 3.0-----------------------------

    /**
     * <!-- start generic documentation -->
     *
     * (JDBC4 Clarification:)
     * Changes the default holdability of <code>ResultSet</code> objects
     * created using this <code>Connection</code> object to the given
     * holdability.  The default holdability of <code>ResultSet</code> objects
     * can be be determined by invoking
     * {@link DatabaseMetaData#getResultSetHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div> <!-- end release-specific documentation -->
     *
     * @param holdability a <code>ResultSet</code> holdability constant; one of
     *        <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *        <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @throws SQLException if a database access occurs, this method is called
     * (JDBC4 Clarification:)
     * on a closed connection, or the given parameter
     * (:JDBC4 Clkarification)
     *         is not a <code>ResultSet</code> constant indicating holdability
     * @exception SQLFeatureNotSupportedException if the given holdability is not supported
     * @see #getHoldability
     * @see DatabaseMetaData#getResultSetHoldability
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized void setHoldability(
            int holdability) throws SQLException {

        checkClosed();

        switch (holdability) {

            case JDBCResultSet.HOLD_CURSORS_OVER_COMMIT :
            case JDBCResultSet.CLOSE_CURSORS_AT_COMMIT :
                break;
            default :
                throw Util.invalidArgument();
        }
        rsHoldability = holdability;
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the current holdability of <code>ResultSet</code> objects
     * created using this <code>Connection</code> object.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB returns the current holdability.<p>
     *
     * The default is HOLD_CURSORS_OVER_COMMIT. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return the holdability, one of
     *        <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *        <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @throws SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     * @see #setHoldability
     * @see DatabaseMetaData#getResultSetHoldability
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized int getHoldability() throws SQLException {

        checkClosed();

        return rsHoldability;
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Creates an unnamed savepoint in the current transaction and
     * returns the new <code>Savepoint</code> object that represents it.
     *
     * <p>(JDBC4 clarification:)
     * <p> if setSavepoint is invoked outside of an active transaction, a transaction will be started at this newly created
     * savepoint.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.9.0, HSQLDB supports this feature. <p>
     *
     * Note: Unnamed savepoints are not part of the SQL:2003 standard.
     * Use setSavepoint(String name) instead. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return the new <code>Savepoint</code> object
     * @exception SQLException if a database access error occurs,
     * (JDBC4 Clarification:)
     * this method is called while participating in a distributed transaction,
     * this method is called on a closed connection
     *            or this <code>Connection</code> object is currently in
     *            auto-commit mode
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized Savepoint setSavepoint() throws SQLException {

        checkClosed();

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            throw Util.sqlException(ErrorCode.X_3B001);
        }

        try {
            sessionProxy.savepoint("SYSTEM_SAVEPOINT");
        } catch (HsqlException e) {
            Util.throwError(e);
        }

        return new JDBCSavepoint(this);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Creates a savepoint with the given name in the current transaction
     * and returns the new <code>Savepoint</code> object that represents it.
     *
     * <p> if setSavepoint is invoked outside of an active transaction, a
     * transaction will be started at this newly created savepoint.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Previous to JDBC 4, if the connection is autoCommit,
     * setting a savepoint has no effect, as it is cleared upon the execution
     * of the next transactional statement. When built for JDBC 4, this method
     * throws an SQLException when this <tt>Connection</tt> object is currently
     * in auto-commit mode, as per the JDBC 4 standard.
     * </div> <!-- end release-specific documentation -->
     *
     * @param name a <code>String</code> containing the name of the savepoint
     * @return the new <code>Savepoint</code> object
     * @exception SQLException if a database access error occurs,
     * (JDBC4 Clarification:)
     *      this method is called while participating in a distributed transaction,
     * this method is called on a closed connection
     *            or this <code>Connection</code> object is currently in
     *            auto-commit mode
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized Savepoint setSavepoint(
            String name) throws SQLException {

        checkClosed();

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            throw Util.sqlException(ErrorCode.X_3B001);
        }

        if (name == null) {
            throw Util.nullArgument();
        }

        if ("SYSTEM_SAVEPOINT".equals(name)) {
            throw Util.invalidArgument();
        }

        try {
            sessionProxy.savepoint(name);
        } catch (HsqlException e) {
            Util.throwError(e);
        }

        return new JDBCSavepoint(name, this);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Undoes all changes made after the given <code>Savepoint</code> object
     * was set.
     * <P>
     * This method should be used only when auto-commit has been disabled.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Previous to JDBC 4, <tt>JDBCSavepoint</tt> objects are valid for the life of
     * the originating <tt>Connection</tt> object and hence can be used
     * interchangably, as long as they have equal savepoint names. <p>
     *
     * When built for JDBC 4, <tt>JDBCConnection</tt> objects invalidate
     * <tt>JDBCSavepoint</tt> objects when auto-commit mode is entered as well
     * as when they are used to successfully release or roll back to a named SQL
     * savepoint.  As per the JDBC 4 standard, when built for JDBC 4, this
     * method throws an <tt>SQLException</tt> when this <tt>Connection</tt>
     * object is currently in auto-commit mode and an invalidated
     * <tt>JDBCSavepoint</tt> is specified.
     * </div> <!-- end release-specific documentation -->
     *
     * @param savepoint the <code>Savepoint</code> object to roll back to
     * @exception SQLException if a database access error occurs,
     * this method is called while participating in a distributed transaction,
     * this method is called on a closed connection,
     *            the <code>Savepoint</code> object is no longer valid,
     *            or this <code>Connection</code> object is currently in
     *            auto-commit mode
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @see #rollback
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized void rollback(
            Savepoint savepoint) throws SQLException {

        JDBCSavepoint sp;

        checkClosed();

        if (savepoint == null) {
            throw Util.nullArgument();
        }

        if (!(savepoint instanceof JDBCSavepoint)) {
            String msg = Error.getMessage(ErrorCode.X_3B001);

            throw Util.invalidArgument(msg);
        }
        sp = (JDBCSavepoint) savepoint;

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && sp.name == null) {
            String msg = Error.getMessage(ErrorCode.X_3B001);

            throw Util.invalidArgument(msg);
        }

        if (this != sp.connection) {
            String msg = Error.getMessage(ErrorCode.X_3B001);

            throw Util.invalidArgument(msg);
        }

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            sp.name       = null;
            sp.connection = null;

            throw Util.sqlException(ErrorCode.X_3B001);
        }

        try {
            sessionProxy.rollbackToSavepoint(sp.name);

            if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4) {
                sp.connection = null;
                sp.name       = null;
            }
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Removes the specified <code>Savepoint</code> (JDBC4 Clarification:) and subsequent <code>Savepoint</code> objects from the current
     * transaction. Any reference to the savepoint after it have been removed
     * will cause an <code>SQLException</code> to be thrown.
     *
     * <!-- end generic documentation -->
     *
     *
     * <b>HSLQDB Note:</b><p>
     *
     * Previous to JDBC 4, <tt>JDBCSavepoint</tt> objects are valid for the life of
     * the originating <tt>Connection</tt> object and hence can be used
     * interchangably, as long as they have equal savepoint names. <p>
     *
     * When built for JDBC 4, <tt>JDBCConnection</tt> objects invalidate
     * <tt>JDBCSavepoint</tt> objects when auto-commit mode is entered as well
     * as when they are used to successfully release or roll back to a named SQL
     * savepoint.  As per the JDBC 4 standard, when built for JDBC 4, this
     * method throws an <tt>SQLException</tt> when this <tt>Connection</tt>
     * object is currently in auto-commit mode and when an invalidated
     * <tt>JDBCSavepoint</tt> is specified. <p>
     *
     * @param savepoint the <code>Savepoint</code> object to be removed
     * @exception SQLException if a database access error occurs, this
     *  (JDBC4 Clarification:)
     *  method is called on a closed connection or
     *            the given <code>Savepoint</code> object is not a valid
     *            savepoint in the current transaction
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized void releaseSavepoint(
            Savepoint savepoint) throws SQLException {

        JDBCSavepoint sp;
        Result        req;

        checkClosed();

        if (savepoint == null) {
            throw Util.nullArgument();
        }

        if (!(savepoint instanceof JDBCSavepoint)) {
            String msg = Error.getMessage(ErrorCode.X_3B001);

            throw Util.invalidArgument(msg);
        }
        sp = (JDBCSavepoint) savepoint;

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && sp.name == null) {
            String msg = Error.getMessage(ErrorCode.X_3B001);

            throw Util.invalidArgument(msg);
        }

        if (this != sp.connection) {
            String msg = Error.getMessage(ErrorCode.X_3B001);

            throw Util.invalidArgument(msg);
        }

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            sp.name       = null;
            sp.connection = null;

            throw Util.sqlException(ErrorCode.X_3B001);
        }

        try {
            sessionProxy.releaseSavepoint(sp.name);

            if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4) {
                sp.connection = null;
                sp.name       = null;
            }
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>Statement</code> object that will generate
     * <code>ResultSet</code> objects with the given type, concurrency,
     * and holdability.
     * This method is the same as the <code>createStatement</code> method
     * above, but it allows the default result set
     * type, concurrency, and holdability to be overridden.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports <code>TYPE_FORWARD_ONLY</code>,
     * <code>TYPE_SCROLL_INSENSITIVE</code>,
     * <code>CONCUR_READ_ONLY</code>,
     * <code>CONCUR_UPDATABLE</code>
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param resultSetType one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.CONCUR_READ_ONLY</code> or
     *         <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>Statement</code> object that will generate
     *         <code>ResultSet</code> objects with the given type,
     *         concurrency, and holdability
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     * method is called on a closed connection
     * (:JDBC4 Clarification)
     *            or the given parameters are not <code>ResultSet</code>
     *            constants indicating type, concurrency, and holdability
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type, result set holdability and result set concurrency.
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized Statement createStatement(int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        checkClosed();

        return new JDBCStatement(this, resultSetType, resultSetConcurrency,
                                 resultSetHoldability);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>PreparedStatement</code> object that will generate
     * <code>ResultSet</code> objects with the given type, concurrency,
     * and holdability.
     * <P>
     * This method is the same as the <code>prepareStatement</code> method
     * above, but it allows the default result set
     * type, concurrency, and holdability to be overridden.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports <code>TYPE_FORWARD_ONLY</code>,
     * <code>TYPE_SCROLL_INSENSITIVE</code>,
     * <code>CONCUR_READ_ONLY</code>,
     * <code>CONCUR_UPDATABLE</code>
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql a <code>String</code> object that is the SQL statement to
     *            be sent to the database; may contain one or more '?' IN
     *            parameters
     * @param resultSetType one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.CONCUR_READ_ONLY</code> or
     *         <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>PreparedStatement</code> object, containing the
     *         pre-compiled SQL statement, that will generate
     *         <code>ResultSet</code> objects with the given type,
     *         concurrency, and holdability
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     * method is called on a closed connection
     * (:JDBC4 Clarification)
     *            or the given parameters are not <code>ResultSet</code>
     *            constants indicating type, concurrency, and holdability
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type, result set holdability and result set concurrency.
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(this, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability,
                    ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Creates a <code>CallableStatement</code> object that will generate
     * <code>ResultSet</code> objects with the given type and concurrency.
     * This method is the same as the <code>prepareCall</code> method
     * above, but it allows the default result set
     * type, result set concurrency type and holdability to be overridden.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports <code>TYPE_FORWARD_ONLY</code>,
     * <code>TYPE_SCROLL_INSENSITIVE</code>,
     * <code>CONCUR_READ_ONLY</code>,
     * <code>CONCUR_UPDATABLE</code>
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql a <code>String</code> object that is the SQL statement to
     *            be sent to the database; may contain on or more '?' parameters
     * @param resultSetType one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param resultSetConcurrency one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.CONCUR_READ_ONLY</code> or
     *         <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability one of the following <code>ResultSet</code>
     *        constants:
     *         <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @return a new <code>CallableStatement</code> object, containing the
     *         pre-compiled SQL statement, that will generate
     *         <code>ResultSet</code> objects with the given type,
     *         concurrency, and holdability
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     * method is called on a closed connection
     * (:JDBC4 Clarification)
     *            or the given parameters are not <code>ResultSet</code>
     *            constants indicating type, concurrency, and holdability
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type, result set holdability and result set concurrency.
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        checkClosed();

        try {
            return new JDBCCallableStatement(this, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Creates a default <code>PreparedStatement</code> object that has
     * the capability to retrieve auto-generated keys. The given constant
     * tells the driver whether it should make auto-generated keys
     * available for retrieval.  This parameter is ignored if the SQL statement
     * is not an <code>INSERT</code> statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * <B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method <code>prepareStatement</code> will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the <code>PreparedStatement</code>
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     * (JDBC4 Clarification:)
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Supported in 1.9.0.x
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @param autoGeneratedKeys a flag indicating whether auto-generated keys
     *        should be returned; one of
     *        <code>Statement.RETURN_GENERATED_KEYS</code> or
     *        <code>Statement.NO_GENERATED_KEYS</code>
     * @return a new <code>PreparedStatement</code> object, containing the
     *         pre-compiled SQL statement, that will have the capability of
     *         returning auto-generated keys
     * @exception SQLException if a database access error occurs, this
     * (JDBC4 Clarification:)
     *  method is called on a closed connection
     * (:JDBC4 Clarification)
     *         or the given parameter is not a <code>Statement</code>
     *         constant indicating whether auto-generated keys should be
     *         returned
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method with a constant of Statement.RETURN_GENERATED_KEYS
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized PreparedStatement prepareStatement(String sql,
            int autoGeneratedKeys) throws SQLException {

        checkClosed();

        try {
            if (autoGeneratedKeys != ResultConstants.RETURN_GENERATED_KEYS
                    && autoGeneratedKeys
                       != ResultConstants.RETURN_NO_GENERATED_KEYS) {
                throw Util.invalidArgument("autoGeneratedKeys");
            }

            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    autoGeneratedKeys, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     * Creates a default <code>PreparedStatement</code> object capable
     * of returning the auto-generated keys designated by the given array.
     * This array contains the indexes of the columns in the target
     * table that contain the auto-generated keys that should be made
     * available.  The driver will ignore the array if the SQL statement
     * is not an <code>INSERT</code> statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <p>
     * An SQL statement with or without IN parameters can be
     * pre-compiled and stored in a <code>PreparedStatement</code> object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method <code>prepareStatement</code> will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the <code>PreparedStatement</code>
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     * (JDBC4 Clarification:)
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Supported in 1.9.0
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @param columnIndexes an array of column indexes indicating the columns
     *        that should be returned from the inserted row or rows
     * @return a new <code>PreparedStatement</code> object, containing the
     *         pre-compiled statement, that is capable of returning the
     *         auto-generated keys designated by the given array of column
     *         indexes
     * @exception SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized PreparedStatement prepareStatement(String sql,
            int[] columnIndexes) throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES,
                    columnIndexes, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Creates a default <code>PreparedStatement</code> object capable
     * of returning the auto-generated keys designated by the given array.
     * This array contains the names of the columns in the target
     * table that contain the auto-generated keys that should be returned.
     * The driver will ignore the array if the SQL statement
     * is not an <code>INSERT</code> statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * An SQL statement with or without IN parameters can be
     * pre-compiled and stored in a <code>PreparedStatement</code> object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method <code>prepareStatement</code> will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the <code>PreparedStatement</code>
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     * (JDBC4 Clarification:)
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Supported in 1.9.0
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @param columnNames an array of column names indicating the columns
     *        that should be returned from the inserted row or rows
     * @return a new <code>PreparedStatement</code> object, containing the
     *         pre-compiled statement, that is capable of returning the
     *         auto-generated keys designated by the given array of column
     *         names
     * @exception SQLException if a database access error occurs
     * (JDBC4 Clarification:)
     * or this method is called on a closed connection
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     */
//#ifdef JAVA4
    public synchronized PreparedStatement prepareStatement(String sql,
            String[] columnNames) throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES, null,
                    columnNames);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

//#endif JAVA4
    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Constructs an object that implements the <code>Clob</code> interface. The object
     * returned initially contains no data.  The <code>setAsciiStream</code>,
     * <code>setCharacterStream</code> and <code>setString</code> methods of
     * the <code>Clob</code> interface may be used to add data to the <code>Clob</code>.
     * @return An object that implements the <code>Clob</code> interface
     * @throws SQLException if an object that implements the
     * <code>Clob</code> interface can not be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     *
     * @since JDK 1.6, HSQLDB 1.9.0
     */
    public Clob createClob() throws SQLException {

        checkClosed();

        return new JDBCClob();
    }

    /**
     * Constructs an object that implements the <code>Blob</code> interface. The object
     * returned initially contains no data.  The <code>setBinaryStream</code> and
     * <code>setBytes</code> methods of the <code>Blob</code> interface may be used to add data to
     * the <code>Blob</code>.
     * @return  An object that implements the <code>Blob</code> interface
     * @throws SQLException if an object that implements the
     * <code>Blob</code> interface can not be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     *
     * @since JDK 1.6, HSQLDB 1.9.0
     */
    public Blob createBlob() throws SQLException {

        checkClosed();

        return new JDBCBlob();
    }

    /**
     * Constructs an object that implements the <code>NClob</code> interface. The object
     * returned initially contains no data.  The <code>setAsciiStream</code>,
     * <code>setCharacterStream</code> and <code>setString</code> methods of the <code>NClob</code> interface may
     * be used to add data to the <code>NClob</code>.
     * @return An object that implements the <code>NClob</code> interface
     * @throws SQLException if an object that implements the
     * <code>NClob</code> interface can not be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     *
     * @since JDK 1.6, HSQLDB 1.9.0
     */

//#ifdef JAVA6
    public NClob createNClob() throws SQLException {

        checkClosed();

        return new JDBCNClob();
    }

//#endif JAVA6

    /**
     * Constructs an object that implements the <code>SQLXML</code> interface. The object
     * returned initially contains no data. The <code>createXmlStreamWriter</code> object and
     * <code>setString</code> method of the <code>SQLXML</code> interface may be used to add data to the <code>SQLXML</code>
     * object.
     * @return An object that implements the <code>SQLXML</code> interface
     * @throws SQLException if an object that implements the <code>SQLXML</code> interface can not
     * be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public SQLXML createSQLXML() throws SQLException {

        checkClosed();

        return new JDBCSQLXML();
    }

//#endif JAVA6

    /** @todo:  ThreadPool? HsqlTimer with callback? */

    /**
     * Returns true if the connection has not been closed and is still valid.
     * The driver shall submit a query on the connection or use some other
     * mechanism that positively verifies the connection is still valid when
     * this method is called.
     * <p>
     * The query submitted by the driver to validate the connection shall be
     * executed in the context of the current transaction.
     *
     * @param timeout -         The time in seconds to wait for the database operation
     *                                          used to validate the connection to complete.  If
     *                                          the timeout period expires before the operation
     *                                          completes, this method returns false.  A value of
     *                                          0 indicates a timeout is not applied to the
     *                                          database operation.
     * <p>
     * @return true if the connection is valid, false otherwise
     * @exception SQLException if the value supplied for <code>timeout</code>
     * is less then 0
     * @since JDK 1.6, HSQLDB 1.9.0
     * <p>
     * @see JDBCDatabaseMetaData#getClientInfoProperties
     */
//#ifdef JAVA6
    public boolean isValid(int timeout) throws SQLException {

        if (timeout < 0) {
            throw Util.outOfRangeArgument("timeout: " + timeout);
        }

        if (this.isInternal) {
            return true;
        } else if (!this.isNetConn) {
            return !this.isClosed();
        } else if (this.isClosed()) {
            return false;
        } else {
            Thread t = new Thread() {

                public void run() {

                    try {
                        getMetaData().getDatabaseMajorVersion();
                    } catch (Exception e) {
                        throw new RuntimeException();
                    }
                }
            };

            // Remember:  parm is in *seconds*
            timeout *= 1000;

            try {
                t.start();

                final long start = System.currentTimeMillis();

                t.join(timeout);

                return (timeout > 0)
                       ? (System.currentTimeMillis() - start) < timeout
                       : true;
            } catch (Exception e) {
                return false;
            }
        }
    }

//#endif JAVA6

    /** @todo 20051207 */

    /**
     * Sets the value of the client info property specified by name to the
     * value specified by value.
     * <p>
     * Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code>
     * method to determine the client info properties supported by the driver
     * and the maximum length that may be specified for each property.
     * <p>
     * The driver stores the value specified in a suitable location in the
     * database.  For example in a special register, session parameter, or
     * system table column.  For efficiency the driver may defer setting the
     * value in the database until the next time a statement is executed or
     * prepared.  Other than storing the client information in the appropriate
     * place in the database, these methods shall not alter the behavior of
     * the connection in anyway.  The values supplied to these methods are
     * used for accounting, diagnostics and debugging purposes only.
     * <p>
     * The driver shall generate a warning if the client info name specified
     * is not recognized by the driver.
     * <p>
     * If the value specified to this method is greater than the maximum
     * length for the property the driver may either truncate the value and
     * generate a warning or generate a <code>SQLClientInfoException</code>.  If the driver
     * generates a <code>SQLClientInfoException</code>, the value specified was not set on the
     * connection.
     * <p>
     * The following are standard client info properties.  Drivers are not
     * required to support these properties however if the driver supports a
     * client info property that can be described by one of the standard
     * properties, the standard property name should be used.
     * <p>
     * <ul>
     * <li>ApplicationName      -       The name of the application currently utilizing
     *                                                  the connection</li>
     * <li>ClientUser           -       The name of the user that the application using
     *                                                  the connection is performing work for.  This may
     *                                                  not be the same as the user name that was used
     *                                                  in establishing the connection.</li>
     * <li>ClientHostname       -       The hostname of the computer the application
     *                                                  using the connection is running on.</li>
     * </ul>
     * <p>
     * @param name              The name of the client info property to set
     * @param value             The value to set the client info property to.  If the
     *                                  value is null, the current value of the specified
     *                                  property is cleared.
     * <p>
     * @throws  SQLClientInfoException if the database server returns an error while
     *                  setting the client info value on the database server or this method
     * is called on a closed connection
     * <p>
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public void setClientInfo(String name,
                              String value) throws SQLClientInfoException {

        try {
            checkClosed();
        } catch (SQLException ex) {
            SQLClientInfoException e =
                new SQLClientInfoException(ex.getMessage(), null);

            e.initCause(ex);

            throw e;
        }

        SQLWarning warning = new SQLWarning("ClientInfo name not recognized: "
            + name);

        warning.initCause(Util.notSupported());
        addWarning(warning);
    }

//#endif JAVA6

    /** @todo 20051207 */

    /**
     * Sets the value of the connection's client info properties.  The
     * <code>Properties</code> object contains the names and values of the client info
     * properties to be set.  The set of client info properties contained in
     * the properties list replaces the current set of client info properties
     * on the connection.  If a property that is currently set on the
     * connection is not present in the properties list, that property is
     * cleared.  Specifying an empty properties list will clear all of the
     * properties on the connection.  See <code>setClientInfo (String, String)</code> for
     * more information.
     * <p>
     * If an error occurs in setting any of the client info properties, a
     * <code>SQLClientInfoException</code> is thrown. The <code>SQLClientInfoException</code>
     * contains information indicating which client info properties were not set.
     * The state of the client information is unknown because
     * some databases do not allow multiple client info properties to be set
     * atomically.  For those databases, one or more properties may have been
     * set before the error occurred.
     * <p>
     *
     * @param properties                the list of client info properties to set
     * <p>
     * @see java.sql.Connection#setClientInfo(String, String) setClientInfo(String, String)
     * @since JDK 1.6, HSQLDB 1.9.0
     * <p>
     * @throws SQLClientInfoException if the database server returns an error while
     *                  setting the clientInfo values on the database server or this method
     * is called on a closed connection
     * <p>
     */
//#ifdef JAVA6
    public void setClientInfo(
            Properties properties) throws SQLClientInfoException {

        if (!this.isClosed && (properties == null || properties.isEmpty())) {
            return;
        }

        SQLClientInfoException ex = new SQLClientInfoException();

        if (this.isClosed) {
            ex.initCause(Util.connectionClosedException());
        } else {
            ex.initCause(Util.notSupported());
        }

        throw ex;
    }

//#endif JAVA6

    /** @todo 1.9.0 */

    /**
     * Returns the value of the client info property specified by name.  This
     * method may return null if the specified client info property has not
     * been set and does not have a default value.  This method will also
     * return null if the specified client info property name is not supported
     * by the driver.
     * <p>
     * Applications may use the <code>DatabaseMetaData.getClientInfoProperties</code>
     * method to determine the client info properties supported by the driver.
     * <p>
     * @param name              The name of the client info property to retrieve
     * <p>
     * @return                  The value of the client info property specified
     * <p>
     * @throws SQLException             if the database server returns an error when
     *                                                  fetching the client info value from the database
     * or this method is called on a closed connection
     * <p>
     * @since JDK 1.6, HSQLDB 1.9.0
     * <p>
     * @see java.sql.DatabaseMetaData#getClientInfoProperties
     */
//#ifdef JAVA6
    public String getClientInfo(String name) throws SQLException {

        checkClosed();

        return null;
    }

//#endif JAVA6

    /** @todo - 1.9 */

    /**
     * Returns a list containing the name and current value of each client info
     * property supported by the driver.  The value of a client info property
     * may be null if the property has not been set and does not have a
     * default value.
     * <p>
     * @return  A <code>Properties</code> object that contains the name and current value of
     *                  each of the client info properties supported by the driver.
     * <p>
     * @throws  SQLException if the database server returns an error when
     *                  fetching the client info values from the database
     * or this method is called on a closed connection
     * <p>
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public Properties getClientInfo() throws SQLException {

        checkClosed();

        return null;
    }

//#endif JAVA6
// --------------------------- Added: Mustang Build 80 -------------------------

    /**
     * Factory method for creating Array objects.
     *
     * @param typeName the SQL name of the type the elements of the array map to. The typeName is a
     * database-specific name which may be the name of a built-in type, a user-defined type or a standard  SQL type supported by this database. This
     *  is the value returned by <code>Array.getBaseTypeName</code>
     * @param elements the elements that populate the returned object
     * @return an Array object whose elements map to the specified SQL type
     * @throws SQLException if a database error occurs, the typeName is null or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this data type
     * @since JDK 1.6_b80, HSQLDB 1.9.0
     * @revised JDK 1.6_b86 method renamed from createArray to createArrayOf
     */
//#ifdef JAVA6
    public Array createArrayOf(String typeName,
                               Object[] elements) throws SQLException {

        checkClosed();

        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * Factory method for creating Struct objects.
     * @param typeName the SQL type name of the SQL structured type that this <code>Struct</code>
     * object maps to. The typeName is the name of  a user-defined type that
     * has been defined for this database. It is the value returned by
     * <code>Struct.getSQLTypeName</code>.
     * @param attributes the attributes that populate the returned object
     * @return a Struct object that maps to the given SQL type and is populated with the given attributes
     * @throws SQLException if a database error occurs, the typeName is null or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this data type
     * @since JDK 1.6_b80, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public Struct createStruct(String typeName,
                               Object[] attributes) throws SQLException {

        checkClosed();

        throw Util.notSupported();
    }

//#endif JAVA6
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

        checkClosed();

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

        checkClosed();

        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//#endif JAVA6
//---------------------- internal implementation ---------------------------
// -------------------------- Common Attributes ------------------------------

    /** Initial holdability */
    int rsHoldability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;

    /**
     * Properties for the connection
     *
     */
    HsqlProperties connProperties;

    /**
     * This connection's interface to the corresponding Session
     * object in the database engine.
     */
    SessionInterface sessionProxy;

    /**
     * Is this an internal connection?
     */
    boolean isInternal;

    /** Is this connection to a network server instance. */
    protected boolean isNetConn;

    /**
     * Is this connection closed?
     */
    boolean isClosed;

    /** The first warning in the chain. Null if there are no warnings. */
    private SQLWarning rootWarning;

    /** Synchronizes concurrent modification of the warning chain */
    private Object rootWarning_mutex = new Object();

    /** ID sequence for unnamed savepoints */
    private int savepointIDSequence;

    /**
     * Constructs a new external <code>Connection</code> to an HSQLDB
     * <code>Database</code>. <p>
     *
     * This constructor is called on behalf of the
     * <code>java.sql.DriverManager</code> when getting a
     * <code>Connection</code> for use in normal (external)
     * client code. <p>
     *
     * Internal client code, that being code located in HSQLDB SQL
     * functions and stored procedures, receives an INTERNAL
     * connection constructed by the {@link
     * #JDBCConnection(org.hsqldb_voltpatches.SessionInterface)
     * JDBCConnection(SessionInterface)} constructor. <p>
     *
     * @param props A <code>Properties</code> object containing the connection
     *      properties
     * @exception SQLException when the user/password combination is
     *     invalid, the connection url is invalid, or the
     *     <code>Database</code> is unavailable. <p>
     *
     *     The <code>Database</code> may be unavailable for a number
     *     of reasons, including network problems or the fact that it
     *     may already be in use by another process.
     */
    public JDBCConnection(HsqlProperties props) throws SQLException {

        String user     = props.getProperty("user");
        String password = props.getProperty("password");
        String connType = props.getProperty("connection_type");
        String host     = props.getProperty("host");
        int    port     = props.getIntegerProperty("port", 0);
        String path     = props.getProperty("path");
        String database = props.getProperty("database");
        boolean isTLS = (connType == DatabaseURL.S_HSQLS
                         || connType == DatabaseURL.S_HTTPS);

        if (user == null) {
            user = "SA";
        }

        if (password == null) {
            password = "";
        }

        Calendar cal         = Calendar.getInstance();
        int      zoneSeconds = HsqlDateTime.getZoneSeconds(cal);

        try {
            if (DatabaseURL.isInProcessDatabaseType(connType)) {

                /**
                 * @todo fredt - this should be the only static reference to a core class in
                 *   the jdbc package - we may make it dynamic
                 */
                sessionProxy = DatabaseManager.newSession(connType, database,
                        user, password, props, zoneSeconds);
            // A VoltDB extension to disable a module dependencies?
            /* disable 10 lines ...
            } else if (connType == DatabaseURL.S_HSQL
                       || connType == DatabaseURL.S_HSQLS) {
                sessionProxy = new ClientConnection(host, port, path,
                        database, isTLS, user, password, zoneSeconds);
                isNetConn = true;
            } else if (connType == DatabaseURL.S_HTTP
                       || connType == DatabaseURL.S_HTTPS) {
                sessionProxy = new ClientConnectionHTTP(host, port, path,
                        database, isTLS, user, password, zoneSeconds);
                isNetConn = true;
            ... disabled 10 lines */
            // End of VoltDB extension
            } else {    // alias: type not yet implemented
                throw Util.invalidArgument(connType);
            }
            connProperties = props;
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    /**
     * Constructs an <code>INTERNAL</code> <code>Connection</code>,
     * using the specified {@link org.hsqldb_voltpatches.SessionInterface
     * SessionInterface}. <p>
     *
     * This constructor is called only on behalf of an existing
     * <code>Session</code> (the internal parallel of a
     * <code>Connection</code>), to be used as a parameter to a SQL
     * function or stored procedure that needs to execute in the context
     * of that <code>Session</code>. <p>
     *
     * When a Java SQL function or stored procedure is called and its
     * first parameter is of type <code>Connection</code>, HSQLDB
     * automatically notices this and constructs an <code>INTERNAL</code>
     * <code>Connection</code> using the current <code>Session</code>.
     * HSQLDB then passes this <code>Connection</code> in the first
     * parameter position, moving any other parameter values
     * specified in the SQL statement to the right by one position.
     * <p>
     *
     * To read more about this, see
     * {@link org.hsqldb_voltpatches.Routine Routine}. <p>
     *
     * <B>Notes:</B> <p>
     *
     * Starting with HSQLDB 1.7.2, <code>INTERNAL</code> connections are not
     * closed by a call to close() or by a SQL DISCONNECT.
     *
     * For HSQLDB developers not involved with writing database
     * internals, this change only applies to connections obtained
     * automatically from the database as the first parameter to
     * stored procedures and SQL functions. This is mainly an issue
     * to developers writing custom SQL function and stored procedure
     * libraries for HSQLDB. Presently, it is recommended that SQL function and
     * stored procedure code avoid depending on closing or issuing a
     * DISCONNECT on a connection obtained in this manner. <p>
     *
     * @param c the Session requesting the construction of this
     *     Connection
     * @exception HsqlException never (reserved for future use);
     * @see org.hsqldb_voltpatches.Routine
     */
    public JDBCConnection(SessionInterface c) {

        // PRE: SessionInterface is non-null
        isInternal   = true;
        sessionProxy = c;
    }

    /**
     *  The default implementation simply attempts to silently {@link
     *  #close() close()} this <code>Connection</code>
     */
    protected void finalize() {

        try {
            close();
        } catch (SQLException e) {
        }
    }

    synchronized int getSavepointID() {
        return savepointIDSequence++;
    }

    /**
     * Retrieves this connection's JDBC url.
     *
     * This method is in support of the JDBCDatabaseMetaData.getURL() method.
     * @return the database connection url with which this object was
     *      constructed
     * @throws SQLException if this connection is closed
     */
    synchronized String getURL() throws SQLException {

        checkClosed();

        return isInternal ? sessionProxy.getInternalConnectionURL()
                          : connProperties.getProperty("url");
    }

    /**
     * An internal check for closed connections. <p>
     *
     * @throws SQLException when the connection is closed
     */
    synchronized void checkClosed() throws SQLException {

        if (isClosed) {
            throw Util.connectionClosedException();
        }
    }

    /**
     * Adds another SQLWarning to this Connection object's warning chain.
     *
     * @param w the SQLWarning to add to the chain
     */
    void addWarning(SQLWarning w) {

        // PRE:  w is never null
        synchronized (rootWarning_mutex) {
            if (rootWarning == null) {
                rootWarning = w;
            } else {
                rootWarning.setNextWarning(w);
            }
        }
    }

    /**
     * Clears the warning chain without checking if this Connection is closed.
     */
    void clearWarningsNoCheck() {

        synchronized (rootWarning_mutex) {
            rootWarning = null;
        }
    }

    /**
     * Translates <code>ResultSet</code> type, adding to the warning
     * chain if the requested type is downgraded. <p>
     *
     * Up to and including HSQLDB 1.7.2,  <code>TYPE_FORWARD_ONLY</code> and
     * <code>TYPE_SCROLL_INSENSITIVE</code> are passed through. <p>
     *
     * Starting with 1.7.2, while <code>TYPE_SCROLL_SENSITIVE</code> is
     * downgraded to <code>TYPE_SCROLL_INSENSITIVE</code> and an SQLWarning is
     * issued. <p>
     *
     * @param type of <code>ResultSet</code>; one of
     *     <code>JDBCResultSet.TYPE_XXX</code>
     * @return the actual type that will be used
     * @throws SQLException if type is not one of the defined values
     */
    int xlateRSType(int type) throws SQLException {

        SQLWarning w;
        String     msg;

        switch (type) {

            case JDBCResultSet.TYPE_FORWARD_ONLY :
            case JDBCResultSet.TYPE_SCROLL_INSENSITIVE : {
                return type;
            }
            case JDBCResultSet.TYPE_SCROLL_SENSITIVE : {
                msg = "TYPE_SCROLL_SENSITIVE => TYPE_SCROLL_SENSITIVE";
                w = new SQLWarning(msg, "SOO10",
                                   ErrorCode.JDBC_INVALID_ARGUMENT);

                addWarning(w);

                return JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
            }
            default : {
                msg = "ResultSet type: " + type;

                throw Util.invalidArgument(msg);
            }
        }
    }

    /**
     * Translates <code>ResultSet</code> concurrency, adding to the warning
     * chain if the requested concurrency is downgraded. <p>
     *
     * Starting with HSQLDB 1.7.2, <code>CONCUR_READ_ONLY</code> is
     * passed through while <code>CONCUR_UPDATABLE</code> is downgraded
     * to <code>CONCUR_READ_ONLY</code> and an SQLWarning is issued.
     *
     * @param concurrency of <code>ResultSet</code>; one of
     *     <code>JDBCResultSet.CONCUR_XXX</code>
     * @return the actual concurrency that will be used
     * @throws SQLException if concurrency is not one of the defined values
     */
    int xlateRSConcurrency(int concurrency) throws SQLException {

        SQLWarning w;
        String     msg;

        switch (concurrency) {

            case JDBCResultSet.CONCUR_READ_ONLY : {
                return concurrency;
            }
            case JDBCResultSet.CONCUR_UPDATABLE : {
                msg = "CONCUR_UPDATABLE => CONCUR_READ_ONLY";
                w = new SQLWarning(msg, "SOO10",
                                   ErrorCode.JDBC_INVALID_ARGUMENT);

                addWarning(w);

                return JDBCResultSet.CONCUR_READ_ONLY;
            }
            default : {
                msg = "ResultSet concurrency: " + concurrency;

                throw Util.invalidArgument(msg);
            }
        }
    }

    /**
     * Resets this connection so it can be used again. Used when connections are
     * returned to a connection pool.
     *
     * @throws SQLException if a database access error occurs
     */
    public void reset() throws SQLException {

        try {
            this.sessionProxy.resetSession();
        } catch (HsqlException e) {
            throw Util.sqlException(ErrorCode.X_08006, e.getMessage(), e);
        }
    }

    /**
     * is called from within nativeSQL when the start of an JDBC escape sequence is encountered
     */
    private int onStartEscapeSequence(String sql, StringBuffer sb,
                                      int i) throws SQLException {

        sb.setCharAt(i++, ' ');

        i = StringUtil.skipSpaces(sql, i);

        if (sql.regionMatches(true, i, "fn ", 0, 3)
                || sql.regionMatches(true, i, "oj ", 0, 3)
                || sql.regionMatches(true, i, "ts ", 0, 3)) {
            sb.setCharAt(i++, ' ');
            sb.setCharAt(i++, ' ');
        } else if (sql.regionMatches(true, i, "d ", 0, 2)
                   || sql.regionMatches(true, i, "t ", 0, 2)) {
            sb.setCharAt(i++, ' ');
        } else if (sql.regionMatches(true, i, "call ", 0, 5)) {
            i += 4;
        } else if (sql.regionMatches(true, i, "?= call ", 0, 8)) {
            sb.setCharAt(i++, ' ');
            sb.setCharAt(i++, ' ');

            i += 5;
        } else if (sql.regionMatches(true, i, "escape ", 0, 7)) {
            i += 6;
        } else {
            i--;

            throw Util.sqlException(
                Error.error(
                    ErrorCode.JDBC_CONNECTION_NATIVE_SQL, sql.substring(i)));
        }

        return i;
    }

    /************************* Volt DB Extensions *************************/

    public void setSchema(String schema) throws SQLException {
        throw new SQLException();
    }

    public String getSchema() throws SQLException {
        throw new SQLException();
    }

    public void abort(java.util.concurrent.Executor executor) throws SQLException {
        throw new SQLException();
    }

    public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds)
            throws SQLException {
        throw new SQLException();
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLException();
    }
    /**********************************************************************/
}
