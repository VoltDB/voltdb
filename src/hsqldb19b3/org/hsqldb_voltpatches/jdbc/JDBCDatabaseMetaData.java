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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

//#ifdef JAVA6
import java.sql.RowIdLifetime;

//#endif JAVA6
import java.sql.SQLException;

import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.FunctionCustom;

/* $Id: JDBCDatabaseMetaData.java 2952 2009-03-26 00:20:19Z fredt $ */

// fredt@users 20020320 - patch 1.7.0 - JDBC 2 support and error trapping
// JDBC 2 methods can now be called from jdk 1.1.x - see javadoc comments
//
// boucherb &     20020409 - extensive review and update of docs and behaviour
// fredt@users  - 20020505   to comply with previous and latest java.sql
//                           specification
// boucherb@users 20020509 - update to JDK 1.4 / JDBC3 methods and docs
// boucherb@users 2002     - extensive rewrite to support new
//              - 20030121   1.7.2 system table and metadata features.
// boucherb@users 20040422 - doc 1.7.2 - javadoc updates toward 1.7.2 final
// fredt@users    20050505 - patch 1.8.0 - enforced JDBC rules for non-pattern params
// boucherb@users 20051207 - update to JDK 1.6 JDBC 4.0 methods and docs
//              - 20060709
// fredt@users    20080805 - full review and update to doc and method return values

/**
 * @todo 1.9.0 - fredt - revise all selects from system tables to use
 *  SQL/SCHEMATA views with column renaming to JDBC spec
 */
// Revision 1.20  2006/07/12 12:06:54  boucherb
// patch 1.9.0
// - java.sql.Wrapper implementation section title added
// Revision 1.19  2006/07/09 07:07:01  boucherb
// - getting the CVS Log variable ouptut format right
//
// Revision 1.18  2006/07/09 07:02:38  boucherb
// - patch 1.9.0 full synch up to Mustang Build 90
// - getColumns() (finally!!!) officially includes IS_AUTOINCREMENT
//

/**
 * Comprehensive information about the database as a whole.
 * <P>
 * This interface is implemented by driver vendors to let users know the capabilities
 * of a Database Management System (DBMS) in combination with
 * the driver based on JDBC<sup><font size=-2>TM</font></sup> technology
 * ("JDBC driver") that is used with it.  Different relational DBMSs often support
 * different features, implement features in different ways, and use different
 * data types.  In addition, a driver may implement a feature on top of what the
 * DBMS offers.  Information returned by methods in this interface applies
 * to the capabilities of a particular driver and a particular DBMS working
 * together. Note that as used in this documentation, the term "database" is
 * used generically to refer to both the driver and DBMS.
 * <P>
 * A user for this interface is commonly a tool that needs to discover how to
 * deal with the underlying DBMS.  This is especially true for applications
 * that are intended to be used with more than one DBMS. For example, a tool might use the method
 * <code>getTypeInfo</code> to find out what data types can be used in a
 * <code>CREATE TABLE</code> statement.  Or a user might call the method
 * <code>supportsCorrelatedSubqueries</code> to see if it is possible to use
 * a correlated subquery or <code>supportsBatchUpdates</code> to see if it is
 * possible to use batch updates.
 * <P>
 * Some <code>DatabaseMetaData</code> methods return lists of information
 * in the form of <code>ResultSet</code> objects.
 * Regular <code>ResultSet</code> methods, such as
 * <code>getString</code> and <code>getInt</code>, can be used
 * to retrieve the data from these <code>ResultSet</code> objects.  If
 * a given form of metadata is not available, an empty <code>ResultSet</code>
 * will be returned. Additional columns beyond the columns defined to be
 * returned by the <code>ResultSet</code> object for a given method
 * can be defined by the JDBC driver vendor and must be accessed
 * by their <B>column label</B>.
 * <P>
 * Some <code>DatabaseMetaData</code> methods take arguments that are
 * String patterns.  These arguments all have names such as fooPattern.
 * Within a pattern String, "%" means match any substring of 0 or more
 * characters, and "_" means match any one character. Only metadata
 * entries matching the search pattern are returned. If a search pattern
 * argument is set to <code>null</code>, that argument's criterion will
 * be dropped from the search.
 * <P>
 * A method that gets information about a feature that the driver does not
 * support will throw an <code>SQLException</code>.
 * In the case of methods that return a <code>ResultSet</code>
 * object, either a <code>ResultSet</code> object (which may be empty) is
 * returned or an <code>SQLException</code> is thrown.
 *
 * <!-- start release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * Starting with HSQLDB 1.7.2, an option is provided to allow alternate
 * system table production implementations.  In this distribution, there are
 * three implementations whose behaviour ranges from producing no system
 * tables at all to producing a richer and more complete body of information
 * about an HSQLDB database than was previously available. The information
 * provided through the default implementation is, unlike previous
 * versions, accessible to all database users, regardless of admin status.
 * This is now possible because the table content it produces for each
 * user is pre-filtered, based on the user's access rights. That is, each
 * system table now acts like a security-aware View.<p>
 *
 * The process of installing a system table production class is transparent and
 * occurs dynamically at runtime during the opening sequence of a
 * <code>Database</code> instance, in the newDatabaseInformation() factory
 * method of the revised DatabaseInformation class, using the following
 * steps: <p>
 *
 * <div class="GeneralExample">
 * <ol>
 * <li>If a class whose fully qualified name is org.hsqldb_voltpatches.DatabaseInformationFull
 *     can be found and it has an accesible constructor that takes an
 *     org.hsqldb_voltpatches.Database object as its single parameter, then an instance of
 *     that class is reflectively instantiated and is used by the database
 *     instance to produce its system tables. <p>
 *
 * <li>If 1.) fails, then the process is repeated, attempting to create an
 *     instance of org.hsqldb_voltpatches.DatabaseInformationMain (which provides just the
 *     core set of system tables required to service this class, but now does
 *     so in a more security aware and comprehensive fashion). <p>
 *
 * <li>If 2.) fails, then an instance of org.hsqldb_voltpatches.DatabaseInformation is
 *     installed (that, by default, produces no system tables, meaning that
 *     calls to all related methods in this class will fail, throwing an
 *     SQLException stating that a required system table is not found). <p>
 *
 * </ol>
 * </div> <p>
 *
 * The process of searching for alternate implementations of database
 * support classes, ending with the installation of a minimal but functional
 * default will be refered to henceforth as <i>graceful degradation</i>.
 * This process is advantageous in that it allows developers and administrators
 * to easily choose packaging options, simply by adding to or deleting concerned
 * classes from an  HSQLDB installation, without worry over providing complex
 * initialization properties or disrupting the core operation of the engine.
 * In this particular context, <i>graceful degradation</i> allows easy choices
 * regarding database metadata, spanning the range of full (design-time),
 * custom-written, minimal (production-time) or <CODE>null</CODE>
 * (space-constrained) system table production implementations. <p>
 *
 * In the default full implementation, a number of new system tables are
 * provided that, although not used directly by this class, present previously
 * unavailable information about the database, such as about its triggers and
 * aliases. <p>
 *
 * In order to better support graphical database exploration tools and as an
 * experimental intermediate step toward more fully supporting SQL9n and
 * SQL200n, the default installed DatabaseInformation implementation
 * is also capable of reporting pseudo name space information, such as
 * the catalog (database URI) of database objects. <p>
 *
 * The catalog reporting feature is turned off by default but
 * can be turned on by providing the appropriate entries in the database
 * properties file (see the advanced topics section of the product
 * documentation). <p>
 *
 * When the feature is turned on, catalog is reported using
 * the following conventions: <p>
 *
 * <ol>
 * <li>All objects are reported as having a catalog equal to the URI of the
 *     database, which is equivalent to the catenation of the
 *     <b>&lt;type&gt;</b> and <b>&lt;path&gt;</b> portions of the HSQLDB
 *     internal JDBC connection URL.<p>
 *
 *     Examples: <p>
 *
 *     <pre class="JavaCodeExample">
 *     <span class="JavaStringLiteral">&quot;jdbc:hsqldb:file:test&quot;</span>      => <span class="JavaStringLiteral">&quot;file:test&quot;</span>
 *     <span class="JavaStringLiteral">&quot;jdbc:hsqldb:mem:.&quot;</span>          => <span class="JavaStringLiteral">&quot;mem:.&quot;</span>
 *     <span class="JavaStringLiteral">&quot;jdbc:hsqldb:hsql:/host/<alias>...&quot;</span> => URI of aliased database
 *     <span class="JavaStringLiteral">&quot;jdbc:hsqldb:http:/host/<alias>...&quot;</span> => URI of aliased database
 *     </pre>
 *
 *     <b>Note:</b> No provision is made for qualifying database objects
 *     by catalog in DML or DDL SQL.  This feature is functional only with
 *     respect to browsing the database through the DatabaseMetaData and system
 *     table interfaces. <p>
 *
 * </ol>
 *
 * Again, it should be well understood that this feature provide an
 * <i>emulation</i> of catalog support and is intended only
 * as an experimental implementation to enhance the browsing experience
 * when using graphical database explorers and to make a first foray
 * into tackling the issue of implementing true catalog support
 * in the future. <p>
 *
 * Due the nature of the new database system table production process, fewer
 * assumptions can be made by this class about what information is made
 * available in the system tables supporting <code>DatabaseMetaData</code>
 * methods. Because of this, the SQL queries behind the <code>ResultSet</code>
 * producing methods have been cleaned up and made to adhere more strictly to
 * the JDBC contracts specified in relation to the method parameters. <p>
 *
 * One of the remaining assumptions concerns the <code>approximate</code>
 * argument of {@link #getIndexInfo getIndexInfo()}. This parameter is still
 * ignored since there is not yet any process in place to internally gather
 * and persist table and index statistics.  A primitive version of a statistics
 * gathering and reporting subsystem <em>may</em> be introduced at some time in
 * the future. <p>
 *
 * Another assumption is that simple select queries against certain system
 * tables will return rows in JDBC contract order in the absence of an
 * &quot;ORDER BY&quot; clause.  The reason for this is that results
 * come back much faster when no &quot;ORDER BY&quot; clause is used.
 * Developers wishing to extend or replace an existing system table production
 * class should be aware of this, either adding the contract
 * &quot;ORDER BY&quot; clause to the SQL in corresponding methods in this class,
 * or, better, by maintaing rows in the correct order in the underlying
 * system tables, prefereably by creating appropriate primary indices. <p>
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
 * However, some of these method calls require <code>int</code> values that
 * are defined only in the JDBC 2 or greater version of the
 * {@link java.sql.ResultSet ResultSet} interface.  For this reason, when the
 * product is compiled under JDK 1.1.x, these values are defined in
 * {@link JDBCResultSet JDBCResultSet}.<p>
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
 * // etc
 * </pre>
 *
 * However, please note that code written in such a manner will not be
 * compatible for use with other JDBC 2 drivers, since they expect and use
 * <code>ResultSet</code>, rather than <code>JDBCResultSet</code>.  Also
 * note, this feature is offered solely as a convenience to developers
 * who must work under JDK 1.1.x due to operating constraints, yet wish to
 * use some of the more advanced features available under the JDBC 2
 * specification.<p>
 *
 * (fredt@users)<br>
 * (boucherb@users)
 * </div>
 * <!-- end release-specific documentation -->
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @revised JDK 1.6, HSQLDB 1.9.0
 * @see org.hsqldb_voltpatches.dbinfo.DatabaseInformation
 * @see org.hsqldb_voltpatches.dbinfo.DatabaseInformationMain
 * @see org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull
 */
//#ifdef JAVA6
public class JDBCDatabaseMetaData implements DatabaseMetaData,
        java.sql.Wrapper {

//#else
/*
public class JDBCDatabaseMetaData implements DatabaseMetaData {
*/

//#endif
    //----------------------------------------------------------------------
    // First, a variety of minor information about the target database.

    /**
     * Retrieves whether the current user can call all the procedures
     * returned by the method <code>getProcedures</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * This method still <em>always</em> returns
     * <code>true</code>. <p>
     *
     * In a future release, the plugin interface may be modified to allow
     * implementors to report different values here, based on their
     * implementations.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether the current user can use all the tables returned
     * by the method <code>getTables</code> in a <code>SELECT</code>
     * statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB always reports <code>true</code>.<p>
     *
     * Please note that the default HSQLDB <code>getTables</code> behaviour is
     * omit from the list of <em>requested</em> tables only those to which the
     * invoking user has <em>no</em> access of any kind. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    /**
     * Retrieves the URL for this DBMS.
     *
     * @return the URL for this DBMS or <code>null</code> if it cannot be
     *          generated
     * @exception SQLException if a database access error occurs
     */
    public String getURL() throws SQLException {
        return connection.getURL();
    }

    /**
     * Retrieves the user name as known to this database.
     *
     * @return the database user name
     * @exception SQLException if a database access error occurs
     */
    public String getUserName() throws SQLException {

        ResultSet rs = execute("CALL USER()");

        rs.next();

        String result = rs.getString(1);

        rs.close();

        return result;
    }

    /**
     * Retrieves whether this database is in read-only mode.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.7.2, this makes
     * an SQL call to the new isReadOnlyDatabase function
     * which provides correct determination of the read-only status for
     * both local and remote database instances.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isReadOnly() throws SQLException {

        ResultSet rs = execute("CALL isReadOnlyDatabase()");

        rs.next();

        boolean result = rs.getBoolean(1);

        rs.close();

        return result;
    }

    /**
     * Retrieves whether <code>NULL</code> values are sorted high.
     * Sorted high means that <code>NULL</code> values
     * sort higher than any other value in a domain.  In an ascending order,
     * if this method returns <code>true</code>,  <code>NULL</code> values
     * will appear at the end. By contrast, the method
     * <code>nullsAreSortedAtEnd</code> indicates whether <code>NULL</code> values
     * are sorted at the end regardless of sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB sorts null low; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether <code>NULL</code> values are sorted low.
     * Sorted low means that <code>NULL</code> values
     * sort lower than any other value in a domain.  In an ascending order,
     * if this method returns <code>true</code>,  <code>NULL</code> values
     * will appear at the beginning. By contrast, the method
     * <code>nullsAreSortedAtStart</code> indicates whether <code>NULL</code> values
     * are sorted at the beginning regardless of sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB sorts null low; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether <code>NULL</code> values are sorted at the start regardless
     * of sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB sorts null low; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether <code>NULL</code> values are sorted at the end regardless of
     * sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB sorts null low; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    /**
     * Retrieves the name of this database product.
     *
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.7.2, this value is retrieved through an
     * SQL call to the new {@link org.hsqldb_voltpatches.Library#getDatabaseProductName} method
     * which allows correct determination of the database product name
     * for both local and remote database instances.
     * </div> <p>
     *
     * @return database product name
     * @exception SQLException if a database access error occurs
     */
    public String getDatabaseProductName() throws SQLException {

        ResultSet rs =
            execute("call \"org.hsqldb_voltpatches.Library.getDatabaseProductName\"()");

        rs.next();

        String result = rs.getString(1);

        rs.close();

        return result;
    }

    /**
     * Retrieves the version number of this database product.
     *
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with HSQLDB 1.7.2, this value is retrieved through an
     * SQL call to the new {@link org.hsqldb_voltpatches.Library#getDatabaseProductVersion} method
     * which allows correct determination of the database product name
     * for both local and remote database instances.
     * </div> <p>
     *
     * @return database version number
     * @exception SQLException if a database access error occurs
     */
    public String getDatabaseProductVersion() throws SQLException {

        ResultSet rs =
            execute("call \"org.hsqldb_voltpatches.Library.getDatabaseProductVersion\"()");

        rs.next();

        String result = rs.getString(1);

        rs.close();

        return result;
    }

    /**
     * Retrieves the name of this JDBC driver.
     *
     * @return JDBC driver name
     * @exception SQLException if a database access error occurs
     */
    public String getDriverName() throws SQLException {
        return HsqlDatabaseProperties.PRODUCT_NAME + " Driver";
    }

    /**
     * Retrieves the version number of this JDBC driver as a <code>String</code>.
     *
     * @return JDBC driver version
     * @exception SQLException if a database access error occurs
     */
    public String getDriverVersion() throws SQLException {
        return HsqlDatabaseProperties.THIS_VERSION;
    }

    /**
     * Retrieves this JDBC driver's major version number.
     *
     * @return JDBC driver major version
     */
    public int getDriverMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    /**
     * Retrieves this JDBC driver's minor version number.
     *
     * @return JDBC driver minor version number
     */
    public int getDriverMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }

    /**
     * Retrieves whether this database stores tables in a local file.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From HSQLDB 1.7.2 it is assumed that this refers to data being stored
     * by the JDBC client. This method always returns false.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database uses a file for each table.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not use a file for each table.
     * This method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if this database uses a local file for each table;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case sensitive and as a result stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case insensitive and stores them in upper case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case insensitive and stores them in lower case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case insensitive and stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case sensitive and as a result stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case insensitive and stores them in upper case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case insensitive and stores them in lower case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case insensitive and stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves the string used to quote SQL identifiers.
     * This method returns a space " " if identifier quoting is not supported.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB uses the standard SQL identifier quote character
     * (the double quote character); this method always returns <b>"</b>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the quoting string or a space if quoting is not supported
     * @exception SQLException if a database access error occurs
     */
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    /**
     * Retrieves a comma-separated list of all of this database's SQL keywords
     * that are NOT also SQL:2003 keywords.
     * (JDBC4 modified => SQL:2003)
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * The list is empty. However, HSQLDB also supports SQL:2008 keywords
     * and disallows them for database object names without double quoting.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return the list of this database's keywords that are not also
     *         SQL:2003 keywords
     *         (JDBC4 modified => SQL:2003)
     * @exception SQLException if a database access error occurs
     */
    public String getSQLKeywords() throws SQLException {

        return "";
        /*
        return "BEFORE,BIGINT,BINARY,CACHED,DATETIME,"
               + "LIMIT,LONGVARBINARY,LONGVARCHAR,OBJECT,OTHER,SAVEPOINT,"
               + "TEMP,TEXT,TOP,TRIGGER,TINYINT,VARBINARY,VARCHAR_IGNORECASE";
        */
    }

    /**
     * Retrieves a comma-separated list of math functions available with
     * this database.  These are the Open /Open CLI math function names used in
     * the JDBC function escape clause.
     *
     * @return the list of math functions supported by this database
     * @exception SQLException if a database access error occurs
     */
    public String getNumericFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupNumericFunctions,
                                  ",", "");
    }

    /**
     * Retrieves a comma-separated list of string functions available with
     * this database.  These are the  Open Group CLI string function names used
     * in the JDBC function escape clause.
     *
     * @return the list of string functions supported by this database
     * @exception SQLException if a database access error occurs
     */
    public String getStringFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupStringFunctions,
                                  ",", "");
    }

    /**
     * Retrieves a comma-separated list of system functions available with
     * this database.  These are the  Open Group CLI system function names used
     * in the JDBC function escape clause.
     *
     * @return a list of system functions supported by this database
     * @exception SQLException if a database access error occurs
     */
    public String getSystemFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupSystemFunctions,
                                  ",", "");
    }

    /**
     * Retrieves a comma-separated list of the time and date functions available
     * with this database.
     *
     * @return the list of time and date functions supported by this database
     * @exception SQLException if a database access error occurs
     */
    public String getTimeDateFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupDateTimeFunctions,
                                  ",", "");
    }

    /**
     * Retrieves the string that can be used to escape wildcard characters.
     * This is the string that can be used to escape '_' or '%' in
     * the catalog search parameters that are a pattern (and therefore use one
     * of the wildcard characters).
     *
     * <P>The '_' character represents any single character;
     * the '%' character represents any sequence of zero or
     * more characters.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB uses the "\" character to escape wildcard characters.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return the string used to escape wildcard characters
     * @exception SQLException if a database access error occurs
     */
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    /**
     * Retrieves all the "extra" characters that can be used in unquoted
     * identifier names (those beyond a-z, A-Z, 0-9 and _).
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not support using any "extra" characters in unquoted
     * identifier names; this method always returns the empty String.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return the string containing the extra characters
     * @exception SQLException if a database access error occurs
     */
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    //--------------------------------------------------------------------
    // Functions describing which features are supported.

    /**
     * Retrieves whether this database supports <code>ALTER TABLE</code>
     * with add column.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.7.0, HSQLDB supports this type of
     * <code>ALTER TABLE</code> statement; this method always
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports <code>ALTER TABLE</code>
     * with drop column.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.7.0, HSQLDB supports this type of
     * <code>ALTER TABLE</code> statement; this method always
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports column aliasing.
     *
     * <P>If so, the SQL AS clause can be used to provide names for
     * computed columns or to provide alias names for columns as
     * required.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports column aliasing; this method always
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports concatenations between
     * <code>NULL</code> and non-<code>NULL</code> values being
     * <code>NULL</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this; this method always
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    /**
     * (JDBC4 clarification:)
     * Retrieves whether this database supports the JDBC scalar function
     * <code>CONVERT</code> for the conversion of one JDBC type to another.
     * The JDBC types are the generic SQL data types defined
     * in <code>java.sql.Types</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports conversions; this method always
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    /** @todo needs the full conversion matrix here. Should use org.hsqldb_voltpatches.types */

    /**
     * (JDBC4 clarification:)
     * Retrieves whether this database supports the JDBC scalar function
     * <code>CONVERT</code> for conversions between the JDBC types <i>fromType</i>
     * and <i>toType</i>.  The JDBC types are the generic SQL data types defined
     * in <code>java.sql.Types</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports conversion according to SQL standards. In addition,
     * it supports conversion between values of BOOLEAN and BIT types.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param fromType the type to convert from; one of the type codes from
     *        the class <code>java.sql.Types</code>
     * @param toType the type to convert to; one of the type codes from
     *        the class <code>java.sql.Types</code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @see java.sql.Types
     */
    public boolean supportsConvert(int fromType,
                                   int toType) throws SQLException {

//#ifdef JAVA6
        switch (fromType) {

            case java.sql.Types.NCHAR : {
                fromType = java.sql.Types.CHAR;

                break;
            }
            case java.sql.Types.NCLOB : {
                fromType = java.sql.Types.CLOB;

                break;
            }
            case java.sql.Types.NVARCHAR : {
                fromType = java.sql.Types.VARCHAR;

                break;
            }
        }

        switch (toType) {

            case java.sql.Types.NCHAR : {
                toType = java.sql.Types.CHAR;

                break;
            }
            case java.sql.Types.NCLOB : {
                toType = java.sql.Types.CLOB;

                break;
            }
            case java.sql.Types.NVARCHAR : {
                toType = java.sql.Types.VARCHAR;

                break;
            }
        }

//#endif JAVA6
        Type from = Type.getDefaultType(Type.getHSQLDBTypeCode(fromType));
        Type to   = Type.getDefaultType(Type.getHSQLDBTypeCode(toType));

        if (from == null || to == null) {
            return false;
        }

        return to.canConvertFrom(from);
    }

    /**
     * Retrieves whether this database supports table correlation names.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports table correlation names; this method always
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether, when table correlation names are supported, they
     * are restricted to being different from the names of the tables.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB requires that table correlation names are different from the
     * names of the tables; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports expressions in
     * <code>ORDER BY</code> lists.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports expressions in <code>ORDER BY</code> lists; this
     * method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports using a column that is
     * not in the <code>SELECT</code> statement in an
     * <code>ORDER BY</code> clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports using a column that is not in the <code>SELECT</code>
     * statement in an <code>ORDER BY</code> clause; this method always
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports some form of
     * <code>GROUP BY</code> clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports using the <code>GROUP BY</code> clause; this method
     * always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports using a column that is
     * not in the <code>SELECT</code> statement in a
     * <code>GROUP BY</code> clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports using a column that is
     * not in the <code>SELECT</code> statement in a
     * <code>GROUP BY</code> clause; this method
     * always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports using columns not included in
     * the <code>SELECT</code> statement in a <code>GROUP BY</code> clause
     * provided that all of the columns in the <code>SELECT</code> statement
     * are included in the <code>GROUP BY</code> clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports using columns not included in
     * the <code>SELECT</code> statement in a <code>GROUP BY</code> clause
     * provided that all of the columns in the <code>SELECT</code> statement
     * are included in the <code>GROUP BY</code> clause; this method
     * always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports specifying a
     * <code>LIKE</code> escape clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports specifying a
     * <code>LIKE</code> escape clause; this method
     * always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    /** @todo 1.9.0 - return according to multiple result set cpability */

    /**
     * Retrieves whether this database supports getting multiple
     * <code>ResultSet</code> objects from a single call to the
     * method <code>execute</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.8.0.x, HSQLDB does not support getting multiple
     * <code>ResultSet</code> objects from a single call to the method
     * <code>execute</code>; this method always returns <code>false</code>. <p>
     *
     * This behaviour <i>may</i> change in a future release.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database allows having multiple
     * transactions open at once (on different connections).
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB allows having multiple
     * transactions open at once (on different connections); this method
     * always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether columns in this database may be defined as non-nullable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the specification of non-nullable columns; this method
     * always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ODBC Minimum SQL grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports the ODBC Minimum SQL grammar;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ODBC Core SQL grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports core SQL grammar;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ODBC Extended SQL grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports the ODBC Extended SQL grammar;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ANSI92 entry level SQL
     * grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports the ANSI92 entry level SQL grammar;
     * this method always returns <code>true</code>. <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ANSI92 intermediate SQL grammar supported.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports the ANSI92 intermediate SQL grammar;
     * this method always returns <code>true</code>.
     * <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ANSI92 full SQL grammar supported.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports the ANSI92 full SQL grammar;
     * this method always returns <code>true</code>. <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsANSI92FullSQL() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the SQL Integrity
     * Enhancement Facility.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * This method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports some form of outer join.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports outer joins; this method always returns
     * <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports full nested outer joins.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.9.0, HSQLDB supports full nested outer
     * joins; this method always returns <code>true</code>. <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database provides limited support for outer
     * joins.  (This will be <code>true</code> if the method
     * <code>supportsFullOuterJoins</code> returns <code>true</code>).
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the LEFT OUTER join syntax;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Retrieves the database vendor's preferred term for "schema".
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB supports schemas.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the vendor term for "schema"
     * @exception SQLException if a database access error occurs
     */
    public String getSchemaTerm() throws SQLException {
        return "SCHEMA";
    }

    /**
     * Retrieves the database vendor's preferred term for "procedure".
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.9.0, HSQLDB does not support declaration of
     * functions or procedures directly in SQL but instead relies on the
     * HSQLDB-specific CLASS grant mechanism to make public static
     * Java methods available as SQL routines; this method always returns
     * an empty <code>String</code>. <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return the vendor term for "procedure"
     * @exception SQLException if a database access error occurs
     */
    public String getProcedureTerm() throws SQLException {
        return "";
    }

    /**
     * Retrieves the database vendor's preferred term for "catalog".
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB uses the standard name CATALOG.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the vendor term for "catalog"
     * @exception SQLException if a database access error occurs
     */
    public String getCatalogTerm() throws SQLException {
        return "CATALOG";
    }

    /**
     * Retrieves whether a catalog appears at the start of a fully qualified
     * table name.  If not, the catalog appears at the end.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * When allowed, a catalog appears at the start of a fully qualified
     * table name; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if the catalog name appears at the beginning
     *         of a fully qualified table name; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    /**
     * Retrieves the <code>String</code> that this database uses as the
     * separator between a catalog and table name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * When used, a catalog name is separated with period;
     * this method <em>always</em> returns a period
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the separator string
     * @exception SQLException if a database access error occurs
     */
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    /**
     * Retrieves whether a schema name can be used in a data manipulation statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * From 1.9.0, HSQLDB supports schemas where allowed by the standard;
     * this method always returns <code>true</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException {

        // false for OOo client server compatibility
        // otherwise schema name is used by OOo in column references
        return supportsSchemasIn;
    }

    /**
     * Retrieves whether a schema name can be used in a procedure call statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.9.0, HSQLDB does not support schema-qualified
     * procedure identifiers; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return supportsSchemasIn;
    }

    /**
     * Retrieves whether a schema name can be used in a table definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB supports schemas;
     * By default, this method returns <code>true</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return supportsSchemasIn;
    }

    /**
     * Retrieves whether a schema name can be used in an index definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB supports schemas;
     * By default, this method returns <code>true</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return supportsSchemasIn;
    }

    /**
     * Retrieves whether a schema name can be used in a privilege definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB supports schemas;
     * By default, this method returns <code>true</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return supportsSchemasIn;
    }

    /**
     * Retrieves whether a catalog name can be used in a data manipulation statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB supports catalog-qualified
     * data manipulation; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether a catalog name can be used in a procedure call statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.9.0, HSQLDB does not support catalog-qualified
     * procedure calls; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether a catalog name can be used in a table definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB supports catalog-qualified
     * table definitions; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether a catalog name can be used in an index definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB supports catalog-qualified
     * index definitions; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether a catalog name can be used in a privilege definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB supports catalog-qualified
     * privilege definitions; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports positioned <code>DELETE</code>
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports updateable result sets;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsPositionedDelete() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports positioned <code>UPDATE</code>
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports updateable result sets;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsPositionedUpdate() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports <code>SELECT FOR UPDATE</code>
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports updateable result sets;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSelectForUpdate() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports stored procedure calls
     * that use the stored procedure escape syntax.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports calling public static Java methods in the context of SQL
     * Stored Procedures; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @see JDBCParameterMetaData
     * @see JDBCConnection#prepareCall
     */
    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in comparison
     * expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB has always supported subqueries in comparison expressions;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in
     * <code>EXISTS</code> expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB has always supported subqueries in <code>EXISTS</code>
     * expressions; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    /**
     * (JDBC4 correction:)
     * Retrieves whether this database supports subqueries in
     * <code>IN</code> expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB has always supported subqueries in <code>IN</code>
     * statements; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in quantified
     * expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB has always supported subqueries in quantified
     * expressions; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports correlated subqueries.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB has always supported correlated subqueries;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports SQL <code>UNION</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports SQL <code>UNION</code>;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports SQL <code>UNION ALL</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports SQL <code>UNION ALL</code>;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports keeping cursors open
     * across commits.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports keeping cursors open across commits.
     * This method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if cursors always remain open;
     *       <code>false</code> if they might not remain open
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports keeping cursors open
     * across rollbacks.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.9.0 supports keeping cursors open across rollbacks in specific
     * situations.
     * This method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if cursors always remain open;
     *       <code>false</code> if they might not remain open
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports keeping statements open
     * across commits.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports keeping statements open across commits;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if statements always remain open;
     *       <code>false</code> if they might not remain open
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports keeping statements open
     * across rollbacks.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports keeping statements open  across rollbacks;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if statements always remain open;
     *       <code>false</code> if they might not remain open
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return true;
    }

    //----------------------------------------------------------------------
    // The following group of methods exposes various limitations
    // based on the target database with the current driver.
    // Unless otherwise specified, a result of zero means there is no
    // limit, or the limit is not known.

    /**
     * Retrieves the maximum number of hex characters this database allows in an
     * inline binary literal.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return max the maximum length (in hex characters) for a binary literal;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxBinaryLiteralLength() throws SQLException {

        // hard limit is Integer.MAX_VALUE
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows
     * for a character literal.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a character literal;
     *      a result of zero means that there is no limit or the limit is
     *      not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows
     * for a column name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a column name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxColumnNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of columns this database allows in a
     * <code>GROUP BY</code> clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in an index.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in an
     * <code>ORDER BY</code> clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in a
     * <code>SELECT</code> list.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in a table.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of concurrent connections to this
     * database that are possible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of active connections possible at one time;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * cursor name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed in a cursor name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxCursorNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of bytes this database allows for an
     * index, including all of the parts of the index.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory and disk availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of bytes allowed; this limit includes the
     *      composite of all the constituent parts of the index;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * schema name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the maximum number of characters allowed in a schema name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxSchemaNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * procedure name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed in a procedure name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxProcedureNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * catalog name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed in a catalog name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxCatalogNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of bytes this database allows in
     * a single row.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory and disk availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of bytes allowed for a row; a result of
     *         zero means that there is no limit or the limit is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    /**
     * Retrieves whether the return value for the method
     * <code>getMaxRowSize</code> includes the SQL data types
     * <code>LONGVARCHAR</code> and <code>LONGVARBINARY</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Indormation:</h3><p>
     *
     * Including 1.9.0, {@link #getMaxRowSize} <em>always</em> returns
     * 0, indicating that the maximum row size is unknown or has no limit.
     * This applies to the above types as well; this method <em>always</em>
     * returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    /**
     * Retrieves the maximum number of characters this database allows in
     * an SQL statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for an SQL statement;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of active statements to this database
     * that can be open at the same time.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of statements that can be open at one time;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows in
     * a table name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.8.0.x, HSQLDB did not impose a "known" limit.  Th
     * hard limit was the maximum length of a java.lang.String
     * (java.lang.Integer.MAX_VALUE); this method always returned
     * <code>0</code>.
     *
     * Starting with 1.9.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a table name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxTableNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of tables this database allows in a
     * <code>SELECT</code> statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availabily; this method always returns <code>0</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of tables allowed in a <code>SELECT</code>
     *         statement; a result of zero means that there is no limit or
     *         the limit is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows in
     * a user name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a user name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @exception SQLException if a database access error occurs
     */
    public int getMaxUserNameLength() throws SQLException {
        return 128;
    }

    //----------------------------------------------------------------------

    /**
     * Retrieves this database's default transaction isolation level.  The
     * possible values are defined in <code>java.sql.Connection</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information</h3>
     *
     * Default isolation mode in version 1.9.0 is TRANSACTION_READ_COMMITED.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the default isolation level
     * @exception SQLException if a database access error occurs
     * @see JDBCConnection
     */
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Retrieves whether this database supports transactions. If not, invoking the
     * method <code>commit</code> is a noop, and the isolation level is
     * <code>TRANSACTION_NONE</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports transactions;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if transactions are supported;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    /** @todo update javadoc */

    /**
     * Retrieves whether this database supports the given transaction isolation level.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information</h3>
     * HSQLDB supports all levels.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param level one of the transaction isolation levels defined in
     *         <code>java.sql.Connection</code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @see JDBCConnection
     */
    public boolean supportsTransactionIsolationLevel(
            int level) throws SQLException {

        return level == Connection.TRANSACTION_READ_UNCOMMITTED
               || level == Connection.TRANSACTION_READ_COMMITTED
               || level == Connection.TRANSACTION_REPEATABLE_READ
               || level == Connection.TRANSACTION_SERIALIZABLE;
    }

    /**
     * Retrieves whether this database supports both data definition and
     * data manipulation statements within a transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not support a mix of both data definition and
     * data manipulation statements within a transaction.  DDL commits the
     * current transaction before proceding;
     * this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports only data manipulation
     * statements within a transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports only data manipulation
     * statements within a transaction.  DDL commits the
     * current transaction before proceeding, while DML does not;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether a data definition statement within a transaction forces
     * the transaction to commit.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Including 1.9.0, a data definition statement within a transaction forces
     * the transaction to commit; this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database ignores a data definition statement
     * within a transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Including 1.9.0, a data definition statement is not ignored within a
     * transaction.  Rather, a data definition statement within a
     * transaction forces the transaction to commit; this method
     * <em>always</em> returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /**
     * Retrieves a description of the stored procedures available in the given
     * catalog.
     * <P>
     * Only procedure descriptions matching the schema and
     * procedure name criteria are returned.  They are ordered by
     * <code>PROCEDURE_SCHEM</code>, <code>PROCEDURE_NAME</code> and (new to JDBC4) <code>SPECIFIC_ NAME</code>.
     *
     * <P>Each procedure description has the the following columns:
     *  <OL>
     *  <LI><B>PROCEDURE_CAT</B> String => procedure catalog (may be <code>null</code>)
     *  <LI><B>PROCEDURE_SCHEM</B> String => procedure schema (may be <code>null</code>)
     *  <LI><B>PROCEDURE_NAME</B> String => procedure name
     *  <LI> reserved for future use
     *       (HSQLDB-specific: NUM_INPUT_PARAMS)
     *  <LI> reserved for future use
     *       (HSQLDB-specific: NUM_OUTPUT_PARAMS)
     *  <LI> reserved for future use
     *       (HSQLDB-specific: NUM_RESULT_SETS)
     *  <LI><B>REMARKS</B> String => explanatory comment on the procedure
     *  <LI><B>PROCEDURE_TYPE</B> short => kind of procedure:
     *      <UL>
     *      <LI> procedureResultUnknown - (JDBC4 clarification:) Cannot determine if  a return value
     *       will be returned
     *      <LI> procedureNoResult - (JDBC4 clarification:) Does not return a return value
     *      <LI> procedureReturnsResult - (JDBC4 clarification:) Returns a return value
     *      </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  => (JDBC4 new:) The name which uniquely identifies this procedure within its schema
     *  </OL>
     * <p>
     * A user may not have permissions to execute any of the procedures that are
     * returned by <code>getProcedures</code>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param procedureNamePattern a procedure name pattern; must match the
     *        procedure name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a procedure description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getProcedures(
            String catalog, String schemaPattern,
            String procedureNamePattern) throws SQLException {

        if (wantsIsNull(procedureNamePattern)) {
            return executeSelect("SYSTEM_PROCEDURES", "0=1");
        }
        schemaPattern = translateSchema(schemaPattern);

        StringBuffer select =
            toQueryPrefix("SYSTEM_PROCEDURES").append(and("PROCEDURE_CAT",
                "=", catalog)).append(and("PROCEDURE_SCHEM", "LIKE",
                    schemaPattern)).append(and("PROCEDURE_NAME", "LIKE",
                        procedureNamePattern));

        // By default, query already returns the result ordered by
        // PROCEDURE_SCHEM, PROCEDURE_NAME...
        return execute(select.toString());
    }

    /**
     * Indicates that it is not known whether the procedure returns
     * a result.
     * <P>
     * A possible value for column <code>PROCEDURE_TYPE</code> in the
     * <code>ResultSet</code> object returned by the method
     * <code>getProcedures</code>.
     */

//    int procedureResultUnknown        = 0;

    /**
     * Indicates that the procedure does not return a result.
     * <P>
     * A possible value for column <code>PROCEDURE_TYPE</code> in the
     * <code>ResultSet</code> object returned by the method
     * <code>getProcedures</code>.
     */
//    int procedureNoResult             = 1;

    /**
     * Indicates that the procedure returns a result.
     * <P>
     * A possible value for column <code>PROCEDURE_TYPE</code> in the
     * <code>ResultSet</code> object returned by the method
     * <code>getProcedures</code>.
     */
//    int procedureReturnsResult        = 2;

    /**
     * Retrieves a description of the given catalog's stored procedure parameter
     * and result columns.
     *
     * <P>Only descriptions matching the schema, procedure and
     * parameter name criteria are returned.  They are ordered by
     * PROCEDURE_SCHEM, PROCEDURE_NAME and SPECIFIC_NAME. Within this, the return value,
     * if any, is first. Next are the parameter descriptions in call
     * order. The column descriptions follow in column number order.
     *
     * <P>Each row in the <code>ResultSet</code> is a parameter description or
     * column description with the following fields:
     *  <OL>
     *  <LI><B>PROCEDURE_CAT</B> String => procedure catalog (may be <code>null</code>)
     *  <LI><B>PROCEDURE_SCHEM</B> String => procedure schema (may be <code>null</code>)
     *  <LI><B>PROCEDURE_NAME</B> String => procedure name
     *  <LI><B>COLUMN_NAME</B> String => column/parameter name
     *  <LI><B>COLUMN_TYPE</B> Short => kind of column/parameter:
     *      <UL>
     *      <LI> procedureColumnUnknown - nobody knows
     *      <LI> procedureColumnIn - IN parameter
     *      <LI> procedureColumnInOut - INOUT parameter
     *      <LI> procedureColumnOut - OUT parameter
     *      <LI> procedureColumnReturn - procedure return value
     *      <LI> procedureColumnResult - result column in <code>ResultSet</code>
     *      </UL>
     *  <LI><B>DATA_TYPE</B> int => SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String => SQL type name, for a UDT type the
     *  type name is fully qualified
     *  <LI><B>PRECISION</B> int => precision
     *  <LI><B>LENGTH</B> int => length in bytes of data
     *  <LI><B>SCALE</B> short => scale -  null is returned for data types where
     * SCALE is not applicable.
     *  <LI><B>RADIX</B> short => radix
     *  <LI><B>NULLABLE</B> short => can it contain NULL.
     *      <UL>
     *      <LI> procedureNoNulls - does not allow NULL values
     *      <LI> procedureNullable - allows NULL values
     *      <LI> procedureNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String => comment describing parameter/column
     *  <LI><B>COLUMN_DEF</B> String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
     *      <UL>
     *      <LI> The string NULL (not enclosed in quotes) - if NULL was specified as the default value
     *      <LI> TRUNCATE (not enclosed in quotes)        - if the specified default value cannot be represented without truncation
     *      <LI> NULL                                     - if a default value was not specified
     *      </UL>
     *  <LI><B>SQL_DATA_TYPE</B> int  => (JDBC4 new:) Reserved for future use
     *
     *        <p>HSQLDB-specific: CLI type from SQL 2003 Table 37,
     *        tables 6-9 Annex A1, and/or addendums in other
     *        documents, such as:<br>
     *        SQL 2003 Part 9: Management of External Data (SQL/MED) : DATALINK<br>
     *        SQL 2003 Part 14: XML-Related Specifications (SQL/XML) : XML<p>
     *
     *  <LI><B>SQL_DATETIME_SUB</B> int  => (JDBC4 new:) reserved for future use
     *
     *        <p>HSQLDB-specific: CLI SQL_DATETIME_SUB from SQL 2003 Table 37
     *
     *  <LI><B>CHAR_OCTET_LENGTH</B> int  => (JDBC4 new:) the maximum length of binary and character based columns.  For any other datatype the returned value is a
     * NULL
     *  <LI><B>ORDINAL_POSITION</B> int  => (JDBC4 new:) the ordinal position, starting from 1, for the input and output parameters for a procedure. A value of 0
     * is returned if this row describes the procedure's return value.
     *  <LI><B>IS_NULLABLE</B> String  => ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the parameter can include NULLs
     *       <LI> NO            --- if the parameter cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * parameter is unknown
     *       </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  => (JDBC4 new:) the name which uniquely identifies this procedure within its schema.
     * </OL>
     *
     * <P><B>Note:</B> Some databases may not return the column
     * descriptions for a procedure. Additional columns beyond (JDBC4 modified:)
     * SPECIFIC_NAME can be defined by the database and must be accessed by their <B>column name</B>.
     *
     * <p>(JDBC4 clarification:)
     * <p>The PRECISION column represents the specified column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the [declared or implicit maximum] length in characters.
     * For datetime datatypes, this is the [maximum] length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the [maximum] length in bytes.  For the ROWID datatype,
     * this is the length in bytes[, as returned by the implementation-specific java.sql.RowId.getBytes() method]. 0 is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param procedureNamePattern a procedure name pattern; must match the
     *        procedure name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name
     *        as it is stored in the database
     * @return <code>ResultSet</code> - each row describes a stored procedure parameter or
     *      column
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
            String procedureNamePattern,
            String columnNamePattern) throws SQLException {

        if (wantsIsNull(procedureNamePattern)
                || wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_PROCEDURECOLUMNS", "0=1");
        }
        schemaPattern = translateSchema(schemaPattern);

        StringBuffer select = toQueryPrefix("SYSTEM_PROCEDURECOLUMNS").append(
            and("PROCEDURE_CAT", "=", catalog)).append(
            and("PROCEDURE_SCHEM", "LIKE", schemaPattern)).append(
            and("PROCEDURE_NAME", "LIKE", procedureNamePattern)).append(
            and("COLUMN_NAME", "LIKE", columnNamePattern));

        // By default, query already returns result ordered by
        // PROCEDURE_SCHEM and PROCEDURE_NAME...
        return execute(select.toString());
    }

    /**
     * Retrieves a description of the tables available in the given catalog.
     * Only table descriptions matching the catalog, schema, table
     * name and type criteria are returned.  They are ordered by
     * TABLE_TYPE, TABLE_SCHEM and TABLE_NAME.
     * <P>
     * Each table description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>TABLE_TYPE</B> String => table type.  Typical types are "TABLE",
     *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
     *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *  <LI><B>REMARKS</B> String => explanatory comment on the table
     *  <LI><B>TYPE_CAT</B> String => the types catalog (may be <code>null</code>)
     *  <LI><B>TYPE_SCHEM</B> String => the types schema (may be <code>null</code>)
     *  <LI><B>TYPE_NAME</B> String => type name (may be <code>null</code>)
     *  <LI><B>SELF_REFERENCING_COL_NAME</B> String => name of the designated
     *                  "identifier" column of a typed table (may be <code>null</code>)
     *  <LI><B>REF_GENERATION</B> String => specifies how values in
     *                  SELF_REFERENCING_COL_NAME are created. Values are
     *                  "SYSTEM", "USER", "DERIVED". (may be <code>null</code>)
     *  </OL>
     *
     * <P><B>Note:</B> Some databases may not return information for
     * all tables.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.0, HSQLDB returns extra information on TEXT tables
     * in the REMARKS column. <p>
     *
     * Since 1.7.0, HSQLDB includes the new JDBC3 columns TYPE_CAT,
     * TYPE_SCHEM, TYPE_NAME and SELF_REFERENCING_COL_NAME in anticipation
     * of JDBC3 compliant tools. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param tableNamePattern a table name pattern; must match the
     *        table name as it is stored in the database
     * @param types a list of table types, which must be from the list of table types
     *         returned from {@link #getTableTypes},to include; <code>null</code> returns
     * all types
     * @return <code>ResultSet</code> - each row is a table description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern,
                               String[] types) throws SQLException {

        if (wantsIsNull(tableNamePattern)
                || (types != null && types.length == 0)) {
            return executeSelect("SYSTEM_TABLES", "0=1");
        }
        schemaPattern = translateSchema(schemaPattern);

        StringBuffer select =
            toQueryPrefix("SYSTEM_TABLES").append(and("TABLE_CAT", "=",
                catalog)).append(and("TABLE_SCHEM", "LIKE",
                                     schemaPattern)).append(and("TABLE_NAME",
                                         "LIKE", tableNamePattern));

        if (types == null) {

            // do not use to narrow search
        } else {

            // JDBC4 clarification:
            // fredt - we shouldn't impose this test as it breaks compatibility with tools
/*
            String[] allowedTypes = new String[] {
                "GLOBAL TEMPORARY", "SYSTEM TABLE", "TABLE", "VIEW"
            };
            int      illegalIndex = 0;
            String   illegalType  = null;

            outer_loop:
            for (int i = 0; i < types.length; i++) {
                for (int j = 0; j < allowedTypes.length; j++) {
                    if (allowedTypes[j].equals(types[i])) {
                        continue outer_loop;
                    }
                }

                illegalIndex = i;
                illegalType  = types[illegalIndex];

                break;
            }

            if (illegalType != null) {
                throw Util.sqlException(Trace.JDBC_INVALID_ARGUMENT,
                                        "types[" + illegalIndex + "]=>\""
                                        + illegalType + "\"");
            }
*/

            // end JDBC4 clarification
            //
            select.append(" AND TABLE_TYPE IN (").append(
                StringUtil.getList(types, ",", "'")).append(')');
        }

        // By default, query already returns result ordered by
        // TABLE_TYPE, TABLE_SCHEM and TABLE_NAME...
        return execute(select.toString());
    }

    /**
     * Retrieves the schema names available in this database.  The results
     * are ordered by schema name.
     *
     * <P>The schema columns are:
     *  <OL>
     *  <LI><B>TABLE_SCHEM</B> String => schema name
     *  <LI><B>TABLE_CATALOG</B> String => catalog name (may be <code>null</code>)
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, the list of schemas is returned.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a <code>ResultSet</code> object in which each row is a
     *         schema description
     * @exception SQLException if a database access error occurs
     *
     */
    public ResultSet getSchemas() throws SQLException {

        // By default, query already returns the result in contract order
        return executeSelect("SYSTEM_SCHEMAS", null);
    }

    /**
     * Retrieves the catalog names available in this database.  The results
     * are ordered by catalog name.
     *
     * <P>The catalog column is:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => catalog name
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a <code>ResultSet</code> object in which each row has a
     *         single <code>String</code> column that is a catalog name
     * @exception SQLException if a database access error occurs
     */
    public ResultSet getCatalogs() throws SQLException {
        return executeSelect("INFORMATION_SCHEMA_CATALOG_NAME", null);
    }

    /**
     * Retrieves the table types available in this database.  The results
     * are ordered by table type.
     *
     * <P>The table type is:
     *  <OL>
     *  <LI><B>TABLE_TYPE</B> String => table type.  Typical types are "TABLE",
     *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
     *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Since 1.7.1, HSQLDB reports: "TABLE", "VIEW" and "GLOBAL TEMPORARY"
     * types.
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a <code>ResultSet</code> object in which each row has a
     *         single <code>String</code> column that is a table type
     * @exception SQLException if a database access error occurs
     */
    public ResultSet getTableTypes() throws SQLException {

        // system table producer returns rows in contract order
        return executeSelect("SYSTEM_TABLETYPES", null);
    }

    /**
     * Retrieves a description of table columns available in
     * the specified catalog.
     *
     * <P>Only column descriptions matching the catalog, schema, table
     * and column name criteria are returned.  They are ordered by
     * <code>TABLE_SCHEM</code>, <code>TABLE_NAME</code>, and
     * <code>ORDINAL_POSITION</code>.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>COLUMN_NAME</B> String => column name
     *  <LI><B>DATA_TYPE</B> int => SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String => Data source dependent type name,
     *  for a UDT the type name is fully qualified
     *  <LI><B>COLUMN_SIZE</B> int => column size.
     *  <LI><B>BUFFER_LENGTH</B> is not used.
     *  <LI><B>DECIMAL_DIGITS</B> int => the number of fractional digits. Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
     *  <LI><B>NULLABLE</B> int => is NULL allowed.
     *      <UL>
     *      <LI> columnNoNulls - might not allow <code>NULL</code> values
     *      <LI> columnNullable - definitely allows <code>NULL</code> values
     *      <LI> columnNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String => comment describing column (may be <code>null</code>)
     *  <LI><B>COLUMN_DEF</B> String => (JDBC4 clarification:) default value for the column, which should be interpreted as a string when the value is enclosed in quotes (may be <code>null</code>)
     *  <LI><B>SQL_DATA_TYPE</B> int => unused
     *
     *        <p>HSQLDB-specific: CLI type from SQL 2003 Table 37,
     *        tables 6-9 Annex A1, and/or addendums in other
     *        documents, such as:<br>
     *        SQL 2003 Part 9: Management of External Data (SQL/MED) : DATALINK<br>
     *        SQL 2003 Part 14: XML-Related Specifications (SQL/XML) : XML<p>
     *
     *  <LI><B>SQL_DATETIME_SUB</B> int => unused (HSQLDB-specific: SQL 2003 CLI datetime/interval subcode)
     *  <LI><B>CHAR_OCTET_LENGTH</B> int => for char types the
     *       maximum number of bytes in the column
     *  <LI><B>ORDINAL_POSITION</B> int => index of column in table
     *      (starting at 1)
     *  <LI><B>IS_NULLABLE</B> String  => ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the parameter can include NULLs
     *       <LI> NO            --- if the parameter cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * parameter is unknown
     *       </UL>
     *  <LI><B>SCOPE_CATLOG</B> String => catalog of table that is the scope
     *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_SCHEMA</B> String => schema of table that is the scope
     *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_TABLE</B> String => table name that this the scope
     *      of a reference attribure (<code>null</code> if the DATA_TYPE isn't REF)
     *  <LI><B>SOURCE_DATA_TYPE</B> short => source type of a distinct type or user-generated
     *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
     *      isn't DISTINCT or user-generated REF)
     *   <LI><B>IS_AUTOINCREMENT</B> String  => Indicates whether this column is auto incremented
     *       <UL>
     *       <LI> YES           --- if the column is auto incremented
     *       <LI> NO            --- if the column is not auto incremented
     *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
     * parameter is unknown
     *       </UL>
     *  </OL>
     *
     * <p>(JDBC4 clarification:) The COLUMN_SIZE column represents the specified column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the [declared or implicit maximum] length in characters.
     * For datetime datatypes, this is the [maximum] length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the [maximum] length in bytes.  For the ROWID datatype,
     * this is the length in bytes[, as returned by the implementation-specific java.sql.RowId.getBytes() method]. 0 is returned for data types where the
     * column size is not applicable. <p>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.0, HSQLDB includes the new JDBC 3 columns SCOPE_CATLOG,
     * SCOPE_SCHEMA, SCOPE_TABLE and SOURCE_DATA_TYPE in anticipation
     * of JDBC 3 compliant tools.  However, these columns are never filled in;
     * the engine does not support the related features. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param tableNamePattern a table name pattern; must match the
     *        table name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column
     *        name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a column description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern) throws SQLException {

        if (wantsIsNull(tableNamePattern) || wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_COLUMNS", "0=1");
        }
        schemaPattern = translateSchema(schemaPattern);

        StringBuffer select = toQueryPrefix("SYSTEM_COLUMNS").append(
            and("TABLE_CAT", "=", catalog)).append(
            and("TABLE_SCHEM", "LIKE", schemaPattern)).append(
            and("TABLE_NAME", "LIKE", tableNamePattern)).append(
            and("COLUMN_NAME", "LIKE", columnNamePattern));

        // by default, query already returns the result ordered
        // by TABLE_SCHEM, TABLE_NAME and ORDINAL_POSITION
        return execute(select.toString());
    }

    /**
     * Retrieves a description of the access rights for a table's columns.
     *
     * <P>Only privileges matching the column name criteria are
     * returned.  They are ordered by COLUMN_NAME and PRIVILEGE.
     *
     * <P>Each privilige description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>COLUMN_NAME</B> String => column name
     *  <LI><B>GRANTOR</B> => grantor of access (may be <code>null</code>)
     *  <LI><B>GRANTEE</B> String => grantee of access
     *  <LI><B>PRIVILEGE</B> String => name of access (SELECT,
     *      INSERT, UPDATE, REFRENCES, ...)
     *  <LI><B>IS_GRANTABLE</B> String => "YES" if grantee is permitted
     *      to grant to others; "NO" if not; <code>null</code> if unknown
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name as it is
     *        stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is
     *        stored in the database
     * @param columnNamePattern a column name pattern; must match the column
     *        name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a column privilege description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getColumnPrivileges(String catalog, String schema,
            String table, String columnNamePattern) throws SQLException {

        if (table == null) {
            throw Util.nullArgument("table");
        }
/*
        if (wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_COLUMNPRIVILEGES", "0=1");
        }
*/
        schema = translateSchema(schema);

        String sql =
            "SELECT TABLE_CATALOG TABLE_CAT, TABLE_SCHEMA TABLE_SCHEM,"
            + "TABLE_NAME, COLUMN_NAME, GRANTOR, GRANTEE, PRIVILEGE_TYPE PRIVILEGE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE TRUE "
            + and("TABLE_CAT", "=", catalog) + and("TABLE_SCHEM", "=", schema)
            + and("TABLE_NAME", "=", table)
            + and("COLUMN_NAME", "LIKE", columnNamePattern)
        ;

        // By default, the query already returns the result
        // ordered by column name, privilege...
        return execute(sql);
    }

    /**
     * Retrieves a description of the access rights for each table available
     * in a catalog. Note that a table privilege applies to one or
     * more columns in the table. It would be wrong to assume that
     * this privilege applies to all columns (this may be true for
     * some systems but is not true for all.)
     *
     * <P>Only privileges matching the schema and table name
     * criteria are returned.  They are ordered by TABLE_SCHEM,
     * TABLE_NAME, and PRIVILEGE.
     *
     * <P>Each privilige description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>GRANTOR</B> => grantor of access (may be <code>null</code>)
     *  <LI><B>GRANTEE</B> String => grantee of access
     *  <LI><B>PRIVILEGE</B> String => name of access (SELECT,
     *      INSERT, UPDATE, REFRENCES, ...)
     *  <LI><B>IS_GRANTABLE</B> String => "YES" if grantee is permitted
     *      to grant to others; "NO" if not; <code>null</code> if unknown
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param tableNamePattern a table name pattern; must match the
     *        table name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a table privilege description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getTablePrivileges(
            String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {

        schemaPattern = translateSchema(schemaPattern);

        String sql =
            "SELECT TABLE_CATALOG TABLE_CAT, TABLE_SCHEMA TABLE_SCHEM,"
            + "TABLE_NAME, GRANTOR, GRANTEE, PRIVILEGE_TYPE PRIVILEGE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE TRUE "
            + and("TABLE_CATALOG", "=", catalog)
            + and("TABLE_SCHEMA", "LIKE", schemaPattern)
            + and("TABLE_NAME", "LIKE", tableNamePattern);

/*
        if (wantsIsNull(tableNamePattern)) {
            return executeSelect("SYSTEM_TABLEPRIVILEGES", "0=1");
        }
*/

        // By default, the query already returns a result ordered by
        // TABLE_SCHEM, TABLE_NAME, and PRIVILEGE...
        return execute(sql);
    }

    /**
     * Retrieves a description of a table's optimal set of columns that
     * uniquely identifies a row. They are ordered by SCOPE.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>SCOPE</B> short => actual scope of result
     *      <UL>
     *      <LI> bestRowTemporary - very temporary, while using row
     *      <LI> bestRowTransaction - valid for remainder of current transaction
     *      <LI> bestRowSession - valid for remainder of current session
     *      </UL>
     *  <LI><B>COLUMN_NAME</B> String => column name
     *  <LI><B>DATA_TYPE</B> int => SQL data type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String => Data source dependent type name,
     *  for a UDT the type name is fully qualified
     *  <LI><B>COLUMN_SIZE</B> int => precision
     *  <LI><B>BUFFER_LENGTH</B> int => not used
     *  <LI><B>DECIMAL_DIGITS</B> short  => scale - Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>PSEUDO_COLUMN</B> short => is this a pseudo column
     *      like an Oracle ROWID
     *      <UL>
     *      <LI> bestRowUnknown - may or may not be pseudo column
     *      <LI> bestRowNotPseudo - is NOT a pseudo column
     *      <LI> bestRowPseudo - is a pseudo column
     *      </UL>
     *  </OL>
     *
     * <p>(JDBC4 clarification:)<p>
     * The COLUMN_SIZE column represents the specified column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the [declared or implicit maximum] length in characters.
     * For datetime datatypes, this is the [maximum] length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the [maximum] length in bytes.  For the ROWID datatype,
     * this is the length in bytes[, as returned by the implementation-specific java.sql.RowId.getBytes() method]. 0 is returned for data types where the
     * column size is not applicable. <p>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * If the name of a column is defined in the database without double
     * quotes, an all-uppercase name must be specified when calling this
     * method. Otherwise, the name must be specified in the exact case of
     * the column definition in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @param scope the scope of interest; use same values as SCOPE
     * @param nullable include columns that are nullable.
     * @return <code>ResultSet</code> - each row is a column description
     * @exception SQLException if a database access error occurs
     */
    public ResultSet getBestRowIdentifier(String catalog, String schema,
            String table, int scope, boolean nullable) throws SQLException {

        if (table == null) {
            throw Util.nullArgument("table");
        }

        String scopeIn;

        switch (scope) {

            case bestRowTemporary :
                scopeIn = BRI_TEMPORARY_SCOPE_IN_LIST;

                break;
            case bestRowTransaction :
                scopeIn = BRI_TRANSACTION_SCOPE_IN_LIST;

                break;
            case bestRowSession :
                scopeIn = BRI_SESSION_SCOPE_IN_LIST;

                break;
            default :
                throw Util.invalidArgument("scope");
        }
        schema = translateSchema(schema);

        Integer Nullable = (nullable) ? null
                                      : INT_COLUMNS_NO_NULLS;
        StringBuffer select =
            toQueryPrefix("SYSTEM_BESTROWIDENTIFIER").append(and("TABLE_CAT",
                "=", catalog)).append(and("TABLE_SCHEM", "=",
                    schema)).append(and("TABLE_NAME", "=",
                                        table)).append(and("NULLABLE", "=",
                                            Nullable)).append(" AND SCOPE IN "
                                                + scopeIn);

        // By default, query already returns rows in contract order.
        // However, the way things are set up, there should never be
        // a result where there is > 1 distinct scope value:  most requests
        // will want only one table and the system table producer (for
        // now) guarantees that a maximum of one BRI scope column set is
        // produced for each table
        return execute(select.toString());
    }

    /**
     * Retrieves a description of a table's columns that are automatically
     * updated when any value in a row is updated.  They are
     * unordered.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>SCOPE</B> short => is not used
     *  <LI><B>COLUMN_NAME</B> String => column name
     *  <LI><B>DATA_TYPE</B> int => SQL data type from <code>java.sql.Types</code>
     *  <LI><B>TYPE_NAME</B> String => Data source-dependent type name
     *  <LI><B>COLUMN_SIZE</B> int => precision
     *  <LI><B>BUFFER_LENGTH</B> int => length of column value in bytes
     *  <LI><B>DECIMAL_DIGITS</B> short  => scale - Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>PSEUDO_COLUMN</B> short => whether this is pseudo column
     *      like an Oracle ROWID
     *      <UL>
     *      <LI> versionColumnUnknown - may or may not be pseudo column
     *      <LI> versionColumnNotPseudo - is NOT a pseudo column
     *      <LI> versionColumnPseudo - is a pseudo column
     *      </UL>
     *  </OL>
     *
     * <p>(JDBC4 clarification:)
     *
     * <p>The COLUMN_SIZE column represents the specified column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the [declared or implicit maximum] length in characters.
     * For datetime datatypes, this is the [maximum] length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the [maximum] length in bytes.  For the ROWID datatype,
     * this is the length in bytes[, as returned by the implementation-specific java.sql.RowId.getBytes() method]. 0 is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @return a <code>ResultSet</code> object in which each row is a
     *         column description
     * @exception SQLException if a database access error occurs
     */
    public ResultSet getVersionColumns(String catalog, String schema,
                                       String table) throws SQLException {

        if (table == null) {
            throw Util.nullArgument("table");
        }
        schema = translateSchema(schema);

        StringBuffer select =
            toQueryPrefix("SYSTEM_VERSIONCOLUMNS").append(and("TABLE_CAT",
                "=", catalog)).append(and("TABLE_SCHEM", "=",
                    schema)).append(and("TABLE_NAME", "=", table));

        // result does not need to be ordered
        return execute(select.toString());
    }

    /**
     * Retrieves a description of the given table's primary key columns.  They
     * are ordered by COLUMN_NAME.
     *
     * <P>Each primary key column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>COLUMN_NAME</B> String => column name
     *  <LI><B>KEY_SEQ</B> short => (JDBC4 Clarification:) sequence number within primary key( a value
     *  of 1 represents the first column of the primary key, a value of 2 would
     *  represent the second column within the primary key).
     *  <LI><B>PK_NAME</B> String => primary key name (may be <code>null</code>)
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @return <code>ResultSet</code> - each row is a primary key column description
     * @exception SQLException if a database access error occurs
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getPrimaryKeys(String catalog, String schema,
                                    String table) throws SQLException {

        if (table == null) {
            throw Util.nullArgument("table");
        }
        schema = translateSchema(schema);

        StringBuffer select =
            toQueryPrefix("SYSTEM_PRIMARYKEYS").append(and("TABLE_CAT", "=",
                catalog)).append(and("TABLE_SCHEM", "=",
                                     schema)).append(and("TABLE_NAME", "=",
                                         table));

        // By default, query already returns result in contract order
        return execute(select.toString());
    }

    /**
     * Retrieves a description of the primary key columns that are
     * referenced by the given table's foreign key columns (the primary keys
     * imported by a table).  They are ordered by PKTABLE_CAT,
     * PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
     *
     * <P>Each primary key column description has the following columns:
     *  <OL>
     *  <LI><B>PKTABLE_CAT</B> String => primary key table catalog
     *      being imported (may be <code>null</code>)
     *  <LI><B>PKTABLE_SCHEM</B> String => primary key table schema
     *      being imported (may be <code>null</code>)
     *  <LI><B>PKTABLE_NAME</B> String => primary key table name
     *      being imported
     *  <LI><B>PKCOLUMN_NAME</B> String => primary key column name
     *      being imported
     *  <LI><B>FKTABLE_CAT</B> String => foreign key table catalog (may be <code>null</code>)
     *  <LI><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be <code>null</code>)
     *  <LI><B>FKTABLE_NAME</B> String => foreign key table name
     *  <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
     *  <LI><B>KEY_SEQ</B> short => (JDBC4 clarification) sequence number within a foreign key (a value
     *  of 1 represents the first column of the foreign key, a value of 2 would
     *  represent the second column within the foreign key).
     *  <LI><B>UPDATE_RULE</B> short => What happens to a
     *       foreign key when the primary key is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with primary key update
     *      <LI> importedKeySetNull - change imported key to <code>NULL</code>
     *               if its primary key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *  <LI><B>DELETE_RULE</B> short => What happens to
     *      the foreign key when primary is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its primary key has been deleted
     *      </UL>
     *  <LI><B>FK_NAME</B> String => foreign key name (may be <code>null</code>)
     *  <LI><B>PK_NAME</B> String => primary key name (may be <code>null</code>)
     *  <LI><B>DEFERRABILITY</B> short => can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @return <code>ResultSet</code> - each row is a primary key column description
     * @exception SQLException if a database access error occurs
     * @see #getExportedKeys
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getImportedKeys(String catalog, String schema,
                                     String table) throws SQLException {

        if (table == null) {
            throw Util.nullArgument("table");
        }
        schema = translateSchema(schema);

        StringBuffer select = toQueryPrefix("SYSTEM_CROSSREFERENCE").append(
            and("FKTABLE_CAT", "=", catalog)).append(
            and("FKTABLE_SCHEM", "=", schema)).append(
            and("FKTABLE_NAME", "=", table)).append(
            " ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ");

        return execute(select.toString());
    }

    /**
     * Retrieves a description of the foreign key columns that reference the
     * given table's primary key columns (the foreign keys exported by a
     * table).  They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
     * FKTABLE_NAME, and KEY_SEQ.
     *
     * <P>Each foreign key column description has the following columns:
     *  <OL>
     *  <LI><B>PKTABLE_CAT</B> String => primary key table catalog (may be <code>null</code>)
     *  <LI><B>PKTABLE_SCHEM</B> String => primary key table schema (may be <code>null</code>)
     *  <LI><B>PKTABLE_NAME</B> String => primary key table name
     *  <LI><B>PKCOLUMN_NAME</B> String => primary key column name
     *  <LI><B>FKTABLE_CAT</B> String => foreign key table catalog (may be <code>null</code>)
     *      being exported (may be <code>null</code>)
     *  <LI><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be <code>null</code>)
     *      being exported (may be <code>null</code>)
     *  <LI><B>FKTABLE_NAME</B> String => foreign key table name
     *      being exported
     *  <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
     *      being exported
     *  <LI><B>KEY_SEQ</B> short => (JDBC4 clarification:) sequence number within foreign key( a value
     *  of 1 represents the first column of the foreign key, a value of 2 would
     *  represent the second column within the foreign key).
     *  <LI><B>UPDATE_RULE</B> short => What happens to
     *       foreign key when primary is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with primary key update
     *      <LI> importedKeySetNull - change imported key to <code>NULL</code> if
     *               its primary key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *  <LI><B>DELETE_RULE</B> short => What happens to
     *      the foreign key when primary is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to <code>NULL</code> if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its primary key has been deleted
     *      </UL>
     *  <LI><B>FK_NAME</B> String => foreign key name (may be <code>null</code>)
     *  <LI><B>PK_NAME</B> String => primary key name (may be <code>null</code>)
     *  <LI><B>DEFERRABILITY</B> short => can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in this database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in this database
     * @return a <code>ResultSet</code> object in which each row is a
     *         foreign key column description
     * @exception SQLException if a database access error occurs
     * @see #getImportedKeys
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getExportedKeys(String catalog, String schema,
                                     String table) throws SQLException {

        if (table == null) {
            throw Util.nullArgument("table");
        }
        schema = translateSchema(schema);

        StringBuffer select =
            toQueryPrefix("SYSTEM_CROSSREFERENCE").append(and("PKTABLE_CAT",
                "=", catalog)).append(and("PKTABLE_SCHEM", "=",
                    schema)).append(and("PKTABLE_NAME", "=", table));

        // By default, query already returns the table ordered by
        // FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
        return execute(select.toString());
    }

    /**
     * (JDBC4 clarification:)
     * Retrieves a description of the foreign key columns in the given foreign key
     * table that reference the primary key or the columns representing a unique constraint of the  parent table (could be the same or a different table).
     * The number of columns returned from the parent table must match the number of
     * columns that make up the foreign key.  They
     * are ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and
     * KEY_SEQ.
     *
     * <P>Each foreign key column description has the following columns:
     *  <OL>
     *  <LI><B>PKTABLE_CAT</B> String => parent key table catalog (may be <code>null</code>)
     *  <LI><B>PKTABLE_SCHEM</B> String => parent key table schema (may be <code>null</code>)
     *  <LI><B>PKTABLE_NAME</B> String => parent key table name
     *  <LI><B>PKCOLUMN_NAME</B> String => parent key column name
     *  <LI><B>FKTABLE_CAT</B> String => foreign key table catalog (may be <code>null</code>)
     *      being exported (may be <code>null</code>)
     *  <LI><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be <code>null</code>)
     *      being exported (may be <code>null</code>)
     *  <LI><B>FKTABLE_NAME</B> String => foreign key table name
     *      being exported
     *  <LI><B>FKCOLUMN_NAME</B> String => foreign key column name
     *      being exported
     *  <LI><B>KEY_SEQ</B> short => sequence number within foreign key( a value
     *  of 1 represents the first column of the foreign key, a value of 2 would
     *  represent the second column within the foreign key).
     *  <LI><B>UPDATE_RULE</B> short => What happens to
     *       foreign key when parent key is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of parent
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with parent key update
     *      <LI> importedKeySetNull - change imported key to <code>NULL</code> if
     *               its parent key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its parent key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *  <LI><B>DELETE_RULE</B> short => What happens to
     *      the foreign key when parent key is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of parent
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to <code>NULL</code> if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its parent key has been deleted
     *      </UL>
     *  <LI><B>FK_NAME</B> String => foreign key name (may be <code>null</code>)
     *  <LI><B>PK_NAME</B> String => parent key name (may be <code>null</code>)
     *  <LI><B>DEFERRABILITY</B> short => can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parentCatalog a catalog name; must match the catalog name
     * as it is stored in the database; "" retrieves those without a
     * catalog; <code>null</code> means drop catalog name from the selection criteria
     * @param parentSchema a schema name; must match the schema name as
     * it is stored in the database; "" retrieves those without a schema;
     * <code>null</code> means drop schema name from the selection criteria
     * @param parentTable the name of the table that exports the key; must match
     * the table name as it is stored in the database
     * @param foreignCatalog a catalog name; must match the catalog name as
     * it is stored in the database; "" retrieves those without a
     * catalog; <code>null</code> means drop catalog name from the selection criteria
     * @param foreignSchema a schema name; must match the schema name as it
     * is stored in the database; "" retrieves those without a schema;
     * <code>null</code> means drop schema name from the selection criteria
     * @param foreignTable the name of the table that imports the key; must match
     * the table name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a foreign key column description
     * @exception SQLException if a database access error occurs
     * @see #getImportedKeys
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getCrossReference(
            String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema,
            String foreignTable) throws SQLException {

        if (parentTable == null) {
            throw Util.nullArgument("parentTable");
        }

        if (foreignTable == null) {
            throw Util.nullArgument("foreignTable");
        }
        parentSchema  = translateSchema(parentSchema);
        foreignSchema = translateSchema(foreignSchema);

        StringBuffer select =
            toQueryPrefix("SYSTEM_CROSSREFERENCE").append(and("PKTABLE_CAT",
                "=", parentCatalog)).append(and("PKTABLE_SCHEM", "=",
                    parentSchema)).append(and("PKTABLE_NAME", "=",
                        parentTable)).append(and("FKTABLE_CAT", "=",
                            foreignCatalog)).append(and("FKTABLE_SCHEM", "=",
                                foreignSchema)).append(and("FKTABLE_NAME",
                                    "=", foreignTable));

        // by default, query already returns the table ordered by
        // FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
        return execute(select.toString());
    }

    /**
     * Retrieves a description of all the (JDBC4 clarification:) data types supported by
     * this database. They are ordered by DATA_TYPE and then by how
     * closely the data type maps to the corresponding JDBC SQL type.
     *
     * <P>(JDBC4 clarification:) If the database supports SQL distinct types, then getTypeInfo() will return
     * a single row with a TYPE_NAME of DISTINCT and a DATA_TYPE of Types.DISTINCT.
     * If the database supports SQL structured types, then getTypeInfo() will return
     * a single row with a TYPE_NAME of STRUCT and a DATA_TYPE of Types.STRUCT.
     *
     * <P>(JDBC4 clarification:)
     * <P>If SQL distinct or structured types are supported, then information on the
     * individual types may be obtained from the getUDTs() method.
     *
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *  <LI><B>TYPE_NAME</B> String => Type name
     *  <LI><B>DATA_TYPE</B> int => SQL data type from java.sql.Types
     *  <LI><B>PRECISION</B> int => maximum precision
     *  <LI><B>LITERAL_PREFIX</B> String => prefix used to quote a literal
     *      (may be <code>null</code>)
     *  <LI><B>LITERAL_SUFFIX</B> String => suffix used to quote a literal
     * (may be <code>null</code>)
     *  <LI><B>CREATE_PARAMS</B> String => parameters used in creating
     *      the type (may be <code>null</code>)
     *  <LI><B>NULLABLE</B> short => can you use NULL for this type.
     *      <UL>
     *      <LI> typeNoNulls - does not allow NULL values
     *      <LI> typeNullable - allows NULL values
     *      <LI> typeNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>CASE_SENSITIVE</B> boolean=> is it case sensitive.
     *  <LI><B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
     *      <UL>
     *      <LI> typePredNone - No support
     *      <LI> typePredChar - Only supported with WHERE .. LIKE
     *      <LI> typePredBasic - Supported except for WHERE .. LIKE
     *      <LI> typeSearchable - Supported for all WHERE ..
     *      </UL>
     *  <LI><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned.
     *  <LI><B>FIXED_PREC_SCALE</B> boolean => can it be a money value.
     *  <LI><B>AUTO_INCREMENT</B> boolean => can it be used for an
     *      auto-increment value.
     *  <LI><B>LOCAL_TYPE_NAME</B> String => localized version of type name
     *      (may be <code>null</code>)
     *  <LI><B>MINIMUM_SCALE</B> short => minimum scale supported
     *  <LI><B>MAXIMUM_SCALE</B> short => maximum scale supported
     *  <LI><B>SQL_DATA_TYPE</B> int => unused
     *  <LI><B>SQL_DATETIME_SUB</B> int => unused
     *  <LI><B>NUM_PREC_RADIX</B> int => usually 2 or 10
     *  </OL>
     *
     * <p>(JDBC4 clarification:) The PRECISION column represents the maximum column size that the server supports for the given datatype.
     * For numeric data, this is the maximum precision.  For character data, this is the [maximum] length in characters.
     * For datetime datatypes, this is the [maximum] length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the [maximum] length in bytes.  For the ROWID datatype,
     * this is the length in bytes[, as returned by the implementation-specific java.sql.RowId.getBytes() method]. 0 is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a <code>ResultSet</code> object in which each row is an SQL
     *         type description
     * @exception SQLException if a database access error occurs
     */
    public ResultSet getTypeInfo() throws SQLException {

        // system table producer returns rows in contract order
        return executeSelect("SYSTEM_TYPEINFO", null);
    }

    /**
     * Retrieves a description of the given table's indices and statistics. They are
     * ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     *
     * <P>Each index column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>NON_UNIQUE</B> boolean => Can index values be non-unique.
     *      false when TYPE is tableIndexStatistic
     *  <LI><B>INDEX_QUALIFIER</B> String => index catalog (may be <code>null</code>);
     *      <code>null</code> when TYPE is tableIndexStatistic
     *  <LI><B>INDEX_NAME</B> String => index name; <code>null</code> when TYPE is
     *      tableIndexStatistic
     *  <LI><B>TYPE</B> short => index type:
     *      <UL>
     *      <LI> tableIndexStatistic - this identifies table statistics that are
     *           returned in conjuction with a table's index descriptions
     *      <LI> tableIndexClustered - this is a clustered index
     *      <LI> tableIndexHashed - this is a hashed index
     *      <LI> tableIndexOther - this is some other style of index
     *      </UL>
     *  <LI><B>ORDINAL_POSITION</B> short => column sequence number
     *      within index; zero when TYPE is tableIndexStatistic
     *  <LI><B>COLUMN_NAME</B> String => column name; <code>null</code> when TYPE is
     *      tableIndexStatistic
     *  <LI><B>ASC_OR_DESC</B> String => column sort sequence, "A" => ascending,
     *      "D" => descending, may be <code>null</code> if sort sequence is not supported;
     *      <code>null</code> when TYPE is tableIndexStatistic
     *  <LI><B>CARDINALITY</B> int => When TYPE is tableIndexStatistic, then
     *      this is the number of rows in the table; otherwise, it is the
     *      number of unique values in the index.
     *  <LI><B>PAGES</B> int => When TYPE is  tableIndexStatisic then
     *      this is the number of pages used for the table, otherwise it
     *      is the number of pages used for the current index.
     *  <LI><B>FILTER_CONDITION</B> String => Filter condition, if any.
     *      (may be <code>null</code>)
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Since 1.7.2, this feature is supported by default. If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in this database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in this database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in this database
     * @param unique when true, return only indices for unique values;
     *     when false, return indices regardless of whether unique or not
     * @param approximate when true, result is allowed to reflect approximate
     *     or out of data values; when false, results are requested to be
     *     accurate
     * @return <code>ResultSet</code> - each row is an index column description
     * @exception SQLException if a database access error occurs
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique,
                                  boolean approximate) throws SQLException {

        if (table == null) {
            throw Util.nullArgument("table");
        }
        schema = translateSchema(schema);

        Boolean nu = (unique) ? Boolean.FALSE
                              : null;
        StringBuffer select =
            toQueryPrefix("SYSTEM_INDEXINFO").append(and("TABLE_CAT", "=",
                catalog)).append(and("TABLE_SCHEM", "=",
                                     schema)).append(and("TABLE_NAME", "=",
                                         table)).append(and("NON_UNIQUE", "=",
                                             nu));

        // By default, this query already returns the table ordered by
        // NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION...
        return execute(select.toString());
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * Retrieves whether this database supports the given result set type.
     *
     * @param type defined in <code>java.sql.ResultSet</code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @see JDBCConnection
     * @since  JDK 1.2 (JDK 1.1.x developers: read the overview
     *      for JDBCDatabaseMetaData)
     */
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == JDBCResultSet.TYPE_FORWARD_ONLY
                || type == JDBCResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    /**
     * Retrieves whether this database supports the given concurrency type
     * in combination with the given result set type.
     *
     * @param type defined in <code>java.sql.ResultSet</code>
     * @param concurrency type defined in <code>java.sql.ResultSet</code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @see JDBCConnection
     * @since  JDK 1.2 (JDK 1.1.x developers: read the overview
     *      for JDBCDatabaseMetaData)
     */
    public boolean supportsResultSetConcurrency(int type,
            int concurrency) throws SQLException {

        return supportsResultSetType(type)
               && (concurrency == JDBCResultSet.CONCUR_READ_ONLY
                   || concurrency == JDBCResultSet.CONCUR_UPDATABLE);
    }

    /**
     *
     * Retrieves whether for the given type of <code>ResultSet</code> object,
     * the result set's own updates are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     * Updates to ResultSet rows are not visible after moving from the updated
     * row.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if updates are visible for the given result set type;
     *        <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether a result set's own deletes are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Rows deleted from the ResultSet are still visible after moving from the
     * deleted row.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if deletes are visible for the given result set type;
     *        <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether a result set's own inserts are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Rows added to a ResultSet are not visible after moving from the
     * insert row; this method always returns <code>false</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if inserts are visible for the given result set type;
     *        <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether updates made by others are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Updates made by other connections or the same connection while the
     * ResultSet is open are not visible in the ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if updates made by others
     *        are visible for the given result set type;
     *        <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether deletes made by others are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Deletes made by other connections or the same connection while the
     * ResultSet is open are not visible in the ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if deletes made by others
     *        are visible for the given result set type;
     *        <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether inserts made by others are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Inserts made by other connections or the same connection while the
     * ResultSet is open are not visible in the ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if inserts made by others
     *         are visible for the given result set type;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether or not a visible row update can be detected by
     * calling the method <code>ResultSet.rowUpdated</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Updates made to the rows of the ResultSet are not detected by
     * calling the <code>ResultSet.rowUpdated</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if changes are detected by the result set type;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether or not a visible row delete can be detected by
     * calling the method <code>ResultSet.rowDeleted</code>.  If the method
     * <code>deletesAreDetected</code> returns <code>false</code>, it means that
     * deleted rows are removed from the result set.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Deletes made to the rows of the ResultSet are not detected by
     * calling the <code>ResultSet.rowDeleted/code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if deletes are detected by the given result set type;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether or not a visible row insert can be detected
     * by calling the method <code>ResultSet.rowInserted</code>.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Inserts made into the ResultSet are not visible and thus not detected by
     * calling the <code>ResultSet.rowInserted</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the <code>ResultSet</code> type; one of
     *        <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *        <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @return <code>true</code> if changes are detected by the specified result
     *         set type; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports batch updates.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports batch updates;
     * this method always returns <code>true</code>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if this database supports batch upcates;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    /**
     * Retrieves a description of the user-defined types (UDTs) defined
     * in a particular schema.  Schema-specific UDTs may have type
     * <code>JAVA_OBJECT</code>, <code>STRUCT</code>,
     * or <code>DISTINCT</code>.
     *
     * <P>Only types matching the catalog, schema, type name and type
     * criteria are returned.  They are ordered by DATA_TYPE, TYPE_SCHEM
     * and TYPE_NAME.  The type name parameter may be a fully-qualified
     * name.  In this case, the catalog and schemaPattern parameters are
     * ignored.
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *  <LI><B>TYPE_CAT</B> String => the type's catalog (may be <code>null</code>)
     *  <LI><B>TYPE_SCHEM</B> String => type's schema (may be <code>null</code>)
     *  <LI><B>TYPE_NAME</B> String => type name
     *  <LI><B>CLASS_NAME</B> String => Java class name
     *  <LI><B>DATA_TYPE</B> int => type value defined in java.sql.Types.
     *     One of JAVA_OBJECT, STRUCT, or DISTINCT
     *  <LI><B>REMARKS</B> String => explanatory comment on the type
     *  <LI><B>BASE_TYPE</B> short => type code of the source type of a
     *     DISTINCT type or the type that implements the user-generated
     *     reference type of the SELF_REFERENCING_COLUMN of a structured
     *     type as defined in java.sql.Types (<code>null</code> if DATA_TYPE is not
     *     DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
     *  </OL>
     *
     * <P><B>Note:</B> If the driver does not support UDTs, an empty
     * result set is returned.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Up to and including 1.7.1, HSQLDB does not support UDTs and
     * thus produces an empty result. <p>
     *
     * Starting with 1.7.2, there is an option to support this feature
     * to greater or lesser degrees.  See the documentation specific to the
     * selected system table provider implementation. The default implementation
     * is org.hsqldb_voltpatches.DatabaseInformationFull.
     * </div>
     * <!-- end release-specific documentation -->
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema pattern name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param typeNamePattern a type name pattern; must match the type name
     *        as it is stored in the database; may be a fully qualified name
     * @param types a list of user-defined types (JAVA_OBJECT,
     *        STRUCT, or DISTINCT) to include; <code>null</code> returns all types
     * @return <code>ResultSet</code> object in which each row describes a UDT
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape (JDBC4 clarification)
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview
     *     for JDBCDatabaseMetaData)
     */
    public ResultSet getUDTs(String catalog, String schemaPattern,
                             String typeNamePattern,
                             int[] types) throws SQLException {

        if (wantsIsNull(typeNamePattern)
                || (types != null && types.length == 0)) {
            executeSelect("SYSTEM_UDTS", "0=1");
        }
        schemaPattern = translateSchema(schemaPattern);

        StringBuffer select =
            toQueryPrefix("SYSTEM_UDTS").append(and("TYPE_CAT", "=",
                catalog)).append(and("TYPE_SCHEM", "LIKE",
                                     schemaPattern)).append(and("TYPE_NAME",
                                         "LIKE", typeNamePattern));

        if (types == null) {

            // do not use to narrow search
        } else {
            select.append(" AND DATA_TYPE IN (").append(
                StringUtil.getList(types, ",", "'")).append(')');
        }

        // By default, the query already returns a result ordered by
        // DATA_TYPE, TYPE_SCHEM, and TYPE_NAME...
        return execute(select.toString());
    }

    /**
     * Retrieves the connection that produced this metadata object.
     * <P>
     * @return the connection that produced this metadata object
     * @exception SQLException if a database access error occurs
     * @since  JDK 1.2 (JDK 1.1.x developers: read the overview
     *      for JDBCDatabaseMetaData)
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    // ------------------- JDBC 3.0 -------------------------

    /**
     * Retrieves whether this database supports savepoints.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Beginning with 1.7.2, this SQL feature is supported
     * through JDBC as well as SQL. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if savepoints are supported;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

//#endif JAVA4

    /**
     * Retrieves whether this database supports named parameters to callable
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.7.2, HSQLDB supports JDBC named parameters to
     * callable statements; this method returns true. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if named parameters are supported;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

//#endif JAVA4

    /**
     * Retrieves whether it is possible to have multiple <code>ResultSet</code> objects
     * returned from a <code>CallableStatement</code> object
     * simultaneously.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports multiple ResultSet
     * objects returned from a <code>CallableStatement</code>;
     * this method always returns <code>true</code>. <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if a <code>CallableStatement</code> object
     *         can return multiple <code>ResultSet</code> objects
     *         simultaneously; <code>false</code> otherwise
     * @exception SQLException if a datanase access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

//#endif JAVA4

    /**
     * Retrieves whether auto-generated keys can be retrieved after
     * a statement has been executed
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports retrieval of
     * autogenerated keys through the JDBC interface;
     * this method always returns <code>true</code>. <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return <code>true</code> if auto-generated keys can be retrieved
     *         after a statement has executed; <code>false</code> otherwise
     * <p>(JDBC4 Clarification:)
     * <p>If <code>true</code> is returned, the JDBC driver must support the
     * returning of auto-generated keys for at least SQL INSERT statements
     * <p>
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

//#endif JAVA4

    /**
     * Retrieves a description of the user-defined type (UDT) hierarchies defined in a
     * particular schema in this database. Only the immediate super type
     * sub type relationship is modeled.
     * <P>
     * Only supertype information for UDTs matching the catalog,
     * schema, and type name is returned. The type name parameter
     * may be a fully-qualified name. When the UDT name supplied is a
     * fully-qualified name, the catalog and schemaPattern parameters are
     * ignored.
     * <P>
     * If a UDT does not have a direct super type, it is not listed here.
     * A row of the <code>ResultSet</code> object returned by this method
     * describes the designated UDT and a direct supertype. A row has the following
     * columns:
     *  <OL>
     *  <LI><B>TYPE_CAT</B> String => the UDT's catalog (may be <code>null</code>)
     *  <LI><B>TYPE_SCHEM</B> String => UDT's schema (may be <code>null</code>)
     *  <LI><B>TYPE_NAME</B> String => type name of the UDT
     *  <LI><B>SUPERTYPE_CAT</B> String => the direct super type's catalog
     *                           (may be <code>null</code>)
     *  <LI><B>SUPERTYPE_SCHEM</B> String => the direct super type's schema
     *                             (may be <code>null</code>)
     *  <LI><B>SUPERTYPE_NAME</B> String => the direct super type's name
     *  </OL>
     *
     * <P><B>Note:</B> If the driver does not support type hierarchies, an
     * empty result set is returned.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * From 1.9.0, this feature is supported by default and return supertypes
     * for DOMAIN and DISTINCT types.<p>
     *
     * If the jar is
     * compiled without org.hsqldb_voltpatches.DatabaseInformationFull or
     * org.hsqldb_voltpatches.DatabaseInformationMain, the feature is
     * not supported. The default implementation is
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; "" retrieves those without a catalog;
     *        <code>null</code> means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     *        without a schema
     * @param typeNamePattern a UDT name pattern; may be a fully-qualified
     *        name
     * @return a <code>ResultSet</code> object in which a row gives information
     *         about the designated UDT
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape (JDBC4 clarification)
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public ResultSet getSuperTypes(
            String catalog, String schemaPattern,
            String typeNamePattern) throws SQLException {

        if (wantsIsNull(typeNamePattern)) {
            return executeSelect("SYSTEM_SUPERTYPES", "0=1");
        }
        schemaPattern = translateSchema(schemaPattern);

        StringBuffer select = toQueryPrefixNoSelect(
            "SELECT * FROM (SELECT USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME,"
            + "CAST (NULL AS INFORMATION_SCHEMA.SQL_IDENTIFIER), CAST (NULL AS INFORMATION_SCHEMA.SQL_IDENTIFIER), DATA_TYPE "
            + "FROM INFORMATION_SCHEMA.USER_DEFINED_TYPES "
            + "UNION SELECT DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME,NULL,NULL, DATA_TYPE "
            + "FROM INFORMATION_SCHEMA.DOMAINS) "
            + "AS SUPERTYPES(TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SUPERTYPE_CAT, SUPERTYPE_SCHEM, SUPERTYPE_NAME) ").append(
                and("TYPE_CAT", "=", catalog)).append(
                and("TYPE_SCHEM", "LIKE", schemaPattern)).append(
                and("TYPE_NAME", "LIKE", typeNamePattern));

        return execute(select.toString());
    }

//#endif JAVA4

    /**
     * Retrieves a description of the table hierarchies defined in a particular
     * schema in this database.
     *
     * <P>Only supertable information for tables matching the catalog, schema
     * and table name are returned. The table name parameter may be a fully-
     * qualified name, in which case, the catalog and schemaPattern parameters
     * are ignored. If a table does not have a super table, it is not listed here.
     * Supertables have to be defined in the same catalog and schema as the
     * sub tables. Therefore, the type description does not need to include
     * this information for the supertable.
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => the type's catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => type's schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => type name
     *  <LI><B>SUPERTABLE_NAME</B> String => the direct super type's name
     *  </OL>
     *
     * <P><B>Note:</B> If the driver does not support type hierarchies, an
     * empty result set is returned.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * This method is intended for tables of structured types.
     * From 1.9.0 this method returns an empty ResultSet.
     * {@link org.hsqldb_voltpatches.dbinfo.DatabaseInformationFull}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; "" retrieves those without a catalog;
     *        <code>null</code> means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     *        without a schema
     * @param tableNamePattern a table name pattern; may be a fully-qualified
     *        name
     * @return a <code>ResultSet</code> object in which each row is a type description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape (JDBC4 clarification)
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public ResultSet getSuperTables(
            String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {

        // query with no result
        StringBuffer select = toQueryPrefixNoSelect(
            "SELECT TABLE_NAME AS TABLE_CAT, TABLE_NAME AS TABLE_SCHEM, TABLE_NAME, TABLE_NAME AS SUPERTABLE_NAME "
            + "FROM INFORMATION_SCHEMA.TABLES ").append(
                and("TABLE_NAME", "=", ""));

        return execute(select.toString());
    }

//#endif JAVA4

    /**
     * Retrieves a description of the given attribute of the given type
     * for a user-defined type (UDT) that is available in the given schema
     * and catalog.
     * <P>
     * Descriptions are returned only for attributes of UDTs matching the
     * catalog, schema, type, and attribute name criteria. They are ordered by
     * TYPE_SCHEM, TYPE_NAME and ORDINAL_POSITION. This description
     * does not contain inherited attributes.
     * <P>
     * The <code>ResultSet</code> object that is returned has the following
     * columns:
     * <OL>
     *  <LI><B>TYPE_CAT</B> String => type catalog (may be <code>null</code>)
     *  <LI><B>TYPE_SCHEM</B> String => type schema (may be <code>null</code>)
     *  <LI><B>TYPE_NAME</B> String => type name
     *  <LI><B>ATTR_NAME</B> String => attribute name
     *  <LI><B>DATA_TYPE</B> int => attribute type SQL type from java.sql.Types
     *  <LI><B>ATTR_TYPE_NAME</B> String => Data source dependent type name.
     *  For a UDT, the type name is fully qualified. For a REF, the type name is
     *  fully qualified and represents the target type of the reference type.
     *  <LI><B>ATTR_SIZE</B> int => column size.  For char or date
     *      types this is the maximum number of characters; for numeric or
     *      decimal types this is precision.
     *  <LI><B>DECIMAL_DIGITS</B> int => the number of fractional digits. Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
     *  <LI><B>NULLABLE</B> int => whether NULL is allowed
     *      <UL>
     *      <LI> attributeNoNulls - might not allow NULL values
     *      <LI> attributeNullable - definitely allows NULL values
     *      <LI> attributeNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String => comment describing column (may be <code>null</code>)
     *  <LI><B>ATTR_DEF</B> String => default value (may be <code>null</code>)
     *  <LI><B>SQL_DATA_TYPE</B> int => unused
     *  <LI><B>SQL_DATETIME_SUB</B> int => unused
     *  <LI><B>CHAR_OCTET_LENGTH</B> int => for char types the
     *       maximum number of bytes in the column
     *  <LI><B>ORDINAL_POSITION</B> int => index of column in table
     *      (starting at 1)
     *  <LI><B>IS_NULLABLE</B> String  => ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the parameter can include NULLs
     *       <LI> NO            --- if the parameter cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * parameter is unknown
     *       </UL>
     *  <LI><B>SCOPE_CATALOG</B> String => catalog of table that is the
     *      scope of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_SCHEMA</B> String => schema of table that is the
     *      scope of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_TABLE</B> String => table name that is the scope of a
     *      reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
     * <LI><B>SOURCE_DATA_TYPE</B> short => source type of a distinct type or user-generated
     *      Ref type,SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
     *      isn't DISTINCT or user-generated REF)
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * This method is intended for attributes of structured types.
     * From 1.9.0 this method returns an empty ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param typeNamePattern a type name pattern; must match the
     *        type name as it is stored in the database
     * @param attributeNamePattern an attribute name pattern; must match the attribute
     *        name as it is declared in the database
     * @return a <code>ResultSet</code> object in which each row is an
     *         attribute description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public ResultSet getAttributes(
            String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {

        StringBuffer select = toQueryPrefixNoSelect(
            "SELECT TABLE_NAME AS TYPE_CAT, TABLE_NAME AS TYPE_SCHME, TABLE_NAME AS TYPE_NAME, "
            + "TABLE_NAME AS ATTR_NAME, CAST(0 AS INTEGER) AS DATA_TYPE, TABLE_NAME AS ATTR_TYPE_NAME, "
            + "CAST(0 AS INTEGER) AS ATTR_SIZE, CAST(0 AS INTEGER) AS DECIMAL_DIGITS, "
            + "CAST(0 AS INTEGER) AS NUM_PREC_RADIX, CAST(0 AS INTEGER) AS NULLABLE, "
            + "'' AS REMARK, '' AS ATTR_DEF, CAST(0 AS INTEGER) AS SQL_DATA_TYPE, "
            + "CAST(0 AS INTEGER) AS SQL_DATETIME_SUB, CAST(0 AS INTEGER) AS CHAR_OCTECT_LENGTH, "
            + "CAST(0 AS INTEGER) AS ORDINAL_POSITION, '' AS NULLABLE, "
            + "'' AS SCOPE_CATALOG, '' AS SCOPE_SCHEMA, '' AS SCOPE_TABLE, "
            + "CAST(0 AS SMALLINT) AS SCOPE_DATA_TYPE "
            + "FROM INFORMATION_SCHEMA.TABLES ").append(
                and("TABLE_NAME", "=", ""));

        return execute(select.toString());
    }

//#endif JAVA4

    /**
     * Retrieves whether this database supports the given result set holdability.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB returns true for both alternatives. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @param holdability one of the following constants:
     *          <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *          <code>ResultSet.CLOSE_CURSORS_AT_COMMIT<code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @see JDBCConnection
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public boolean supportsResultSetHoldability(
            int holdability) throws SQLException {
        return holdability == JDBCResultSet.HOLD_CURSORS_OVER_COMMIT
               || holdability == JDBCResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

//#endif JAVA4

    /**
     * (JDBC4 clarification:)
     * Retrieves this database's default holdability for <code>ResultSet</code>
     * objects.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB defaults to HOLD_CURSORS_OVER_COMMIT for CONSUR_READ_ONLY
     * ResultSet objects.
     * If the ResultSet concurrency is CONCUR_UPDATABLE, then holdability is
     * is enforced as CLOSE_CURSORS_AT_COMMIT. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the default holdability; either
     *         <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public int getResultSetHoldability() throws SQLException {
        return JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

//#endif JAVA4

    /**
     * Retrieves the major version number of the underlying database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.7.2, the feature is supported under JDK1.4+ builds. <p>
     *
     * This value is retrieved through an SQL call to the new
     * {@link org.hsqldb_voltpatches.Library#getDatabaseMajorVersion} method which allows
     * correct determination of the database major version for both local
     * and remote database instances.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the underlying database's major version
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public int getDatabaseMajorVersion() throws SQLException {

        ResultSet rs =
            execute("call \"org.hsqldb_voltpatches.Library.getDatabaseMajorVersion\"()");

        rs.next();

        int result = rs.getInt(1);

        rs.close();

        return result;
    }

//#endif JAVA4

    /**
     * Retrieves the minor version number of the underlying database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.7.2, the feature is supported under JDK1.4+ builds. <p>
     *
     * This value is retrieved through an SQL call to the new
     * {@link org.hsqldb_voltpatches.Library#getDatabaseMinorVersion} method which allows
     * correct determination of the database minor version for both local
     * and remote database instances.
     * </div>
     * <!-- end release-specific documentation -->
     * @return underlying database's minor version
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public int getDatabaseMinorVersion() throws SQLException {

        ResultSet rs =
            execute("call \"org.hsqldb_voltpatches.Library.getDatabaseMinorVersion\"()");

        rs.next();

        int result = rs.getInt(1);

        rs.close();

        return result;
    }

//#endif JAVA4

    /**
     * Retrieves the major JDBC version number for this
     * driver.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return JDBC version major number
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public int getJDBCMajorVersion() throws SQLException {
        return JDBC_MAJOR;
    }

//#endif JAVA4

    /**
     * Retrieves the minor JDBC version number for this
     * driver.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return JDBC version minor number
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

//#endif JAVA4

    /**
     * (JDBC4 modified:)
     * Indicates whether the SQLSTATE returned by <code>SQLException.getSQLState</code>
     * is X/Open (now known as Open Group) SQL CLI or SQL:2003.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB returns  <code>sqlStateSQL</code> under JDBC4 which is equivalent
     * to JDBC3 value of sqlStateSQL99. <p>
     * </div>
     * <!-- end release-specific documentation -->
     * @return the type of SQLSTATE; one of:
     *        sqlStateXOpen or
     *        sqlStateSQL
     *
     * <p>sqlStateSQL is new in JDBC4 and its value is the same as JDBC3 sqlStateSQL99
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL99;
    }

//#endif JAVA4

    /**
     * Indicates whether updates made to a LOB are made on a copy or directly
     * to the LOB.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Updates to a LOB are made directly.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if updates are made to a copy of the LOB;
     *         <code>false</code> if updates are made directly to the LOB
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

//#endif JAVA4

    /**
     * Retrieves whether this database supports statement pooling.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.9.0, HSQLDB supports statement pooling when built under
     * JDK 1.6+. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
//#ifdef JAVA4
    public boolean supportsStatementPooling() throws SQLException {
        return (JDBC_MAJOR >= 4);
    }

//#endif JAVA4
    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Indicates whether or not this data source supports the SQL <code>ROWID</code> type,
     * and if so  the lifetime for which a <code>RowId</code> object remains valid.
     * <p>
     * The returned int values have the following relationship:
     * <pre>
     *     ROWID_UNSUPPORTED < ROWID_VALID_OTHER < ROWID_VALID_TRANSACTION
     *         < ROWID_VALID_SESSION < ROWID_VALID_FOREVER
     * </pre>
     * so conditional logic such as
     * <pre>
     *     if (metadata.getRowIdLifetime() > DatabaseMetaData.ROWID_VALID_TRANSACTION)
     * </pre>
     * can be used. Valid Forever means valid across all Sessions, and valid for
     * a Session means valid across all its contained Transactions.
     *
     * @return the status indicating the lifetime of a <code>RowId</code>
     * @throws SQLException if a database access error occurs
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

//#endif JAVA6

    /**
     * Retrieves the schema names available in this database.  The results
     * are ordered by schema name.
     *
     * <P>The schema columns are:
     *  <OL>
     *  <LI><B>TABLE_SCHEM</B> String => schema name
     *  <LI><B>TABLE_CATALOG</B> String => catalog name (may be <code>null</code>)
     *  </OL>
     *
     *
     * @param catalog a catalog name; must match the catalog name as it is stored
     * in the database;"" retrieves those without a catalog; null means catalog
     * name should not be used to narrow down the search.
     * @param schemaPattern a schema name; must match the schema name as it is
     * stored in the database; null means
     * schema name should not be used to narrow down the search.
     * @return a <code>ResultSet</code> object in which each row is a
     *         schema description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public ResultSet getSchemas(String catalog,
                                String schemaPattern) throws SQLException {

        StringBuffer select =
            toQueryPrefix("SYSTEM_SCHEMAS").append(and("TABLE_CATALOG", "=",
                catalog)).append(and("TABLE_SCHEM", "LIKE", schemaPattern));

        // By default, query already returns result in contract order
        return execute(select.toString());
    }

//#endif JAVA6

    /**
     * Retrieves whether this database supports invoking user-defined or vendor functions
     * using the stored procedure escape syntax.
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

//#endif JAVA6

    /** @todo */

    /**
     * Retrieves whether a <code>SQLException</code> while autoCommit is <code>true</code> inidcates
     * that all open ResultSets are closed, even ones that are holdable.  When a <code>SQLException</code> occurs while
     * autocommit is <code>true</code>, it is vendor specific whether the JDBC driver responds with a commit operation, a
     * rollback operation, or by doing neither a commit nor a rollback.  A potential result of this difference
     * is in whether or not holdable ResultSets are closed.
     *
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

//#endif JAVA6

    /**
     * Retrieves a list of the client info properties
     * that the driver supports.  The result set contains the following columns
     * <p>
     * <ol>
     * <li><b>NAME</b> String=> The name of the client info property<br>
     * <li><b>MAX_LEN</b> int=> The maximum length of the value for the property<br>
     * <li><b>DEFAULT_VALUE</b> String=> The default value of the property<br>
     * <li><b>DESCRIPTION</b> String=> A description of the property.  This will typically
     *                                                  contain information as to where this property is
     *                                                  stored in the database.
     * </ol>
     * <p>
     * The <code>ResultSet</code> is sorted by the NAME column
     * <p>
     * @return  A <code>ResultSet</code> object; each row is a supported client info
     * property
     * <p>
     *  @exception SQLException if a database access error occurs
     * <p>
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public ResultSet getClientInfoProperties() throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * Retrieves a description of the user functions available in the given
     * catalog.
     * <P>
     * Only system and user function descriptions matching the schema and
     * function name criteria are returned.  They are ordered by
     * <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>,
     * <code>FUNCTION_NAME</code> and
     * <code>SPECIFIC_ NAME</code>.
     *
     * <P>Each function description has the the following columns:
     *  <OL>
     *  <LI><B>FUNCTION_CAT</B> String => function catalog (may be <code>null</code>)
     *  <LI><B>FUNCTION_SCHEM</B> String => function schema (may be <code>null</code>)
     *  <LI><B>FUNCTION_NAME</B> String => function name.  This is the name
     * used to invoke the function
     *  <LI><B>REMARKS</B> String => explanatory comment on the function
     * <LI><B>FUNCTION_TYPE</B> short => kind of function:
     *      <UL>
     *      <LI>functionResultUnknown - Cannot determine if a return value
     *       or table will be returned
     *      <LI> functionNoTable- Does not return a table
     *      <LI> functionReturnsTable - Returns a table
     *      </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  => the name which uniquely identifies
     *  this function within its schema.  This is a user specified, or DBMS
     * generated, name that may be different then the <code>FUNCTION_NAME</code>
     * for example with overload functions
     *  </OL>
     * <p>
     * A user may not have permission to execute any of the functions that are
     * returned by <code>getFunctions</code>
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param functionNamePattern a function name pattern; must match the
     *        function name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a function description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public ResultSet getFunctions(
            String catalog, String schemaPattern,
            String functionNamePattern) throws SQLException {

        StringBuffer sb = new StringBuffer(256);

        sb.append("select ").append(
            "sp.procedure_cat as FUNCTION_CAT,").append(
            "sp.procedure_schem as FUNCTION_SCHEM,").append(
            "sp.procedure_name as FUNCTION_NAME,").append(
            "sp.remarks as REMARKS,").append("1 as FUNCTION_TYPE,").append(
            "sp.specific_name as SPECIFIC_NAME,").append("sp.origin ").append(
            "from information_schema.system_procedures sp ").append(
            "where sp.procedure_type = 2 ");

        if (wantsIsNull(functionNamePattern)) {
            return execute(sb.append("and 1=0").toString());
        }
        schemaPattern = translateSchema(schemaPattern);

        sb.append(and("sp.procedure_cat", "=",
                      catalog)).append(and("sp.procedure_schem", "LIKE",
                          schemaPattern)).append(and("sp.procedure_name",
                              "LIKE", functionNamePattern));

        // By default, query already returns the result ordered by
        // FUNCTION_SCHEM, FUNCTION_NAME...
        return execute(sb.toString());
    }

//#endif JAVA6

    /**
     * Retrieves a description of the given catalog's system or user
     * function parameters and return type.
     *
     * <P>Only descriptions matching the schema,  function and
     * parameter name criteria are returned. They are ordered by
     * <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>,
     * <code>FUNCTION_NAME</code> and
     * <code>SPECIFIC_ NAME</code>. Within this, the return value,
     * if any, is first. Next are the parameter descriptions in call
     * order. The column descriptions follow in column number order.
     *
     * <P>Each row in the <code>ResultSet</code>
     * is a parameter description, column description or
     * return type description with the following fields:
     *  <OL>
     *  <LI><B>FUNCTION_CAT</B> String => function catalog (may be <code>null</code>)
     *  <LI><B>FUNCTION_SCHEM</B> String => function schema (may be <code>null</code>)
     *  <LI><B>FUNCTION_NAME</B> String => function name.  This is the name
     * used to invoke the function
     *  <LI><B>COLUMN_NAME</B> String => column/parameter name
     *  <LI><B>COLUMN_TYPE</B> Short => kind of column/parameter:
     *      <UL>
     *      <LI> functionColumnUnknown - nobody knows
     *      <LI> functionColumnIn - IN parameter
     *      <LI> functionColumnInOut - INOUT parameter
     *      <LI> functionColumnOut - OUT parameter
     *      <LI> functionColumnReturn - function return value
     *      <LI> functionColumnResult - Indicates that the parameter or column
     *  is a column in the <code>ResultSet</code>
     *      </UL>
     *  <LI><B>DATA_TYPE</B> int => SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String => SQL type name, for a UDT type the
     *  type name is fully qualified
     *  <LI><B>PRECISION</B> int => precision
     *  <LI><B>LENGTH</B> int => length in bytes of data
     *  <LI><B>SCALE</B> short => scale -  null is returned for data types where
     * SCALE is not applicable.
     *  <LI><B>RADIX</B> short => radix
     *  <LI><B>NULLABLE</B> short => can it contain NULL.
     *      <UL>
     *      <LI> functionNoNulls - does not allow NULL values
     *      <LI> functionNullable - allows NULL values
     *      <LI> functionNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String => comment describing column/parameter
     *  <LI><B>CHAR_OCTET_LENGTH</B> int  => the maximum length of binary
     * and character based parameters or columns.  For any other datatype the returned value
     * is a NULL
     *  <LI><B>ORDINAL_POSITION</B> int  => the ordinal position, starting
     * from 1, for the input and output parameters. A value of 0
     * is returned if this row describes the function's return value.
     * For result set columns, it is the
     * ordinal position of the column in the result set starting from 1.
     *  <LI><B>IS_NULLABLE</B> String  => ISO rules are used to determine
     * the nullability for a parameter or column.
     *       <UL>
     *       <LI> YES           --- if the parameter or column can include NULLs
     *       <LI> NO            --- if the parameter or column  cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * parameter  or column is unknown
     *       </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  => the name which uniquely identifies
     * this function within its schema.  This is a user specified, or DBMS
     * generated, name that may be different then the <code>FUNCTION_NAME</code>
     * for example with overload functions
     *  </OL>
     *
     * <p>The PRECISION column represents the specified column size for the given
     * parameter or column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        <code>null</code> means that the schema name should not be used to narrow
     *        the search
     * @param functionNamePattern a procedure name pattern; must match the
     *        function name as it is stored in the database
     * @param columnNamePattern a parameter name pattern; must match the
     * parameter or column name as it is stored in the database
     * @return <code>ResultSet</code> - each row describes a
     * user function parameter, column  or return type
     *
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public ResultSet getFunctionColumns(
            String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {

        StringBuffer sb = new StringBuffer(256);

        sb.append("select pc.procedure_cat as FUNCTION_CAT,").append(
            "pc.procedure_schem as FUNCTION_SCHEM,").append(
            "pc.procedure_name as FUNCTION_NAME,").append(
            "pc.column_name as COLUMN_NAME,").append(
            "case pc.column_type").append(" when 3 then 5").append(
            " when 4 then 3").append(" when 5 then 4").append(
            " else pc.column_type").append(" end as COLUMN_TYPE,").append(
            "pc.DATA_TYPE,").append("pc.TYPE_NAME,").append(
            "pc.PRECISION,").append("pc.LENGTH,").append("pc.SCALE,").append(
            "pc.RADIX,").append("pc.NULLABLE,").append("pc.REMARKS,").append(
            "pc.CHAR_OCTET_LENGTH,").append("pc.ORDINAL_POSITION,").append(
            "pc.IS_NULLABLE,").append("pc.SPECIFIC_NAME,").append(
            "case pc.column_type").append(" when 3 then 1").append(
            " else 0").append(" end AS COLUMN_GROUP ").append(
            "from information_schema.system_procedurecolumns pc ").append(
            "join (select procedure_schem,").append("procedure_name,").append(
            "specific_name ").append(
            "from information_schema.system_procedures ").append(
            "where procedure_type = 2) p ").append(
            "on pc.procedure_schem = p.procedure_schem ").append(
            "and pc.procedure_name = p.procedure_name ").append(
            "and pc.specific_name = p.specific_name ").append(
            "and ((pc.column_type = 3 and pc.column_name = '@p0') ").append(
            "or ").append("(pc.column_type <> 3)) ");

        if (wantsIsNull(functionNamePattern)
                || wantsIsNull(columnNamePattern)) {
            return execute(sb.append("where 1=0").toString());
        }
        schemaPattern = translateSchema(schemaPattern);

        sb.append("where 1=1 ").append(
            and("pc.procedure_cat", "=", catalog)).append(
            and("pc.procedure_schem", "LIKE", schemaPattern)).append(
            and("pc.procedure_name", "LIKE", functionNamePattern)).append(
            and("pc.column_name", "LIKE", columnNamePattern)).append(
            " order by 1, 2, 3, 17, 18 , 15");

        // Order by FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME
        //      COLUMN_GROUP and ORDINAL_POSITION
        return execute(sb.toString());
    }

//#endif JAVA6

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
     * @since JDK 1.6, HSQLDB 1.9
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
     * @since JDK 1.6, HSQLDB 1.9
     */
//#ifdef JAVA6
    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//#endif JAVA6
    //----------------------- Internal Implementation --------------------------

    /** Used by getBestRowIdentifier to avoid extra object construction */
    static final Integer INT_COLUMNS_NO_NULLS = new Integer(columnNoNulls);

    // -----------------------------------------------------------------------
    // private attributes
    // -----------------------------------------------------------------------

    /**
     * The connection this object uses to retrieve database instance-specific
     * metadata.
     */
    private JDBCConnection connection;

    /**
     * Connection property for schema reporting.
     */
    private boolean useSchemaDefault;

    /**
     * true if internal connection; false external connection and !useSchemaDefault
     */
    private boolean supportsSchemasIn;

    /**
     * A CSV list representing the SQL IN list to use when generating
     * queries for <code>getBestRowIdentifier</code> when the
     * <code>scope</code> argument is <code>bestRowSession</code>.
     * @since HSQLDB 1.7.2
     */
    private static final String BRI_SESSION_SCOPE_IN_LIST = "("
        + bestRowSession + ")";

    /**
     * A CSV list representing the SQL IN list to use when generating
     * queries for <code>getBestRowIdentifier</code> when the
     * <code>scope</code> argument is <code>bestRowTemporary</code>.
     * @since HSQLDB 1.7.2
     */
    private static final String BRI_TEMPORARY_SCOPE_IN_LIST = "("
        + bestRowTemporary + "," + bestRowTransaction + "," + bestRowSession
        + ")";

    /**
     * A CSV list representing the SQL IN list to use when generating
     * queries for <code>getBestRowIdentifier</code> when the
     * <code>scope</code> argument is <code>bestRowTransaction</code>.
     * @since HSQLDB 1.7.2
     */
    private static final String BRI_TRANSACTION_SCOPE_IN_LIST = "("
        + bestRowTransaction + "," + bestRowSession + ")";

    /**
     * "SELECT * FROM ". <p>
     *
     * This attribute is in support of methods that use SQL SELECT statements to
     * generate returned <code>ResultSet</code> objects. <p>
     *
     * @since HSQLDB 1.7.2
     */
    private static final String selstar = "SELECT * FROM INFORMATION_SCHEMA.";

    /**
     * " WHERE TRUE ". <p>
     *
     * This attribute is in support of methods that use SQL SELECT statements to
     * generate returned <code>ResultSet</code> objects. <p>
     *
     * A good optimizer will simply drop this when parsing a condition
     * expression. And it makes our code much easier to write, since we don't
     * have to check our "WHERE" clause productions as strictly for proper
     * conjunction:  we just stick additional conjunctive predicates on the
     * end of this and Presto! Everything works :-) <p>
     * @since HSQLDB 1.7.2
     */
    private static final String whereTrue = " WHERE TRUE";

//#ifdef JAVA6
    public static final int JDBC_MAJOR = 4;

//#else
/*
    public static final int JDBC_MAJOR = 3;
*/

//#endif JAVA6

    /**
     * Constructs a new <code>JDBCDatabaseMetaData</code> object using the
     * specified connection.  This contructor is used by <code>JDBCConnection</code>
     * when producing a <code>DatabaseMetaData</code> object from a call to
     * {@link JDBCConnection#getMetaData() getMetaData}.
     * @param c the connection this object will use to retrieve
     *         instance-specific metadata
     * @throws SQLException never - reserved for future use
     */
    JDBCDatabaseMetaData(JDBCConnection c) throws SQLException {

        // PRE: is non-null and not closed
        connection       = c;
        useSchemaDefault = c.isInternal ? false
                                        : c.connProperties
                                        .isPropertyTrue(HsqlDatabaseProperties
                                            .url_default_schema);
        supportsSchemasIn = c.isInternal || !useSchemaDefault;
    }

    /**
     * Retrieves an "AND" predicate based on the (column) <code>id</code>,
     * <code>op</code>(erator) and<code>val</code>(ue) arguments to be
     * included in an SQL "WHERE" clause, using the conventions laid out for
     * JDBC DatabaseMetaData filter parameter values. <p>
     *
     * @return an "AND" predicate built from the arguments
     * @param id the simple, non-quoted identifier of a system table
     *      column upon which to filter. <p>
     *
     *      No checking is done for column name validity. <br>
     *      It is assumed the system table column name is correct. <p>
     *
     * @param op the conditional operation to perform using the system table
     *      column name value and the <code>val</code> argument. <p>
     *
     * @param val an object representing the value to use in some conditional
     *      operation, op, between the column identified by the id argument
     *      and this argument. <p>
     *
     *      <UL>
     *          <LI>null causes the empty string to be returned. <p>
     *
     *          <LI>toString().length() == 0 causes the returned expression
     *              to be built so that the IS NULL operation will occur
     *              against the specified column. <p>
     *
     *          <LI>instanceof String causes the returned expression to be
     *              built so that the specified operation will occur between
     *              the specified column and the specified value, converted to
     *              an SQL string (single quoted, with internal single quotes
     *              escaped by doubling). If <code>op</code> is "LIKE" and
     *              <code>val</code> does not contain any "%" or "_" wild
     *              card characters, then <code>op</code> is silently
     *              converted to "=". <p>
     *
     *          <LI>!instanceof String causes an expression to built so that
     *              the specified operation will occur between the specified
     *              column and <code>String.valueOf(val)</code>. <p>
     *
     *      </UL>
     */
    private static String and(String id, String op, Object val) {

        // The JDBC standard for pattern arguments seems to be:
        //
        // - pass null to mean ignore (do not include in query),
        // - pass "" to mean filter on <column-ident> IS NULL,
        // - pass "%" to filter on <column-ident> IS NOT NULL.
        // - pass sequence with "%" and "_" for wildcard matches
        // - when searching on values reported directly from DatabaseMetaData
        //   results, typically an exact match is desired.  In this case, it
        //   is the client's responsibility to escape any reported "%" and "_"
        //   characters using whatever DatabaseMetaData returns from
        //   getSearchEscapeString(). In our case, this is the standard escape
        //   character: '\'. Typically, '%' will rarely be encountered, but
        //   certainly '_' is to be expected on a regular basis.
        // - checkme:  what about the (silly) case where an identifier
        //   has been declared such as:  'create table "xxx\_yyy"(...)'?
        //   Must the client still escape the Java string like this:
        //   "xxx\\\\_yyy"?
        //   Yes: because otherwise the driver is expected to
        //   construct something like:
        //   select ... where ... like 'xxx\_yyy' escape '\'
        //   which will try to match 'xxx_yyy', not 'xxx\_yyy'
        //   Testing indicates that indeed, higher quality popular JDBC
        //   database browsers do the escapes "properly."
        if (val == null) {
            return "";
        }

        StringBuffer sb    = new StringBuffer();
        boolean      isStr = (val instanceof String);

        if (isStr && ((String) val).length() == 0) {
            return sb.append(" AND ").append(id).append(" IS NULL").toString();
        }

        String v = isStr ? Type.SQL_VARCHAR.convertToSQLString(val)
                         : String.valueOf(val);

        sb.append(" AND ").append(id).append(' ');

        // add the escape to like if required
        if (isStr && "LIKE".equalsIgnoreCase(op)) {
            if (v.indexOf('_') < 0 && v.indexOf('%') < 0) {

                // then we can optimize.
                sb.append("=").append(' ').append(v);
            } else {
                sb.append("LIKE").append(' ').append(v);

                if ((v.indexOf("\\_") >= 0) || (v.indexOf("\\%") >= 0)) {

                    // then client has requested at least one escape.
                    sb.append(" ESCAPE '\\'");
                }
            }
        } else {
            sb.append(op).append(' ').append(v);
        }

        return sb.toString();
    }

    /**
     * The main SQL statement executor.  All SQL destined for execution
     * ultimately goes through this method. <p>
     *
     * The sqlStatement field for the result is set autoClose to comply with
     * ResultSet.getStatement() semantics for result sets that are not from
     * a user supplied Statement object. (fredt) <p>
     *
     * @param sql SQL statement to execute
     * @return the result of issuing the statement
     * @throws SQLException is a database error occurs
     */
    private ResultSet execute(String sql) throws SQLException {

        // NOTE:
        // Need to create a JDBCStatement here so JDBCResultSet can return
        // its Statement object on call to getStatement().
        // The native JDBCConnection.execute() method does not
        // automatically assign a Statement object for the ResultSet, but
        // JDBCStatement does.  That is, without this, there is no way for the
        // JDBCResultSet to find its way back to its Connection (or Statement)
        // Also, cannot use single, shared JDBCStatement object, as each
        // fetchResult() closes any old JDBCResultSet before fetching the
        // next, causing the JDBCResultSet's Result object to be nullified
        final int scroll = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        final int concur = JDBCResultSet.CONCUR_READ_ONLY;
        ResultSet r = connection.createStatement(scroll,
            concur).executeQuery(sql);

        ((JDBCResultSet) r).autoClose = true;

        return r;
    }

    /**
     * An SQL statement executor that knows how to create a "SELECT
     * * FROM" statement, given a table name and a <em>where</em> clause.<p>
     *
     *  If the <em>where</em> clause is null, it is ommited.  <p>
     *
     *  It is assumed that the table name is non-null, since this is a private
     *  method.  No check is performed. <p>
     *
     * @return the result of executing "SELECT * FROM " + table " " + where
     * @param table the name of a table to "select * from"
     * @param where the where condition for the select
     * @throws SQLException if database error occurs
     */
    private ResultSet executeSelect(String table,
                                    String where) throws SQLException {

        String select = selstar + table;

        if (where != null) {
            select += " WHERE " + where;
        }

        return execute(select);
    }

    /**
     * Retrieves "SELECT * FROM INFORMATION_SCHEMA.&lt;table&gt; WHERE 1=1" in string
     * buffer form. <p>
     *
     * This is a convenience method provided because, for most
     * <code>DatabaseMetaData</code> queries, this is the most suitable
     * thing upon which to start building. <p>
     *
     * @return an StringBuffer whose content is:
     *      "SELECT * FROM &lt;table&gt; WHERE 1=1"
     * @param t the name of the table
     */
    private StringBuffer toQueryPrefix(String t) {

        StringBuffer sb = new StringBuffer(255);

        return sb.append(selstar).append(t).append(whereTrue);
    }

    /**
     * Retrieves "&lt;expression&gt; WHERE 1=1" in string
     */
    private StringBuffer toQueryPrefixNoSelect(String t) {

        StringBuffer sb = new StringBuffer(255);

        return sb.append(t).append(whereTrue);
    }

    /**
     * Retrieves whether the JDBC <code>DatabaseMetaData</code> contract
     * specifies that the argument <code>s</code>code> is filter parameter
     * value that requires a corresponding IS NULL predicate. <p>
     *
     * @param s the filter parameter to test
     * @return true if the argument, s, is filter parameter value that
     *        requires a corresponding IS NULL predicate
     */
    private static boolean wantsIsNull(String s) {
        return (s != null && s.length() == 0);
    }

    /**
     * For compatibility, when the connection property "default_schema=true"
     * is present, any DatabaseMetaData call with an empty string as the
     * schema parameter will use the default schema (noramlly "PUBLIC").
     */
    private String translateSchema(String schemaName) throws SQLException {

        if (useSchemaDefault && schemaName != null
                && schemaName.length() == 0) {
            ResultSet rs = executeSelect("SYSTEM_SCHEMAS", "IS_DEFAULT=TRUE");

            if (rs.next()) {
                return rs.getString(1);
            }

            return schemaName;
        }

        return schemaName;
    }

    /************************* Volt DB Extensions *************************/

    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern)
            throws SQLException {
        throw new SQLException();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLException();
    }
    /**********************************************************************/
}
