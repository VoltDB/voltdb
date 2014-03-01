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

import java.io.Reader;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

//#ifdef JAVA6
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;

//#endif JAVA6
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.HsqlDateTime;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.IntValueHashMap;
import org.hsqldb_voltpatches.result.ResultConstants;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.ClobDataID;
import org.hsqldb_voltpatches.types.JavaObjectData;
import org.hsqldb_voltpatches.types.TimeData;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

/* $Id: JDBCCallableStatement.java 2993 2009-05-12 16:20:39Z fredt $ */

/** @todo fredt 1.9.0 - review wrt multiple result sets, named parameters etc. */

// boucherb@users patch 1.7.2 - CallableStatement impl removed
// from JDBCParameterMetaData and moved here; sundry changes elsewhere to
// comply
// TODO: 1.7.2 Alpha N :: DONE
//       maybe implement set-by-parameter-name.  We have an informal spec,
//       being "@p1" => 1, "@p2" => 2, etc.  Problems: return value is "@p0"
//       and there is no support for registering the return value as an out
//       parameter.
// TODO: 1.9.x
//       engine and client-side mechanisms for adding, retrieving,
//       navigating (and perhaps controlling holdability of) multiple
//       results generated from a single execution.
// boucherb@users 2004-03/04-xx - patch 1.7.2 - some minor code cleanup
//                                            - parameter map NPE correction
//                                            - embedded SQL/SQLCLI client usability
//                                              (parameter naming changed from @n to @pn)
// boucherb@users 2004-04-xx - doc 1.7.2 - javadocs added/updated
// boucherb@users 2005-12-07 - patch 1.8.0.x - initial JDBC 4.0 support work
// boucherb@users 2006-05-22 - doc 1.9.0 - full synch up to Mustang Build 84
// Revision 1.14  2006/07/12 11:58:49  boucherb
//  - full synch up to Mustang b90

/**
 * <!-- start generic documentation -->
 *
 * The interface used to execute SQL stored procedures.  The JDBC API
 * provides a stored procedure SQL escape syntax that allows stored procedures
 * to be called in a standard way for all RDBMSs. This escape syntax has one
 * form that includes a result parameter and one that does not. If used, the result
 * parameter must be registered as an OUT parameter. The other parameters
 * can be used for input, output or both. Parameters are referred to
 * sequentially, by number, with the first parameter being 1.
 * <p>(JDBC4 clarification:)
 * <PRE>
 *   {?= call &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]}
 *   {call &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]}
 * </PRE>
 * <P>
 * IN parameter values are set using the <code>set</code> methods inherited from
 * {@link java.sql.PreparedStatement}.  The type of all OUT parameters must be
 * registered prior to executing the stored procedure; their values
 * are retrieved after execution via the <code>get</code> methods provided here.
 * <P>
 * A <code>CallableStatement</code> can return one {@link java.sql.ResultSet} object or
 * multiple <code>ResultSet</code> objects.  Multiple
 * <code>ResultSet</code> objects are handled using operations
 * inherited from {@link java.sql.Statement}.
 * <P>
 * For maximum portability, a call's <code>ResultSet</code> objects and
 * update counts should be processed prior to getting the values of output
 * parameters.
 * <P>
 *
 * <!-- end generic documentation -->
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * Some of the previously unsupported features of this interface
 * are now supported, such as the parameterName-based setter methods. <p>
 *
 * More importantly, JDBCCallableStatement objects are now backed by a true
 * compiled parameteric representation. Hence, there are now significant
 * performance gains to be had by using a CallableStatement object instead of
 * a Statement object, if a short-running CALL statement is to be executed more
 * than a small number of times.  Moreover, the recent work lays the foundation
 * for work in a subsequenct release to support CallableStatement OUT and
 * IN OUT style parameters, as well as the generation and retrieval of multiple
 * results in response to the execution of a CallableStatement object. <p>
 *
 * For a more in-depth discussion of performance issues regarding HSQLDB
 * prepared and callable statement objects, please see overview section of
 * {@link JDBCParameterMetaData JDBCPreparedStatment}.
 *
 * <hr>
 *
 * As with many DBMS, HSQLDB support for stored procedures is not provided in
 * a completely standard fashion. <p>
 *
 * Beyond the XOpen/ODBC extended scalar functions, stored procedures are
 * typically supported in ways that vary greatly from one DBMS implementation
 * to the next.  So, it is almost guaranteed that the code for a stored
 * procedure written under a specific DBMS product will not work without
 * at least some modification in the context of another vendor's product
 * or even across a single vendor's product lines.  Moving stored procedures
 * from one DBMS product line to another almost invariably involves complex
 * porting issues and often may not be possible at all. <em>Be warned</em>. <p>
 *
 * One kind of HSQLDB stored procedures is Java routines that map directly onto
 * the static methods of compiled Java classes found on the classpath of the
 * engine at runtime. <p>
 *
 * Overloaded methods are supported and resolved according to the type of
 * para;meters.
 *
 * The other kind of HSQLDB stored procedures is SQL routines that are created
 * as part of schemas.
 *
 * With SQL routines, <code>OUT</code> and <code>IN OUT</code> parameters
 * are also supported. <p>
 *
 * In addition, HSQLDB stored procedure call mechanism allows the
 * more general HSQLDB SQL expression evaluation mechanism.  This
 * extension provides the ability to evaluate simple SQL expressions, possibly
 * containing Java method invocations. <p>
 *
 * With HSQLDB, executing a <code>CALL</code> statement that produces an opaque
 * (OTHER) or known scalar object reference has virtually the same effect as:
 *
 * <PRE class="SqlCodeExample">
 * CREATE TABLE DUAL (dummy VARCHAR);
 * INSERT INTO DUAL VALUES(NULL);
 * SELECT &lt;simple-expression&gt; FROM DUAL;
 * </PRE>
 *
 * As a transitional measure, HSQLDB provides the ability to materialize a
 * general result set in response to stored procedure execution.  In this case,
 * the stored procedure's Java method descriptor must specify a return type of
 * java.lang.Object for external use (although at any point in the devlopment
 * cycle, other, proprietary return types may accepted internally for engine
 * development purposes).
 *
 * When HSQLDB detects that the runtime class of the resulting Object is
 * elligible, an automatic internal unwrapping is performed to correctly
 * expose the underlying result set to the client, whether local or remote. <p>
 *
 * Additionally, HSQLDB automatically detects if java.sql.Connection is
 * the class of the first argument of any underlying Java method(s).  If so,
 * then the engine transparently supplies the internal Connection object
 * corresponding to the Session executing the call, adjusting the positions
 * of other arguments to suite the SQL context. <p>
 *
 * The features above are not intended to be permanent.  Rather, the intention
 * is to offer more general and powerful mechanisms in a future release;
 * it is recommend to use them only as a temporary convenience. <p>
 *
 * For instance, one might be well advised to future-proof by writing
 * HSQLDB-specific adapter methods that in turn call the real logic of an
 * underlying generalized JDBC stored procedure library. <p>
 *
 * Here is a very simple example of an HSQLDB stored procedure generating a
 * user-defined result set:
 *
 * <pre class="JavaCodeExample">
 * <span class="JavaKeyWord">package</span> mypackage;
 *
 * <span class="JavaKeyWord">class</span> MyClass {
 *
 *      <span class="JavaKeyWord">public static</span> Object <b>mySp</b>(Connection conn) <span class="JavaKeyWord">throws</span> SQLException {
 *          <span class="JavaKeyWord">return</span> conn.<b>createStatement</b>().<b>executeQuery</b>(<span class="JavaStringLiteral">"select * from my_table"</span>);
 *      }
 * }
 * </pre>
 *
 * Here is a refinement demonstrating no more than the bare essence of the idea
 * behind a more portable style:
 *
 * <pre class="JavaCodeExample">
 * <span class="JavaKeyWord">package</span> mypackage;
 *
 * <span class="JavaKeyWord">import</span> java.sql.ResultSet;
 * <span class="JavaKeyWord">import</span> java.sql.SQLException;
 *
 * <span class="JavaKeyWord">class</span> MyLibraryClass {
 *
 *      <span class="JavaKeyWord">public static</span> ResultSet <b>mySp()</b> <span class="JavaKeyWord">throws</span> SQLException {
 *          <span class="JavaKeyWord">return</span> ctx.<b>getConnection</b>().<b>createStatement</b>().<b>executeQuery</b>(<span class="JavaStringLiteral">"select * from my_table"</span>);
 *      }
 * }
 *
 * //--
 *
 * <span class="JavaKeyWord">package</span> myadaptorpackage;
 *
 * <span class="JavaKeyWord">import</span> java.sql.Connection;
 * <span class="JavaKeyWord">import</span> java.sql.SQLException;
 *
 * <span class="JavaKeyWord">class</span> MyAdaptorClass {
 *
 *      <span class="JavaKeyWord">public static</span> Object <b>mySp</b>(Connection conn) <span class="JavaKeyWord">throws</span> SQLException {
 *          MyLibraryClass.<b>getCtx()</b>.<b>setConnection</b>(conn);
 *          <span class="JavaKeyWord">return</span> MyLibraryClass.<b>mySp</b>();
 *      }
 * }
 * </pre>
 *
 * In a future release, it is intended to provided some new features
 * that will support writing fairly portable JDBC-based stored procedure
 * code: <P>
 *
 * <ul>
 *  <li> Support for the <span class="JavaStringLiteral">"jdbc:default:connection"</span>
 *       standard database connection url. <p>
 *
 *  <li> A well-defined specification of the behaviour of the HSQLDB execution
 *       stack under stored procedure calls. <p>
 *
 *  <li> A well-defined, pure JDBC specification for generating multiple
 *       results from HSQLDB stored procedures for client retrieval.
 * </ul>
 *
 * (boucherb@users)
 * </div>
 * <!-- end Release-specific documentation -->
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt{@users dot sourceforge.net
 * @version 1.9.0
 * @since 1.7.2
 * @revised JDK 1.6, HSQLDB 1.9.0
 * @see JDBCConnection#prepareCall
 * @see JDBCResultSet
 */
public class JDBCCallableStatement extends JDBCPreparedStatement implements CallableStatement {

// ----------------------------------- JDBC 1 ----------------------------------

    /**
     * <!-- start generic documentation -->
     *
     * Registers the OUT parameter in ordinal position
     * <code>parameterIndex</code> to the JDBC type
     * <code>sqlType</code>.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, <code>sqlType</code>
     * should be <code>java.sql.Types.OTHER</code>.  The method
     * {@link #getObject} retrieves the value.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param sqlType the JDBC type code defined by <code>java.sql.Types</code>.
     *        If the parameter is of JDBC type <code>NUMERIC</code>
     *        or <code>DECIMAL</code>, the version of
     *        <code>registerOutParameter</code> that accepts a scale value
     *        should be used.
     *
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     */
    public synchronized void registerOutParameter(int parameterIndex,
            int sqlType) throws SQLException {

        checkGetParameterIndex(parameterIndex);

        if (parameterModes[--parameterIndex]
                == SchemaObject.ParameterModes.PARAM_IN) {
            throw Util.invalidArgument();
        }
    }

    /**
     * <!-- start generic documentation -->
     *
     * Registers the parameter in ordinal position
     * <code>parameterIndex</code> to be of JDBC type
     * <code>sqlType</code>.  (JDBC4 clarification:) All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * This version of <code>registerOutParameter</code> should be
     * used when the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param sqlType the SQL type code defined by <code>java.sql.Types</code>.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     */
    public synchronized void registerOutParameter(int parameterIndex,
            int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves whether the last OUT parameter read had the value of
     * SQL <code>NULL</code>.  Note that this method should be called only after
     * calling a getter method; otherwise, there is no value to use in
     * determining whether it is <code>null</code> or not.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return <code>true</code> if the last parameter read was SQL
     * <code>NULL</code>; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     */
    public synchronized boolean wasNull() throws SQLException {
        return wasNullValue;
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>CHAR</code>,
     * <code>VARCHAR</code>, or <code>LONGVARCHAR</code> parameter as a
     * <code>String</code> in the Java programming language.
     * <p>
     * For the fixed-length type JDBC <code>CHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the (JDBC4 clarification:) SQL
     * <code>CHAR</code> value had in the
     * database, including any padding added by the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value. If the value is SQL <code>NULL</code>,
     *         the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setString
     */
    public synchronized String getString(
            int parameterIndex) throws SQLException {
        return (String) getColumnInType(parameterIndex, Type.SQL_VARCHAR);
    }

    /**
     * <!-- start generic documentation -->
     *
     * (JDBC4 modified:)
     * Retrieves the value of the designated JDBC <code>BIT</code>
     * or <code>BOOLEAN</code> parameter as a
     * <code>boolean</code> in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>false</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setBoolean
     */
    public synchronized boolean getBoolean(
            int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_BOOLEAN);

        return o == null ? false
                         : ((Boolean) o).booleanValue();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>TINYINT</code> parameter
     * as a <code>byte</code> in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setByte
     */
    public synchronized byte getByte(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.TINYINT);

        return o == null ? 0
                         : ((Number) o).byteValue();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>SMALLINT</code> parameter
     * as a <code>short</code> in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setShort
     */
    public synchronized short getShort(
            int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_SMALLINT);

        return o == null ? 0
                         : ((Number) o).shortValue();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>INTEGER</code> parameter
     * as an <code>int</code> in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setInt
     */
    public synchronized int getInt(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_INTEGER);

        return o == null ? 0
                         : ((Number) o).intValue();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>BIGINT</code> parameter
     * as a <code>long</code> in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setLong
     */
    public synchronized long getLong(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_BIGINT);

        return o == null ? 0
                         : ((Number) o).longValue();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>FLOAT</code> parameter
     * as a <code>float</code> in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setFloat
     */
    public synchronized float getFloat(
            int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_DOUBLE);

        return o == null ? (float) 0.0
                         : ((Number) o).floatValue();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>DOUBLE</code> parameter as a <code>double</code>
     * in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setDouble
     */
    public synchronized double getDouble(
            int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_DOUBLE);

        return o == null ? 0.0
                         : ((Number) o).doubleValue();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>NUMERIC</code> parameter as a
     * <code>java.math.BigDecimal</code> object with <i>scale</i> digits to
     * the right of the decimal point.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param scale the number of digits to the right of the decimal point
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @deprecated use <code>getBigDecimal(int parameterIndex)</code>
     *             or <code>getBigDecimal(String parameterName)</code>
     * @see #setBigDecimal
     */

//#ifdef DEPRECATEDJDBC
    public synchronized BigDecimal getBigDecimal(int parameterIndex,
            int scale) throws SQLException {

        if (scale < 0) {
            throw Util.outOfRangeArgument();
        }

        // boucherb@users 20020502 - added conversion
        BigDecimal bd = (BigDecimal) getColumnInType(parameterIndex,
            Type.SQL_DECIMAL);

        if (bd != null) {
            bd = bd.setScale(scale, BigDecimal.ROUND_DOWN);
        }

        return bd;
    }

//#endif

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>BINARY</code> or
     * <code>VARBINARY</code> parameter as an array of <code>byte</code>
     * values in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setBytes
     */
    public synchronized byte[] getBytes(
            int parameterIndex) throws SQLException {

        Object x = getColumnInType(parameterIndex, Type.SQL_VARBINARY);

        if (x == null) {
            return null;
        }

        return ((BinaryData) x).getBytes();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>DATE</code> parameter as a
     * <code>java.sql.Date</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setDate
     */
    public synchronized Date getDate(int parameterIndex) throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_DATE);

        if (t == null) {
            return null;
        }

        return (Date) Type.SQL_DATE.convertSQLToJava(session, t);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>TIME</code> parameter as a
     * <code>java.sql.Time</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTime
     */
    public synchronized Time getTime(int parameterIndex) throws SQLException {

        TimeData t = (TimeData) getColumnInType(parameterIndex, Type.SQL_TIME);

        if (t == null) {
            return null;
        }

        return (Time) Type.SQL_TIME.convertSQLToJava(session, t);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>TIMESTAMP</code>
     * parameter as a <code>java.sql.Timestamp</code> object. <p>
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTimestamp
     */
    public synchronized Timestamp getTimestamp(
            int parameterIndex) throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_TIMESTAMP);

        if (t == null) {
            return null;
        }

        return (Timestamp) Type.SQL_TIMESTAMP.convertSQLToJava(session, t);
    }

    //----------------------------------------------------------------------
    // Advanced features:

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated parameter as an <code>Object</code>
     * in the Java programming language. If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     * <p>
     * This method returns a Java object whose type corresponds to the JDBC
     * type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target JDBC
     * type as <code>java.sql.Types.OTHER</code>, this method can be used
     * to read database-specific abstract data types.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return A <code>java.lang.Object</code> holding the OUT parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see java.sql.Types
     * @see #setObject
     */
    public synchronized Object getObject(
            int parameterIndex) throws SQLException {

        checkGetParameterIndex(parameterIndex);

        Type sourceType = parameterTypes[parameterIndex - 1];

        switch (sourceType.typeCode) {

            case Types.SQL_DATE :
                return getDate(parameterIndex);
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return getTime(parameterIndex);
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return getTimestamp(parameterIndex);
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                return getBytes(parameterIndex);
            case Types.OTHER :
            case Types.JAVA_OBJECT : {
                Object o = getColumnInType(parameterIndex, sourceType);

                if (o == null) {
                    return null;
                }

                try {
                    return ((JavaObjectData) o).getObject();
                } catch (HsqlException e) {
                    throw Util.sqlException(e);
                }
            }
            default :
                return getColumnInType(parameterIndex, sourceType);
        }
    }

// ----------------------------------- JDBC 2 ----------------------------------

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>NUMERIC</code> parameter as a
     * <code>java.math.BigDecimal</code> object with as many digits to the
     * right of the decimal point as the value contains.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value in full precision.  If the value is
     * SQL <code>NULL</code>, the result is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setBigDecimal
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *  JDBCParameterMetaData)
     */
    public synchronized BigDecimal getBigDecimal(
            int parameterIndex) throws SQLException {
        return (BigDecimal) getColumnInType(parameterIndex, Type.SQL_DECIMAL);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Returns an object representing the value of OUT parameter
     * <code>parameterIndex</code> and uses <code>map</code> for the custom
     * mapping of the parameter value.
     * <p>
     * This method returns a Java object whose type corresponds to the
     * JDBC type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target
     * JDBC type as <code>java.sql.Types.OTHER</code>, this method can
     * be used to read database-specific abstract data types.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @param map the mapping from SQL type names to Java classes
     * @return a <code>java.lang.Object</code> holding the OUT parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setObject
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *   JDBCParameterMetaData)
     */

//#ifdef JAVA6
    public Object getObject(int parameterIndex,
                            Map<String, Class<?>> map) throws SQLException {
        throw Util.notSupported();
    }

//#else
/*
    public Object getObject(int parameterIndex, Map map) throws SQLException {
        throw Util.notSupported();
    }
*/

//#endif

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>REF(&lt;structured-type&gt;)</code>
     * parameter as a {@link java.sql.Ref} object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value as a <code>Ref</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the value
     * <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     * JDBCParameterMetaData)
     */
    public Ref getRef(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>BLOB</code> parameter as a
     * {@link java.sql.Blob} object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value as a <code>Blob</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the value
     * <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *  JDBCParameterMetaData)
     */
    public synchronized Blob getBlob(int parameterIndex) throws SQLException {

        Object o = getObject(parameterIndex);

        if (o == null) {
            return null;
        }

        if (o instanceof BlobDataID) {
            return new JDBCBlobClient(session, (BlobDataID) o);
        }

        throw Util.sqlException(ErrorCode.X_42561);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>CLOB</code> parameter as a
     * <code>java.sql.Clob</code> object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as a <code>Clob</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *  JDBCParameterMetaData)
     */
    public synchronized Clob getClob(int parameterIndex) throws SQLException {

        Object o = getObject(parameterIndex);

        if (o == null) {
            return null;
        }

        if (o instanceof ClobDataID) {
            return new JDBCClobClient(session, (ClobDataID) o);
        }

        throw Util.sqlException(ErrorCode.X_42561);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>ARRAY</code> parameter as an
     * {@link java.sql.Array} object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as an <code>Array</code> object in
     * the Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *  JDBCParameterMetaData)
     */
    public Array getArray(int parameterIndex) throws SQLException {

        checkGetParameterIndex(parameterIndex);

        throw Util.notSupported();
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>DATE</code> parameter as a
     * <code>java.sql.Date</code> object, using
     * the given <code>Calendar</code> object
     * to construct the date.
     * With a <code>Calendar</code> object, the driver
     * can calculate the date taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the date
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setDate
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *      JDBCParameterMetaData)
     */
    public synchronized Date getDate(int parameterIndex,
                                     Calendar cal) throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_DATE);
        long millis     = t.getSeconds() * 1000;
        int  zoneOffset = HsqlDateTime.getZoneMillis(cal, millis);

        return new Date(millis - zoneOffset);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>TIME</code> parameter as a
     * <code>java.sql.Time</code> object, using
     * the given <code>Calendar</code> object
     * to construct the time.
     * With a <code>Calendar</code> object, the driver
     * can calculate the time taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the time
     * @return the parameter value; if the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTime
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *    JDBCParameterMetaData)
     */
    public synchronized Time getTime(int parameterIndex,
                                     Calendar cal) throws SQLException {

        TimeData t = (TimeData) getColumnInType(parameterIndex, Type.SQL_TIME);

        if (t == null) {
            return null;
        }

        long millis = t.getSeconds() * 1000;

        if (parameterTypes[--parameterIndex].isDateTimeTypeWithZone()) {}
        else {

            // UTC - calZO == (UTC - sessZO) + (sessionZO - calZO)
            if (cal != null) {
                int zoneOffset = HsqlDateTime.getZoneMillis(cal, millis);

                millis += session.getZoneSeconds() * 1000 - zoneOffset;
            }
        }

        return new Time(millis);
    }

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>TIMESTAMP</code> parameter as a
     * <code>java.sql.Timestamp</code> object, using
     * the given <code>Calendar</code> object to construct
     * the <code>Timestamp</code> object.
     * With a <code>Calendar</code> object, the driver
     * can calculate the timestamp taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the timestamp
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     *         is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #setTimestamp
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *    JDBCParameterMetaData)
     */
    public synchronized Timestamp getTimestamp(int parameterIndex,
            Calendar cal) throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_TIMESTAMP);

        if (t == null) {
            return null;
        }

        long millis = t.getSeconds() * 1000;

        if (parameterTypes[--parameterIndex].isDateTimeTypeWithZone()) {}
        else {

            // UTC - calZO == (UTC - sessZO) + (sessionZO - calZO)
            if (cal != null) {
                int zoneOffset = HsqlDateTime.getZoneMillis(cal, millis);

                millis += session.getZoneSeconds() * 1000 - zoneOffset;
            }
        }

        Timestamp ts = new Timestamp(millis);

        ts.setNanos(t.getNanos());

        return ts;
    }

    /**
     * <!-- start generic documentation -->
     *
     * Registers the designated output parameter.
     * This version of
     * the method <code>registerOutParameter</code>
     * should be used for a user-defined or <code>REF</code> output parameter.  Examples
     * of user-defined types include: <code>STRUCT</code>, <code>DISTINCT</code>,
     * <code>JAVA_OBJECT</code>, and named array types.
     * <p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>  For a user-defined parameter, the fully-qualified SQL
     * type name of the parameter should also be given, while a <code>REF</code>
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-defined and <code>REF</code> parameters.
     *
     * Although it is intended for user-defined and <code>REF</code> parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-defined or <code>REF</code> type, the
     * <i>typeName</i> parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the getter method whose Java type corresponds to the
     * parameter's registered SQL type.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *  JDBCParameterMetaData)
     *
     */
    public synchronized void registerOutParameter(int parameterIndex,
            int sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

// ----------------------------------- JDBC 3 ----------------------------------

    /**
     * <!-- start generic documentation -->
     *
     * Registers the OUT parameter named
     * <code>parameterName</code> to the JDBC type
     * <code>sqlType</code>.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, <code>sqlType</code>
     * should be <code>java.sql.Types.OTHER</code>.  The method
     * {@link #getObject} retrieves the value.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by <code>java.sql.Types</code>.
     * If the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>, the version of
     * <code>registerOutParameter</code> that accepts a scale value
     * should be used.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQL 1.7.0
     * @see java.sql.Types
     */
//#ifdef JAVA4
    public synchronized void registerOutParameter(String parameterName,
            int sqlType) throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Registers the parameter named
     * <code>parameterName</code> to be of JDBC type
     * <code>sqlType</code>.  (JDBC4 clarification:) All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.
     * <p>
     * This version of <code>registerOutParameter</code> should be
     * used when the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType SQL type code defined by <code>java.sql.Types</code>.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     * @see java.sql.Types
     */
//#ifdef JAVA4
    public synchronized void registerOutParameter(String parameterName,
            int sqlType, int scale) throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Registers the designated output parameter.  This version of
     * the method <code>registerOutParameter</code>
     * should be used for a user-named or REF output parameter.  Examples
     * of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     * <p>
     * (JDBC4 clarification:)<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * For a user-named parameter the fully-qualified SQL
     * type name of the parameter should also be given, while a REF
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-named and REF parameters.
     *
     * Although it is intended for user-named and REF parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-named or REF type, the
     * typeName parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the <code>getXXX</code> method whose Java type XXX corresponds to the
     * parameter's registered SQL type.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>sqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @see java.sql.Types
     * @since JDK 1.4, HSQL 1.7.0
     */
//#ifdef JAVA4
    public synchronized void registerOutParameter(String parameterName,
            int sqlType, String typeName) throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>DATALINK</code> parameter as a
     * <code>java.net.URL</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @return a <code>java.net.URL</code> object that represents the
     *         JDBC <code>DATALINK</code> value used as the designated
     *         parameter
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code>,
     *            or if the URL being returned is
     *            not a valid URL on the Java platform
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setURL
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public java.net.URL getURL(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.net.URL</code> object.
     * The driver converts this to an SQL <code>DATALINK</code> value when
     * it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param val the parameter value
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code>,
     *            or if a URL is malformed
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getURL
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public void setURL(String parameterName,
                       java.net.URL val) throws SQLException {
        setURL(findParameterIndex(parameterName), val);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to SQL <code>NULL</code>.
     *
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType the SQL type code defined in <code>java.sql.Types</code>
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setNull(String parameterName,
                                     int sqlType) throws SQLException {
        setNull(findParameterIndex(parameterName), sqlType);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>boolean</code> value.
     *
     * <p>(JDBC4 clarification:)<p>
     *
     * The driver converts this
     * to an SQL <code>BIT</code> or <code>BOOLEAN</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @see #getBoolean
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBoolean
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setBoolean(String parameterName,
                                        boolean x) throws SQLException {
        setBoolean(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>byte</code> value.
     * The driver converts this
     * to an SQL <code>TINYINT</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getByte
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setByte(String parameterName,
                                     byte x) throws SQLException {
        setByte(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>short</code> value.
     * The driver converts this
     * to an SQL <code>SMALLINT</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getShort
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setShort(String parameterName,
                                      short x) throws SQLException {
        setShort(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>int</code> value.
     * The driver converts this
     * to an SQL <code>INTEGER</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getInt
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setInt(String parameterName,
                                    int x) throws SQLException {
        setInt(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>long</code> value.
     * The driver converts this
     * to an SQL <code>BIGINT</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getLong
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setLong(String parameterName,
                                     long x) throws SQLException {
        setLong(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>float</code> value.
     * The driver converts this
     * to an SQL <code>FLOAT</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getFloat
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setFloat(String parameterName,
                                      float x) throws SQLException {
        setFloat(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>double</code> value.
     * The driver converts this
     * to an SQL <code>DOUBLE</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDouble
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setDouble(String parameterName,
                                       double x) throws SQLException {
        setDouble(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given
     * <code>java.math.BigDecimal</code> value.
     * The driver converts this to an SQL <code>NUMERIC</code> value when
     * it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBigDecimal
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setBigDecimal(String parameterName,
            BigDecimal x) throws SQLException {
        setBigDecimal(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java <code>String</code> value.
     * The driver converts this
     * to an SQL <code>VARCHAR</code> or <code>LONGVARCHAR</code> value
     * (depending on the argument's
     * size relative to the driver's limits on <code>VARCHAR</code> values)
     * when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getString
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setString(String parameterName,
                                       String x) throws SQLException {
        setString(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given Java array of bytes.
     * The driver converts this to an SQL <code>VARBINARY</code> or
     * <code>LONGVARBINARY</code> (depending on the argument's size relative
     * to the driver's limits on <code>VARBINARY</code> values) when it sends
     * it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBytes
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setBytes(String parameterName,
                                      byte[] x) throws SQLException {
        setBytes(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.sql.Date</code> value
     * (JDBC4 clarification:)<p>
     * using the default time zone of the virtual machine that is running
     * the application.
     * The driver converts this
     * to an SQL <code>DATE</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setDate(String parameterName,
                                     Date x) throws SQLException {
        setDate(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.sql.Time</code> value.
     * The driver converts this
     * to an SQL <code>TIME</code> value when it sends it to the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setTime(String parameterName,
                                     Time x) throws SQLException {
        setTime(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value.
     * The driver
     * converts this to an SQL <code>TIMESTAMP</code> value when it sends it to the
     * database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setTimestamp(String parameterName,
            Timestamp x) throws SQLException {
        setTimestamp(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setAsciiStream(String parameterName,
            java.io.InputStream x, int length) throws SQLException {
        setAsciiStream(findParameterIndex(parameterName), x, length);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setBinaryStream(String parameterName,
            java.io.InputStream x, int length) throws SQLException {
        setBinaryStream(findParameterIndex(parameterName), x, length);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the value of the designated parameter with the given object. The second
     * argument must be an object type; for integral values, the
     * <code>java.lang</code> equivalent objects should be used.
     *
     * <p>The given Java object will be converted to the given targetSqlType
     * before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the
     * interface <code>SQLData</code>),
     * the JDBC driver should call the method <code>SQLData.writeSQL</code> to write it
     * to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,  <code>NClob</code>,
     *  <code>Struct</code>, <code>java.net.URL</code>,
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * Note that this method may be used to pass datatabase-
     * specific abstract data types.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     * sent to the database. The scale argument may further qualify this type.
     * @param scale for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
     *          this is the number of digits after the decimal point.  For all other
     *          types, this value will be ignored.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>targetSqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     * @see #getObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setObject(String parameterName, Object x,
                                       int targetSqlType,
                                       int scale) throws SQLException {
        setObject(findParameterIndex(parameterName), x, targetSqlType, scale);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the value of the designated parameter with the given object.
     * This method is like the method <code>setObject</code>
     * above, except that it assumes a scale of zero.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     *                      sent to the database
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if <code>targetSqlType</code> is
     * a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     * <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     * <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *  <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     * or  <code>STRUCT</code> data type and the JDBC driver does not support
     * this data type
     * @see #getObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setObject(String parameterName, Object x,
                                       int targetSqlType) throws SQLException {
        setObject(findParameterIndex(parameterName), x, targetSqlType);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the value of the designated parameter with the given object.
     * The second parameter must be of type <code>Object</code>; therefore, the
     * <code>java.lang</code> equivalent objects should be used for built-in types.
     *
     * <p>The JDBC specification specifies a standard mapping from
     * Java <code>Object</code> types to SQL types.  The given argument
     * will be converted to the corresponding SQL type before being
     * sent to the database.
     *
     * <p>Note that this method may be used to pass datatabase-
     * specific abstract data types, by using a driver-specific Java
     * type.
     *
     * If the object is of a class implementing the interface <code>SQLData</code>,
     * the JDBC driver should call the method <code>SQLData.writeSQL</code>
     * to write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,  <code>NClob</code>,
     *  <code>Struct</code>, <code>java.net.URL</code>,
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * This method throws an exception if there is an ambiguity, for example, if the
     * object is of a class implementing more than one of the interfaces named above.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code> or if the given
     *            <code>Object</code> parameter is ambiguous
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setObject(String parameterName,
                                       Object x) throws SQLException {
        setObject(findParameterIndex(parameterName), x);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param reader the <code>java.io.Reader</code> object that
     *        contains the UNICODE data used as the designated parameter
     * @param length the number of characters in the stream
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setCharacterStream(String parameterName,
            java.io.Reader reader, int length) throws SQLException {
        setCharacterStream(findParameterIndex(parameterName), reader, length);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.sql.Date</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>DATE</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the date
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the date
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setDate(String parameterName, Date x,
                                     Calendar cal) throws SQLException {
        setDate(findParameterIndex(parameterName), x, cal);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.sql.Time</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIME</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the time
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the time
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setTime(String parameterName, Time x,
                                     Calendar cal) throws SQLException {
        setTime(findParameterIndex(parameterName), x, cal);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIMESTAMP</code> value,
     * which the driver then sends to the database.  With a
     * a <code>Calendar</code> object, the driver can calculate the timestamp
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the timestamp
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setTimestamp(String parameterName, Timestamp x,
            Calendar cal) throws SQLException {
        setTimestamp(findParameterIndex(parameterName), x, cal);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to SQL <code>NULL</code>.
     * This version of the method <code>setNull</code> should
     * be used for user-defined types and REF type parameters.  Examples
     * of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *
     * <P><B>Note:</B> To be portable, applications must give the
     * SQL type code and the fully-qualified SQL type name when specifying
     * a NULL user-defined or REF parameter.  In the case of a user-defined type
     * the name is the type name of the parameter itself.  For a REF
     * parameter, the name is the type name of the referenced type.  If
     * a JDBC driver does not need the type code or type name information,
     * it may ignore it.
     *
     * Although it is intended for user-defined and Ref parameters,
     * this method may be used to set a null parameter of any JDBC type.
     * If the parameter does not have a user-defined or REF type, the given
     * typeName is ignored.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from <code>java.sql.Types</code>
     * @param typeName the fully-qualified name of an SQL user-defined type;
     *        ignored if the parameter is not a user-defined type or
     *        SQL <code>REF</code> value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized void setNull(String parameterName, int sqlType,
                                     String typeName) throws SQLException {
        setNull(findParameterIndex(parameterName), sqlType, typeName);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>CHAR</code>, <code>VARCHAR</code>,
     * or <code>LONGVARCHAR</code> parameter as a <code>String</code> in
     * the Java programming language.
     * <p>
     * For the fixed-length type JDBC <code>CHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the (JDBC4 clarification:) SQL
     * <code>CHAR</code> value had in the
     * database, including any padding added by the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value. If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setString
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized String getString(
            String parameterName) throws SQLException {
        return getString(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * (JDBC4 modified:)<p>
     * Retrieves the value of a JDBC <code>BIT</code> or <code>BOOLEAN</code>
     * parameter as a
     * <code>boolean</code> in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>false</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBoolean
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized boolean getBoolean(
            String parameterName) throws SQLException {
        return getBoolean(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>TINYINT</code> parameter as a <code>byte</code>
     * in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setByte
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized byte getByte(
            String parameterName) throws SQLException {
        return getByte(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>SMALLINT</code> parameter as a <code>short</code>
     * in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setShort
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized short getShort(
            String parameterName) throws SQLException {
        return getShort(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>INTEGER</code> parameter as an <code>int</code>
     * in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setInt
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized int getInt(String parameterName) throws SQLException {
        return getInt(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>BIGINT</code> parameter as a <code>long</code>
     * in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setLong
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized long getLong(
            String parameterName) throws SQLException {
        return getLong(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>FLOAT</code> parameter as a <code>float</code>
     * in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setFloat
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized float getFloat(
            String parameterName) throws SQLException {
        return getFloat(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>DOUBLE</code> parameter as a <code>double</code>
     * in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     *         the result is <code>0</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDouble
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized double getDouble(
            String parameterName) throws SQLException {
        return getDouble(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>BINARY</code> or <code>VARBINARY</code>
     * parameter as an array of <code>byte</code> values in the Java
     * programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result is
     *  <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBytes
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized byte[] getBytes(
            String parameterName) throws SQLException {
        return getBytes(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>DATE</code> parameter as a
     * <code>java.sql.Date</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Date getDate(
            String parameterName) throws SQLException {
        return getDate(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>TIME</code> parameter as a
     * <code>java.sql.Time</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Time getTime(
            String parameterName) throws SQLException {
        return getTime(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
     * <code>java.sql.Timestamp</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result
     * is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Timestamp getTimestamp(
            String parameterName) throws SQLException {
        return getTimestamp(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a parameter as an <code>Object</code> in the Java
     * programming language. If the value is an SQL <code>NULL</code>, the
     * driver returns a Java <code>null</code>.
     * <p>
     * This method returns a Java object whose type corresponds to the JDBC
     * type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target JDBC
     * type as <code>java.sql.Types.OTHER</code>, this method can be used
     * to read database-specific abstract data types.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return A <code>java.lang.Object</code> holding the OUT parameter value.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see java.sql.Types
     * @see #setObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Object getObject(
            String parameterName) throws SQLException {
        return getObject(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>NUMERIC</code> parameter as a
     * <code>java.math.BigDecimal</code> object with as many digits to the
     * right of the decimal point as the value contains.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value in full precision.  If the value is
     * SQL <code>NULL</code>, the result is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBigDecimal
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized BigDecimal getBigDecimal(
            String parameterName) throws SQLException {
        return getBigDecimal(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Returns an object representing the value of OUT parameter
     * <code>parameterName</code> and uses <code>map</code> for the custom
     * mapping of the parameter value.
     * <p>
     * This method returns a Java object whose type corresponds to the
     * JDBC type that was registered for this parameter using the method
     * <code>registerOutParameter</code>.  By registering the target
     * JDBC type as <code>java.sql.Types.OTHER</code>, this method can
     * be used to read database-specific abstract data types.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param map the mapping from SQL type names to Java classes
     * @return a <code>java.lang.Object</code> holding the OUT parameter value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */

    /** @todo: how to use CodeSwitcher and +JAVA6 to specifiy Map<String,Class<?>> */
//#ifdef JAVA6
    @SuppressWarnings("unchecked")

//#endif JAVA6
//#ifdef JAVA4
    public synchronized Object getObject(String parameterName,
            Map map) throws SQLException {
        return getObject(findParameterIndex(parameterName), map);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>REF(&lt;structured-type&gt;)</code>
     * parameter as a {@link java.sql.Ref} object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Ref</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Ref getRef(String parameterName) throws SQLException {
        return getRef(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>BLOB</code> parameter as a
     * {@link java.sql.Blob} object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Blob</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Blob getBlob(
            String parameterName) throws SQLException {
        return getBlob(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>CLOB</code> parameter as a
     * {@link java.sql.Clob} object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>Clob</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Clob getClob(
            String parameterName) throws SQLException {
        return getClob(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>ARRAY</code> parameter as an
     * {@link java.sql.Array} object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as an <code>Array</code> object in
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Array getArray(
            String parameterName) throws SQLException {
        return getArray(findParameterIndex(parameterName));
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>DATE</code> parameter as a
     * <code>java.sql.Date</code> object, using
     * the given <code>Calendar</code> object
     * to construct the date.
     * With a <code>Calendar</code> object, the driver
     * can calculate the date taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the date
     * @return the parameter value.  If the value is SQL <code>NULL</code>,
     * the result is <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Date getDate(String parameterName,
                                     Calendar cal) throws SQLException {
        return getDate(findParameterIndex(parameterName), cal);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>TIME</code> parameter as a
     * <code>java.sql.Time</code> object, using
     * the given <code>Calendar</code> object
     * to construct the time.
     * With a <code>Calendar</code> object, the driver
     * can calculate the time taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the time
     * @return the parameter value; if the value is SQL <code>NULL</code>, the result is
     * <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Time getTime(String parameterName,
                                     Calendar cal) throws SQLException {
        return getTime(findParameterIndex(parameterName), cal);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
     * <code>java.sql.Timestamp</code> object, using
     * the given <code>Calendar</code> object to construct
     * the <code>Timestamp</code> object.
     * With a <code>Calendar</code> object, the driver
     * can calculate the timestamp taking into account a custom timezone and locale.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param parameterName the name of the parameter
     * @param cal the <code>Calendar</code> object the driver will use
     *            to construct the timestamp
     * @return the parameter value.  If the value is SQL <code>NULL</code>, the result is
     * <code>null</code>.
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public synchronized Timestamp getTimestamp(String parameterName,
            Calendar cal) throws SQLException {
        return getTimestamp(findParameterIndex(parameterName), cal);
    }

//#endif JAVA4

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>DATALINK</code> parameter as a
     * <code>java.net.URL</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>java.net.URL</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code>,
     *            or if there is a problem with the URL
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setURL
     * @since JDK 1.4, HSQLDB 1.7.0
     */
//#ifdef JAVA4
    public java.net.URL getURL(String parameterName) throws SQLException {
        return getURL(findParameterIndex(parameterName));
    }

//#endif JAVA4
    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>ROWID</code> parameter as a
     * <code>java.sql.RowId</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @return a <code>RowId</code> object that represents the JDBC <code>ROWID</code>
     *     value is used as the designated parameter. If the parameter contains
     * a SQL <code>NULL</code>, then a <code>null</code> value is returned.
     * @throws SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>ROWID</code> parameter as a
     * <code>java.sql.RowId</code> object.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a <code>RowId</code> object that represents the JDBC <code>ROWID</code>
     *     value is used as the designated parameter. If the parameter contains
     * a SQL <code>NULL</code>, then a <code>null</code> value is returned.
     * @throws SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized RowId getRowId(
            String parameterName) throws SQLException {
        return getRowId(findParameterIndex(parameterName));
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Sets the designated parameter to the given <code>java.sql.RowId</code> object. The
     * driver converts this to a SQL <code>ROWID</code> when it sends it to the
     * database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setRowId(String parameterName,
                                      RowId x) throws SQLException {
        super.setRowId(findParameterIndex(parameterName), x);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given <code>String</code> object.
     * The driver converts this to a SQL <code>NCHAR</code> or
     * <code>NVARCHAR</code> or <code>LONGNVARCHAR</code>
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @throws SQLException if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setNString(String parameterName,
                                        String value) throws SQLException {
        super.setNString(findParameterIndex(parameterName), value);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The
     * <code>Reader</code> reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @param length the number of characters in the parameter data.
     * @throws SQLException if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setNCharacterStream(String parameterName,
            Reader value, long length) throws SQLException {
        super.setNCharacterStream(findParameterIndex(parameterName), value,
                                  length);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>java.sql.NClob</code> object. The object
     * implements the <code>java.sql.NClob</code> interface. This <code>NClob</code>
     * object maps to a SQL <code>NCLOB</code>.
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @throws SQLException if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setNClob(String parameterName,
                                      NClob value) throws SQLException {
        super.setNClob(findParameterIndex(parameterName), value);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The <code>reader</code> must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>CallableStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     * @param parameterName the name of the parameter to be set
     * @param reader An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @throws SQLException if parameterIndex does not correspond to a parameter
     * marker in the SQL statement; if the length specified is less than zero;
     * a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setClob(String parameterName, Reader reader,
                                     long length) throws SQLException {
        super.setClob(findParameterIndex(parameterName), reader, length);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.  The <code>inputstream</code> must contain  the number
     * of characters specified by length, otherwise a <code>SQLException</code> will be
     * generated when the <code>CallableStatement</code> is executed.
     * This method differs from the <code>setBinaryStream (int, InputStream, int)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * @param parameterName the name of the parameter to be set
     * the second is 2, ...
     *
     * @param inputStream An object that contains the data to set the parameter
     * value to.
     * @param length the number of bytes in the parameter data.
     * @throws SQLException  if parameterIndex does not correspond
     * to a parameter marker in the SQL statement,  or if the length specified
     * is less than zero; if the number of bytes in the inputstream does not match
     * the specfied length; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setBlob(String parameterName,
                                     InputStream inputStream,
                                     long length) throws SQLException {
        super.setBlob(findParameterIndex(parameterName), inputStream, length);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The <code>reader</code> must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>CallableStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     *
     * @param parameterName the name of the parameter to be set
     * @param reader An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @throws SQLException if parameterIndex does not correspond to a parameter
     * marker in the SQL statement; if the length specified is less than zero;
     * if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setNClob(String parameterName, Reader reader,
                                      long length) throws SQLException {
        super.setNClob(findParameterIndex(parameterName), reader, length);
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated JDBC <code>NCLOB</code> parameter as a
     * <code>java.sql.NClob</code> object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as a <code>NClob</code> object in the
     * Java programming language.  If the value was SQL <code>NULL</code>, the
     * value <code>null</code> is returned.
     * @exception SQLException if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of a JDBC <code>NCLOB</code> parameter as a
     * <code>java.sql.NClob</code> object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a <code>NClob</code> object in the
     *         Java programming language.  If the value was SQL <code>NULL</code>,
     *         the value <code>null</code> is returned.
     * @exception SQLException if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized NClob getNClob(
            String parameterName) throws SQLException {
        return getNClob(findParameterIndex(parameterName));
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given <code>java.sql.SQLXML</code> object. The driver converts this to an
     * <code>SQL XML</code> value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param xmlObject a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws SQLException if a database access error occurs,
     * this method is called on a closed <code>CallableStatement</code> or
     * the <code>java.xml.transform.Result</code>,
     *  <code>Writer</code> or <code>OutputStream</code> has not been closed for the <code>SQLXML</code> object
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setSQLXML(String parameterName,
                                       SQLXML xmlObject) throws SQLException {
        super.setSQLXML(findParameterIndex(parameterName), xmlObject);
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated <code>SQL XML</code> parameter as a
     * <code>java.sql.SQLXML</code> object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated <code>SQL XML</code> parameter as a
     * <code>java.sql.SQLXML</code> object in the Java programming language.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized SQLXML getSQLXML(
            String parameterName) throws SQLException {
        return getSQLXML(findParameterIndex(parameterName));
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated <code>NCHAR</code>,
     * <code>NVARCHAR</code>
     * or <code>LONGNVARCHAR</code> parameter as
     * a <code>String</code> in the Java programming language.
     *  <p>
     * For the fixed-length type JDBC <code>NCHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the SQL
     * <code>NCHAR</code> value had in the
     * database, including any padding added by the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @return a <code>String</code> object that maps an
     * <code>NCHAR</code>, <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     * @see #setNString
     */
//#ifdef JAVA6
    public String getNString(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     *  Retrieves the value of the designated <code>NCHAR</code>,
     * <code>NVARCHAR</code>
     * or <code>LONGNVARCHAR</code> parameter as
     * a <code>String</code> in the Java programming language.
     * <p>
     * For the fixed-length type JDBC <code>NCHAR</code>,
     * the <code>String</code> object
     * returned has exactly the same value the SQL
     * <code>NCHAR</code> value had in the
     * database, including any padding added by the database.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a <code>String</code> object that maps an
     * <code>NCHAR</code>, <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     * @see #setNString
     */
//#ifdef JAVA6
    public synchronized String getNString(
            String parameterName) throws SQLException {
        return getNString(findParameterIndex(parameterName));
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> parameters.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> parameters.
     *
     * <!-- end generic documentation -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized Reader getNCharacterStream(
            String parameterName) throws SQLException {
        return getNCharacterStream(findParameterIndex(parameterName));
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     *
     * <!-- end generic documentstion -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an <code>SQLException</code>.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw Util.notSupported();
    }

//#endif JAVA6

    /**
     * <!-- start generic documentation -->
     *
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     *
     * <!-- end generic documentstion -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB supports this feature. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized Reader getCharacterStream(
            String parameterName) throws SQLException {
        return getCharacterStream(findParameterIndex(parameterName));
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given <code>java.sql.Blob</code> object.
     * The driver converts this to an SQL <code>BLOB</code> value when it
     * sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x a <code>Blob</code> object that maps an SQL <code>BLOB</code> value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *  @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setBlob(String parameterName,
                                     Blob x) throws SQLException {
        super.setBlob(findParameterIndex(parameterName), x);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given <code>java.sql.Clob</code> object.
     * The driver converts this to an SQL <code>CLOB</code> value when it
     * sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x a <code>Clob</code> object that maps an SQL <code>CLOB</code> value
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *  @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setClob(String parameterName,
                                     Clob x) throws SQLException {
        super.setClob(findParameterIndex(parameterName), x);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setAsciiStream(String parameterName,
            java.io.InputStream x, long length) throws SQLException {

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum ASCII input octet length exceeded: "
                         + length;    // NOI18N

            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }
        this.setAsciiStream(parameterName, x, (int) length);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setBinaryStream(String parameterName,
            java.io.InputStream x, long length) throws SQLException {

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum Binary input octet length exceeded: "
                         + length;    // NOI18N

            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }
        setBinaryStream(parameterName, x, (int) length);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param reader the <code>java.io.Reader</code> object that
     *        contains the UNICODE data used as the designated parameter
     * @param length the number of characters in the stream
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 1.9.0
     */
//#ifdef JAVA6
    public synchronized void setCharacterStream(String parameterName,
            java.io.Reader reader, long length) throws SQLException {

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum character input length exceeded: " + length;    // NOI18N

            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }
        setCharacterStream(parameterName, reader, (int) length);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setAsciiStream</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @exception SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *   @since 1.6
     */
//#ifdef JAVA6
    public synchronized void setAsciiStream(String parameterName,
            java.io.InputStream x) throws SQLException {
        super.setAsciiStream(findParameterIndex(parameterName), x);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBinaryStream</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @exception SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.6
     */
//#ifdef JAVA6
    public synchronized void setBinaryStream(String parameterName,
            java.io.InputStream x) throws SQLException {
        super.setBinaryStream(findParameterIndex(parameterName), x);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setCharacterStream</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader the <code>java.io.Reader</code> object that contains the
     *        Unicode data
     * @exception SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.6
     */
//#ifdef JAVA6
    public synchronized void setCharacterStream(String parameterName,
            java.io.Reader reader) throws SQLException {
        super.setCharacterStream(findParameterIndex(parameterName), reader);
    }

//#endif JAVA6

    /**
     *   Sets the designated parameter to a <code>Reader</code> object. The
     *   <code>Reader</code> reads the data till end-of-file is reached. The
     *   driver does the necessary conversion from Java character format to
     *   the national character set in the database.
     *
     *   <P><B>Note:</B> This stream object can either be a standard
     *   Java stream object or your own subclass that implements the
     *   standard interface.
     *   <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     *   it might be more efficient to use a version of
     *   <code>setNCharacterStream</code> which takes a length parameter.
     *
     *   @param parameterName the name of the parameter
     *   @param value the parameter value
     *   @throws SQLException if parameterName does not correspond to a named
     *   parameter; if the driver does not support national
     *           character sets;  if the driver can detect that a data conversion
     *    error could occur; if a database access error occurs; or
     *   this method is called on a closed <code>CallableStatement</code>
     *   @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *   @since 1.6
     */
//#ifdef JAVA6
    public synchronized void setNCharacterStream(String parameterName,
            Reader value) throws SQLException {
        super.setNCharacterStream(findParameterIndex(parameterName), value);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setClob</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader An object that contains the data to set the parameter value to.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or this method is called on
     * a closed <code>CallableStatement</code>
     *
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.6
     */
//#ifdef JAVA6
    public synchronized void setClob(String parameterName,
                                     Reader reader) throws SQLException {
        super.setClob(findParameterIndex(parameterName), reader);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.
     * This method differs from the <code>setBinaryStream (int, InputStream)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBlob</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param inputStream An object that contains the data to set the parameter
     * value to.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *
     * @since 1.6
     */
//#ifdef JAVA6
    public synchronized void setBlob(
            String parameterName,
            InputStream inputStream) throws SQLException {
        super.setBlob(findParameterIndex(parameterName), inputStream);
    }

//#endif JAVA6

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be send to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setNClob</code> which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader An object that contains the data to set the parameter value to.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national character sets;
     * if the driver can detect that a data conversion
     *  error could occur;  if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *
     * @since 1.6
     */
//#ifdef JAVA6
    public synchronized void setNClob(String parameterName,
                                      Reader reader) throws SQLException {
        super.setNClob(findParameterIndex(parameterName), reader);
    }

//#endif JAVA6
// --------------------------- Internal Implementation -------------------------

    /** parameter name => parameter index */
    private IntValueHashMap  parameterNameMap;
    private boolean          wasNullValue;
    private SessionInterface session;

    /** parameter index => registered OUT type */

//  private IntKeyIntValueHashMap outRegistrationMap;

    /**
     * Constructs a new JDBCCallableStatement with the specified connection and
     * result type.
     *
     * @param  c the connection on which this statement will execute
     * @param sql the SQL statement this object represents
     * @param resultSetType the type of result this statement will produce
     * @throws HsqlException if the statement is not accepted by the database
     * @throws SQLException if preprocessing by driver fails
     */
    public JDBCCallableStatement(
            JDBCConnection c, String sql, int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws HsqlException, SQLException {

        super(c, sql, resultSetType, JDBCResultSet.CONCUR_READ_ONLY,
              JDBCResultSet.HOLD_CURSORS_OVER_COMMIT,
              ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);

        session = c.sessionProxy;

        String[] names;
        String   name;

        // outRegistrationMap = new IntKeyIntValueHashMap();
        parameterNameMap = new IntValueHashMap();

        if (parameterMetaData != null) {
            names = parameterMetaData.columnLabels;

            for (int i = 0; i < names.length; i++) {
                name = names[i];

                // PRE:  should never happen in practice
                if (name == null || name.length() == 0) {
                    continue;    // throw?
                }
                parameterNameMap.put(name, i);
            }
        }
    }

    void fetchResult() throws SQLException {

        super.fetchResult();

        if (resultIn.getType() == ResultConstants.CALL_RESPONSE) {
            Object[] data = resultIn.getParameterData();

            for (int i = 0; i < parameterValues.length; i++) {
                parameterValues[i] = data[i];
            }
        }
    }

    /**
     * Retrieves the parameter index corresponding to the given
     * parameter name. <p>
     *
     * @param parameterName to look up
     * @throws SQLException if not found
     * @return index for name
     */
    int findParameterIndex(String parameterName) throws SQLException {

        checkClosed();

        int index = parameterNameMap.get(parameterName, -1);

        if (index >= 0) {
            return index + 1;
        }

        throw Util.sqlException(ErrorCode.JDBC_COLUMN_NOT_FOUND,
                                parameterName);
    }

    /**
     * Does the specialized work required to free this object's resources and
     * that of it's parent classes. <p>
     *
     * @throws SQLException if a database access error occurs
     */
    public synchronized void close() throws SQLException {

        if (isClosed()) {
            return;
        }

        // outRegistrationMap = null;
        parameterNameMap = null;

        super.close();
    }

    /**
     * Checks if the parameter of the given index has been successfully
     * registered as an OUT parameter. <p>
     *
     * @param parameterIndex to check
     * @throws SQLException if not registered
     */
/*
    private void checkIsRegisteredParameterIndex(int parameterIndex)
    throws SQLException {

        int    type;
        String msg;

        checkClosed();

        type = outRegistrationMap.get(parameterIndex, Integer.MIN_VALUE);

        if (type == Integer.MIN_VALUE) {
            msg = "Parameter not registered: " + parameterIndex; //NOI18N

            throw Util.sqlException(ErrorCode.INVALID_JDBC_ARGUMENT, msg);
        }
    }
*/

    /**
     * Internal value converter. Similar to its counterpart in JDBCResultSet <p>
     *
     * All trivially successful getXXX methods eventually go through this
     * method, converting if neccessary from the source type to the
     * requested type.  <p>
     *
     * Conversion to the JDBC representation, if different, is handled by the
     * calling methods.
     *
     * @param columnIndex of the column value for which to perform the
     *                 conversion
     * @param targetType the org.hsqldb_voltpatches.types.Type object for targetType
     * @return an Object of the requested targetType, representing the value of the
     *       specified column
     * @throws SQLException when there is no rowData, the column index is
     *    invalid, or the conversion cannot be performed
     */
    private Object getColumnInType(int columnIndex,
                                   Type targetType) throws SQLException {

        Type   sourceType;
        Object value;

        sourceType = parameterTypes[--columnIndex];
        value      = parameterValues[columnIndex];

        if (trackNull(value)) {
            return null;
        }

        if (sourceType.typeCode != targetType.typeCode) {
            try {
                value = targetType.convertToTypeJDBC(session, value,
                        sourceType);
            } catch (HsqlException e) {
                String stringValue =
                    (value instanceof Number || value instanceof String
                     || value instanceof java.util.Date) ? value.toString()
                        : "instance of " + value.getClass().getName();
                String msg = "from SQL type " + sourceType.getNameString()
                             + " to " + targetType.getJDBCClassName()
                             + ", value: " + stringValue;
                HsqlException err = Error.error(ErrorCode.X_42561, msg);

                throw Util.sqlException(err, e);
            }
        }

        return value;
    }

    private boolean trackNull(Object o) {
        return (wasNullValue = (o == null));
    }

    /************************* Volt DB Extensions *************************/

    public void closeOnCompletion() throws SQLException {
        throw new SQLException();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLException();
    }

    public <T> T getObject(int parameterIndex, Class<T> type)
            throws SQLException {
        throw new SQLException();
    }

    public <T> T getObject(String parameterName, Class<T> type)
            throws SQLException {
        throw new SQLException();
    }
    /**********************************************************************/
}
