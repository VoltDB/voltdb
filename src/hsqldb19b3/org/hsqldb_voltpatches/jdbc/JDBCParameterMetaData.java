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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ParameterMetaData;
import java.sql.SQLException;

import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.types.Type;

/* $Id: JDBCParameterMetaData.java 2944 2009-03-21 22:53:43Z fredt $ */

/** @todo 1.9.0 - implement internal support for INOUT, OUT return parameter */

// fredt@users 20040412 - removed DITypeInfo dependencies
// fredt@usres 1.9.0 - utilise the new type support
// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
// boucherb@users 20060522 - doc   1.9.0 full synch up to Mustang Build 84
// Revision 1.14  2006/07/12 12:20:49  boucherb
// - remove unused imports
// - full synch up to Mustang b90

/**
 * An object that can be used to get information about the types
 * and properties for each parameter marker in a
 * <code>PreparedStatement</code> object. (JDBC4 Clarification:) For some queries and driver
 * implementations, the data that would be returned by a <code>ParameterMetaData</code>
 * object may not be available until the <code>PreparedStatement</code> has
 * been executed.
 * <p>
 * Some driver implementations may not be able to provide information about the
 * types and properties for each parameter marker in a <code>CallableStatement</code>
 * object.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @version 1.9.0
 * @since JDK 1.4, HSQLDB 1.7.2
 * @revised JDK 1.6, HSQLDB 1.9.0
 */
//#ifdef JAVA6
public class JDBCParameterMetaData implements ParameterMetaData,
        java.sql.Wrapper {

//#else
/*
public class JDBCParameterMetaData
    implements ParameterMetaData {
*/

//#endif JAVA6

    /**
     * Retrieves the number of parameters in the <code>PreparedStatement</code>
     * object for which this <code>ParameterMetaData</code> object contains
     * information.
     *
     * @return the number of parameters
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    /**
     * Retrieves whether null values are allowed in the designated parameter.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return the nullability status of the given parameter; one of
     *        <code>ParameterMetaData.parameterNoNulls</code>,
     *        <code>ParameterMetaData.parameterNullable</code>, or
     *        <code>ParameterMetaData.parameterNullableUnknown</code>
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public int isNullable(int param) throws SQLException {

        checkRange(param);

        return ParameterMetaData.parameterNullableUnknown;
    }

    /**
     * Retrieves whether values for the designated parameter can be signed numbers.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public boolean isSigned(int param) throws SQLException {

        checkRange(param);

        return rmd.columnTypes[--param].isNumberType();
    }

    /**
     * Retrieves the designated parameter's specified column size.
     *
     * <P>The returned value represents the maximum column size for the given parameter.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return precision
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public int getPrecision(int param) throws SQLException {

        checkRange(param);

        Type type = rmd.columnTypes[--param];

        if (type.isDateTimeType()) {
            return type.displaySize();
        } else {
            long size = type.precision;

            if (size > Integer.MAX_VALUE) {
                size = 0;
            }

            return (int) size;
        }
    }

    /**
     * Retrieves the designated parameter's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return scale
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public int getScale(int param) throws SQLException {

        checkRange(param);

        return rmd.columnTypes[--param].scale;
    }

    /**
     * Retrieves the designated parameter's SQL type.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return SQL type from <code>java.sql.Types</code>
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     * @see java.sql.Types
     */
    public int getParameterType(int param) throws SQLException {

        checkRange(param);

        return rmd.columnTypes[--param].getJDBCTypeCode();
    }

    /**
     * Retrieves the designated parameter's database-specific type name.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return type the name used by the database. If the parameter type is
     * a user-defined type, then a fully-qualified type name is returned.
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public String getParameterTypeName(int param) throws SQLException {

        checkRange(param);

        return rmd.columnTypes[--param].getNameString();
    }

    /**
     * Retrieves the fully-qualified name of the Java class whose instances
     * should be passed to the method <code>PreparedStatement.setObject</code>.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming
     *         language that would be used by the method
     *         <code>PreparedStatement.setObject</code> to set the value
     *         in the specified parameter. This is the class name used
     *         for custom mapping.
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public String getParameterClassName(int param) throws SQLException {

        checkRange(param);

        return rmd.columnTypes[--param].getJDBCClassName();
    }

    /**
     * Retrieves the designated parameter's mode.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return mode of the parameter; one of
     *        <code>ParameterMetaData.parameterModeIn</code>,
     *        <code>ParameterMetaData.parameterModeOut</code>, or
     *        <code>ParameterMetaData.parameterModeInOut</code>
     *        <code>ParameterMetaData.parameterModeUnknown</code>.
     * @exception SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public int getParameterMode(int param) throws SQLException {

        checkRange(param);

        return rmd.paramModes[--param];
    }

    //----------------------------- JDBC 4.0 -----------------------------------
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
    // -------------------------- Internal Implementation ----------------------

    /** The metadata object with which this object is constructed */
    ResultMetaData rmd;

    /**
     * The fully-qualified name of the Java class whose instances should
     * be passed to the method PreparedStatement.setObject. <p>
     *
     * Note that changes to Function.java and Types.java allow passing
     * objects of any class implementing java.io.Serializable and that,
     * as such, the parameter expression resolution mechanism has been
     * upgraded to provide the precise FQN for SQL function and stored
     * procedure arguments, rather than the more generic
     * org.hsqldb_voltpatches.JavaObject class that is used internally to represent
     * and transport objects whose class is not in the standard mapping.
     */
    String[] classNames;

    /** The number of parameters in the described statement */
    int parameterCount;

    /**
     * Creates a new instance of JDBCParameterMetaData. <p>
     *
     * @param metaData A ResultMetaData object describing the statement parameters
     * @throws SQLException never - reserved for future use
     */
    JDBCParameterMetaData(ResultMetaData metaData) throws SQLException {
        rmd            = metaData;
        parameterCount = rmd.getColumnCount();
    }

    /**
     * Checks if the value of the param argument is a valid parameter
     * position. <p>
     *
     * @param param position to check
     * @throws SQLException if the value of the param argument is not a
     *      valid parameter position
     */
    void checkRange(int param) throws SQLException {

        if (param < 1 || param > parameterCount) {
            String msg = param + " is out of range";

            throw Util.outOfRangeArgument(msg);
        }
    }

    /**
     * Retrieves a String repsentation of this object. <p>
     *
     * @return a String repsentation of this object
     */
    public String toString() {

        try {
            return toStringImpl();
        } catch (Throwable t) {
            return super.toString() + "[toStringImpl_exception=" + t + "]";
        }
    }

    /**
     * Provides the implementation of the toString() method. <p>
     *
     * @return a String representation of this object
     * @throws Exception if a reflection error occurs
     */
    private String toStringImpl() throws Exception {

        StringBuffer sb;
        Method[]     methods;
        Method       method;
        int          count;

        sb = new StringBuffer();

        sb.append(super.toString());

        count = getParameterCount();

        if (count == 0) {
            sb.append("[parameterCount=0]");

            return sb.toString();
        }
        methods = getClass().getDeclaredMethods();

        sb.append('[');

        int len = methods.length;

        for (int i = 0; i < count; i++) {
            sb.append('\n');
            sb.append("    parameter_");
            sb.append(i + 1);
            sb.append('=');
            sb.append('[');

            for (int j = 0; j < len; j++) {
                method = methods[j];

                if (!Modifier.isPublic(method.getModifiers())) {
                    continue;
                }

                if (method.getParameterTypes().length != 1) {
                    continue;
                }
                sb.append(method.getName());
                sb.append('=');
                sb.append(method.invoke(this,
                                        new Object[] { new Integer(i + 1) }));

                if (j + 1 < len) {
                    sb.append(',');
                    sb.append(' ');
                }
            }
            sb.append(']');

            if (i + 1 < count) {
                sb.append(',');
                sb.append(' ');
            }
        }
        sb.append('\n');
        sb.append(']');

        return sb.toString();
    }
}
