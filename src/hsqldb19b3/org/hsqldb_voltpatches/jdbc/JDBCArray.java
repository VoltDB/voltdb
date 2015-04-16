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


package org.hsqldb_voltpatches.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hsqldb_voltpatches.ColumnBase;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.types.Type;

/**
 * The mapping in the Java programming language for the SQL type
 * <code>ARRAY</code>.
 * By default, an <code>Array</code> value is a transaction-duration
 * reference to an SQL <code>ARRAY</code> value.  By default, an <code>Array</code>
 * object is implemented using an SQL LOCATOR(array) internally, which
 * means that an <code>Array</code> object contains a logical pointer
 * to the data in the SQL <code>ARRAY</code> value rather
 * than containing the <code>ARRAY</code> value's data.
 * <p>
 * The <code>Array</code> interface provides methods for bringing an SQL
 * <code>ARRAY</code> value's data to the client as either an array or a
 * <code>ResultSet</code> object.
 * If the elements of the SQL <code>ARRAY</code>
 * are a UDT, they may be custom mapped.  To create a custom mapping,
 * a programmer must do two things:
 * <ul>
 * <li>create a class that implements the {@link java.sql.SQLData}
 * interface for the UDT to be custom mapped.
 * <li>make an entry in a type map that contains
 *   <ul>
 *   <li>the fully-qualified SQL type name of the UDT
 *   <li>the <code>Class</code> object for the class implementing
 *       <code>SQLData</code>
 *   </ul>
 * </ul>
 * <p>
 * When a type map with an entry for
 * the base type is supplied to the methods <code>getArray</code>
 * and <code>getResultSet</code>, the mapping
 * it contains will be used to map the elements of the <code>ARRAY</code> value.
 * If no type map is supplied, which would typically be the case,
 * the connection's type map is used by default.
 * If the connection's type map or a type map supplied to a method has no entry
 * for the base type, the elements are mapped according to the standard mapping.
 * <p>
 * All methods on the <code>Array</code> interface must be fully implemented if the
 * JDBC driver supports the data type.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.1
 * @since JDK 1.2, HSQLDB 2.0
 */
public class JDBCArray implements Array {

    /**
     * Retrieves the SQL type name of the elements in
     * the array designated by this <code>Array</code> object.
     * If the elements are a built-in type, it returns
     * the database-specific type name of the elements.
     * If the elements are a user-defined type (UDT),
     * this method returns the fully-qualified SQL type name.
     *
     * @return a <code>String</code> that is the database-specific
     * name for a built-in base type; or the fully-qualified SQL type
     * name for a base type that is a UDT
     * @exception SQLException if an error occurs while attempting
     * to access the type name
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public String getBaseTypeName() throws SQLException {

        checkClosed();

        return elementType.getNameString();
    }

    /**
     * Retrieves the JDBC type of the elements in the array designated
     * by this <code>Array</code> object.
     *
     * @return a constant from the class {@link java.sql.Types} that is
     * the type code for the elements in the array designated by this
     * <code>Array</code> object
     * @exception SQLException if an error occurs while attempting
     * to access the base type
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public int getBaseType() throws SQLException {

        checkClosed();

        return elementType.getJDBCTypeCode();
    }

    /**
     * <!-- start generic documentation -->
     * Retrieves the contents of the SQL <code>ARRAY</code> value designated
     * by this
     * <code>Array</code> object in the form of an array in the Java
     * programming language. This version of the method <code>getArray</code>
     * uses the type map associated with the connection for customizations of
     * the type mappings.
     * <p>
     * <strong>Note:</strong> When <code>getArray</code> is used to materialize
     * a base type that maps to a primitive data type, then it is
     * implementation-defined whether the array returned is an array of
     * that primitive data type or an array of <code>Object</code>.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB always returns an array of <code>Object</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return an array in the Java programming language that contains
     * the ordered elements of the SQL <code>ARRAY</code> value
     * designated by this <code>Array</code> object
     * @exception SQLException if an error occurs while attempting to
     * access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public Object getArray() throws SQLException {

        checkClosed();

        Object[] array = new Object[data.length];

        for (int i = 0; i < data.length; i++) {
            array[i] = elementType.convertSQLToJava(sessionProxy, data[i]);
        }

        return array;
    }

    /**
     * <!-- start generic documentation -->
     * Retrieves the contents of the SQL <code>ARRAY</code> value designated by this
     * <code>Array</code> object.
     * This method uses
     * the specified <code>map</code> for type map customizations
     * unless the base type of the array does not match a user-defined
     * type in <code>map</code>, in which case it
     * uses the standard mapping. This version of the method
     * <code>getArray</code> uses either the given type map or the standard mapping;
     * it never uses the type map associated with the connection.
     * <p>
     * <strong>Note:</strong> When <code>getArray</code> is used to materialize
     * a base type that maps to a primitive data type, then it is
     * implementation-defined whether the array returned is an array of
     * that primitive data type or an array of <code>Object</code>.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB always returns an array of <code>Object</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param map a <code>java.util.Map</code> object that contains mappings
     *            of SQL type names to classes in the Java programming language
     * @return an array in the Java programming language that contains the ordered
     *         elements of the SQL array designated by this object
     * @exception SQLException if an error occurs while attempting to
     *                         access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public Object getArray(java.util.Map<String,
            Class<?>> map) throws SQLException {
        return getArray();
    }

    /**
     * <!-- start generic documentation -->
     * Retrieves a slice of the SQL <code>ARRAY</code>
     * value designated by this <code>Array</code> object, beginning with the
     * specified <code>index</code> and containing up to <code>count</code>
     * successive elements of the SQL array.  This method uses the type map
     * associated with the connection for customizations of the type mappings.
     * <p>
     * <strong>Note:</strong> When <code>getArray</code> is used to materialize
     * a base type that maps to a primitive data type, then it is
     * implementation-defined whether the array returned is an array of
     * that primitive data type or an array of <code>Object</code>.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB always returns an array of <code>Object</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param index the array index of the first element to retrieve;
     *              the first element is at index 1
     * @param count the number of successive SQL array elements to retrieve
     * @return an array containing up to <code>count</code> consecutive elements
     * of the SQL array, beginning with element <code>index</code>
     * @exception SQLException if an error occurs while attempting to
     * access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public Object getArray(long index, int count) throws SQLException {

        checkClosed();

        if (!JDBCClobClient.isInLimits(data.length, index - 1, count)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        Object[] slice = new Object[count];

        for (int i = 0; i < count; i++) {
            slice[i] = elementType.convertSQLToJava(sessionProxy,
                    data[(int) index + i - 1]);
        }

        return slice;
    }

    /**
     * <!-- start generic documentation -->
     * Retreives a slice of the SQL <code>ARRAY</code> value
     * designated by this <code>Array</code> object, beginning with the specified
     * <code>index</code> and containing up to <code>count</code>
     * successive elements of the SQL array.
     * <P>
     * This method uses
     * the specified <code>map</code> for type map customizations
     * unless the base type of the array does not match a user-defined
     * type in <code>map</code>, in which case it
     * uses the standard mapping. This version of the method
     * <code>getArray</code> uses either the given type map or the standard mapping;
     * it never uses the type map associated with the connection.
     * <p>
     * <strong>Note:</strong> When <code>getArray</code> is used to materialize
     * a base type that maps to a primitive data type, then it is
     * implementation-defined whether the array returned is an array of
     * that primitive data type or an array of <code>Object</code>.
     *
     * <!-- end generic documentation -->
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB always returns an array of <code>Object</code>.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param index the array index of the first element to retrieve;
     *              the first element is at index 1
     * @param count the number of successive SQL array elements to
     * retrieve
     * @param map a <code>java.util.Map</code> object
     * that contains SQL type names and the classes in
     * the Java programming language to which they are mapped
     * @return an array containing up to <code>count</code>
     * consecutive elements of the SQL <code>ARRAY</code> value designated by this
     * <code>Array</code> object, beginning with element
     * <code>index</code>
     * @exception SQLException if an error occurs while attempting to
     * access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public Object getArray(long index, int count,
                           java.util.Map<String,
                               Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    /**
     * Retrieves a result set that contains the elements of the SQL
     * <code>ARRAY</code> value
     * designated by this <code>Array</code> object.  If appropriate,
     * the elements of the array are mapped using the connection's type
     * map; otherwise, the standard mapping is used.
     * <p>
     * The result set contains one row for each array element, with
     * two columns in each row.  The second column stores the element
     * value; the first column stores the index into the array for
     * that element (with the first array element being at index 1).
     * The rows are in ascending order corresponding to
     * the order of the indices.
     *
     * @return a {@link ResultSet} object containing one row for each
     * of the elements in the array designated by this <code>Array</code>
     * object, with the rows in ascending order based on the indices.
     * @exception SQLException if an error occurs while attempting to
     * access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public ResultSet getResultSet() throws SQLException {

        checkClosed();

        Result result = this.newColumnResult(0, data.length);

        return new JDBCResultSet(connection, result, result.metaData);
    }

    /**
     * Retrieves a result set that contains the elements of the SQL
     * <code>ARRAY</code> value designated by this <code>Array</code> object.
     * This method uses
     * the specified <code>map</code> for type map customizations
     * unless the base type of the array does not match a user-defined
     * type in <code>map</code>, in which case it
     * uses the standard mapping. This version of the method
     * <code>getResultSet</code> uses either the given type map or the standard mapping;
     * it never uses the type map associated with the connection.
     * <p>
     * The result set contains one row for each array element, with
     * two columns in each row.  The second column stores the element
     * value; the first column stores the index into the array for
     * that element (with the first array element being at index 1).
     * The rows are in ascending order corresponding to
     * the order of the indices.
     *
     * @param map contains the mapping of SQL user-defined types to
     * classes in the Java programming language
     * @return a <code>ResultSet</code> object containing one row for each
     * of the elements in the array designated by this <code>Array</code>
     * object, with the rows in ascending order based on the indices.
     * @exception SQLException if an error occurs while attempting to
     * access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public ResultSet getResultSet(java.util.Map<String,
            Class<?>> map) throws SQLException {
        return getResultSet();
    }

    /**
     * Retrieves a result set holding the elements of the subarray that
     * starts at index <code>index</code> and contains up to
     * <code>count</code> successive elements.  This method uses
     * the connection's type map to map the elements of the array if
     * the map contains an entry for the base type. Otherwise, the
     * standard mapping is used.
     * <P>
     * The result set has one row for each element of the SQL array
     * designated by this object, with the first row containing the
     * element at index <code>index</code>.  The result set has
     * up to <code>count</code> rows in ascending order based on the
     * indices.  Each row has two columns:  The second column stores
     * the element value; the first column stores the index into the
     * array for that element.
     *
     * @param index the array index of the first element to retrieve;
     *              the first element is at index 1
     * @param count the number of successive SQL array elements to retrieve
     * @return a <code>ResultSet</code> object containing up to
     * <code>count</code> consecutive elements of the SQL array
     * designated by this <code>Array</code> object, starting at
     * index <code>index</code>.
     * @exception SQLException if an error occurs while attempting to
     * access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public ResultSet getResultSet(long index, int count) throws SQLException {

        checkClosed();

        Result result = this.newColumnResult(index - 1, count);

        return new JDBCResultSet(connection, result, result.metaData);
    }

    /**
     * Retrieves a result set holding the elements of the subarray that
     * starts at index <code>index</code> and contains up to
     * <code>count</code> successive elements.
     * This method uses
     * the specified <code>map</code> for type map customizations
     * unless the base type of the array does not match a user-defined
     * type in <code>map</code>, in which case it
     * uses the standard mapping. This version of the method
     * <code>getResultSet</code> uses either the given type map or the standard mapping;
     * it never uses the type map associated with the connection.
     * <P>
     * The result set has one row for each element of the SQL array
     * designated by this object, with the first row containing the
     * element at index <code>index</code>.  The result set has
     * up to <code>count</code> rows in ascending order based on the
     * indices.  Each row has two columns:  The second column stores
     * the element value; the first column stroes the index into the
     * array for that element.
     *
     * @param index the array index of the first element to retrieve;
     *              the first element is at index 1
     * @param count the number of successive SQL array elements to retrieve
     * @param map the <code>Map</code> object that contains the mapping
     * of SQL type names to classes in the Java(tm) programming language
     * @return a <code>ResultSet</code> object containing up to
     * <code>count</code> consecutive elements of the SQL array
     * designated by this <code>Array</code> object, starting at
     * index <code>index</code>.
     * @exception SQLException if an error occurs while attempting to
     * access the array
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public ResultSet getResultSet(long index, int count,
                                  java.util.Map<String,
                                      Class<?>> map) throws SQLException {
        return getResultSet(index, count);
    }

    /**
     * Returns a string representation in the form <code>ARRAY[..., ...]</code>
     */
    public String toString() {

        if (arrayType == null) {
            arrayType = Type.getDefaultArrayType(elementType.typeCode);
        }

        return arrayType.convertToString(data);
    }

    /**
     * This method frees the <code>Array</code> object and releases the resources that
     * it holds. The object is invalid once the <code>free</code>
     * method is called.
     * <p>
     * After <code>free</code> has been called, any attempt to invoke a
     * method other than <code>free</code> will result in a <code>SQLException</code>
     * being thrown.  If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     * <p>
     *
     * @throws SQLException if an error occurs releasing
     * the Array's resources
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6
     */
    public void free() throws SQLException {

        if (!closed) {
            closed       = true;
            connection   = null;
            sessionProxy = null;
        }
    }

    //-------------
    volatile boolean closed;
    Type             arrayType;
    Type             elementType;
    Object[]         data;
    JDBCConnection   connection;
    SessionInterface sessionProxy;

    public JDBCArray(Object[] data, Type type, Type arrayType,
                     SessionInterface session) {

        this(data, type, arrayType, session.getJDBCConnection());

        this.sessionProxy = session;
    }

    /**
     * Constructor should reject unsupported types.
     */
    JDBCArray(Object[] data, Type type,
              JDBCConnection connection) throws SQLException {
        this(data, type, null, connection);
    }

    JDBCArray(Object[] data, Type type, Type arrayType,
              JDBCConnection connection) {

        this.data         = data;
        this.elementType  = type;
        this.arrayType    = arrayType;
        this.connection   = connection;

        if (connection != null) {
            this.sessionProxy = connection.sessionProxy;
        }
    }

    public Object[] getArrayInternal() {
        return data;
    }

    private Result newColumnResult(long position,
                                   int count) throws SQLException {

        if (!JDBCClobClient.isInLimits(data.length, position, count)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        Type[] types = new Type[2];

        types[0] = Type.SQL_INTEGER;
        types[1] = elementType;

        ResultMetaData meta = ResultMetaData.newSimpleResultMetaData(types);

        meta.columnLabels = new String[] {
            "C1", "C2"
        };
        meta.colIndexes   = new int[] {
            -1, -1
        };
        meta.columns      = new ColumnBase[2];

        ColumnBase column = new ColumnBase("", "", "", "");

        column.setType(types[0]);

        meta.columns[0] = column;
        column          = new ColumnBase("", "", "", "");

        column.setType(types[1]);

        meta.columns[1] = column;

        RowSetNavigatorClient navigator = new RowSetNavigatorClient();

        for (int i = (int) position; i < position + count; i++) {
            Object[] rowData = new Object[2];

            rowData[0] = Integer.valueOf(i + 1);
            rowData[1] = data[i];

            navigator.add(rowData);
        }

        Result result = Result.newDataResult(meta);

        result.setNavigator(navigator);

        return result;
    }

    private void checkClosed() throws SQLException {

        if (closed) {
            throw JDBCUtil.sqlException(ErrorCode.X_07501);
        }
    }
}
