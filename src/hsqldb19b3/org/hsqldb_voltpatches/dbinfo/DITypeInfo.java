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


package org.hsqldb_voltpatches.dbinfo;

import java.util.Locale;

import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.resources.BundleHandler;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.Type;

// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work

/**
 * Provides information intrinsic to each standard data type known to
 * HSQLDB.  This includes all types for which standard type codes are known
 * at the time of writing and thus includes types that may not yet be
 * supported as table or procedure columns. <p>
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
final class DITypeInfo {

    // related to DatabaseMetaDataValues
    static final int columnNoNulls  = 0;
    static final int columnNullable = 1;
    static final int typePredNone   = 0;
    static final int typePredChar   = 1;
    static final int typePredBasic  = 2;
    static final int typeSearchable = 3;

    /** BundleHandler id for create params resource bundle. */
    private int hnd_create_params = -1;

    /** BundleHandler id for local names resource bundle. */
    private int hnd_local_names = -1;

    /** BundleHandler id for data type remarks resource bundle. */
    private int hnd_remarks = -1;

    /** The SQL type code on which this object is reporting. */
    private int type = Types.SQL_ALL_TYPES;

    /** The HSQLDB subtype code on which this object is reporting. */
    private int typeSub = Types.TYPE_SUB_DEFAULT;
    boolean     locale_set;

    /** Creates a new DITypeInfo object having the default Locale. */
    DITypeInfo() {

        // boucherb@users 20050515 - patch 1.8.0 - make setLocale lazy
        //setLocale(Locale.getDefault());
    }

    /**
     * Retrieves the maximum Integer.MAX_VALUE bounded length, in bytes, for
     * character types. <p>
     *
     * @return the maximum Integer.MAX_VALUE bounded length, in
     *    bytes, for character types
     */
    Integer getCharOctLen() {
        return null;
    }

    /**
     * Retrieves the maximum Long.MAX_VALUE bounded length, in bytes, for
     * character types. <p>
     *
     * @return the maximum Long.MAX_VALUE bounded length, in
     *    bytes, for character types
     */
    Long getCharOctLenAct() {

        switch (type) {

            case Types.SQL_CHAR :
            case Types.SQL_NCHAR :
            case Types.SQL_VARCHAR :
            case Types.SQL_NVARCHAR :
                return ValuePool.getLong(2L * Integer.MAX_VALUE);

            case Types.SQL_CLOB :
            case Types.NCLOB :
                return ValuePool.getLong(Long.MAX_VALUE);

            default :
                return null;
        }
    }

    /**
     * Retrieves the fully-qualified name of the Java class whose instances
     * are manufactured by HSQLDB to store table column values in
     * memory. <p>
     *
     * This is also typically the fully-qualified name of the Java class whose
     * instances are manufactured by HSQLDB in response to
     * JDBCResultSet.getObject(int). <p>
     *
     * @return the fully-qualified name of the Java class whose
     *    instances are manufactured by HSQLDB to store
     *    table column values in memory
     */
    String getColStClsName() {
        return Types.getColStClsName(type);
    }

    /**
     * Retrieves a character sequence representing a CSV list, in
     * DDL declaraion order, of the create parameters for the type. <p>
     *
     * @return a character sequence representing a CSV
     *    list, in DDL declaraion order, of the create
     *    parameters for the type.
     */
    String getCreateParams() {

        if (!locale_set) {
            setLocale(Locale.getDefault());
        }

        String names;

        switch (type) {

            case Types.SQL_BINARY :
            case Types.SQL_CHAR :
            case Types.SQL_NCHAR :
            case Types.SQL_CLOB :
            case Types.NCLOB :
            case Types.SQL_VARBINARY :
            case Types.SQL_VARCHAR :
            case Types.SQL_NVARCHAR :
                names = "LENGTH";
                break;

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                names = "PRECISION,SCALE";
                break;

            case Types.SQL_FLOAT :
            case Types.SQL_TIMESTAMP :
                names = "PRECISION";
                break;

            default :
                names = null;
                break;
        }

        return names;
    }

    /**
     * Retrieves the fully-qualified name of the HSQLDB-provided java.sql
     * interface implementation class whose instances would be manufactured
     * by HSQLDB to retrieve column values of this type, if the
     * the type does not have a standard Java mapping.  <p>
     *
     * This value is simply the expected class name, regardless of whether
     * HSQLDB, the specific HSQLDB distribution instance or the hosting JVM
     * actually provide or support such implementations. That is, as of a
     * specific release, HSQLDB may not yet provide such an implementation
     * or may not automatically map to it or may not support it as a table
     * column type, the version of java.sql may not define the interface to
     * implement and the HSQLDB jar may not contain the implementation
     * classes, even if they are defined in the corresponding release
     * and build options and are supported under the hosting JVM's java.sql
     * version.<p>
     *
     * @return the fully-qualified name of the HSQLDB-provided java.sql
     *    interface implementation class whose instances would
     *    be manufactured by HSQLDB to retrieve column values of
     *    this type, given that the type does not have a standard Java
     *    mapping and regardless of whether a class with the indicated
     *    name is actually implemented or available on the class path
     */
    String getCstMapClsName() {

        switch (type) {

            case Types.SQL_ARRAY :
                return "org.hsqldb_voltpatches.jdbc.JDBCArray";

            case Types.SQL_BLOB :
                return "org.hsqldb_voltpatches.jdbc.JDBCBlob";

            case Types.SQL_CLOB :
                return "org.hsqldb_voltpatches.jdbc.JDBCClob";

            case Types.NCLOB :
                return "org.hsqldb_voltpatches.jdbc.JDBCNClob";

            case Types.DISTINCT :
                return "org.hsqldb_voltpatches.jdbc.JDBCDistinct";

            case Types.SQL_REF :
                return "org.hsqldb_voltpatches.jdbc.JDBCRef";

            case Types.ROWID :
                return "org.hsqldb_voltpatches.jdbc.JDBCRowId";

            case Types.STRUCT :
                return "org.hsqldb_voltpatches.jdbc.JDBCStruct";

            case Types.SQL_XML :
                return "org.hsqldb_voltpatches.jdbc.JDBCSQLXML";

            default :
                return null;
        }
    }

// NOTES:
// From recent usability testing, this patch and the corresponding patch in
// JDBCResultSetMetaData together provide better compatibility with existing
// tools than through the previous method of dynamically scanning each result
// in JDBCResultSetMetaData.  This is especially true given that many tools use
// the display size to set their internal max # of characters reserved for
// result columns at said position. Also, since adding implementation of
// ResultSetMetaData retreived from PreparedStatement's getMetaData method,
// there exists the case where there is no data to scan in order to determine
// an approximation.  Finally, since for plain old Statement objects the only
// way to get metadata is to execute the query and since the scan approximation
// can vary from one execution to the next, it turns out it's much "safer" and
// more usable in a tool setting to simply indicate a large but expected
// acceptable number of characters, rather than report either a number generally
// too large to handle or a number too small to hold the actual expected maximum
// number of characters for result colums of CHAR or VARCHAR types

    /**
     * Retrieves the maximum length that a String representation of
     * the type may have. <p>
     *
     * The calculation follows these rules: <p>
     *
     * <ol>
     * <li>Long character and datetime types:<p>
     *
     *     The maximum length/precision, repectively.<p>
     *
     * <li>CHAR and VARCHAR types: <p>
     *
     *      The value of the system property hsqldb.max_xxxchar_display_size
     *      or the magic value 32766 (0x7FFE) (tested usable/accepted by most
     *      tools and compatible with assumptions made by java.io read/write
     *      UTF) when the system property is not defined or is not accessible,
     *      due to security constraints. <p>
     *
     * <li>Number types: <p>
     *
     *     The max precision, plus the length of the negation character (1),
     *     plus (if applicable) the maximum number of characters that may
     *     occupy the exponent character sequence. <p>
     *
     * <li>BOOLEAN/BIT types: <p>
     *
     *     The length of the character sequence "false" (5), the longer of the
     *     two boolean value String representations. <p>
     *
     * <li>All remaining types: <p>
     *
     *     The result of whatever calculation must be performed to determine
     *     the maximum length of the type's String representation. If the
     *     maximum display size is unknown, unknowable or inapplicable, then
     *     zero is returned. <p>
     *
     * </ol>
     *
     * @return the maximum length that a String representation of
     *      the type may have, or zero (0) if unknown or unknowable
     */
    int getMaxDisplaySize() {
        return Types.getMaxDisplaySize(type);
    }

    /**
     * Retrieves the data type, as an Integer. <p>
     *
     * @return the data type, as an Integer.
     */
    Integer getDataType() {
        return ValuePool.getInt(type);
    }

    /**
     * Retrieves the data subtype, as an Integer. <p>
     *
     * @return the data subtype, as an Integer
     */
    Integer getDataTypeSub() {
        return ValuePool.getInt(typeSub);
    }

    /**
     * Retrieves the implied or single permitted scale for exact numeric
     * types, when no scale is declared or no scale declaration is
     * permitted. <p>
     *
     * @return the implied or single permitted scale for exact numeric
     *    types, when no scale is declared or no scale declaration
     *    is permitted
     */
    Integer getDefaultScale() {

        switch (type) {

            case Types.SQL_BIGINT :
            case Types.SQL_INTEGER :
            case Types.SQL_SMALLINT :
            case Types.TINYINT :
                return ValuePool.INTEGER_0;

            default :
                return null;
        }
    }

    /**
     * Retrieves null (not implemented). <p>
     *
     * @return null (not implemented)
     */
    Integer getIntervalPrecision() {
        return null;
    }

    /**
     * Retrieves the character(s) prefixing a literal of this type. <p>
     *
     * @return the character(s) prefixing a literal of this type.
     */
    String getLiteralPrefix() {

        switch (type) {

            case Types.SQL_BINARY :
            case Types.SQL_BLOB :
            case Types.SQL_CHAR :
            case Types.SQL_NCHAR :
            case Types.SQL_CLOB :
            case Types.SQL_VARBINARY :
            case Types.SQL_VARCHAR :
            case Types.SQL_NVARCHAR :
                return "'";

            case Types.SQL_DATALINK :
                return "'";    // hypothetically: "{url '";

            case Types.SQL_DATE :
                return "'";    // or JDBC escape: "{d '";

            case Types.OTHER :
                return "'";    // hypothetically: "{o '"; or new "pkg.cls"(...)

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                return "'";    // or JDBC escape: "{t '";

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                return "'";    // or JDBC escape: "{ts '";

            case Types.SQL_XML :
                return "'";    // hypothetically: "{xml '";

            default :
                return null;
        }
    }

    /**
     * Retrieves the character(s) terminating a literal of this type. <p>
     *
     * @return the character(s) terminating a literal of this type
     */
    String getLiteralSuffix() {

        switch (type) {

            case Types.SQL_BINARY :
            case Types.SQL_BLOB :
            case Types.SQL_CHAR :
            case Types.SQL_NCHAR :
            case Types.SQL_CLOB :
            case Types.NCLOB :
            case Types.SQL_LONGVARBINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_VARCHAR :
            case Types.SQL_NVARCHAR :
                return "'";

            case Types.SQL_DATALINK :
            case Types.SQL_DATE :
            case Types.OTHER :
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_XML :
                return "'";    // or JDBC close escape: "'}";

            default :
                return null;
        }
    }

    /**
     * Retrieves a localized representation of the type's name, for human
     * consumption only. <p>
     *
     * @return a localized representation of the type's name
     */
    String getLocalName() {

        if (!locale_set) {
            setLocale(Locale.getDefault());
        }

        String key = this.getTypeName();

        return BundleHandler.getString(hnd_local_names, key);
    }

    /**
     * Retrieves the maximum Short.MAX_VALUE bounded scale supported
     * for the type. <p>
     *
     * @return the maximum Short.MAX_VALUE bounded scale supported
     *    for the type
     */
    Integer getMaxScale() {

        switch (type) {

            case Types.SQL_BIGINT :
            case Types.SQL_DATE :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
            case Types.SQL_INTEGER :
            case Types.SQL_SMALLINT :
            case Types.TINYINT :
                return ValuePool.INTEGER_0;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                return ValuePool.getInt(6);

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                return ValuePool.getInt(Short.MAX_VALUE);

            case Types.SQL_FLOAT :
            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return ValuePool.getInt(306);

            default :
                return null;
        }
    }

    /**
     * Retrieves the maximum Integer.MAX_VALUE bounded scale supported for
     * the type. <p>
     *
     * @return the maximum Integer.MAX_VALUE bounded scale supported
     *    for the type
     */
    Integer getMaxScaleAct() {

        switch (type) {

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                return ValuePool.getInt(Integer.MAX_VALUE);

            default :
                return getMaxScale();
        }
    }

    /**
     * Retrieves the minumum Short.MIN_VALUE bounded scale supported for
     * the type. <p>
     *
     * @return the minumum Short.MIN_VALUE bounded scale
     *    supported for the type
     */
    Integer getMinScale() {

        switch (type) {

            case Types.SQL_BIGINT :
            case Types.SQL_DATE :
            case Types.SQL_INTEGER :
            case Types.SQL_SMALLINT :
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.TINYINT :
            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                return ValuePool.INTEGER_0;

            case Types.SQL_FLOAT :
            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return ValuePool.getInt(-324);

            default :
                return null;
        }
    }

    /**
     * Retrieves the minumum Integer.MIN_VALUE bounded scale supported
     * for the type. <p>
     *
     * @return the minumum Integer.MIN_VALUE bounded scale supported
     *    for the type
     */
    Integer getMinScaleAct() {
        return getMinScale();
    }

    /**
     * Retrieves the DatabaseMetaData default nullability code for
     * the type. <p>
     *
     * @return the DatabaseMetaData nullability code for the type.
     */
    Integer getNullability() {
        return ValuePool.getInt(columnNullable);
    }

    /**
     * Retrieves the number base which is to be used to interpret the value
     * reported by getPrecision(). <p>
     *
     * @return the number base which is to be used to interpret the
     *    value reported by getPrecision()
     */
    Integer getNumPrecRadix() {

        switch (type) {

            case Types.SQL_BIGINT :
            case Types.SQL_DECIMAL :
            case Types.SQL_INTEGER :
            case Types.SQL_NUMERIC :
            case Types.SQL_SMALLINT :
            case Types.TINYINT :
                return ValuePool.getInt(10);

            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_REAL :
                return ValuePool.getInt(2);

            default :
                return null;
        }
    }

    /**
     * Retrieves the maximum Integer.MAX_VALUE bounded length or precision for
     * the type. <p>
     *
     * @return the maximum Integer.MAX_VALUE bounded length or
     *    precision for the type
     */
    Integer getPrecision() {

        int p = Types.getPrecision(type);

        return p == 0 ? null
                      : ValuePool.getInt(p);
    }

    /**
     * Retrieves the maximum Long.MAX_VALUE bounded length or precision for
     * the type. <p>
     *
     * @return the maximum Long.MAX_VALUE bounded length or
     *    precision for the type
     */
    Long getPrecisionAct() {

        Integer temp = getPrecision();

        if (temp == null) {
            return ValuePool.getLong(Long.MAX_VALUE);
        } else {
            return ValuePool.getLong(temp.longValue());
        }
    }

    /**
     * Retrieves the localized remarks (if any) on the type. <p>
     *
     * @return the localized remarks on the type.
     */
    String getRemarks() {

        if (!locale_set) {
            setLocale(Locale.getDefault());
        }

        String key = this.getTypeName();

        return BundleHandler.getString(hnd_remarks, key);
    }

    /**
     * Retrieves the DatabaseMetaData searchability code for the type. <p>
     *
     * @return the DatabaseMetaData searchability code for the type
     */
    Integer getSearchability() {
        return Types.isSearchable(type) ? ValuePool.getInt(typeSearchable)
                                        : ValuePool.getInt(typePredNone);
    }

    /**
     * Retrieves the SQL CLI data type code for the type. <p>
     *
     * @return the SQL CLI data type code for the type
     */
    Integer getSqlDataType() {

        // values from SQL200n SQL CLI spec, or DITypes (which in turn borrows
        // first from java.sql.Types and then SQL200n SQL CLI spec) if there
        // was no corresponding value in SQL CLI
        switch (type) {

            case Types.SQL_ARRAY :
                return ValuePool.getInt(Types.SQL_ARRAY);           // SQL_ARRAY

            case Types.SQL_BIGINT :
                return ValuePool.getInt(Types.SQL_BIGINT);          // SQL_BIGINT

            case Types.SQL_BINARY :
                return ValuePool.getInt(Types.SQL_BLOB);            // fredt- was SQL_BIT_VARYING

            case Types.SQL_BOOLEAN :
                return ValuePool.getInt(Types.SQL_BOOLEAN);         // SQL_BOOLEAN

            case Types.SQL_BLOB :
                return ValuePool.getInt(Types.SQL_BLOB);            // SQL_BLOB

            case Types.SQL_CHAR :
                return ValuePool.getInt(Types.SQL_CHAR);            // SQL_CHAR

            case Types.SQL_NCHAR :
                return ValuePool.getInt(Types.SQL_NCHAR);

            case Types.SQL_CLOB :
                return ValuePool.getInt(Types.SQL_CLOB);            // SQL_CLOB

            case Types.NCLOB :
                return ValuePool.getInt(Types.SQL_CLOB);            // N/A : SQL_CLOB

            case Types.SQL_DATALINK :
                return ValuePool.getInt(Types.SQL_DATALINK);        // SQL_DATALINK

            case Types.SQL_DATE :

                // NO:  This is the _concise_ code, whereas what we want to
                //      return here is the Data Type Code column value from
                //      Table 38 in the SQL 200n FCD.  This method is used
                //      by DatabaseInformationMain to list the sql type
                //      column in the SYSTEM_TYPEINFO table, which is specified
                //      by JDBC as the sql data type code, not the concise code.
                //      That is why there is a sql datetime sub column
                //      specified as well.
                // return ValuePool.getInt(Types.SQL_DATE);         // fredt - was SQL_DATETIME
                return ValuePool.getInt(Types.SQL_DATETIME);

            case Types.SQL_DECIMAL :
                return ValuePool.getInt(Types.SQL_DECIMAL);         // SQL_DECIMAL

            case Types.DISTINCT :
                return ValuePool.getInt(Types.SQL_UDT);             // SQL_UDT

            case Types.SQL_DOUBLE :
                return ValuePool.getInt(Types.SQL_DOUBLE);          // SQL_DOUBLE

            case Types.SQL_FLOAT :
                return ValuePool.getInt(Types.SQL_FLOAT);           // SQL_FLOAT

            case Types.SQL_INTEGER :
                return ValuePool.getInt(Types.SQL_INTEGER);         // SQL_INTEGER

            case Types.JAVA_OBJECT :
                return ValuePool.getInt(Types.JAVA_OBJECT);         // N/A - maybe SQL_UDT?

            case Types.SQL_ALL_TYPES :
                return ValuePool.getInt(Types.SQL_ALL_TYPES);       // SQL_ALL_TYPES

            case Types.SQL_NUMERIC :
                return ValuePool.getInt(Types.SQL_NUMERIC);         // SQL_NUMERIC

            case Types.OTHER :
                return ValuePool.getInt(Types.OTHER);               // N/A - maybe SQL_UDT?

            case Types.SQL_REAL :
                return ValuePool.getInt(Types.SQL_REAL);            // SQL_REAL

            case Types.ROWID :
                return ValuePool.getInt(Types.SQL_GUID);

            case Types.SQL_REF :
                return ValuePool.getInt(Types.SQL_REF);             // SQL_REF

            case Types.SQL_SMALLINT :
                return ValuePool.getInt(Types.SQL_SMALLINT);        // SQL_SMALLINTEGER

            case Types.STRUCT :
                return ValuePool.getInt(Types.SQL_UDT);             // SQL_UDT

            case Types.SQL_TIME :

                // NO:  This is the _concise_ code, whereas what we want to
                //      return here is the Data Type Code column value from
                //      Table 38 in the SQL 200n FCD.  This method is used
                //      by DatabaseInformationMain to list the sql type
                //      column in the SYSTEM_TYPEINFO table, which is specified
                //      by JDBC as the sql data type code, not the concise code.
                //      That is why there is a sql datetime sub column
                //      specified as well.
                // return ValuePool.getInt(Types.SQL_TIME);         // fredt - was SQL_DATETIME
                return ValuePool.getInt(Types.SQL_DATETIME);

            case Types.SQL_TIMESTAMP :

                // NO:  This is the _concise_ code, whereas what we want to
                //      return here is the Data Type Code column value from
                //      Table 38 in the SQL CLI 200n FCD.  This method is used
                //      by DatabaseInformationMain to list the sql type
                //      column in the SYSTEM_TYPEINFO table, which is specified
                //      by JDBC as the sql data type code, not the concise code.
                //      That is why there is a sql datetime sub column
                //      specified as well.
                // return ValuePool.getInt(Types.SQL_TIMESTAMP);    // fredt - was SQL_DATETIME
                return ValuePool.getInt(Types.SQL_DATETIME);

            case Types.TINYINT :
                return ValuePool.getInt(Types.TINYINT);             // N/A

            case Types.SQL_VARBINARY :
                return ValuePool.getInt(Types.SQL_BLOB);            // SQL_BIT_VARYING

            case Types.SQL_VARCHAR :
                return ValuePool.getInt(Types.SQL_VARCHAR);         // SQL_VARCHAR

            case Types.VARCHAR_IGNORECASE :
                return ValuePool.getInt(Types.SQL_VARCHAR);         // SQL_VARCHAR

            case Types.SQL_NVARCHAR :
                return ValuePool.getInt(Types.SQL_NVARCHAR);

            case Types.SQL_XML :
                return ValuePool.getInt(Types.SQL_XML);             // SQLXML

            default :
                return null;
        }
    }

    /**
     * Retrieves the SQL CLI datetime subcode for the type. <p>
     *
     * @return the SQL CLI datetime subcode for the type
     */
    Integer getSqlDateTimeSub() {

        switch (type) {

            case Types.SQL_DATE :
                return ValuePool.INTEGER_1;

            case Types.SQL_TIME :
                return ValuePool.getInt(2);

            case Types.SQL_TIMESTAMP :
                return ValuePool.getInt(3);

            default :
                return null;
        }
    }

    /**
     * Retrieve the fully qualified name of the recommended standard Java
     * primitive, class or java.sql interface mapping for the type. <p>
     *
     * @return the fully qualified name of the recommended standard Java
     *    primitive, class or java.sql interface mapping for
     *    the type
     */
    String getStdMapClsName() {

        switch (type) {

            case Types.SQL_ARRAY :
                return "java.sql.Array";

            case Types.SQL_BIGINT :
                return "long";

            case Types.SQL_BINARY :
            case Types.SQL_LONGVARBINARY :
            case Types.SQL_VARBINARY :
                return "[B";

            case Types.SQL_BOOLEAN :
                return "boolean";

            case Types.SQL_BLOB :
                return "java.sql.Blob";

            case Types.SQL_CHAR :
            case Types.SQL_NCHAR :
            case Types.SQL_VARCHAR :
            case Types.SQL_NVARCHAR :
                return "java.lang.String";

            case Types.SQL_CLOB :
                return "java.sql.Clob";

            case Types.NCLOB :
                return "java.sql.NClob";

            case Types.SQL_DATALINK :
                return "java.net.URL";

            case Types.SQL_DATE :
                return "java.sql.Date";

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                return "java.math.BigDecimal";

            case Types.DISTINCT :
                return "java.sql.SQLData";

            case Types.JAVA_OBJECT :
            case Types.OTHER :
                return "java.lang.Object";

            case Types.SQL_XML :
                return "java.sql.SQLXML";

            case Types.SQL_REAL :
                return "float";

            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return "double";

            case Types.SQL_INTEGER :
                return "int";

            case Types.SQL_ALL_TYPES :
                return "java.lang.Void";

            case Types.SQL_REF :
                return "java.sql.Ref";

            case Types.SQL_SMALLINT :
                return "short";

            case Types.STRUCT :
                return "java.sql.Struct";

            case Types.SQL_TIME :
                return "java.sql.Time";

            case Types.SQL_TIMESTAMP :
                return "java.sql.Timestamp";

            case Types.TINYINT :
                return "byte";

            case Types.ROWID :
                return "java.sql.RowId";

            default :
                return null;
        }
    }

    /**
     * Retrieves the data type as an int. <p>
     *
     * @return the data type as an int
     */
    int getTypeCode() {
        return type;
    }

    /**
     * Retrieves the canonical data type name HSQLDB associates with
     * the type.  <p>
     *
     * This typically matches the designated JDBC name, with one or
     * two exceptions. <p>
     *
     * @return the canonical data type name HSQLDB associates with the type
     */
    String getTypeName() {

        return typeSub == Types.TYPE_SUB_IGNORECASE
               ? Types.getTypeName(Types.VARCHAR_IGNORECASE)
               : Types.getTypeName(type);
    }

    /**
     * Retrieves the HSQLDB data subtype as an int. <p>
     *
     * @return the HSQLDB data subtype as an int
     */
    int getTypeSub() {
        return this.typeSub;
    }

    /**
     * Retrieves whether the type can be an IDENTITY type. <p>
     *
     * @return whether the type can be an IDENTITY type.
     */
    Boolean isAutoIncrement() {

        switch (type) {

            case Types.SQL_DECIMAL :
            case Types.SQL_DOUBLE :
            case Types.SQL_FLOAT :
            case Types.SQL_NUMERIC :
            case Types.SQL_REAL :
            case Types.SQL_SMALLINT :
            case Types.TINYINT :
                return Boolean.FALSE;

            case Types.SQL_BIGINT :
            case Types.SQL_INTEGER :
                return Boolean.TRUE;

            default :
                return null;
        }
    }

    /**
     * Retrieves whether the type is case-sensitive in collations and
     * comparisons. <p>
     *
     * @return whether the type is case-sensitive in collations and
     *    comparisons.
     */
    Boolean isCaseSensitive() {

        return typeSub == Types.TYPE_SUB_IGNORECASE ? Boolean.FALSE
                                                    : Types.isCaseSensitive(
                                                    type);
    }

    /**
     * Retrieves whether, under the current release, class path and hosting
     * JVM, HSQLDB supports storing this type in table columns. <p>
     *
     * This value also typically represents whether HSQLDB supports retrieving
     * values of this type in the columns of ResultSets.
     * @return whether, under the current release, class path
     *    and hosting JVM, HSQLDB supports storing this
     *    type in table columns
     */
    Boolean isColStClsSupported() {

        return ValuePool.getBoolean(type == Types.SQL_ALL_TYPES ? true
                                                       : getColStClsName()
                                                       != null);
    }

    /**
     * Retrieves whether values of this type have a fixed precision and
     * scale. <p>
     *
     * @return whether values of this type have a fixed
     *    precision and scale.
     */
    Boolean isFixedPrecisionScale() {

        switch (type) {

            case Types.SQL_BIGINT :
            case Types.SQL_DECIMAL :
            case Types.SQL_DOUBLE :
            case Types.SQL_FLOAT :
            case Types.SQL_INTEGER :
            case Types.SQL_NUMERIC :
            case Types.SQL_REAL :
            case Types.SQL_SMALLINT :
            case Types.TINYINT :
                return Boolean.FALSE;

            default :
                return null;
        }
    }

    /**
     * Retrieve whether the fully qualified name reported by getStdMapClsName()
     * is implemented (although possibly not fully or at all supported) as a
     * ResultSet/CallableStatment getXXX/updateXXX or PreparedStatement
     * setXXX type under the current HSQLDB release, class path and hosting
     * JVM. <p>
     *
     * @return whether the fully qualified name reported by getStdMapClsName()
     * is supported as a JDBCResultSet.getXXX return type under the current
     * HSQLDB release, class path and hosting JVM.
     */
    Boolean isStdMapClsSupported() {

        // its ok to use Class.forName here instead of nameSpace.classForName,
        // because all standard map classes are loaded by the boot loader
        boolean isSup = false;

        switch (type) {

            case Types.SQL_ARRAY : {
                try {
                    Class.forName("java.sql.Array");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            case Types.SQL_BLOB : {
                try {
                    Class.forName("java.sql.Blob");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            case Types.SQL_CLOB : {
                try {
                    Class.forName("java.sql.Clob");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            case Types.NCLOB : {
                try {
                    Class.forName("java.sql.NClob");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            case Types.DISTINCT : {
                isSup = false;

                break;
            }
            case Types.REF : {
                try {
                    Class.forName("java.sql.Ref");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            case Types.STRUCT : {
                try {
                    Class.forName("java.sql.Struct");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            case Types.ROWID : {
                try {
                    Class.forName("java.sql.RowId");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            case Types.SQL_XML : {
                try {
                    Class.forName("java.sql.SQLXML");

                    isSup = true;
                } catch (Exception e) {
                    isSup = false;
                }

                break;
            }
            default : {
                isSup = (getStdMapClsName() != null);

                break;
            }
        }

        return ValuePool.getBoolean(isSup);
    }

    /**
     * Retrieves whether, under the current release, class path and
     * hosting JVM, HSQLDB supports passing or receiving this type as
     * the value of a procedure column. <p>
     *
     * @return whether, under the current release, class path and
     *    hosting JVM, HSQLDB supports passing or receiving
     *    this type as the value of a procedure column.
     */
    Boolean isSupportedAsPCol() {

        switch (type) {

            case Types.SQL_ALL_TYPES :           // - for void return type
            case Types.JAVA_OBJECT :    // - for Connection as first parm and

            //   Object for return type
            case Types.SQL_ARRAY :          // - for Object[] row of Trigger.fire()
                return Boolean.TRUE;

            default :
                return isSupportedAsTCol();
        }
    }

    /**
     * Retrieves whether, under the current release, class path and
     * hosting JVM, HSQLDB supports this as the type of a table
     * column. <p>
     *
     * @return whether, under the current release, class path
     *    and hosting JVM, HSQLDB supports this type
     *    as the values of a table column
     */
    Boolean isSupportedAsTCol() {
        return Type.isSupportedSQLType(type) ? Boolean.TRUE
                                             : Boolean.FALSE;
    }

    /**
     * Retrieves whether values of this type are unsigned. <p>
     *
     * @return whether values of this type are unsigned
     */
    Boolean isUnsignedAttribute() {
        return Types.isUnsignedAttribute(type);
    }

    /**
     * Assigns the Locale object used to retrieve this object's
     * resource bundle dependent values. <p>
     *
     * @param l the Locale object used to retrieve this object's resource
     *      bundle dependent values
     */
    void setLocale(Locale l) {

        if (l == null) {
            hnd_create_params = hnd_local_names = hnd_remarks = -1;
            locale_set        = false;

            return;
        }

        Locale oldLocale;

        synchronized (BundleHandler.class) {
            oldLocale = BundleHandler.getLocale();

            BundleHandler.setLocale(l);

            hnd_create_params =
                BundleHandler.getBundleHandle("data-type-create-parameters",
                                              null);
            hnd_local_names = BundleHandler.getBundleHandle("data-type-names",
                    null);
            hnd_remarks = BundleHandler.getBundleHandle("data-type-remarks",
                    null);

            BundleHandler.setLocale(oldLocale);

            locale_set = true;
        }
    }

    /**
     * Assigns the SQL data type code on which this object is to report. <p>
     *
     * @param type the SQL data type code on which this object is to report
     */
    void setTypeCode(int type) {
        this.type = type;
    }

    /**
     * Assigns the HSQLDB data subtype code on which this object is
     * to report. <p>
     *
     * @param typeSub the HSQLDB data subtype code on which this object
     *      is to report
     */
    void setTypeSub(int typeSub) {
        this.typeSub = typeSub;
    }
}
