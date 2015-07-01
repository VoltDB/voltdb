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

import java.io.Serializable;
import java.lang.reflect.Method;

import org.hsqldb_voltpatches.StatementDMQL;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.resources.BundleHandler;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.Type;

/* $Id: DIProcedureInfo.java 3001 2009-06-04 12:31:11Z fredt $ */

// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
// Revision 1.7  2006/07/12 11:27:53  boucherb
// patch 1.9.0
// - JDBC 4.0,  Mustang b87: spec. changed to unquoted values for IS_NULLABLE procedure and function column metadata

/**
 * Provides information about HSQLDB SQL-invoked routines and SQL functions. <p>
 *
 * In particular, this class provides information about Java Methods in a form
 * compatible with presentation via the related HSQLDB system tables,
 * SYSTEM_PROCEDURES and SYSTEM_PROCEDURECOLUMNS, involved in the production of
 * the DatabaseMetaData getProcedures and getProcedureColumns result sets.
 *
 * @author  boucherb@users
 * @version 1.9.0
 * @since 1.7.2
 */
final class DIProcedureInfo {

    // java.sql dependencies mostly removed
    static final String   conClsName               = "java.sql.Connection";
    static final int      procedureResultUnknown   = 0;
    static final int      procedureNoResult        = 1;
    static final int      procedureReturnsResult   = 2;
    static final int      procedureColumnUnknown   = 0;
    static final int      procedureColumnIn        = 1;
    static final int      procedureColumnInOut     = 2;
    static final int      procedureColumnResult    = 3;
    static final int      procedureColumnOut       = 4;
    static final int      procedureColumnReturn    = 5;
    static final int      procedureNoNulls         = 0;
    static final int      procedureNullable        = 1;
    static final int      procedureNullableUnknown = 2;
    private Class         clazz;
    private Class[]       colClasses;
    private int[]         colTypes;
    private int           colOffset;
    private int           colCount;
    private boolean       colsResolved;
    private String        fqn;
    private String        specificName;
    private int           hnd_remarks;
    private Method        method;
    private String        sig;
    private DINameSpace   nameSpace;
    private final HashMap typeMap = new HashMap();

    public DIProcedureInfo(DINameSpace ns) {
        setNameSpace(ns);
    }

    private int colOffset() {

        if (!colsResolved) {
            resolveCols();
        }

        return colOffset;
    }

/*
    HsqlArrayList getAliases() {
        return (HsqlArrayList) nameSpace.getInverseAliasMap().get(getFQN());
    }
*/
    Class getColClass(int i) {
        return colClasses[i + colOffset()];
    }

    int getColCount() {

        if (!colsResolved) {
            resolveCols();
        }

        return colCount;
    }

    Integer getColJDBCDataType(int i) {
        return ValuePool.getInt(Type.getJDBCTypeCode(getColTypeCode(i)));
    }

    Integer getColLen(int i) {

        int size;
        int type;

        type = getColTypeCode(i);

        switch (type) {

            case Types.SQL_BINARY :
            case Types.SQL_LONGVARBINARY :
            case Types.SQL_VARBINARY : {
                size = Integer.MAX_VALUE;

                break;
            }
            case Types.SQL_BIGINT :
            case Types.SQL_DOUBLE :
            case Types.SQL_DATE :
            case Types.SQL_FLOAT :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME : {
                size = 8;

                break;
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP : {
                size = 12;

                break;
            }
            case Types.SQL_REAL :
            case Types.SQL_INTEGER : {
                size = 4;

                break;
            }
            case Types.SQL_SMALLINT : {
                size = 2;

                break;
            }
            case Types.TINYINT :
            case Types.SQL_BOOLEAN : {
                size = 1;

                break;
            }
            default :
                size = 0;
        }

        return (size == 0) ? null
                           : ValuePool.getInt(size);
    }

    String getColName(int i) {
        return StatementDMQL.PCOL_PREFIX + (i + colOffset());
    }

    Integer getColNullability(int i) {

        int cn;

        cn = getColClass(i).isPrimitive() ? procedureNoNulls
                                          : procedureNullable;

        return ValuePool.getInt(cn);
    }

    // JDBC 4.0
    // - revised Mustang b87 : no quotes
    String getColIsNullable(int i) {
        return getColClass(i).isPrimitive() ? "NO"
                                            : "YES";
    }

    String getColRemark(int i) {

        String       key;
        StringBuffer sb;

        sb  = new StringBuffer(getSignature());
        key = sb.append('@').append(i + colOffset()).toString();

        return BundleHandler.getString(hnd_remarks, key);
    }

    // JDBC sort-contract:
    // out return value column, then in/in out/out parameter columns
    // in formal order, then result columns in column order
    //
    // Currently, we materialize the java method return value, if
    // any, as a result column, not as an OUT return value column, so
    // it should actually appear _after_ the other procedure columns
    // in the row order returned by the JDBC getProcedureColumns() method
    int getColSequence(int i) {

        // colOffset has the side-effect of setting colCount properly
        return (i + colOffset() == 0) ? colCount
                                      : i;
    }

    // JDBC 4.0
    // The ordinal position, starting from 1, for the
    // input and output parameters for a procedure.
    // A value of 0 is returned if this row describes
    // the procedure's return value.
    Integer getColOrdinalPosition(int i) {
        return ValuePool.getInt(colOffset() == 0 ? i
                                                 : i + 1);
    }

    int getColTypeCode(int i) {

        i += colOffset();

        return colTypes[i];
    }

    Integer getColUsage(int i) {

        switch (i + colOffset()) {

            case 0 : {

                // Currently, we materialize the java method return value, if
                // any, as a result column, not as an OUT return column
                return ValuePool.getInt(procedureColumnResult);
            }

            /**
             * @todo - registration and reporting on result columns for routines
             *      that generate real" result sets
             */
            default : {

                // We could get religious here and maybe report IN OUT
                // for newRow of before update for each row trigger methods,
                // but there's not really any added value
                return ValuePool.getInt(procedureColumnIn);
            }
        }
    }

    Class getDeclaringClass() {
        return this.clazz;
    }

    String getFQN() {

        StringBuffer sb;

        if (fqn == null) {
            sb = new StringBuffer();
            fqn = sb.append(clazz.getName()).append('.').append(
                method.getName()).toString();
        }

        return fqn;
    }

    String getSpecificName() {

        if (specificName == null) {
            specificName = clazz.getName() + "." + getSignature();
        }

        return specificName;
    }

    Integer getInputParmCount() {
        return ValuePool.getInt(method.getParameterTypes().length);
    }

    Method getMethod() {
        return this.method;
    }

    String getOrigin(String srcType) {
        return (nameSpace.isBuiltin(clazz) ? "BUILTIN "
                                           : "USER DEFINED ") + srcType;
    }

    Integer getOutputParmCount() {

        // no support for IN OUT or OUT columns yet
        return ValuePool.INTEGER_0;
    }

    String getRemark() {
        return BundleHandler.getString(hnd_remarks, getSignature());
    }

    Integer getResultSetCount() {
        return (method.getReturnType() == Void.TYPE) ? ValuePool.INTEGER_0
                                                     : ValuePool.INTEGER_1;
    }

    Integer getResultType(String origin) {

        int type;

        type = !"ROUTINE".equals(origin) ? procedureResultUnknown
                                         : method.getReturnType() == Void.TYPE
                                           ? procedureNoResult
                                           : procedureReturnsResult;

        return ValuePool.getInt(type);
    }

    String getSignature() {

        if (sig == null) {
            sig = DINameSpace.getSignature(method);
        }

        return sig;
    }

    /**
     * Retrieves the specific name of the given Method object. <p>
     *
     * @param m The Method object for which to retreive the specific name
     * @return the specific name of the specified Method object.
     */
    static String getMethodSpecificName(Method m) {

        return m == null ? null
                         : m.getDeclaringClass().getName() + '.'
                           + DINameSpace.getSignature(m);
    }

    DINameSpace getNameSpace() {
        return nameSpace;
    }

    void setNameSpace(DINameSpace ns) {

        nameSpace = ns;

        Class   c;
        Integer type;

        // can only speed up test significantly for java.lang.Object,
        // final classes, primitive types and hierachy parents.
        // Must still check later if assignable from candidate classes, where
        // hierarchy parent is not final.
        //ARRAY
        try {
            c = nameSpace.classForName("org.hsqldb_voltpatches.jdbc.JDBCArray");

            typeMap.put(c, ValuePool.getInt(Types.SQL_ARRAY));
        } catch (Exception e) {}

        // BIGINT
        type = ValuePool.getInt(Types.SQL_BIGINT);

        typeMap.put(Long.TYPE, type);
        typeMap.put(Long.class, type);

        // BOOLEAN
        type = ValuePool.getInt(Types.SQL_BOOLEAN);

        typeMap.put(Boolean.TYPE, type);
        typeMap.put(Boolean.class, type);

        // BLOB
        type = ValuePool.getInt(Types.SQL_BLOB);

        try {
            c = nameSpace.classForName("org.hsqldb_voltpatches.jdbc.JDBCBlob");

            typeMap.put(c, type);
        } catch (Exception e) {}

        // CHAR
        type = ValuePool.getInt(Types.SQL_CHAR);

        typeMap.put(Character.TYPE, type);
        typeMap.put(Character.class, type);
        typeMap.put(Character[].class, type);
        typeMap.put(char[].class, type);

        // CLOB
        type = ValuePool.getInt(Types.SQL_CLOB);

        try {
            c = nameSpace.classForName("org.hsqldb_voltpatches.jdbc.JDBCClob");

            typeMap.put(c, type);
        } catch (Exception e) {}

        // DATALINK
        type = ValuePool.getInt(Types.SQL_DATALINK);

        typeMap.put(java.net.URL.class, type);

        // DATE
        type = ValuePool.getInt(Types.SQL_DATE);

        typeMap.put(java.util.Date.class, type);
        typeMap.put(java.sql.Date.class, type);

        // DECIMAL
        type = ValuePool.getInt(Types.SQL_DECIMAL);

        try {
            c = nameSpace.classForName("java.math.BigDecimal");

            typeMap.put(c, type);
        } catch (Exception e) {}

        // DISTINCT
        try {
            c = nameSpace.classForName("org.hsqldb_voltpatches.jdbc.JDBCDistinct");

            typeMap.put(c, ValuePool.getInt(Types.DISTINCT));
        } catch (Exception e) {}

        // DOUBLE
        type = ValuePool.getInt(Types.SQL_DOUBLE);

        typeMap.put(Double.TYPE, type);
        typeMap.put(Double.class, type);

        // FLOAT : Not actually a legal IN parameter type yet
        type = ValuePool.getInt(Types.SQL_FLOAT);

        typeMap.put(Float.TYPE, type);
        typeMap.put(Float.class, type);

        // INTEGER
        type = ValuePool.getInt(Types.SQL_INTEGER);

        typeMap.put(Integer.TYPE, type);
        typeMap.put(Integer.class, type);

        // JAVA_OBJECT
        type = ValuePool.getInt(Types.JAVA_OBJECT);

        typeMap.put(Object.class, type);

        // VARBINARY
        type = ValuePool.getInt(Types.SQL_VARBINARY);

        typeMap.put(byte[].class, type);
        typeMap.put(BinaryData.class, type);

        // VARCHAR
        type = ValuePool.getInt(Types.SQL_VARCHAR);

        typeMap.put(String.class, type);

        // NULL
        type = ValuePool.getInt(Types.SQL_ALL_TYPES);

        typeMap.put(Void.TYPE, type);
        typeMap.put(Void.class, type);

        // REF
        type = ValuePool.getInt(Types.SQL_REF);

        try {
            c = nameSpace.classForName("org.hsqldb_voltpatches.jdbc.JDBCRef");

            typeMap.put(c, type);
        } catch (Exception e) {}

        // SMALLINT : Not actually a legal IN parameter type yet
        type = ValuePool.getInt(Types.SQL_SMALLINT);

        typeMap.put(Short.TYPE, type);
        typeMap.put(Short.class, type);

        // STRUCT :
        type = ValuePool.getInt(Types.STRUCT);

        try {
            c = nameSpace.classForName("org.hsqldb_voltpatches.jdbc.JDBCStruct");

            typeMap.put(c, type);
        } catch (Exception e) {}

        // TIME
        type = ValuePool.getInt(Types.SQL_TIME);

        typeMap.put(java.sql.Time.class, type);

        // TIMESTAMP
        type = ValuePool.getInt(Types.SQL_TIMESTAMP);

        typeMap.put(java.sql.Timestamp.class, type);

        // TINYINT : Not actually a legal IN parameter type yet
        type = ValuePool.getInt(Types.TINYINT);

        typeMap.put(Byte.TYPE, type);
        typeMap.put(Byte.class, type);

        // XML : Not actually a legal IN parameter type yet
        type = ValuePool.getInt(Types.SQL_XML);

        try {
            c = nameSpace.classForName("org.w3c.dom.Document");

            typeMap.put(c, type);

            c = nameSpace.classForName("org.w3c.dom.DocumentFragment");

            typeMap.put(c, type);
        } catch (Exception e) {}
    }

    private void resolveCols() {

        Class   rType;
        Class[] pTypes;
        Class   clazz;
        int     ptlen;
        int     pclen;
        boolean isFPCON;

        rType         = method.getReturnType();
        pTypes        = method.getParameterTypes();
        ptlen         = pTypes.length;
        isFPCON       = ptlen > 0 && pTypes[0].getName().equals(conClsName);
        pclen         = 1 + ptlen - (isFPCON ? 1
                                             : 0);
        colClasses    = new Class[pclen];
        colTypes      = new int[pclen];
        colClasses[0] = rType;
        colTypes[0]   = typeForClass(rType);

        for (int i = isFPCON ? 1
                             : 0, idx = 1; i < ptlen; i++, idx++) {
            clazz           = pTypes[i];
            colClasses[idx] = clazz;
            colTypes[idx]   = typeForClass(clazz);
        }

        colOffset = rType == Void.TYPE ? 1
                                       : 0;
        colCount  = pclen - colOffset;
    }

    /**
     * This requires the following properties files:
     *
     * org_hsqldb_Library.properties
     * java_math.properties
     */
    void setMethod(Method m) {

        String remarkKey;

        method       = m;
        clazz        = method.getDeclaringClass();
        fqn          = null;
        specificName = null;
        sig          = null;
        colsResolved = false;
        remarkKey    = clazz.getName().replace('.', '_');
        hnd_remarks  = BundleHandler.getBundleHandle(remarkKey, null);
    }

    int typeForClass(Class c) {

        Class   to;
        Integer type = (Integer) typeMap.get(c);

        if (type != null) {
            return type.intValue();
        }

        // ARRAY (dimension 1)
        // HSQLDB does not yet support ARRAY for SQL, but
        // Trigger.fire takes Object[] row, which we report.
        // Also, it's just friendly to show what "would"
        // be required if/when we support ARRAY in a broader
        // sense
        if (c.isArray() && !c.getComponentType().isArray()) {
            return Types.SQL_ARRAY;
        }

        try {
            to = Class.forName("java.sql.Array");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_ARRAY;
            }
        } catch (Exception e) {}

        // NUMERIC
        // All java.lang.Number impls and BigDecimal have
        // already been covered by lookup in typeMap.
        // They are all final, so this is OK.
        if (Number.class.isAssignableFrom(c)) {
            return Types.SQL_NUMERIC;
        }

        // TIMESTAMP
        try {
            to = Class.forName("java.sql.Timestamp");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_TIMESTAMP;
            }
        } catch (Exception e) {}

        // TIME
        try {
            to = Class.forName("java.sql.Time");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_TIME;
            }
        } catch (Exception e) {}

        // DATE
        try {
            to = Class.forName("java.sql.Date");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_DATE;
            }
        } catch (Exception e) {}

        // BLOB
        try {
            to = Class.forName("java.sql.Blob");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_BLOB;
            }
        } catch (Exception e) {}

        // CLOB
        try {
            to = Class.forName("java.sql.Clob");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_CLOB;
            }
        } catch (Exception e) {}

        // REF
        try {
            to = Class.forName("java.sql.Ref");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_REF;
            }
        } catch (Exception e) {}

        // STRUCT
        try {
            to = Class.forName("java.sql.Struct");

            if (to.isAssignableFrom(c)) {
                return Types.STRUCT;
            }
        } catch (Exception e) {}

        // VARBINARY : org.hsqldb_voltpatches.Binary is not final
        if (BinaryData.class.isAssignableFrom(c)) {
            return Types.SQL_VARBINARY;
        }

        // VARCHAR : really OTHER at this point
        try {

            // @since JDK1.4
            to = Class.forName("java.lang.CharSequence");

            if (to.isAssignableFrom(c)) {
                return Types.SQL_VARCHAR;
            }
        } catch (Exception e) {}

        // we have no standard mapping for the specified class
        // at this point...is it even storable?
        if (Serializable.class.isAssignableFrom(c)) {

            // Yes: it is storable, as an OTHER.
            return Types.OTHER;
        }

        // It may (in future, say using bean contract) or may not be storable
        // (by HSQLDB)...
        // but then it may be possible to pass to an in-process routine,
        // so be lenient and just return the most generic type.
        return Types.JAVA_OBJECT;
    }
}
