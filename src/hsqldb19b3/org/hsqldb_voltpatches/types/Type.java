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


package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.SortAndSlice;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.IntKeyHashMap;
import org.hsqldb_voltpatches.lib.IntValueHashMap;
import org.hsqldb_voltpatches.lib.ObjectComparator;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;

/**
 * Base class for type objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public abstract class Type implements SchemaObject, Cloneable {

    public static final Type[] emptyArray = new Type[]{};

    //
    public final int        typeComparisonGroup;
    public final int        typeDataGroup;
    public final int        typeCode;
    public final long       precision;
    public final int        scale;
    public UserTypeModifier userTypeModifier;

    //
    Type(int typeGroup, int type, long precision, int scale) {

        this.typeComparisonGroup = typeGroup;
        this.typeCode            = type;
        this.precision           = precision;
        this.scale               = scale;
        this.typeDataGroup = typeCode == Types.SQL_CHAR ? Types.SQL_VARCHAR
                                                        : typeCode;
    }

    // interface specific methods
    public final int getType() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getType();
    }

    public final HsqlName getName() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getName();
    }

    public final HsqlName getCatalogName() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getSchemaName().schema;
    }

    public final HsqlName getSchemaName() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getSchemaName();
    }

    public final Grantee getOwner() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getOwner();
    }

    public final OrderedHashSet getReferences() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getReferences();
    }

    public final OrderedHashSet getComponents() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getComponents();
    }

    public final void compile(Session session, SchemaObject parentObject) {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        userTypeModifier.compile(session);
    }

    /**
     *  Retrieves the SQL character sequence required to (re)create the
     *  trigger, as a StringBuffer
     *
     * @return the SQL character sequence required to (re)create the
     *  trigger
     */
    public String getSQL() {

        if (userTypeModifier == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }

        return userTypeModifier.getSQL();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    public Type duplicate() {

        try {
            return (Type) super.clone();
        } catch (CloneNotSupportedException e) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }
    }

    public abstract int displaySize();

    /**
     * Returns the JDBC type number of type, if it exists,
     * otherwise the HSQLDB / SQL type.
     */
    public abstract int getJDBCTypeCode();

    /**
     * Returns the JDBC class name of type, if it exists,
     * otherwise the HSQLDB class name.
     */
    public abstract String getJDBCClassName();

    public abstract Class getJDBCClass();

    public int getJDBCScale() {
        return scale;
    }

    public int getJDBCPrecision() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    /**
     * Returns the generic SQL CLI type number of type, if it exists,
     * otherwise the HSQLDB type. The generic type is returned for DATETIME
     * and INTERVAL types.
     */
    public int getSQLGenericTypeCode() {
        return typeCode;
    }

    /**
     * Returns the name of the type
     */
    public abstract String getNameString();

    /**
     * Returns the name of the type
     */
    public String getFullNameString() {
        return getNameString();
    }

    /**
     * Returns the full definition of the type, including parameters
     */
    public abstract String getDefinition();

    public Collation getCollation() {
        return null;
    }

    public Charset getCharacterSet() {
        return null;
    }

    public final String getTypeDefinition() {

        if (userTypeModifier == null) {
            return getDefinition();
        }

        return getName().getSchemaQualifiedStatementName();
    }

    public abstract int compare(Session session, Object a, Object b);

    public int compare(Session session, Object a, Object b, int opType) {

        if (a == b) {
            return 0;
        }

        return compare(session, a, b);
    }

    public int compare(Session session, Object a, Object b,
                       SortAndSlice sort) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return sort.sortNullsLast[0] ? 1
                                         : -1;
        }

        if (b == null) {
            return sort.sortNullsLast[0] ? -1
                                         : 1;
        }

        int result = compare(session, a, b);

        return sort.sortDescending[0] ? -result
                                      : result;
    }

    public abstract Object convertToTypeLimits(SessionInterface session,
            Object a);

    /**
     * Explicit casts are handled by this method.
     * SQL standard 6.12 rules for enforcement of size, precision and scale
     * are implemented. For CHARACTER values, it performs truncation in all
     * cases of long strings.
     */
    public Object castToType(SessionInterface session, Object a, Type type) {
        return convertToType(session, a, type);
    }

    /**
     * Same as castToType except for CHARACTER values. Perform string
     * truncation of trailing spaces only. For other long strings, it raises
     * an exception.
     */
    public abstract Object convertToType(SessionInterface session, Object a,
                                         Type type);

    /**
     * Convert type for JDBC reads. Same as convertToType, but supports non-standard
     * SQL conversions supported by JDBC
     */
    public Object convertToTypeJDBC(SessionInterface session, Object a,
                                    Type otherType) {

        if (otherType.isLobType()) {
            throw Error.error(ErrorCode.X_42561);
        }

        return convertToType(session, a, otherType);
    }

    public Object convertJavaToSQL(SessionInterface session, Object a) {
        return a;
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {
        return a;
    }

    /**
     * Converts the object to the given type without limit checks. Used for JDBC writes.
     */
    public abstract Object convertToDefaultType(
        SessionInterface sessionInterface, Object o);

    public abstract String convertToString(Object a);

    public abstract String convertToSQLString(Object a);

    public abstract boolean canConvertFrom(Type otherType);

    /**
     * Can convert without changing the object
     * @return 0 always, 1 range check required, -1 never
     */
    public int canMoveFrom(Type otherType) {

        if (otherType == this) {
            return 0;
        }

        return -1;
    }

    public boolean canBeAssignedFrom(Type otherType) {

        if (otherType == null) {
            return true;
        }

        return otherType.typeCode == Types.SQL_ALL_TYPES
               || typeComparisonGroup == otherType.typeComparisonGroup;
    }

    public int arrayLimitCardinality() {
        return 0;
    }

    public Type collectionBaseType() {
        return null;
    }

    public boolean isArrayType() {
        return false;
    }

    public boolean isMultisetType() {
        return false;
    }

    public boolean isRowType() {
        return false;
    }

    public boolean isStructuredType() {
        return false;
    }

    public boolean isCharacterType() {
        return false;
    }

    public boolean isNumberType() {
        return false;
    }

    public boolean isIntegralType() {
        return false;
    }

    public boolean isExactNumberType() {
        return false;
    }

    public boolean isDecimalType() {
        return false;
    }

    public boolean isDateTimeType() {
        return false;
    }

    public boolean isDateTimeTypeWithZone() {
        return false;
    }

    public boolean isDateOrTimestampType() {
        return false;
    }

    public boolean isIntervalType() {
        return false;
    }

    public boolean isBinaryType() {
        return false;
    }

    public boolean isBooleanType() {
        return false;
    }

    public boolean isLobType() {
        return false;
    }

    public boolean isBitType() {
        return false;
    }

    public boolean isObjectType() {
        return false;
    }

    public boolean isDistinctType() {

        return userTypeModifier == null ? false
                                        : userTypeModifier.schemaObjectType
                                          == SchemaObject.TYPE;
    }

    public boolean isDomainType() {

        return userTypeModifier == null ? false
                                        : userTypeModifier.schemaObjectType
                                          == SchemaObject.DOMAIN;
    }

    public boolean acceptsPrecision() {
        return false;
    }

    public boolean requiresPrecision() {
        return false;
    }

    public long getMaxPrecision() {
        return 0;
    }

    public int getMaxScale() {
        return 0;
    }

    public int getPrecisionRadix() {
        return 0;
    }

    public boolean acceptsFractionalPrecision() {
        return false;
    }

    public boolean acceptsScale() {
        return false;
    }

    public int precedenceDegree(Type other) {

        if (other.typeCode == typeCode) {
            if (typeCode == Types.SQL_ARRAY) {
                return collectionBaseType().precedenceDegree(
                    other.collectionBaseType());
            }

            return 0;
        }

        return Integer.MIN_VALUE;
    }

    /**
     * Common type used in comparison opertions. other must be comparable
     * with this.
     */
    public abstract Type getAggregateType(Type other);

    /**
     * Result type of combining values of two types in different opertions.
     * other type is not allways comparable with this, but a operation should
     * be valid without any explicit CAST
     */
    public abstract Type getCombinedType(Session session, Type other,
                                         int operation);

    public int compareToTypeRange(Object o) {
        return 0;
    }

    /**
     * All arithmetic ops are called on the pre-determined Type object of the result
     */
    public Object absolute(Object a) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Type");
    }

    public Object negate(Object a) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Type");
    }

    public Object add(Session session, Object a, Object b, Type otherType) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Type");
    }

    public Object subtract(Session session, Object a, Object b,
                           Type otherType) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Type");
    }

    public Object multiply(Object a, Object b) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Type");
    }

    public Object divide(Session session, Object a, Object b) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Type");
    }

    public Object concat(Session session, Object a, Object b) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Type");
    }

    public int cardinality(Session session, Object a) {
        return 0;
    }

    int hashCode(Object a) {

        if (a == null) {
            return 0;
        }

        return a.hashCode();
    }

    public boolean equals(Object other) {

        if (other == this) {
            return true;
        }

        if (other instanceof Type) {
            if (((Type) other).typeCode == Types.SQL_ARRAY) {
                return false;
            }

            if (((Type) other).typeCode == Types.SQL_ROW) {
                return false;
            }

            return ((Type) other).typeCode == typeCode
                   && ((Type) other).precision == precision
                   && ((Type) other).scale == scale
                   && ((Type) other).userTypeModifier == userTypeModifier;
        }

        return false;
    }

    public int hashCode() {
        return typeCode + (int) precision << 8 + scale << 16;
    }

    public static TypedComparator newComparator(Session session) {
        return new TypedComparator(session);
    }

    public static class TypedComparator implements ObjectComparator {

        Session      session;
        Type         type;
        SortAndSlice sort;

        TypedComparator(Session session) {
            this.session = session;
        }

        public int compare(Object a, Object b) {
            return type.compare(session, a, b, sort);
        }

        public int hashCode(Object a) {
            return type.hashCode(a);
        }

        public long longKey(Object a) {
            return 0;
        }

        public void setType(Type type, SortAndSlice sort) {
            this.type = type;
            this.sort = sort;
        }
    }

    /** @todo 1.9.0 - review all - need max implementation defined lengths, used for parameters */

    // null type
    public static final Type SQL_ALL_TYPES = NullType.getNullType();

    // character types
    public static final CharacterType SQL_CHAR =
        new CharacterType(Types.SQL_CHAR, 1);
    public static final CharacterType SQL_CHAR_16 =
        new CharacterType(Types.SQL_CHAR, 16);
    public static final CharacterType SQL_CHAR_DEFAULT =
        new CharacterType(Types.SQL_CHAR, CharacterType.defaultCharPrecision);
    public static final CharacterType SQL_VARCHAR =
        new CharacterType(Types.SQL_VARCHAR, 0);
    public static final CharacterType SQL_VARCHAR_DEFAULT =
        new CharacterType(Types.SQL_VARCHAR,
                          CharacterType.defaultVarcharPrecision);
    public static final ClobType SQL_CLOB =
        new ClobType(ClobType.defaultClobSize);

    // binary types
    public static final BitType SQL_BIT = new BitType(Types.SQL_BIT, 1);
    public static final BitType SQL_BIT_VARYING =
        new BitType(Types.SQL_BIT_VARYING, 1);
    public static final BitType SQL_BIT_VARYING_MAX_LENGTH =
        new BitType(Types.SQL_BIT_VARYING, BitType.maxBitPrecision);

    // binary types
    public static final BinaryType SQL_BINARY =
        new BinaryType(Types.SQL_BINARY, 1);
    public static final BinaryType SQL_BINARY_16 =
        new BinaryType(Types.SQL_BINARY, 16);
    public static final BinaryType SQL_BINARY_DEFAULT =
        new BinaryType(Types.SQL_BINARY, 32 * 1024);
    public static final BinaryType SQL_VARBINARY =
        new BinaryType(Types.SQL_VARBINARY, 0);
    public static final BinaryType SQL_VARBINARY_DEFAULT =
        new BinaryType(Types.SQL_VARBINARY, 32 * 1024);
    public static final BlobType SQL_BLOB =
        new BlobType(BlobType.defaultBlobSize);

    // other type
    public static final OtherType OTHER = OtherType.getOtherType();

    // boolean type
    public static final BooleanType SQL_BOOLEAN = BooleanType.getBooleanType();

    // number types
    public static final NumberType SQL_NUMERIC =
        new NumberType(Types.SQL_NUMERIC, NumberType.defaultNumericPrecision,
                       0);
    public static final NumberType SQL_DECIMAL =
        new NumberType(Types.SQL_DECIMAL, NumberType.defaultNumericPrecision,
        // A VoltDB extension MAY BE NEEDED to change this value to 7 which was labeled
        // as a cherry picked code change from hsqldb-2.2.8 -- what happened to it?
        // End of VoltDB extension
                       0);
    public static final NumberType SQL_DECIMAL_DEFAULT =
        new NumberType(Types.SQL_DECIMAL, NumberType.defaultNumericPrecision,
                       NumberType.defaultNumericScale);
    public static final NumberType SQL_DECIMAL_BIGINT_SQR =
        // A VoltDB extension to disable use of giant types in sums
        new NumberType(Types.SQL_BIGINT, NumberType.bigintPrecision, 8);
        /* disable 2 lines ...
        new NumberType(Types.SQL_DECIMAL,
                       NumberType.bigintSquareNumericPrecision, 0);
        ... disabled 2 lines */
        // End of VoltDB extension
    public static final NumberType SQL_DOUBLE =
        // A VoltDB extension -- mysterious
        new NumberType(Types.SQL_DOUBLE, NumberType.doublePrecision, 8);
        /* disable 1 line ...
        new NumberType(Types.SQL_DOUBLE, 0, 0);
        ... disabled 1 line */
        // End of VoltDB extension

    //
    public static final NumberType TINYINT = new NumberType(Types.TINYINT,
        // A VoltDB extension -- mysterious
        NumberType.tinyintPrecision, 1);
        /* disable 1 line ...
        NumberType.tinyintPrecision, 0);
        ... disabled 1 line */
        // End of VoltDB extension
    public static final NumberType SQL_SMALLINT =
        // A VoltDB extension -- mysterious
        new NumberType(Types.SQL_SMALLINT, NumberType.smallintPrecision, 2);
        /* disable 1 line ...
        new NumberType(Types.SQL_SMALLINT, NumberType.smallintPrecision, 0);
        ... disabled 1 line */
        // End of VoltDB extension
    public static final NumberType SQL_INTEGER =
        // A VoltDB extension -- mysterious
        new NumberType(Types.SQL_INTEGER, NumberType.integerPrecision, 4);
        /* disable 1 line ...
        new NumberType(Types.SQL_INTEGER, NumberType.integerPrecision, 0);
        ... disabled 1 line */
        // End of VoltDB extension
    public static final NumberType SQL_BIGINT =
        // A VoltDB extension -- mysterious
        new NumberType(Types.SQL_BIGINT, NumberType.bigintPrecision, 8);
        /* disable 1 line ...
        new NumberType(Types.SQL_BIGINT, NumberType.bigintPrecision, 0);
        ... disabled 1 line */
        // End of VoltDB extension

    // date time
    public static final DateTimeType SQL_DATE =
        new DateTimeType(Types.SQL_TIMESTAMP, Types.SQL_DATE, 0);
    public static final DateTimeType SQL_TIME =
        new DateTimeType(Types.SQL_TIME, Types.SQL_TIME,
                         DTIType.defaultTimeFractionPrecision);
    public static final DateTimeType SQL_TIME_WITH_TIME_ZONE =
        new DateTimeType(Types.SQL_TIME, Types.SQL_TIME_WITH_TIME_ZONE,
                         DTIType.defaultTimeFractionPrecision);
    public static final DateTimeType SQL_TIMESTAMP =
        new DateTimeType(Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP,
                         DTIType.defaultTimestampFractionPrecision);
    public static final DateTimeType SQL_TIMESTAMP_WITH_TIME_ZONE =
        new DateTimeType(Types.SQL_TIMESTAMP,
                         Types.SQL_TIMESTAMP_WITH_TIME_ZONE,
                         DTIType.defaultTimestampFractionPrecision);
    public static final DateTimeType SQL_TIMESTAMP_NO_FRACTION =
        new DateTimeType(Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, 0);

    // interval
    public static final IntervalType SQL_INTERVAL_YEAR =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_YEAR,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_MONTH =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_MONTH,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_HOUR =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_HOUR,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_MINUTE =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_MINUTE,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_SECOND =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_SECOND,
                                     DTIType.defaultIntervalPrecision,
                                     DTIType.defaultIntervalFractionPrecision);
    public static final IntervalType SQL_INTERVAL_SECOND_MAX_FRACTION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_SECOND,
                                     DTIType.defaultIntervalPrecision,
                                     DTIType.maxFractionPrecision);
    public static final IntervalType SQL_INTERVAL_YEAR_TO_MONTH =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_TO_HOUR =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY_TO_HOUR,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_TO_MINUTE =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY_TO_MINUTE,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_TO_SECOND =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                     DTIType.defaultIntervalPrecision,
                                     DTIType.defaultIntervalFractionPrecision);
    public static final IntervalType SQL_INTERVAL_HOUR_TO_MINUTE =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_HOUR_TO_MINUTE,
                                     DTIType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_HOUR_TO_SECOND =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_HOUR_TO_SECOND,
                                     DTIType.defaultIntervalPrecision,
                                     DTIType.defaultIntervalFractionPrecision);
    public static final IntervalType SQL_INTERVAL_MINUTE_TO_SECOND =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_MINUTE_TO_SECOND,
                                     DTIType.defaultIntervalPrecision,
                                     DTIType.defaultIntervalFractionPrecision);

    //
    public static final IntervalType SQL_INTERVAL_YEAR_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_YEAR,
                                     DTIType.maxIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_MONTH_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_MONTH,
                                     DTIType.maxIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY,
                                     DTIType.maxIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_HOUR_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_HOUR,
                                     DTIType.maxIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_MINUTE_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_MINUTE,
                                     DTIType.maxIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_SECOND_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_SECOND,
                                     DTIType.maxIntervalPrecision,
                                     DTIType.defaultIntervalFractionPrecision);
    public static final IntervalType SQL_INTERVAL_SECOND_MAX_FRACTION_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_SECOND,
                                     DTIType.maxIntervalPrecision,
                                     DTIType.maxFractionPrecision);

    //
    public static final IntervalType SQL_INTERVAL_YEAR_TO_MONTH_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                                     DTIType.maxIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_TO_SECOND_MAX_PRECISION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                     DTIType.maxIntervalPrecision,
                                     DTIType.maxFractionPrecision);

    //
    public static final ArrayType SQL_ARRAY_ALL_TYPES =
        new ArrayType(SQL_ALL_TYPES, 0);

    public static ArrayType getDefaultArrayType(int type) {
        return new ArrayType(getDefaultType(type),
                             ArrayType.defaultArrayCardinality);
    }

    public static Type getDefaultType(int type) {

        try {
            return getType(type, Type.SQL_VARCHAR.getCharacterSet(),
                           Type.SQL_VARCHAR.getCollation(), 0, 0);
        } catch (Exception e) {
            return null;
        }
    }

    public static Type getDefaultTypeWithSize(int type) {

        switch (type) {

            case Types.SQL_ALL_TYPES :
                return SQL_ALL_TYPES;

            case Types.SQL_ARRAY :
                return SQL_ARRAY_ALL_TYPES;

            case Types.SQL_CHAR :
                return SQL_CHAR_DEFAULT;

            case Types.SQL_VARCHAR :
                return SQL_VARCHAR_DEFAULT;

            case Types.SQL_CLOB :
                return SQL_CLOB;

            case Types.SQL_INTEGER :
                return SQL_INTEGER;

            case Types.SQL_SMALLINT :
                return SQL_SMALLINT;

            case Types.SQL_BIGINT :
                return SQL_BIGINT;

            case Types.TINYINT :
                return TINYINT;

            case Types.SQL_FLOAT :
            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return SQL_DOUBLE;

            case Types.SQL_NUMERIC :
                return SQL_NUMERIC;

            case Types.SQL_DECIMAL :
                return SQL_DECIMAL;

            case Types.SQL_BOOLEAN :
                return SQL_BOOLEAN;

            case Types.SQL_BINARY :
                return SQL_BINARY_DEFAULT;

            case Types.SQL_VARBINARY :
                return SQL_VARBINARY_DEFAULT;

            case Types.SQL_BLOB :
                return SQL_BLOB;

            case Types.SQL_BIT :
                return SQL_BIT;

            case Types.SQL_BIT_VARYING :
                return SQL_BIT_VARYING;

            case Types.SQL_DATE :
                return SQL_DATE;

            case Types.SQL_TIME :
                return SQL_TIME;

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return SQL_TIME_WITH_TIME_ZONE;

            case Types.SQL_TIMESTAMP :
                return SQL_TIMESTAMP;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return SQL_TIMESTAMP_WITH_TIME_ZONE;

            case Types.SQL_INTERVAL_YEAR :
                return SQL_INTERVAL_YEAR;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                return SQL_INTERVAL_YEAR_TO_MONTH;

            case Types.SQL_INTERVAL_MONTH :
                return SQL_INTERVAL_MONTH;

            case Types.SQL_INTERVAL_DAY :
                return SQL_INTERVAL_DAY;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                return SQL_INTERVAL_DAY_TO_HOUR;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                return SQL_INTERVAL_DAY_TO_MINUTE;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                return SQL_INTERVAL_DAY_TO_SECOND;

            case Types.SQL_INTERVAL_HOUR :
                return SQL_INTERVAL_HOUR;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                return SQL_INTERVAL_HOUR_TO_MINUTE;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                return SQL_INTERVAL_HOUR_TO_SECOND;

            case Types.SQL_INTERVAL_MINUTE :
                return SQL_INTERVAL_MINUTE;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                return SQL_INTERVAL_MINUTE_TO_SECOND;

            case Types.SQL_INTERVAL_SECOND :
                return SQL_INTERVAL_SECOND;

            case Types.OTHER :
                return OTHER;

            default :
                return null;
        }
    }

    public static int getHSQLDBTypeCode(int jdbcTypeNumber) {

        switch (jdbcTypeNumber) {

            case Types.BIGINT :
                return Types.SQL_BIGINT;

            case Types.LONGVARCHAR :
                return Types.SQL_VARCHAR;

            case Types.CLOB :
                return Types.SQL_CLOB;

            case Types.BINARY :
                return Types.SQL_BINARY;

            case Types.BIT :
                return Types.SQL_BIT_VARYING;

            case Types.VARBINARY :
            case Types.LONGVARBINARY :
                return Types.SQL_VARBINARY;

            case Types.BLOB :
                return Types.SQL_BLOB;

            case Types.ARRAY :
                return Types.SQL_ARRAY;

            default :
                return jdbcTypeNumber;
        }
    }

    /**
     * translate an internal type number to JDBC type number if a type is not
     * supported internally, it is returned without translation
     *
     * @param type int
     * @return int
     */
    public static int getJDBCTypeCode(int type) {

        switch (type) {

            case Types.SQL_BLOB :
                return Types.BLOB;

            case Types.SQL_CLOB :
                return Types.CLOB;

            case Types.SQL_BIGINT :
                return Types.BIGINT;

            case Types.SQL_BINARY :
                return Types.BINARY;

            case Types.SQL_VARBINARY :
                return Types.VARBINARY;

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return Types.BIT;

            case Types.SQL_ARRAY :
                return Types.ARRAY;

            default :
                return type;
        }
    }

    public static Type getType(Type type, Collation collation) {

        if (type.getCollation() == collation) {
            return type;
        }

        if (type.isCharacterType()) {
            type                             = type.duplicate();
            ((CharacterType) type).collation = collation;
        }

        return type;
    }

    /**
     * Enforces precision and scale limits on type
     */
    public static Type getType(int type, Charset charset, Collation collation,
                               long precision, int scale) {

        switch (type) {

            case Types.SQL_ALL_TYPES :
                return SQL_ALL_TYPES;

//                return SQL_ALL_TYPES; // needs changes to Expression type resolution
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.SQL_CLOB :
                return CharacterType.getCharacterType(type, precision,
                                                      collation);

            case Types.SQL_INTEGER :
                return SQL_INTEGER;

            case Types.SQL_SMALLINT :
                return SQL_SMALLINT;

            case Types.SQL_BIGINT :
                return SQL_BIGINT;

            case Types.TINYINT :
                return TINYINT;

            case Types.SQL_FLOAT :
                if (precision > 53) {
                    throw Error.error(ErrorCode.X_42592, "" + precision);
                }

            // fall through
            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return SQL_DOUBLE;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                if (precision == 0) {
                    precision = NumberType.defaultNumericPrecision;
                }

                // A VoltDB extension to disable variable scale decimals
                scale = 12;
                // End of VoltDB extension
                return NumberType.getNumberType(type, precision, scale);

            case Types.SQL_BOOLEAN :
                return SQL_BOOLEAN;

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                return BinaryType.getBinaryType(type, precision);

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return BitType.getBitType(type, precision);

            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return DateTimeType.getDateTimeType(type, scale);

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
            case Types.SQL_INTERVAL_SECOND :
                return IntervalType.getIntervalType(type, precision, scale);

            case Types.OTHER :
                return OTHER;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Type");
        }
    }

    public static Type getAggregateType(Type add, Type existing) {

        if (existing == null || existing.typeCode == Types.SQL_ALL_TYPES) {
            return add;
        }

        if (add == null || add.typeCode == Types.SQL_ALL_TYPES) {
            return existing;
        }

        return existing.getAggregateType(add);
    }

    public static final IntValueHashMap typeAliases;
    public static final IntValueHashMap typeNames;
    public static final IntKeyHashMap   jdbcConvertTypes;

    static {
        typeNames = new IntValueHashMap(37);

        // A VoltDB extension to disable unsupported types
        typeNames.put(Tokens.T_TINYINT, Types.TINYINT);
        typeNames.put(Tokens.T_SMALLINT, Types.SQL_SMALLINT);
        typeNames.put(Tokens.T_INTEGER, Types.SQL_INTEGER);
        typeNames.put(Tokens.T_BIGINT, Types.SQL_BIGINT);
        typeNames.put(Tokens.T_FLOAT, Types.SQL_DOUBLE);
        typeNames.put(Tokens.T_DECIMAL, Types.SQL_DECIMAL);
        typeNames.put(Tokens.T_VARCHAR, Types.SQL_VARCHAR);
        typeNames.put(Tokens.T_TIMESTAMP, Types.SQL_TIMESTAMP);
        typeNames.put(Tokens.T_VARBINARY, Types.SQL_VARBINARY);

        typeAliases = new IntValueHashMap(64);

        typeAliases.put(Tokens.T_INT, Types.SQL_INTEGER);
        typeAliases.put(Tokens.T_REAL, Types.SQL_DOUBLE);
        typeAliases.put(Tokens.T_CHARACTER, Types.SQL_CHAR);

        /* disable 34 lines ...
        typeNames.put(Tokens.T_CHARACTER, Types.SQL_CHAR);
        typeNames.put(Tokens.T_VARCHAR, Types.SQL_VARCHAR);
        typeNames.put(Tokens.T_VARCHAR_IGNORECASE, Types.VARCHAR_IGNORECASE);
        typeNames.put(Tokens.T_NVARCHAR, Types.SQL_VARCHAR);
        typeNames.put(Tokens.T_DATE, Types.SQL_DATE);
        typeNames.put(Tokens.T_TIME, Types.SQL_TIME);
        typeNames.put(Tokens.T_TIMESTAMP, Types.SQL_TIMESTAMP);
        typeNames.put(Tokens.T_INTERVAL, Types.SQL_INTERVAL);
        typeNames.put(Tokens.T_TINYINT, Types.TINYINT);
        typeNames.put(Tokens.T_SMALLINT, Types.SQL_SMALLINT);
        typeNames.put(Tokens.T_INTEGER, Types.SQL_INTEGER);
        typeNames.put(Tokens.T_BIGINT, Types.SQL_BIGINT);
        typeNames.put(Tokens.T_REAL, Types.SQL_REAL);
        typeNames.put(Tokens.T_FLOAT, Types.SQL_FLOAT);
        typeNames.put(Tokens.T_DOUBLE, Types.SQL_DOUBLE);
        typeNames.put(Tokens.T_NUMERIC, Types.SQL_NUMERIC);
        typeNames.put(Tokens.T_DECIMAL, Types.SQL_DECIMAL);
        typeNames.put(Tokens.T_BOOLEAN, Types.SQL_BOOLEAN);
        typeNames.put(Tokens.T_BINARY, Types.SQL_BINARY);
        typeNames.put(Tokens.T_VARBINARY, Types.SQL_VARBINARY);
        typeNames.put(Tokens.T_CLOB, Types.SQL_CLOB);
        typeNames.put(Tokens.T_BLOB, Types.SQL_BLOB);
        typeNames.put(Tokens.T_BIT, Types.SQL_BIT);
        typeNames.put(Tokens.T_OTHER, Types.OTHER);

        //
        typeAliases = new IntValueHashMap(64);

        typeAliases.put(Tokens.T_CHAR, Types.SQL_CHAR);
        typeAliases.put(Tokens.T_INT, Types.SQL_INTEGER);
        typeAliases.put(Tokens.T_DEC, Types.SQL_DECIMAL);
        typeAliases.put(Tokens.T_LONGVARCHAR, Types.LONGVARCHAR);
        typeAliases.put(Tokens.T_DATETIME, Types.SQL_TIMESTAMP);
        typeAliases.put(Tokens.T_LONGVARBINARY, Types.LONGVARBINARY);
        typeAliases.put(Tokens.T_OBJECT, Types.OTHER);
        ... disabled 34 lines */
        // End of VoltDB extension

        //
        jdbcConvertTypes = new IntKeyHashMap(37);

        jdbcConvertTypes.put(Tokens.SQL_CHAR, Type.SQL_CHAR_DEFAULT);
        jdbcConvertTypes.put(Tokens.SQL_VARCHAR, Type.SQL_VARCHAR_DEFAULT);
        jdbcConvertTypes.put(Tokens.SQL_LONGVARCHAR, Type.SQL_VARCHAR_DEFAULT);
        jdbcConvertTypes.put(Tokens.SQL_NVARCHAR, Type.SQL_VARCHAR_DEFAULT);
        jdbcConvertTypes.put(Tokens.SQL_DATE, Type.SQL_DATE);
        jdbcConvertTypes.put(Tokens.SQL_TIME, Type.SQL_TIME);
        jdbcConvertTypes.put(Tokens.SQL_TIMESTAMP, Type.SQL_TIMESTAMP);
        jdbcConvertTypes.put(Tokens.SQL_TINYINT, Type.TINYINT);
        jdbcConvertTypes.put(Tokens.SQL_SMALLINT, Type.SQL_SMALLINT);
        jdbcConvertTypes.put(Tokens.SQL_INTEGER, Type.SQL_INTEGER);
        jdbcConvertTypes.put(Tokens.SQL_BIGINT, Type.SQL_BIGINT);
        jdbcConvertTypes.put(Tokens.SQL_REAL, Type.SQL_DOUBLE);
        jdbcConvertTypes.put(Tokens.SQL_FLOAT, Type.SQL_DOUBLE);
        jdbcConvertTypes.put(Tokens.SQL_DOUBLE, Type.SQL_DOUBLE);
        jdbcConvertTypes.put(Tokens.SQL_NUMERIC, Type.SQL_NUMERIC);
        jdbcConvertTypes.put(Tokens.SQL_DECIMAL, Type.SQL_DECIMAL);
        jdbcConvertTypes.put(Tokens.SQL_BOOLEAN, Type.SQL_BOOLEAN);
        jdbcConvertTypes.put(Tokens.SQL_BINARY, Type.SQL_BINARY_DEFAULT);
        jdbcConvertTypes.put(Tokens.SQL_VARBINARY, Type.SQL_VARBINARY_DEFAULT);
        jdbcConvertTypes.put(Tokens.SQL_LONGVARBINARY,
                             Type.SQL_VARBINARY_DEFAULT);
        jdbcConvertTypes.put(Tokens.SQL_CLOB, Type.SQL_CLOB);
        jdbcConvertTypes.put(Tokens.SQL_BLOB, Type.SQL_BLOB);
        jdbcConvertTypes.put(Tokens.SQL_BIT, Type.SQL_BIT);
    }

    public static int getTypeNr(String name) {

        int i = typeNames.get(name, Integer.MIN_VALUE);

        if (i == Integer.MIN_VALUE) {
            i = typeAliases.get(name, Integer.MIN_VALUE);
        }

        return i;
    }

    public static Type getTypeForJDBCConvertToken(int name) {
        return (Type) jdbcConvertTypes.get(name);
    }

    public static boolean isSupportedSQLType(int typeNumber) {

        if (getDefaultType(typeNumber) == null) {
            return false;
        }

        return true;
    }

    public static boolean matches(Type[] one, Type[] other) {

        for (int i = 0; i < one.length; i++) {
            if (one[i].typeCode != other[i].typeCode) {
                return false;
            }
        }

        return true;
    }
}
