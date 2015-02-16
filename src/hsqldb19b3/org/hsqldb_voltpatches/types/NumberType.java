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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.java.JavaSystem;
import org.hsqldb_voltpatches.map.ValuePool;

/**
 * Type subclass for all NUMBER types.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public final class NumberType extends Type {

    static final int        tinyintPrecision             = 3;
    static final int        smallintPrecision            = 5;
    static final int        integerPrecision             = 10;
    static final int        bigintPrecision              = 19;
    static final int        doublePrecision              = 0;
    public static final int defaultNumericPrecision      = 128;
    public static final int defaultNumericScale          = 32;
    public static final int maxNumericPrecision          = Integer.MAX_VALUE;
    static final int        bigintSquareNumericPrecision = 40;

    //
    public static final int TINYINT_WIDTH  = 8;
    public static final int SMALLINT_WIDTH = 16;
    public static final int INTEGER_WIDTH  = 32;
    public static final int BIGINT_WIDTH   = 64;
    public static final int DOUBLE_WIDTH   = 128;    // nominal width
    public static final int DECIMAL_WIDTH  = 256;    // nominal width

    //
    public static final Type SQL_NUMERIC_DEFAULT_INT =
        new NumberType(Types.NUMERIC, defaultNumericPrecision, 0);

    //
    public static final BigDecimal MAX_DOUBLE =
        BigDecimal.valueOf(Double.MAX_VALUE);
    public static final BigDecimal MAX_LONG =
        BigDecimal.valueOf(Long.MAX_VALUE);
    public static final BigDecimal MIN_LONG =
        BigDecimal.valueOf(Long.MIN_VALUE);
    public static final BigDecimal MAX_INT =
        BigDecimal.valueOf(Integer.MAX_VALUE);
    public static final BigDecimal MIN_INT =
        BigDecimal.valueOf(Integer.MIN_VALUE);

    //
    public static final BigInteger MIN_LONG_BI = MIN_LONG.toBigInteger();
    public static final BigInteger MAX_LONG_BI = MAX_LONG.toBigInteger();

    //
    final int typeWidth;

    public NumberType(int type, long precision, int scale) {

        super(Types.SQL_NUMERIC, type, precision, scale);

        switch (type) {

            case Types.TINYINT :
                typeWidth = TINYINT_WIDTH;
                break;

            case Types.SQL_SMALLINT :
                typeWidth = SMALLINT_WIDTH;
                break;

            case Types.SQL_INTEGER :
                typeWidth = INTEGER_WIDTH;
                break;

            case Types.SQL_BIGINT :
                typeWidth = BIGINT_WIDTH;
                break;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                typeWidth = DOUBLE_WIDTH;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                typeWidth = DECIMAL_WIDTH;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    /**
     * Returns decimal precision for NUMERIC/DECIMAL. Returns binary precision
     * for other parts.
     */
    public int getPrecision() {

        switch (typeCode) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                return typeWidth;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return 64;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return (int) precision;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    /**
     * Returns decimal precision.
     */
    public int getDecimalPrecision() {

        switch (typeCode) {

            case Types.TINYINT :
                return tinyintPrecision;

            case Types.SQL_SMALLINT :
                return smallintPrecision;

            case Types.SQL_INTEGER :
                return integerPrecision;

            case Types.SQL_BIGINT :
                return bigintPrecision;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return displaySize() - 1;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public int displaySize() {

        switch (typeCode) {

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                if (scale == 0) {
                    if (precision == 0) {
                        return 646456995;    // precision + "-.".length()}
                    }

                    return (int) precision + 1;
                }

                if (precision == scale) {
                    return (int) precision + 3;
                }

                return (int) precision + 2;

            case Types.SQL_FLOAT :
            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return 23;                   // String.valueOf(-Double.MAX_VALUE).length();

            case Types.SQL_BIGINT :
                return 20;                   // decimal precision + "-".length();

            case Types.SQL_INTEGER :
                return 11;                   // decimal precision + "-".length();

            case Types.SQL_SMALLINT :
                return 6;                    // decimal precision + "-".length();

            case Types.TINYINT :
                return 4;                    // decimal precision + "-".length();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public int getJDBCTypeCode() {
        return typeCode == Types.SQL_BIGINT ? Types.BIGINT
                                            : typeCode;
    }

    public Class getJDBCClass() {

        switch (typeCode) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                return java.lang.Integer.class;

            case Types.SQL_BIGINT :
                return java.lang.Long.class;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return java.lang.Double.class;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return java.math.BigDecimal.class;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public String getJDBCClassName() {

        switch (typeCode) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                return "java.lang.Integer";

            case Types.SQL_BIGINT :
                return "java.lang.Long";

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return "java.lang.Double";

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return "java.math.BigDecimal";

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public int getJDBCPrecision() {
        return getPrecision();
    }

    public String getNameString() {

        switch (typeCode) {

            case Types.TINYINT :
                return Tokens.T_TINYINT;

            case Types.SQL_SMALLINT :
                return Tokens.T_SMALLINT;

            case Types.SQL_INTEGER :
                return Tokens.T_INTEGER;

            case Types.SQL_BIGINT :
                return Tokens.T_BIGINT;

            case Types.SQL_REAL :
                return Tokens.T_REAL;

            case Types.SQL_FLOAT :
                return Tokens.T_FLOAT;

            case Types.SQL_DOUBLE :
                return Tokens.T_DOUBLE;

            case Types.SQL_NUMERIC :
                return Tokens.T_NUMERIC;

            case Types.SQL_DECIMAL :
                return Tokens.T_DECIMAL;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public String getFullNameString() {

        switch (typeCode) {

            case Types.SQL_DOUBLE :
                return "DOUBLE PRECISION";

            default :
                return getNameString();
        }
    }

    public String getDefinition() {

        switch (typeCode) {

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                StringBuffer sb = new StringBuffer(16);

                sb.append(getNameString());
                sb.append('(');
                sb.append(precision);

                if (scale != 0) {
                    sb.append(',');
                    sb.append(scale);
                }

                sb.append(')');

                return sb.toString();

            default :
                return getNameString();
        }
    }

    public long getMaxPrecision() {

        switch (typeCode) {

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return maxNumericPrecision;

            default :
                return getNumericPrecisionInRadix();
        }
    }

    public int getMaxScale() {

        switch (typeCode) {

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return Short.MAX_VALUE;

            default :
                return 0;
        }
    }

    public boolean acceptsPrecision() {

        switch (typeCode) {

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
            case Types.SQL_FLOAT :
                return true;

            default :
                return false;
        }
    }

    public boolean acceptsScale() {

        switch (typeCode) {

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                return true;

            default :
                return false;
        }
    }

    public int getPrecisionRadix() {

        if (typeCode == Types.SQL_DECIMAL || typeCode == Types.SQL_NUMERIC) {
            return 10;
        }

        return 2;
    }

    public boolean isNumberType() {
        return true;
    }

    public boolean isIntegralType() {

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return false;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return scale == 0;

            default :
                return true;
        }
    }

    public boolean isExactNumberType() {

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return false;

            default :
                return true;
        }
    }

    public boolean isDecimalType() {

        switch (typeCode) {

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return true;

            default :
                return false;
        }
    }

    public int getNominalWidth() {
        return typeWidth;
    }

    public int precedenceDegree(Type other) {

        if (other.isNumberType()) {
            int otherWidth = ((NumberType) other).typeWidth;

            return otherWidth - typeWidth;
        }

        return Integer.MIN_VALUE;
    }

    public Type getAggregateType(Type other) {

        if (other == null) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        if (this == other) {
            return this;
        }

        if (other.isCharacterType()) {
            return other.getAggregateType(this);
        }

        switch (other.typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                break;

            default :
                throw Error.error(ErrorCode.X_42562);
        }

        if (typeWidth == DOUBLE_WIDTH) {
            return this;
        }

        if (((NumberType) other).typeWidth == DOUBLE_WIDTH) {
            return other;
        }

        if (typeWidth <= BIGINT_WIDTH
                && ((NumberType) other).typeWidth <= BIGINT_WIDTH) {
            return (typeWidth > ((NumberType) other).typeWidth) ? this
                                                                : other;
        }

        int newScale = scale > other.scale ? scale
                                           : other.scale;
        long newDigits = precision - scale > other.precision - other.scale
                         ? precision - scale
                         : other.precision - other.scale;

        return getNumberType(Types.SQL_DECIMAL, newDigits + newScale,
                             newScale);
    }

    /**
     *  Returns a SQL type "wide" enough to represent the result of the
     *  expression.<br>
     *  A type is "wider" than the other if it can represent all its
     *  numeric values.<BR>
     *  Arithmetic operation terms are promoted to a type that can
     *  represent the resulting values and avoid incorrect results.<p>
     *  FLOAT/REAL/DOUBLE used in an operation results in the same type,
     *  regardless of the type of the other operand.
     *  When the result or the expression is converted to the
     *  type of the target column for storage, an exception is thrown if the
     *  resulting value cannot be stored in the column<p>
     *  Types narrower than INTEGER (int) are promoted to
     *  INTEGER. The order of promotion is as follows<p>
     *
     *  INTEGER, BIGINT, NUMERIC/DECIMAL<p>
     *
     *  TINYINT and SMALLINT in any combination return INTEGER<br>
     *  TINYINT/SMALLINT/INTEGER and INTEGER return BIGINT<br>
     *  TINYINT/SMALLINT/INTEGER and BIGINT return NUMERIC/DECIMAL<br>
     *  BIGINT and BIGINT return NUMERIC/DECIMAL<br>
     *  REAL/FLOAT/DOUBLE and any type return REAL/FLOAT/DOUBLE<br>
     *  NUMERIC/DECIMAL any type other than REAL/FLOAT/DOUBLE returns NUMERIC/DECIMAL<br>
     *  In the case of NUMERIC/DECIMAL returned, the result precision is always
     *  large enough to express any value result, while the scale depends on the
     *  operation:<br>
     *  For ADD/SUBTRACT/DIVIDE, the scale is the larger of the two<br>
     *  For MULTIPLY, the scale is the sum of the two scales<br>
     */
    public Type getCombinedType(Session session, Type other, int operation) {

        if (other.typeCode == Types.SQL_ALL_TYPES) {
            other = this;
        }

        switch (operation) {

            // A VoltDB extension to be more sql compliant
            // drop this special case handling of ADD
            /* disable 2 lines ...
            case OpTypes.ADD :
            case OpTypes.DIVIDE :
                break;
            ... disabled 2 lines */
            // End of VoltDB extension

            case OpTypes.MULTIPLY :
                if (other.isIntervalType()) {
                    return other.getCombinedType(session, this,
                                                 OpTypes.MULTIPLY);
                }
                break;

            case OpTypes.SUBTRACT :
            default :

                // all derivatives of equality ops or comparison ops
                return getAggregateType(other);
        }

        // resolution for ADD and MULTIPLY only
        if (!other.isNumberType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        if (typeWidth == DOUBLE_WIDTH
                || ((NumberType) other).typeWidth == DOUBLE_WIDTH) {
            return Type.SQL_DOUBLE;
        }

        if (operation != OpTypes.DIVIDE || session.database.sqlAvgScale == 0) {
            int sum = typeWidth + ((NumberType) other).typeWidth;

            if (sum <= INTEGER_WIDTH) {
                return Type.SQL_INTEGER;
            }

            if (sum <= BIGINT_WIDTH) {
                return Type.SQL_BIGINT;
            }
        }

        int  newScale;
        long newDigits;

        switch (operation) {

            case OpTypes.ADD :
                newScale = scale > other.scale ? scale
                                               : other.scale;
                newDigits = precision - scale > other.precision - other.scale
                            ? precision - scale
                            : other.precision - other.scale;

                newDigits++;
                break;

            case OpTypes.DIVIDE :
                newDigits = precision - scale + other.scale;
                newScale  = scale > other.scale ? scale
                                                : other.scale;

                if (session.database.sqlAvgScale > newScale) {
                    newScale = session.database.sqlAvgScale;
                }
                break;

            case OpTypes.MULTIPLY :
                newDigits = precision - scale + other.precision - other.scale;
                newScale  = scale + other.scale;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }

        return getNumberType(Types.SQL_DECIMAL, newScale + newDigits,
                             newScale);
    }

    public int compare(Session session, Object a, Object b) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        switch (typeCode) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                if (b instanceof Integer) {
                    int ai = ((Number) a).intValue();
                    int bi = ((Number) b).intValue();

                    return (ai > bi) ? 1
                                     : (bi > ai ? -1
                                                : 0);
                } else if (b instanceof Double) {
                    double ai = ((Number) a).doubleValue();
                    double bi = ((Number) b).doubleValue();

                    return (ai > bi) ? 1
                                     : (bi > ai ? -1
                                                : 0);
                } else if (b instanceof BigDecimal) {
                    BigDecimal ad = convertToDecimal(a);
                    return ad.compareTo((BigDecimal) b);
                }
            }

            // fall through
            case Types.SQL_BIGINT : {
                if (b instanceof Long) {
                    long longa = ((Number) a).longValue();
                    long longb = ((Number) b).longValue();

                    return (longa > longb) ? 1
                                           : (longb > longa ? -1
                                                            : 0);
                } else if (b instanceof Double) {
                    BigDecimal ad =
                        BigDecimal.valueOf(((Number) a).longValue());
                    BigDecimal bd = new BigDecimal(((Double) b).doubleValue());
                    return ad.compareTo(bd);
                } else if (b instanceof BigDecimal) {
                    BigDecimal ad = convertToDecimal(a);
                    return ad.compareTo((BigDecimal) b);
                }
            }

            // fall through
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {

                /** @todo big-decimal etc */
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                if (Double.isNaN(ad)) {
                    return Double.isNaN(bd) ? 0
                                            : -1;
                }

                if (Double.isNaN(bd)) {
                    return 1;
                }

                return Double.compare(ad, bd);
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                BigDecimal bd = convertToDecimal(b);
                return ((BigDecimal) a).compareTo(bd);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    /** @todo - review usage to see if range enforcement / java type conversion is necessary */
    public Object convertToTypeLimits(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                return a;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return a;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                BigDecimal dec = (BigDecimal) a;

                if (scale != dec.scale()) {
                    dec = dec.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
                }

                int p = JavaSystem.precision(dec);

                if (p > precision) {
                    throw Error.error(ErrorCode.X_22003);
                }

                return dec;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return a;
        }

        if (otherType.typeCode == typeCode) {
            switch (typeCode) {

                case Types.SQL_NUMERIC :
                case Types.SQL_DECIMAL : {
                    BigDecimal dec = (BigDecimal) a;

                    if (scale != dec.scale()) {
                        dec = dec.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
                    }

                    if (JavaSystem.precision(dec) > precision) {
                        throw Error.error(ErrorCode.X_22003);
                    }

                    return dec;
                }
                default :
                    return a;
            }
        }

        if (otherType.isIntervalType()) {
            int startType = ((IntervalType) otherType).startIntervalType;

            switch (startType) {

                case Types.SQL_INTERVAL_YEAR :
                case Types.SQL_INTERVAL_MONTH :
                case Types.SQL_INTERVAL_DAY :
                case Types.SQL_INTERVAL_HOUR :
                case Types.SQL_INTERVAL_MINUTE :
                case Types.SQL_INTERVAL_SECOND : {
                    double value =
                        ((IntervalType) otherType).convertToDoubleStartUnits(
                            a);

                    return convertToType(session, Double.valueOf(value),
                                         Type.SQL_DOUBLE);
                }
            }
        }

        switch (otherType.typeCode) {

            case Types.SQL_CLOB :
                a = ((ClobData) a).getSubString(
                    session, 0L, (int) ((ClobData) a).length(session));

            // fall through
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR : {
                a = session.getScanner().convertToNumber((String) a, this);
                a = convertToDefaultType(session, a);

                return convertToTypeLimits(session, a);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                break;

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                if (otherType.precision == 1) {
                    if (((BinaryData) a).getBytes()[0] == 0) {
                        a = ValuePool.INTEGER_0;
                    } else {
                        a = ValuePool.INTEGER_1;
                    }

                    break;
                }
            default :
                throw Error.error(ErrorCode.X_42561);
        }

        switch (this.typeCode) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                return convertToInt(session, a, this.typeCode);

            case Types.SQL_BIGINT :
                return convertToLong(session, a);

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return convertToDouble(a);

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                BigDecimal value = null;

                if (scale == 0 && a instanceof Double) {
                    double d = ((Number) a).doubleValue();

                    if (session instanceof Session) {
                        if (!((Session) session).database.sqlConvertTruncate) {
                            d = java.lang.Math.rint(d);
                        }
                    }

                    if (Double.isInfinite(d) || Double.isNaN(d)) {
                        throw Error.error(ErrorCode.X_22003);
                    }

                    value = BigDecimal.valueOf(d);
                }

                if (value == null) {
                    value = convertToDecimal(a);
                }

                return convertToTypeLimits(session, value);

            default :
                throw Error.error(ErrorCode.X_42561);
        }
    }

    public Object convertToTypeJDBC(SessionInterface session, Object a,
                                    Type otherType) {

        if (a == null) {
            return a;
        }

        if (otherType.isLobType()) {
            throw Error.error(ErrorCode.X_42561);
        }

        switch (otherType.typeCode) {

            case Types.SQL_BOOLEAN :
                a         = ((Boolean) a).booleanValue() ? ValuePool.INTEGER_1
                                                         : ValuePool.INTEGER_0;
                otherType = Type.SQL_INTEGER;
        }

        return convertToType(session, a, otherType);
    }

    /**
     * Relaxes SQL parameter type enforcement for DECIMAL, allowing long values.
     */
    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        Type otherType;

        if (a instanceof Number) {
            if (a instanceof BigInteger) {
                a = new BigDecimal((BigInteger) a);
            } else if (a instanceof Float) {
                a = new Double(((Float) a).doubleValue());
            } else if (a instanceof Byte) {
                a = ValuePool.getInt(((Byte) a).intValue());
            } else if (a instanceof Short) {
                a = ValuePool.getInt(((Short) a).intValue());
            }

            if (a instanceof Integer) {
                otherType = Type.SQL_INTEGER;
            } else if (a instanceof Long) {
                otherType = Type.SQL_BIGINT;
            } else if (a instanceof Double) {
                otherType = Type.SQL_DOUBLE;
            } else if (a instanceof BigDecimal) {
                otherType = Type.SQL_DECIMAL_DEFAULT;
            } else {
                throw Error.error(ErrorCode.X_42561);
            }

            switch (typeCode) {

                case Types.TINYINT :
                case Types.SQL_SMALLINT :
                case Types.SQL_INTEGER :
                    return convertToInt(session, a, Types.INTEGER);

                case Types.SQL_BIGINT :
                    return convertToLong(session, a);

                case Types.SQL_REAL :
                case Types.SQL_FLOAT :
                case Types.SQL_DOUBLE :
                    return convertToDouble(a);

                case Types.SQL_NUMERIC :
                case Types.SQL_DECIMAL : {
                    a = convertToDecimal(a);

                    BigDecimal dec = (BigDecimal) a;

                    if (scale != dec.scale()) {
                        dec = dec.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
                    }

                    return dec;
                }
                default :
                    throw Error.error(ErrorCode.X_42561);
            }
        } else if (a instanceof String) {
            otherType = Type.SQL_VARCHAR;
        } else {
            throw Error.error(ErrorCode.X_42561);
        }

        return convertToType(session, a, otherType);
    }

    public Object convertJavaToSQL(SessionInterface session, Object a) {
        return convertToDefaultType(session, a);
    }

    /**
     * Type narrowing from DOUBLE/DECIMAL/NUMERIC to BIGINT / INT / SMALLINT / TINYINT
     * following SQL rules. When conversion is from a non-integral type,
     * digits to the right of the decimal point are lost.
     */

    /**
     * Converter from a numeric object to Integer. Input is checked to be
     * within range represented by the given number type.
     */
    static Integer convertToInt(SessionInterface session, Object a, int type) {

        int value;

        if (a instanceof Integer) {
            if (type == Types.SQL_INTEGER) {
                return (Integer) a;
            }

            value = ((Integer) a).intValue();
        } else if (a instanceof Long) {
            long temp = ((Long) a).longValue();

            if (Integer.MAX_VALUE < temp || temp < Integer.MIN_VALUE) {
                throw Error.error(ErrorCode.X_22003);
            }

            value = (int) temp;
        } else if (a instanceof BigDecimal) {
            BigDecimal bd = ((BigDecimal) a);

            if (bd.compareTo(MAX_INT) > 0 || bd.compareTo(MIN_INT) < 0) {
                throw Error.error(ErrorCode.X_22003);
            }

            value = bd.intValue();
        } else if (a instanceof Double || a instanceof Float) {
            double d = ((Number) a).doubleValue();

            if (session instanceof Session) {
                if (!((Session) session).database.sqlConvertTruncate) {
                    d = java.lang.Math.rint(d);
                }
            }

            if (Double.isInfinite(d) || Double.isNaN(d)
                    || d >= (double) Integer.MAX_VALUE + 1
                    || d <= (double) Integer.MIN_VALUE - 1) {
                throw Error.error(ErrorCode.X_22003);
            }

            value = (int) d;
        } else {
            throw Error.error(ErrorCode.X_42561);
        }

        if (type == Types.TINYINT) {
            if (Byte.MAX_VALUE < value || value < Byte.MIN_VALUE) {
                throw Error.error(ErrorCode.X_22003);
            }
        } else if (type == Types.SQL_SMALLINT) {
            if (Short.MAX_VALUE < value || value < Short.MIN_VALUE) {
                throw Error.error(ErrorCode.X_22003);
            }
        }

        return Integer.valueOf(value);
    }

    /**
     * Converter from a numeric object to Long. Input is checked to be
     * within range represented by Long.
     */
    static Long convertToLong(SessionInterface session, Object a) {

        if (a instanceof Integer) {
            return ValuePool.getLong(((Integer) a).intValue());
        } else if (a instanceof Long) {
            return (Long) a;
        } else if (a instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) a;

            if (bd.compareTo(MAX_LONG) > 0 || bd.compareTo(MIN_LONG) < 0) {
                throw Error.error(ErrorCode.X_22003);
            }

            return ValuePool.getLong(bd.longValue());
        } else if (a instanceof Double || a instanceof Float) {
            double d = ((Number) a).doubleValue();

            if (session instanceof Session) {
                if (!((Session) session).database.sqlConvertTruncate) {
                    d = java.lang.Math.rint(d);
                }
            }

            if (Double.isInfinite(d) || Double.isNaN(d)
                    || d >= (double) Long.MAX_VALUE + 1
                    || d <= (double) Long.MIN_VALUE - 1) {
                throw Error.error(ErrorCode.X_22003);
            }

            return ValuePool.getLong((long) d);
        } else {
            throw Error.error(ErrorCode.X_42561);
        }
    }

    /**
     * Converter from a numeric object to Double. Input is checked to be
     * within range represented by Double
     */
    private static Double convertToDouble(Object a) {

        if (a instanceof java.lang.Double) {
            return (Double) a;
        }

        double value = toDouble(a);

        return ValuePool.getDouble(Double.doubleToLongBits(value));
    }

    public static double toDouble(Object a) {

        double value;

        if (a instanceof java.lang.Double) {
            return ((Double) a).doubleValue();
        } else if (a instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) a;

            value = bd.doubleValue();

            int        signum = bd.signum();
            BigDecimal bdd    = new BigDecimal(value + signum);

            if (bdd.compareTo(bd) != signum) {
                throw Error.error(ErrorCode.X_22003);
            }
        } else if (a instanceof Number) {
            value = ((Number) a).doubleValue();
        } else {
            throw Error.error(ErrorCode.X_22501);
        }

        return value;
    }

    private static BigDecimal convertToDecimal(Object a) {

        if (a instanceof BigDecimal) {
            return (BigDecimal) a;
        } else if (a instanceof Integer || a instanceof Long) {
            return BigDecimal.valueOf(((Number) a).longValue());
        } else if (a instanceof Double) {
            double value = ((Number) a).doubleValue();

            if (Double.isInfinite(value) || Double.isNaN(value)) {
                throw Error.error(ErrorCode.X_22003);
            }

            return BigDecimal.valueOf(value);
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        switch (this.typeCode) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                return a.toString();

            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                double value = ((Double) a).doubleValue();

                /** @todo - java 5 format change */
                if (value == Double.NEGATIVE_INFINITY) {
                    return "-1E0/0";
                }

                if (value == Double.POSITIVE_INFINITY) {
                    return "1E0/0";
                }

                if (Double.isNaN(value)) {
                    return "0E0/0E0";
                }
                // A VoltDB extension to comply literally with the SQL standard requirement
                // that 0.0 be represented as a special cased "0E0"
                // and NOT "0.0E0" as HSQL had been giving.
                if (value == 0.0) {
                    return "0E0";
                }
                // End of VoltDB extension

                String s = Double.toString(value);

                // ensure the engine treats the value as a DOUBLE, not DECIMAL
                if (s.indexOf('E') < 0) {
                    // A VoltDB extension to ALWAYS use proper E notation,
                    // with a proper single-non-zero-digit integer part.
                    // HSQL originally just had: s = s.concat("E0");
                    int decimalOffset = s.indexOf('.');
                    String optionalSign = (value < 0.0 ? "-" : "");
                    int leadingNonZeroOffset;
                    String decimalPart;
                    int exponent;
                    if (value > -10.0 && value < 10.0) {
                        if (value <= -1.0 || value >= 1.0) {
                            // OK -- exactly 1 leading digit. Done.
                            s = s.concat("E0");
                            return s;
                        }

                        // A zero leading digit, and maybe more zeros after the decimal.
                        // Search for a significant digit past the decimal point.
                        for(leadingNonZeroOffset = decimalOffset+1;
                                leadingNonZeroOffset < s.length();
                                    ++leadingNonZeroOffset) {
                            if (s.charAt(leadingNonZeroOffset) != '0') {
                                break;
                            }
                        }
                        // Count 1 for the leading 0 but not for the decimal point.
                        exponent = decimalOffset - leadingNonZeroOffset;
                        // Since exact 0.0 was eliminated earlier,
                        // s.charAt(leadingNonZeroOffset) must be our leading non-zero digit.
                        // Rewrite 0.[0]*nn* as n.n*E-x where x is the number of leading zeros found
                        // BUT rewrite 0.[0]*n as n.0E-x where x is the number of leading zeros found.
                        if (leadingNonZeroOffset + 1 == s.length()) {
                            decimalPart = "0";
                        }
                        else {
                            decimalPart = s.substring(leadingNonZeroOffset+1);
                        }
                    }
                    else {
                        // Too many leading digits.
                        leadingNonZeroOffset = optionalSign.length();
                        // Set the exponent to how far the original decimal point was from its target
                        // position, just after the leading digit. This is also the length of the
                        // string of extra integer part digits that need to be moved into the decimal part.
                        exponent = decimalOffset - (leadingNonZeroOffset + 1);

                        decimalPart = s.substring(leadingNonZeroOffset+1, exponent) + s.substring(decimalOffset+1);
                        // Trim any trailing zeros from the result.
                        int lastIndex;
                        for (lastIndex = decimalPart.length() - 1; lastIndex > 0; --lastIndex) {
                            if (decimalPart.charAt(lastIndex) != '0') {
                                break;
                            }
                        }
                        if (lastIndex > 0 && decimalPart.charAt(lastIndex) == '0') {
                            decimalPart = decimalPart.substring(lastIndex);
                        }
                    }
                    s = optionalSign + s.charAt(leadingNonZeroOffset) + "." + decimalPart + "E" + exponent;
                    /* disable 1 line ...
                    s = s.concat("E0");
                    ... disabled 1 line */
                    // End of VoltDB extension
                }

                return s;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return JavaSystem.toString((BigDecimal) a);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return Tokens.T_NULL;
        }

        return convertToString(a);
    }

    public boolean canConvertFrom(Type otherType) {

        if (otherType.typeCode == Types.SQL_ALL_TYPES) {
            return true;
        }

        if (otherType.isNumberType()) {
            return true;
        }

        if (otherType.isIntervalType()) {
            return true;
        }

        if (otherType.isCharacterType()) {
            return true;
        }

        if (otherType.isBitType() && otherType.precision == 1) {
            return true;
        }

        return false;
    }

    public int canMoveFrom(Type otherType) {

        if (otherType == this) {
            return 0;
        }

        switch (typeCode) {

            case Types.TINYINT :
                if (otherType.typeCode == Types.SQL_SMALLINT
                        || otherType.typeCode == Types.SQL_INTEGER) {
                    return 1;
                }
                break;

            case Types.SQL_SMALLINT :
                if (otherType.typeCode == Types.TINYINT) {
                    return 0;
                }

                if (otherType.typeCode == Types.SQL_INTEGER) {
                    return 1;
                }
                break;

            case Types.SQL_INTEGER :
                if (otherType.typeCode == Types.SQL_SMALLINT
                        || otherType.typeCode == Types.TINYINT) {
                    return 0;
                }
                break;

            case Types.SQL_BIGINT :
                break;

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                if (otherType.typeCode == Types.SQL_DECIMAL
                        || otherType.typeCode == Types.SQL_NUMERIC) {
                    if (scale == otherType.scale) {
                        if (precision >= otherType.precision) {
                            return 0;
                        } else {
                            return 1;
                        }
                    }
                }
                break;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                if (otherType.typeCode == Types.SQL_REAL
                        || otherType.typeCode == Types.SQL_FLOAT
                        || otherType.typeCode == Types.SQL_DOUBLE) {
                    return 0;
                }
            default :
        }

        return -1;
    }

    public int compareToTypeRange(Object o) {

        if (!(o instanceof Number)) {
            return 0;
        }

        if (o instanceof Integer || o instanceof Long) {
            long temp = ((Number) o).longValue();
            int  min;
            int  max;

            switch (typeCode) {

                case Types.TINYINT :
                    min = Byte.MIN_VALUE;
                    max = Byte.MAX_VALUE;
                    break;

                case Types.SQL_SMALLINT :
                    min = Short.MIN_VALUE;
                    max = Short.MAX_VALUE;
                    break;

                case Types.SQL_INTEGER :
                    min = Integer.MIN_VALUE;
                    max = Integer.MAX_VALUE;
                    break;

                case Types.SQL_BIGINT :
                    return 0;

                case Types.SQL_DECIMAL :
                case Types.SQL_NUMERIC : {
                    if (precision - scale > 18) {
                        return 0;
                    }

                    if (precision - scale > 9 && o instanceof Integer) {
                        return 0;
                    }

                    BigDecimal dec = convertToDecimal(o);
                    int        s   = dec.scale();
                    int        p   = JavaSystem.precision(dec);

                    if (s < 0) {
                        p -= s;
                        s = 0;
                    }

                    return (precision - scale >= p - s) ? 0
                                                        : dec.signum();
                }
                default :
                    return 0;
            }

            if (max < temp) {
                return 1;
            }

            if (temp < min) {
                return -1;
            }

            return 0;
        }

        return 0;
    }

    public Object add(Session session, Object a, Object b, Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad + bd));

//                return new Double(ad + bd);
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                a = convertToDefaultType(null, a);
                b = convertToDefaultType(null, b);

                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                abd = abd.add(bbd);

                return convertToTypeLimits(null, abd);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai + bi);
            }
            case Types.SQL_BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa + longb);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public Object subtract(Session session, Object a, Object b,
                           Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad - bd));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                a = convertToDefaultType(null, a);
                b = convertToDefaultType(null, b);

                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                abd = abd.subtract(bbd);

                return convertToTypeLimits(null, abd);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai - bi);
            }
            case Types.SQL_BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa - longb);
            }
            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
    }

    public Object multiply(Object a, Object b) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad * bd));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                if (!(a instanceof BigDecimal)) {
                    a = convertToDefaultType(null, a);
                }

                if (!(b instanceof BigDecimal)) {
                    b = convertToDefaultType(null, b);
                }

                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;
                BigDecimal bd  = abd.multiply(bbd);

                // A VoltDB extension to use fixed decimal scale ...
                // This replicates VoltDecimalHelper.setDefaultScale(bd);
                // without the library dependency.
                // The replaced line is new to hsql2.2
                // and not the original hsql1.9b code that motivated this patch.
                // Is the patch still required?
                // OR could "convertToTypeLimits be patched instead to do what VoltDB wants?
                return bd.setScale(12 /* == VoltDecimalHelper.kDefaultScale*/,
                                   java.math.RoundingMode.HALF_EVEN);
                /* disable 1 line ...
                return convertToTypeLimits(null, bd);
                ... disabled 1 line */
                // End of VoltDB extension
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai * bi);
            }
            case Types.SQL_BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa * longb);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public Object divide(Session session, Object a, Object b) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                if (bd == 0
                        && (session == null
                            || session.database.sqlDoubleNaN)) {
                    throw Error.error(ErrorCode.X_22012);
                }

                return ValuePool.getDouble(Double.doubleToLongBits(ad / bd));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                if (!(a instanceof BigDecimal)) {
                    a = convertToDefaultType(null, a);
                }

                if (!(b instanceof BigDecimal)) {
                    b = convertToDefaultType(null, b);
                }

                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                if (bbd.signum() == 0) {
                    throw Error.error(ErrorCode.X_22012);
                }

                BigDecimal bd = abd.divide(bbd, scale, BigDecimal.ROUND_DOWN);

                return convertToTypeLimits(null, bd);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                if (bi == 0) {
                    throw Error.error(ErrorCode.X_22012);
                }

                return ValuePool.getInt(ai / bi);
            }
            case Types.SQL_BIGINT : {
                long al = ((Number) a).longValue();
                long bl = ((Number) b).longValue();

                if (bl == 0) {
                    throw Error.error(ErrorCode.X_22012);
                }

                return ValuePool.getLong(al / bl);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public Object modulo(Session session, Object a, Object b, Type otherType) {

        if (!otherType.isNumberType()) {
            throw Error.error(ErrorCode.X_42561);
        }

        Object temp = divide(null, a, b);

        temp = multiply(temp, b);
        temp = convertToDefaultType(null, temp);
        temp = subtract(session, a, temp, this);

        return convertToTypeLimits(null, temp);
    }

    public Object absolute(Object a) {
        return isNegative(a) ? negate(a)
                             : a;
    }

    public Object negate(Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = -((Number) a).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return ((BigDecimal) a).negate();

            case Types.TINYINT : {
                int value = ((Number) a).intValue();

                if (value == Byte.MIN_VALUE) {
                    throw Error.error(ErrorCode.X_22003);
                }

                return ValuePool.getInt(-value);
            }
            case Types.SQL_SMALLINT : {
                int value = ((Number) a).intValue();

                if (value == Short.MIN_VALUE) {
                    throw Error.error(ErrorCode.X_22003);
                }

                return ValuePool.getInt(-value);
            }
            case Types.SQL_INTEGER : {
                int value = ((Number) a).intValue();

                if (value == Integer.MIN_VALUE) {
                    throw Error.error(ErrorCode.X_22003);
                }

                return ValuePool.getInt(-value);
            }
            case Types.SQL_BIGINT : {
                long value = ((Number) a).longValue();

                if (value == Long.MIN_VALUE) {
                    throw Error.error(ErrorCode.X_22003);
                }

                return ValuePool.getLong(-value);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public int getNumericPrecisionInRadix() {

        switch (typeCode) {

            case Types.TINYINT :
                return 8;

            case Types.SQL_SMALLINT :
                return 16;

            case Types.SQL_INTEGER :
                return 32;

            case Types.SQL_BIGINT :
                return 64;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return 64;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return (int) precision;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public Type getIntegralType() {

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return SQL_NUMERIC_DEFAULT_INT;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return scale == 0 ? this
                                  : new NumberType(typeCode, precision, 0);

            default :
                return this;
        }
    }

    public static boolean isZero(Object a) {

        if (a instanceof BigDecimal) {
            return ((BigDecimal) a).signum() == 0;
        } else if (a instanceof Double) {
            return ((Double) a).doubleValue() == 0 || ((Double) a).isNaN();
        } else {
            return ((Number) a).longValue() == 0;
        }
    }

    public boolean isNegative(Object a) {

        if (a == null) {
            return false;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();

                return ad < 0;
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return ((BigDecimal) a).signum() < 0;

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                return ((Number) a).intValue() < 0;

            case Types.SQL_BIGINT :
                return ((Number) a).longValue() < 0;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public int compareToZero(Object a) {

        if (a == null) {
            return 0;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();

                return ad == 0 ? 0
                               : ad < 0 ? -1
                                        : 1;
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return ((BigDecimal) a).signum();

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();

                return ai == 0 ? 0
                               : ai < 0 ? -1
                                        : 1;
            }
            case Types.SQL_BIGINT : {
                long al = ((Number) a).longValue();

                return al == 0 ? 0
                               : al < 0 ? -1
                                        : 1;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }

    public static long scaledDecimal(Object a, int scale) {

        if (a == null) {
            return 0;
        }

        if (scale == 0) {
            return 0;
        }

        BigDecimal value = ((BigDecimal) a);

        if (value.scale() == 0) {
            return 0;
        }

        value = value.setScale(0, BigDecimal.ROUND_FLOOR);
        value = ((BigDecimal) a).subtract(value);

        return value.movePointRight(scale).longValue();
    }

    public static boolean isInLongLimits(BigDecimal result) {

       return NumberType.MIN_LONG.compareTo(result) <= 0
                && NumberType.MAX_LONG.compareTo(result) >= 0;
    }

    public static boolean isInLongLimits(BigInteger result) {

        return NumberType.MIN_LONG_BI.compareTo(result) <= 0
                && NumberType.MAX_LONG_BI.compareTo(result) >= 0;
    }

    public Object ceiling(Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = Math.ceil(((Double) a).doubleValue());

                if (Double.isInfinite(ad)) {
                    throw Error.error(ErrorCode.X_22003);
                }

                return ValuePool.getDouble(Double.doubleToLongBits(ad));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                BigDecimal value = ((BigDecimal) a).setScale(0,
                    BigDecimal.ROUND_CEILING);

                return value;
            }
            default :
                return a;
        }
    }

    public Object floor(Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double value = Math.floor(((Double) a).doubleValue());

                if (Double.isInfinite(value)) {
                    throw Error.error(ErrorCode.X_22003);
                }

                return ValuePool.getDouble(Double.doubleToLongBits(value));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                BigDecimal value = ((BigDecimal) a).setScale(0,
                    BigDecimal.ROUND_FLOOR);

                return value;
            }

            // fall through
            default :
                return a;
        }
    }

    public Object truncate(Object a, int s) {

        if (a == null) {
            return null;
        }

        BigDecimal dec = convertToDecimal(a);

        dec = dec.setScale(s, BigDecimal.ROUND_DOWN);

        if (typeCode == Types.SQL_DECIMAL || typeCode == Types.SQL_NUMERIC) {
            dec = dec.setScale(scale, BigDecimal.ROUND_DOWN);
        }

        a = convertToDefaultType(null, dec);

        return convertToTypeLimits(null, a);
    }

    public Object round(Object a, int s) {

        if (a == null) {
            return null;
        }

        BigDecimal dec = convertToDecimal(a);

        switch (typeCode) {

            case Types.SQL_DOUBLE : {
                dec = dec.setScale(s, BigDecimal.ROUND_HALF_EVEN);

                break;
            }
            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
            default : {
                dec = dec.setScale(s, BigDecimal.ROUND_HALF_UP);
                dec = dec.setScale(scale, BigDecimal.ROUND_DOWN);

                break;
            }
        }

        a = convertToDefaultType(null, dec);

        return convertToTypeLimits(null, a);
    }

    public static NumberType getNumberType(int type, long precision,
                                           int scale) {

        switch (type) {

            case Types.SQL_INTEGER :
                return SQL_INTEGER;

            case Types.SQL_SMALLINT :
                return SQL_SMALLINT;

            case Types.SQL_BIGINT :
                return SQL_BIGINT;

            case Types.TINYINT :
                return TINYINT;

            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
            // A VoltDB extension to ?support FLOAT as alis to DOUBLE?
            case Types.SQL_FLOAT :
            // End of VoltDB extension
                return SQL_DOUBLE;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return new NumberType(type, precision, scale);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberType");
        }
    }
}
