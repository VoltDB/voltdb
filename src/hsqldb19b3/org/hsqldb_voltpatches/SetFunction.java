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


package org.hsqldb_voltpatches;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.types.ArrayType;
import org.hsqldb_voltpatches.types.DTIType;
import org.hsqldb_voltpatches.types.IntervalMonthData;
import org.hsqldb_voltpatches.types.IntervalSecondData;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.RowType;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Type.TypedComparator;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of SQL set functions (currently only aggregate functions).
 * This reduces temporary Object creation by SUM and AVG functions for
 * INTEGER and narrower types.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.7.2
 *
 */
public class SetFunction implements Serializable {

    private HashSet distinctValues;
    private boolean isDistinct;

    //
    private int       setType;
    private int       typeCode;
    private Type      type;
    private ArrayType arrayType;
    private Type      returnType;

    //
    private long count;

    //
    private boolean    hasNull;
    private boolean    every = true;
    private boolean    some  = false;
    private long       currentLong;
    private double     currentDouble;
    private BigDecimal currentBigDecimal;
    private Object     currentValue;

    SetFunction(Session session, int setType, Type type, Type returnType,
                boolean isDistinct, ArrayType arrayType) {

        this.setType    = setType;
        this.type       = type;
        this.returnType = returnType;

        if (isDistinct) {
            this.isDistinct = true;
            this.arrayType  = arrayType;
            distinctValues  = new HashSet();

            if (type.isRowType() || type.isArrayType()) {
                TypedComparator comparator = Type.newComparator(session);
                SortAndSlice    sort       = new SortAndSlice();
                int length = type.isRowType()
                             ? ((RowType) type).getTypesArray().length
                             : 1;

                sort.prepareMultiColumn(length);
                comparator.setType(type, sort);
                distinctValues.setComparator(comparator);
            }
        }

        if (setType == OpTypes.VAR_SAMP || setType == OpTypes.STDDEV_SAMP) {
            this.sample = true;
        }

        if (type != null) {
            typeCode = type.typeCode;

            if (type.isIntervalType()) {
                typeCode = Types.SQL_INTERVAL;
            }
        }
    }

    void add(Session session, Object item) {

        if (item == null) {
            hasNull = true;

            return;
        }

        if (isDistinct && !distinctValues.add(item)) {
            return;
        }

        count++;

        switch (setType) {

            case OpTypes.COUNT :
                return;
            // A VoltDB extension APPROC_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT:
                // Return "unexpected token" error
                throw Error.error(ErrorCode.X_42581, Tokens.T_APPROX_COUNT_DISTINCT);
            // End of VoltDB extension
            case OpTypes.AVG :
            case OpTypes.SUM : {
                switch (typeCode) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        currentLong += ((Number) item).intValue();

                        return;

                    case Types.SQL_INTERVAL : {
                        if (item instanceof IntervalSecondData) {
                            addLong(((IntervalSecondData) item).getSeconds());

                            currentLong +=
                                ((IntervalSecondData) item).getNanos();

                            if (Math.abs(currentLong)
                                    >= DTIType.nanoScaleFactors[0]) {
                                addLong(currentLong
                                        / DTIType.nanoScaleFactors[0]);

                                currentLong %= DTIType.nanoScaleFactors[0];
                            }
                        } else if (item instanceof IntervalMonthData) {
                            addLong(((IntervalMonthData) item).units);
                        }

                        return;
                    }
                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        addLong(((TimestampData) item).getSeconds());

                        currentLong += ((TimestampData) item).getNanos();

                        if (Math.abs(currentLong)
                                >= DTIType.nanoScaleFactors[0]) {
                            addLong(currentLong / DTIType.nanoScaleFactors[0]);

                            currentLong %= DTIType.nanoScaleFactors[0];
                        }

                        currentDouble = ((TimestampData) item).getZone();

                        return;
                    }
                    case Types.SQL_BIGINT :
                        addLong(((Number) item).longValue());

                        return;

                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        currentDouble += ((Number) item).doubleValue();

                        return;

                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        if (currentBigDecimal == null) {
                            currentBigDecimal = (BigDecimal) item;
                        } else {
                            currentBigDecimal =
                                currentBigDecimal.add((BigDecimal) item);
                        }

                        return;

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.MIN : {
                if (currentValue == null) {
                    currentValue = item;

                    return;
                }

                if (type.compare(session, currentValue, item) > 0) {
                    currentValue = item;
                }

                return;
            }
            case OpTypes.MAX : {
                if (currentValue == null) {
                    currentValue = item;

                    return;
                }

                if (type.compare(session, currentValue, item) < 0) {
                    currentValue = item;
                }

                return;
            }
            case OpTypes.EVERY :
                if (!(item instanceof Boolean)) {
                    throw Error.error(ErrorCode.X_42563);
                }

                every = every && ((Boolean) item).booleanValue();

                return;

            case OpTypes.SOME :
                if (!(item instanceof Boolean)) {
                    throw Error.error(ErrorCode.X_42563);
                }

                some = some || ((Boolean) item).booleanValue();

                return;

            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                addDataPoint((Number) item);

                return;

            case OpTypes.USER_AGGREGATE :
                currentValue = item;

                return;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SetFunction");
        }
    }

    Object getValue(Session session) {

        if (hasNull) {
            session.addWarning(Error.error(ErrorCode.W_01003));
        }

        if (setType == OpTypes.COUNT) {

            // todo - strings embedded in array or row
            if (count > 0 && isDistinct && type.isCharacterType()) {
                Object[] array = new Object[distinctValues.size()];

                distinctValues.toArray(array);

                SortAndSlice sort = new SortAndSlice();

                sort.prepareSingleColumn(0);
                arrayType.sort(session, array, sort);

                count = arrayType.deDuplicate(session, array, sort);
            }

            return ValuePool.getLong(count);
        }

        // A VoltDB extension APPROX_COUNT_DISTINCT
        if (setType == OpTypes.APPROX_COUNT_DISTINCT) {
            throw Error.error(ErrorCode.X_42581, Tokens.T_APPROX_COUNT_DISTINCT);
        }
        // End of VoltDB extension
        if (count == 0) {
            return null;
        }

        switch (setType) {

            case OpTypes.AVG : {
                switch (typeCode) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        if (returnType.scale != 0) {
                            return returnType.divide(session, currentLong,
                                                     count);
                        }

                        return Long.valueOf(currentLong / count);

                    case Types.SQL_BIGINT : {
                        long value = getLongSum().divide(
                            BigInteger.valueOf(count)).longValue();

                        return Long.valueOf(value);
                    }
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        return new Double(currentDouble / count);

                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        if (returnType.scale == type.scale) {
                            return currentBigDecimal.divide(
                                new BigDecimal(count), BigDecimal.ROUND_DOWN);
                        } else {
                            return returnType.divide(session,
                                                     currentBigDecimal, count);
                        }
                    case Types.SQL_INTERVAL : {
                        BigInteger bi =
                            getLongSum().divide(BigInteger.valueOf(count));

                        if (!NumberType.isInLongLimits(bi)) {
                            throw Error.error(ErrorCode.X_22015);
                        }

                        if (((IntervalType) type).isDaySecondIntervalType()) {
                            return new IntervalSecondData(bi.longValue(),
                                                          currentLong,
                                                          (IntervalType) type,
                                                          true);
                        } else {
                            return IntervalMonthData.newIntervalMonth(
                                bi.longValue(), (IntervalType) type);
                        }
                    }
                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        BigInteger bi =
                            getLongSum().divide(BigInteger.valueOf(count));

                        if (!NumberType.isInLongLimits(bi)) {
                            throw Error.error(ErrorCode.X_22015);
                        }

                        return new TimestampData(bi.longValue(),
                                                 (int) currentLong,
                                                 (int) currentDouble);
                    }
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "SetFunction");
                }
            }
            case OpTypes.SUM : {
                switch (typeCode) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        return Long.valueOf(currentLong);

                    case Types.SQL_BIGINT :
                        return new BigDecimal(getLongSum());

                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        return new Double(currentDouble);

                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        return currentBigDecimal;

                    case Types.SQL_INTERVAL : {
                        BigInteger bi = getLongSum();

                        if (!NumberType.isInLongLimits(bi)) {
                            throw Error.error(ErrorCode.X_22015);
                        }

                        if (((IntervalType) type).isDaySecondIntervalType()) {
                            return new IntervalSecondData(bi.longValue(),
                                                          currentLong,
                                                          (IntervalType) type,
                                                          true);
                        } else {
                            return IntervalMonthData.newIntervalMonth(
                                bi.longValue(), (IntervalType) type);
                        }
                    }
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "SetFunction");
                }
            }
            case OpTypes.MIN :
            case OpTypes.MAX :
                return currentValue;

            case OpTypes.EVERY :
                return every ? Boolean.TRUE
                             : Boolean.FALSE;

            case OpTypes.SOME :
                return some ? Boolean.TRUE
                            : Boolean.FALSE;

            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
                return getStdDev();

            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                return getVariance();

            case OpTypes.USER_AGGREGATE :
                return currentValue;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SetFunction");
        }
    }

    /**
     * During parsing and before an instance of SetFunction is created,
     * getType is called with type parameter set to correct type when main
     * SELECT statements contain aggregates.
     *
     */
    static Type getType(Session session, int setType, Type type) {

        if (setType == OpTypes.COUNT) {
            return Type.SQL_BIGINT;
        }

        // A VoltDB extension to handle aggfnc(*) syntax errors.
        // If the argument node does not have
        // a data type, it may be '*'.  If the
        // operation is COUNT (optype == 71) this is
        // just fine.  But if it's anything else this
        // is a syntax error.
        if (type == null) {
            throw Error.error(ErrorCode.U_S0500);
        }
        // End of VoltDB extension
        int typeCode = type.isIntervalType() ? Types.SQL_INTERVAL
                                             : type.typeCode;

        switch (setType) {

            case OpTypes.AVG :
            case OpTypes.MEDIAN : {
                switch (typeCode) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                    case Types.SQL_BIGINT :
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        int scale = session.database.sqlAvgScale;

                        if (scale <= type.scale) {
                            return type;
                        }

                        int digits = ((NumberType) type).getDecimalPrecision();

                        return NumberType.getNumberType(Types.SQL_DECIMAL,
                                                        digits + scale, scale);

                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                    case Types.SQL_INTERVAL :
                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        return type;

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.SUM : {
                switch (typeCode) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        return Type.SQL_BIGINT;

                    case Types.SQL_BIGINT :
                        return Type.SQL_DECIMAL_BIGINT_SQR;

                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        return Type.SQL_DOUBLE;

                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        return Type.getType(type.typeCode, null, null,
                                            type.precision * 2, type.scale);

                    case Types.SQL_INTERVAL :
                        return IntervalType.newIntervalType(
                            type.typeCode, DTIType.maxIntervalPrecision,
                            type.scale);

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.MIN :
            case OpTypes.MAX :
                if (type.isArrayType() || type.isLobType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                return type;

            case OpTypes.EVERY :
            case OpTypes.SOME :
                if (type.isBooleanType()) {
                    return Type.SQL_BOOLEAN;
                }
                break;

            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                if (type.isNumberType()) {
                    return Type.SQL_DOUBLE;
                }
                break;

            case OpTypes.USER_AGGREGATE :
                return type;

            // A VoltDB extension for APPROX_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT :
                switch (typeCode) {
                case Types.TINYINT :
                case Types.SQL_SMALLINT :
                case Types.SQL_INTEGER :
                case Types.SQL_BIGINT :
                case Types.SQL_DECIMAL :
                case Types.SQL_TIMESTAMP :
                    return Type.SQL_BIGINT;
                default:
                    // We only support fixed-width types for this
                    // aggregate function.
                    //
                    // FLOAT is not supported since this function
                    // relies on different values having different bit
                    // patterns, and the same values being the
                    // same.  Floating point numbers don't hold to
                    // this---e.g., positive and negative zero.
                    //
                    // Incompatible data types in operation
                    throw Error.error(ErrorCode.X_42561);
                }
            // End of VoltDB extension for APPROX_COUNT_DISTINCT
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SetFunction");
        }

        throw Error.error(ErrorCode.X_42563);
    }

    // long sum - originally a separate class

    /**
     * Maintain the sum of multiple long values without creating a new
     * BigInteger object for each addition.
     */
    static final BigInteger multiplier =
        BigInteger.valueOf(0x0000000100000000L);

//        BigInteger bigint = BigInteger.ZERO;
    long hi;
    long lo;

    void addLong(long value) {

        if (value == 0) {}
        else if (value > 0) {
            hi += value >> 32;
            lo += value & 0x00000000ffffffffL;
        } else {
            if (value == Long.MIN_VALUE) {
                hi -= 0x000000080000000L;
            } else {
                long temp = ~value + 1;

                hi -= temp >> 32;
                lo -= temp & 0x00000000ffffffffL;
            }
        }

//            bigint = bigint.add(BigInteger.valueOf(value));
    }

    BigInteger getLongSum() {

        BigInteger biglo  = BigInteger.valueOf(lo);
        BigInteger bighi  = BigInteger.valueOf(hi);
        BigInteger result = (bighi.multiply(multiplier)).add(biglo);

/*
            if ( result.compareTo(bigint) != 0 ){
                 throw Trace.error(Trace.GENERAL_ERROR, "longSum mismatch");
            }
*/
        return result;
    }

    // end long sum
    // statistics support - written by Campbell
    // this section was orginally an independent class
    private double  sk;
    private double  vk;
    private long    n;
    private boolean initialized;
    private boolean sample;

    private void addDataPoint(Number x) {    // optimized

        double xi;
        double xsi;
        long   nm1;

        if (x == null) {
            return;
        }

        xi = x.doubleValue();

        if (!initialized) {
            n           = 1;
            sk          = xi;
            vk          = 0.0;
            initialized = true;

            return;
        }

        n++;

        nm1 = (n - 1);
        xsi = (sk - (xi * nm1));
        vk  += ((xsi * xsi) / n) / nm1;
        sk  += xi;
    }

    private Number getVariance() {

        if (!initialized) {
            return null;
        }

        return sample ? (n == 1) ? null    // NULL (not NaN) is correct in this case
                                 : new Double(vk / (double) (n - 1))
                      : new Double(vk / (double) (n));
    }

    private Number getStdDev() {

        if (!initialized) {
            return null;
        }

        return sample ? (n == 1) ? null    // NULL (not NaN) is correct in this case
                                 : new Double(Math.sqrt(vk / (double) (n - 1)))
                      : new Double(Math.sqrt(vk / (double) (n)));
    }

    // end statistics support
}
