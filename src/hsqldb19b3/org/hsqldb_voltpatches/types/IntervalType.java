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


package org.hsqldb_voltpatches.types;

import java.math.BigDecimal;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.Session;

/**
 * Type subclass for various typs of INTERVAL.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class IntervalType extends DTIType {

    public final boolean defaultPrecision;
    public final boolean isYearMonth;

    private IntervalType(int typeGroup, int type, long precision, int scale,
                         int startIntervalType, int endIntervalType,
                         boolean defaultPrecision) {

        super(typeGroup, type, precision, scale, startIntervalType,
              endIntervalType);

        switch (startIntervalType) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_MONTH :
                isYearMonth = true;
                break;

            default :
                isYearMonth = false;
                break;
        }

        this.defaultPrecision = defaultPrecision;
    }

    public int displaySize() {

        switch (typeCode) {

            case Types.SQL_INTERVAL_YEAR :
                return (int) precision + 1;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                return (int) precision + 4;

            case Types.SQL_INTERVAL_MONTH :
                return (int) precision + 1;

            case Types.SQL_INTERVAL_DAY :
                return (int) precision + 1;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                return (int) precision + 4;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                return (int) precision + 7;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                return (int) precision + 10 + (scale == 0 ? 0
                                                          : scale + 1);

            case Types.SQL_INTERVAL_HOUR :
                return (int) precision + 1;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                return (int) precision + 4;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                return (int) precision + 7 + (scale == 0 ? 0
                                                         : scale + 1);

            case Types.SQL_INTERVAL_MINUTE :
                return (int) precision + 1;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                return (int) precision + 4 + (scale == 0 ? 0
                                                         : scale + 1);

            case Types.SQL_INTERVAL_SECOND :
                return (int) precision + 1 + (scale == 0 ? 0
                                                         : scale + 1);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public int getJDBCTypeCode() {

        // no JDBC number is available
        return typeCode;
    }

    public String getJDBCClassName() {

        switch (typeCode) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return IntervalMonthData.class.getName();

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
                return org.hsqldb_voltpatches.types.IntervalSecondData.class.getName();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public Integer getJDBCPrecision() {
        return this.displaySize();
    }

    public int getSQLGenericTypeCode() {
        return Types.SQL_INTERVAL;
    }

    public String getNameString() {
        return "INTERVAL " + getQualifier(typeCode);
    }

    public static String getQualifier(int type) {

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                return "YEAR";

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                return "YEAR TO MONTH";

            case Types.SQL_INTERVAL_MONTH :
                return "MONTH";

            case Types.SQL_INTERVAL_DAY :
                return "DAY";

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                return "DAY TO HOUR";

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                return "DAY TO MINUTE";

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                return "DAY TO SECOND";

            case Types.SQL_INTERVAL_HOUR :
                return "HOUR";

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                return "HOUR TO MINUTE";

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                return "HOUR TO SECOND";

            case Types.SQL_INTERVAL_MINUTE :
                return "MINUTE";

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                return "MINUTE TO SECOND";

            case Types.SQL_INTERVAL_SECOND :
                return "SECOND";

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public String getDefinition() {

        if (precision == defaultIntervalPrecision
                && (endIntervalType != Types.SQL_INTERVAL_SECOND
                    || scale == defaultIntervalFractionPrecision)) {
            return getNameString();
        }

        StringBuffer sb = new StringBuffer(32);

        sb.append(Tokens.T_INTERVAL).append(' ');
        sb.append(getQualifier(startIntervalType));

        if (typeCode == Types.SQL_INTERVAL_SECOND) {
            sb.append('(');
            sb.append(precision);

            if (scale != defaultIntervalFractionPrecision) {
                sb.append(',');
                sb.append(scale);
            }

            sb.append(')');

            return sb.toString();
        }

        if (precision != defaultIntervalPrecision) {
            sb.append('(');
            sb.append(precision);
            sb.append(')');
        }

        if (startIntervalType != endIntervalType) {
            sb.append(' ');
            sb.append(Tokens.T_TO);
            sb.append(' ');
            sb.append(Tokens.SQL_INTERVAL_FIELD_NAMES[endPartIndex]);

            if (endIntervalType == Types.SQL_INTERVAL_SECOND
                    && scale != defaultIntervalFractionPrecision) {
                sb.append('(');
                sb.append(scale);
                sb.append(')');
            }
        }

        return sb.toString();
    }

    public boolean isIntervalType() {
        return true;
    }

    public boolean isYearMonthIntervalType() {

        switch (typeCode) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return true;

            default :
                return false;
        }
    }

    public boolean isDaySecondIntervalType() {

        switch (typeCode) {

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
                return true;

            default :
                return false;
        }
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean acceptsFractionalPrecision() {
        return endIntervalType == Types.SQL_INTERVAL_SECOND;
    }

    public Type getAggregateType(Type other) {

        if (typeCode == other.typeCode) {
            if (precision >= other.precision && scale >= other.scale) {
                return this;
            } else if (precision <= other.precision && scale <= other.scale) {
                return other;
            }
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        if (other.isCharacterType()) {
            return other.getAggregateType(this);
        }

        if (!other.isIntervalType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        int startType = ((IntervalType) other).startIntervalType
                        > startIntervalType ? startIntervalType
                                            : ((IntervalType) other)
                                                .startIntervalType;
        int endType = ((IntervalType) other).endIntervalType > endIntervalType
                      ? ((IntervalType) other).endIntervalType
                      : endIntervalType;
        int  newType      = getCombinedIntervalType(startType, endType);
        long newPrecision = precision > other.precision ? precision
                                                        : other.precision;
        int  newScale     = scale > other.scale ? scale
                                                : other.scale;

        try {
            return getIntervalType(newType, startType, endType, newPrecision,
                                   newScale, false);
        } catch (RuntimeException e) {
            throw Error.error(ErrorCode.X_42562);
        }
    }

    public Type getCombinedType(Type other, int operation) {

        switch (operation) {

            case OpTypes.MULTIPLY :
                if (other.isNumberType()) {
                    return getIntervalType(this, maxIntervalPrecision, scale);
                }
                break;

            case OpTypes.DIVIDE :
                if (other.isNumberType()) {
                    return this;
                }
                break;

            case OpTypes.ADD :
                if (other.isDateTimeType()) {
                    return other.getCombinedType(this, operation);
                } else if (other.isIntervalType()) {
                    IntervalType newType =
                        (IntervalType) getAggregateType(other);

                    return getIntervalType(newType, maxIntervalPrecision, 0);
                }
                break;

            case OpTypes.SUBTRACT :
            default :
                return getAggregateType(other);
        }

        throw Error.error(ErrorCode.X_42562);
    }

    public int compare(Object a, Object b) {

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

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return ((IntervalMonthData) a).compareTo(
                    (IntervalMonthData) b);

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
                return ((IntervalSecondData) a).compareTo(
                    (IntervalSecondData) b);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof IntervalMonthData) {
            IntervalMonthData im = (IntervalMonthData) a;

            if (im.units > getIntervalValueLimit()) {
                throw Error.error(ErrorCode.X_22015);
            }
        } else if (a instanceof IntervalSecondData) {
            IntervalSecondData is = (IntervalSecondData) a;

            if (is.units > getIntervalValueLimit()) {
                throw Error.error(ErrorCode.X_22015);
            }

//            int divisor = nanoScaleFactors[scale];
//            is.nanos = (is.nanos / divisor) * divisor;
        }

        return a;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return null;
        }

        switch (otherType.typeCode) {

            case Types.SQL_CLOB :
                a = a.toString();

            // $FALL-THROUGH$
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE : {
                return session.getScanner().convertToDatetimeInterval(
                    (String) a, this);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                if (a instanceof BigDecimal) {
                    if (!NumberType.isInLongLimits((BigDecimal) a)) {
                        throw Error.error(ErrorCode.X_22015);
                    }
                }

                long value = ((Number) a).longValue();

                switch (this.endIntervalType) {

                    case Types.SQL_INTERVAL_YEAR :
                        return IntervalMonthData.newIntervalYear(value, this);

                    case Types.SQL_INTERVAL_MONTH :
                        return IntervalMonthData.newIntervalMonth(value, this);

                    case Types.SQL_INTERVAL_DAY :
                        return IntervalSecondData.newIntervalDay(value, this);

                    case Types.SQL_INTERVAL_HOUR :
                        return IntervalSecondData.newIntervalHour(value, this);

                    case Types.SQL_INTERVAL_MINUTE :
                        return IntervalSecondData.newIntervalMinute(value,
                                this);

                    case Types.SQL_INTERVAL_SECOND : {
                        int nanos = 0;

                        if (scale > 0 && a instanceof BigDecimal) {
                            nanos = (int) NumberType.scaledDecimal(
                                a, DTIType.maxFractionPrecision);
                        }

                        return new IntervalSecondData(value, nanos, this);
                    }
                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            }
            case Types.SQL_INTERVAL_YEAR : {
                long months = (((IntervalMonthData) a).units / 12) * 12;

                return new IntervalMonthData(months, this);
            }
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH : {
                long months = ((IntervalMonthData) a).units;

                return new IntervalMonthData(months, this);
            }
            case Types.SQL_INTERVAL_DAY : {
                long seconds = ((IntervalSecondData) a).units;

                seconds = (seconds / DTIType.yearToSecondFactors[2])
                          * DTIType.yearToSecondFactors[2];

                return new IntervalSecondData(seconds, 0, this);
            }
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_MINUTE : {
                long seconds = ((IntervalSecondData) a).units;

                seconds = (seconds / DTIType.yearToSecondFactors[endPartIndex])
                          * DTIType.yearToSecondFactors[endPartIndex];

                return new IntervalSecondData(seconds, 0, this);
            }
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
            case Types.SQL_INTERVAL_SECOND : {
                long seconds = ((IntervalSecondData) a).units;
                int  nanos   = ((IntervalSecondData) a).nanos;

                if (scale == 0) {
                    nanos = 0;
                } else {
                    nanos = (nanos / (DTIType.nanoScaleFactors[scale]))
                            * (DTIType.nanoScaleFactors[scale]);
                }

                return new IntervalSecondData(seconds, nanos, this);
            }
            default :
                throw Error.error(ErrorCode.X_42561);
        }
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof String) {
            return convertToType(null, a, Type.SQL_VARCHAR);
        } else {
            throw Error.error(ErrorCode.X_42561);
        }
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return intervalMonthToString(a);

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
                return intervalSecondToString(a);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        StringBuffer sb = new StringBuffer(32);

        sb.append(Tokens.T_INTERVAL);
        sb.append(StringConverter.toQuotedString(convertToString(a), '\'',
                false));
        sb.append(Tokens.SQL_INTERVAL_FIELD_NAMES[startPartIndex]);
        sb.append(' ');
        sb.append(Tokens.T_TO);
        sb.append(' ');
        sb.append(Tokens.SQL_INTERVAL_FIELD_NAMES[endPartIndex]);

        return sb.toString();
    }

    public boolean canConvertFrom(Type otherType) {

        if (otherType.typeCode == Types.SQL_ALL_TYPES) {
            return true;
        }

        if (otherType.isCharacterType()) {
            return true;
        }

        if (otherType.isNumberType()) {
            return true;
        }

        if (!otherType.isIntervalType()) {
            return false;
        }

        return !(isYearMonthIntervalType()
                 ^ ((IntervalType) otherType).isYearMonthIntervalType());
    }

    public Object absolute(Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof IntervalMonthData) {
            if (((IntervalMonthData) a).units < 0) {
                return negate(a);
            }
        } else {
            if (((IntervalSecondData) a).units < 0
                    || ((IntervalSecondData) a).nanos < 0) {
                return negate(a);
            }
        }

        return a;
    }

    public Object negate(Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof IntervalMonthData) {
            long units = ((IntervalMonthData) a).units;

            return new IntervalMonthData(-units, this);
        } else {
            long units = ((IntervalSecondData) a).units;
            int  nanos = ((IntervalSecondData) a).nanos;

            return new IntervalSecondData(-units, -nanos, this, true);
        }
    }

    public Object add(Object a, Object b, Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                long months = ((IntervalMonthData) a).units
                              + ((IntervalMonthData) b).units;

                return new IntervalMonthData(months, this);

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
                long seconds = ((IntervalSecondData) a).units
                               + ((IntervalSecondData) b).units;
                long nanos = ((IntervalSecondData) a).nanos
                             + ((IntervalSecondData) b).nanos;

                return new IntervalSecondData(seconds, nanos, this, true);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public Object subtract(Object a, Object b, Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                if (a instanceof IntervalMonthData
                        && b instanceof IntervalMonthData) {
                    long months = ((IntervalMonthData) a).units
                                  - ((IntervalMonthData) b).units;

                    return new IntervalMonthData(months, this);
                } else if (a instanceof TimestampData
                           && b instanceof TimestampData) {
                    boolean isYear = typeCode == Types.SQL_INTERVAL_YEAR;
                    long months =
                        DateTimeType.subtractMonths((TimestampData) a,
                                                    (TimestampData) b, isYear);

                    return new IntervalMonthData(months, this);
                }

                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
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
                if (a instanceof IntervalSecondData
                        && b instanceof IntervalSecondData) {
                    long seconds = ((IntervalSecondData) a).units
                                   - ((IntervalSecondData) b).units;
                    long nanos = ((IntervalSecondData) a).nanos
                                 - ((IntervalSecondData) b).nanos;

                    return new IntervalSecondData(seconds, nanos, this, true);
                } else if (a instanceof TimeData && b instanceof TimeData) {
                    long seconds = ((TimeData) a).getSeconds()
                                   - ((TimeData) b).getSeconds();
                    long nanos = ((TimeData) a).getNanos()
                                 - ((TimeData) b).getNanos();

                    return new IntervalSecondData(seconds, nanos, this, true);
                } else if (a instanceof TimestampData
                           && b instanceof TimestampData) {
                    long seconds = (((TimestampData) a).getSeconds()
                                    - ((TimestampData) b).getSeconds());
                    long nanos = ((TimestampData) a).getNanos()
                                 - ((TimestampData) b).getNanos();

                    return new IntervalSecondData(seconds, nanos, this, true);
                }

            // $FALL-THROUGH$
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public Object multiply(Object a, Object b) {
        return multiplyOrDivide(a, b, false);
    }

    public Object divide(Object a, Object b) {
        return multiplyOrDivide(a, b, true);
    }

    private Object multiplyOrDivide(Object a, Object b, boolean divide) {

        if (a == null || b == null) {
            return null;
        }

        if (a instanceof Number) {
            Object temp = a;

            a = b;
            b = temp;
        }

        if (divide && NumberType.isZero(b)) {
            throw Error.error(ErrorCode.X_22012);
        }

        NumberType factorType = NumberType.getNumberType(Types.SQL_DECIMAL,
            40, maxFractionPrecision);
        BigDecimal factor = (BigDecimal) factorType.convertToDefaultType(null,
            b);
        BigDecimal units;

        if (isYearMonth) {
            units = BigDecimal.valueOf(((IntervalMonthData) a).units);
        } else {
            long value =
                ((IntervalSecondData) a).units * DTIType.nanoScaleFactors[0]
                + ((IntervalSecondData) a).nanos;

            units = BigDecimal.valueOf(value, 9);
        }

        BigDecimal result = divide
                            ? (BigDecimal) factorType.divide(units, factor)
                            : (BigDecimal) factorType.multiply(units, factor);

        if (!NumberType.isInLongLimits(result)) {
            throw Error.error(ErrorCode.X_22015);
        }

        if (isYearMonth) {
            return new IntervalMonthData(result.longValue(), this);
        }

        int nanos = (int) NumberType.scaledDecimal(result,
            DTIType.maxFractionPrecision);

        return new IntervalSecondData(result.longValue(), nanos, this, true);
    }

    String intervalMonthToString(Object a) {

        StringBuffer sb     = new StringBuffer(8);
        long         months = ((IntervalMonthData) a).units;

        if (months < 0) {
            months = -months;

            sb.append('-');
        }

        for (int i = startPartIndex; i <= endPartIndex; i++) {
            int  factor = DTIType.yearToSecondFactors[i];
            long part   = months / factor;

            if (i == startPartIndex) {
                int zeros = (int) precision - getPrecisionExponent(part);
/*
                for (int j = 0; j < zeros; j++) {
                    buffer.append('0');
                }
*/
            } else if (part < 10) {
                sb.append('0');
            }

            sb.append(part);

            months %= factor;

            if (i < endPartIndex) {
                sb.append((char) DTIType.yearToSecondSeparators[i]);
            }
        }

        return sb.toString();
    }

    String intervalSecondToString(Object a) {

        long seconds = ((IntervalSecondData) a).units;
        int  nanos   = ((IntervalSecondData) a).nanos;

        return intervalSecondToString(seconds, nanos, false);
    }

    public int precedenceDegree(Type other) {

        if (other.isIntervalType()) {
            int otherIndex = ((IntervalType) other).endPartIndex;

            return otherIndex - endPartIndex;
        }

        return Integer.MIN_VALUE;
    }

    public int getStartIntervalType() {
        return startIntervalType;
    }

    public int getEndIntervalType() {
        return endIntervalType;
    }

    public static IntervalType newIntervalType(int type, long precision,
            int fractionPrecision) {

        int startType = getStartIntervalType(type);
        int endType   = getEndIntervalType(type);
        int group = startType > Types.SQL_INTERVAL_MONTH
                    ? Types.SQL_INTERVAL_SECOND
                    : Types.SQL_INTERVAL_MONTH;

        return new IntervalType(group, type, precision, fractionPrecision,
                                startType, endType, false);
    }

    public static IntervalType getIntervalType(IntervalType type,
            long precision, int fractionalPrecision) {

        if (type.precision >= precision && type.scale >= fractionalPrecision) {
            return type;
        }

        return getIntervalType(type.typeCode, precision, fractionalPrecision);
    }

    public static IntervalType getIntervalType(int type, long precision,
            int fractionPrecision) {

        int startType = getStartIntervalType(type);
        int endType   = getEndIntervalType(type);

        return getIntervalType(type, startType, endType, precision,
                               fractionPrecision, false);
    }

    public static IntervalType getIntervalType(int startIndex, int endIndex,
            long precision, int fractionPrecision) {

        boolean defaultPrecision = precision == -1;

        if (startIndex == -1 || endIndex == -1) {
            throw Error.error(ErrorCode.X_22006);
        }

        if (startIndex > endIndex) {
            throw Error.error(ErrorCode.X_22006);
        }

        if (startIndex <= DTIType.INTERVAL_MONTH_INDEX
                && endIndex > DTIType.INTERVAL_MONTH_INDEX) {
            throw Error.error(ErrorCode.X_22006);
        }

        int startType = DTIType.intervalParts[startIndex];
        int endType   = DTIType.intervalParts[endIndex];
        int type      = DTIType.intervalTypes[startIndex][endIndex];

        if (precision == 0 || precision > DTIType.maxIntervalPrecision
                || fractionPrecision > DTIType.maxFractionPrecision) {
            throw Error.error(ErrorCode.X_42592);
        }

        if (precision == -1) {
            precision = DTIType.defaultIntervalPrecision;
        }

        if (fractionPrecision == -1) {
            fractionPrecision = endType == Types.SQL_INTERVAL_SECOND
                                ? DTIType.defaultIntervalFractionPrecision
                                : 0;
        }

        return getIntervalType(type, startType, endType, precision,
                               fractionPrecision, defaultPrecision);
    }

    public static IntervalType getIntervalType(int type, int startType,
            int endType, long precision, int fractionPrecision,
            boolean defaultPrecision) {

        int group = startType > Types.SQL_INTERVAL_MONTH
                    ? Types.SQL_INTERVAL_SECOND
                    : Types.SQL_INTERVAL_MONTH;

        if (defaultPrecision) {
            return new IntervalType(group, type, precision, fractionPrecision,
                                    startType, endType, defaultPrecision);
        }

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_YEAR;
                }
                break;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_YEAR_TO_MONTH;
                }
                break;

            case Types.SQL_INTERVAL_MONTH :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_MONTH;
                }
                break;

            case Types.SQL_INTERVAL_DAY :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_DAY;
                }
                break;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_DAY_TO_HOUR;
                }
                break;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_DAY_TO_MINUTE;
                }
                break;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                if (precision == DTIType.defaultIntervalPrecision
                        && fractionPrecision
                           == DTIType.defaultIntervalFractionPrecision) {
                    return SQL_INTERVAL_DAY_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_HOUR :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_HOUR;
                }
                break;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_HOUR_TO_MINUTE;
                }
                break;

            case Types.SQL_INTERVAL_MINUTE :
                if (precision == DTIType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_MINUTE;
                }
                break;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                if (precision == DTIType.defaultIntervalPrecision
                        && fractionPrecision
                           == DTIType.defaultIntervalFractionPrecision) {
                    return SQL_INTERVAL_HOUR_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                if (precision == DTIType.defaultIntervalPrecision
                        && fractionPrecision
                           == DTIType.defaultIntervalFractionPrecision) {
                    return SQL_INTERVAL_MINUTE_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_SECOND :
                if (precision == DTIType.defaultIntervalPrecision
                        && fractionPrecision
                           == DTIType.defaultIntervalFractionPrecision) {
                    return SQL_INTERVAL_SECOND;
                }
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }

        return new IntervalType(group, type, precision, fractionPrecision,
                                startType, endType, defaultPrecision);
    }

    public static int getStartIntervalType(int type) {

        int startType;

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                startType = Types.SQL_INTERVAL_YEAR;
                break;

            case Types.SQL_INTERVAL_MONTH :
                startType = Types.SQL_INTERVAL_MONTH;
                break;

            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                startType = Types.SQL_INTERVAL_DAY;
                break;

            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                startType = Types.SQL_INTERVAL_HOUR;
                break;

            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                startType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_SECOND :
                startType = Types.SQL_INTERVAL_SECOND;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }

        return startType;
    }

    public static int getEndIntervalType(int type) {

        int endType;

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                endType = Types.SQL_INTERVAL_YEAR;
                break;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                endType = Types.SQL_INTERVAL_MONTH;
                break;

            case Types.SQL_INTERVAL_MONTH :
                endType = Types.SQL_INTERVAL_MONTH;
                break;

            case Types.SQL_INTERVAL_DAY :
                endType = Types.SQL_INTERVAL_DAY;
                break;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                endType = Types.SQL_INTERVAL_HOUR;
                break;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                endType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            case Types.SQL_INTERVAL_HOUR :
                endType = Types.SQL_INTERVAL_HOUR;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                endType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                endType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            case Types.SQL_INTERVAL_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }

        return endType;
    }

    public static Type getCombinedIntervalType(IntervalType type1,
            IntervalType type2) {

        int startType = type2.startIntervalType > type1.startIntervalType
                        ? type1.startIntervalType
                        : type2.startIntervalType;
        int endType = type2.endIntervalType > type1.endIntervalType
                      ? type2.endIntervalType
                      : type1.endIntervalType;
        int  type              = getCombinedIntervalType(startType, endType);
        long precision = type1.precision > type2.precision ? type1.precision
                                                           : type2.precision;
        int  fractionPrecision = type1.scale > type2.scale ? type1.scale
                                                           : type2.scale;

        return getIntervalType(type, startType, endType, precision,
                               fractionPrecision, false);
    }

    public static int getCombinedIntervalType(int startType, int endType) {

        if (startType == endType) {
            return startType;
        }

        switch (startType) {

            case Types.SQL_INTERVAL_YEAR :
                if (endType == Types.SQL_INTERVAL_MONTH) {
                    return Types.SQL_INTERVAL_YEAR_TO_MONTH;
                }
                break;

            case Types.SQL_INTERVAL_DAY :
                switch (endType) {

                    case Types.SQL_INTERVAL_HOUR :
                        return Types.SQL_INTERVAL_DAY_TO_HOUR;

                    case Types.SQL_INTERVAL_MINUTE :
                        return Types.SQL_INTERVAL_DAY_TO_MINUTE;

                    case Types.SQL_INTERVAL_SECOND :
                        return Types.SQL_INTERVAL_DAY_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_HOUR :
                switch (endType) {

                    case Types.SQL_INTERVAL_MINUTE :
                        return Types.SQL_INTERVAL_HOUR_TO_MINUTE;

                    case Types.SQL_INTERVAL_SECOND :
                        return Types.SQL_INTERVAL_HOUR_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_MINUTE :
                if (endType == Types.SQL_INTERVAL_SECOND) {
                    return Types.SQL_INTERVAL_MINUTE_TO_SECOND;
                }
                break;

            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
    }

    long getIntervalValueLimit() {

        long limit;

        switch (typeCode) {

            case Types.SQL_INTERVAL_YEAR :
                limit = DTIType.precisionLimits[(int) precision] * 12;
                break;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                limit = DTIType.precisionLimits[(int) precision] * 12;
                limit += 12;
                break;

            case Types.SQL_INTERVAL_MONTH :
                limit = DTIType.precisionLimits[(int) precision];
                break;

            case Types.SQL_INTERVAL_DAY :
                limit = DTIType.precisionLimits[(int) precision] * 24 * 60
                        * 60;
                break;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                limit = DTIType.precisionLimits[(int) precision] * 24 * 60
                        * 60;
                break;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                limit = DTIType.precisionLimits[(int) precision] * 24 * 60
                        * 60;
                break;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                limit = DTIType.precisionLimits[(int) precision] * 24 * 60
                        * 60;
                break;

            case Types.SQL_INTERVAL_HOUR :
                limit = DTIType.precisionLimits[(int) precision] * 60 * 60;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                limit = DTIType.precisionLimits[(int) precision] * 60 * 60;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                limit = DTIType.precisionLimits[(int) precision] * 60 * 60;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                limit = DTIType.precisionLimits[(int) precision] * 60;
                break;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                limit = DTIType.precisionLimits[(int) precision] * 60;
                break;

            case Types.SQL_INTERVAL_SECOND :
                limit = DTIType.precisionLimits[(int) precision];
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }

        return limit;
    }

    public int getPart(Session session, Object interval, int part) {

        long units;

        switch (part) {

            case Types.SQL_INTERVAL_YEAR :
                return (int) (((IntervalMonthData) interval).units / 12);

            case Types.SQL_INTERVAL_MONTH :
                units = ((IntervalMonthData) interval).units;

                return part == startIntervalType ? (int) units
                                                 : (int) (units % 12);

            case Types.SQL_INTERVAL_DAY :
                return (int) (((IntervalSecondData) interval).units
                              / (24 * 60 * 60));

            case Types.SQL_INTERVAL_HOUR : {
                units = ((IntervalSecondData) interval).units / (60 * 60);

                return part == startIntervalType ? (int) units
                                                 : (int) (units % 24);
            }
            case Types.SQL_INTERVAL_MINUTE : {
                units = ((IntervalSecondData) interval).units / 60;

                return part == startIntervalType ? (int) units
                                                 : (int) (units % 60);
            }
            case Types.SQL_INTERVAL_SECOND : {
                units = ((IntervalSecondData) interval).units;

                return part == startIntervalType ? (int) units
                                                 : (int) (units % 60);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }

    public long getSeconds(Object interval) {
        return ((IntervalSecondData) interval).units;
    }

    public BigDecimal getSecondPart(Object interval) {

        long seconds = ((IntervalSecondData) interval).units;

        if (typeCode != Types.SQL_INTERVAL_SECOND) {
            seconds %= 60;
        }

        int nanos = ((IntervalSecondData) interval).nanos;

        return getSecondPart(seconds, nanos);
    }

    public long convertToLong(Object interval) {

        switch (endIntervalType) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_MONTH :
                long months = ((IntervalMonthData) interval).units;

                return (months / DTIType.yearToSecondFactors[endPartIndex]);

            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_SECOND : {
                long seconds = ((IntervalSecondData) interval).units;

                return (seconds / DTIType.yearToSecondFactors[endPartIndex]);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "IntervalType");
        }
    }
}
