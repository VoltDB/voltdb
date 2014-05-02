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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlDateTime;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.StringConverter;

/**
 * Type subclass for DATE, TIME and TIMESTAMP.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class DateTimeType extends DTIType {

    public final boolean withTimeZone;

    public DateTimeType(int typeGroup, int type, int scale) {

        // A VoltDB extension -- mysterious (BACKOUT?)
        super(typeGroup, type, 8, scale);
        /* disable 1 line ...
        super(typeGroup, type, 0, scale);
        ... disabled 1 line */
        // End of VoltDB extension

        withTimeZone = type == Types.SQL_TIME_WITH_TIME_ZONE
                       || type == Types.SQL_TIMESTAMP_WITH_TIME_ZONE;
    }

    public int displaySize() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return 10;

            case Types.SQL_TIME :
                return 8 + (scale == 0 ? 0
                                       : scale + 1);

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return 8 + (scale == 0 ? 0
                                       : scale + 1) + 6;

            case Types.SQL_TIMESTAMP :
                return 19 + (scale == 0 ? 0
                                        : scale + 1);

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return 19 + (scale == 0 ? 0
                                        : scale + 1) + 6;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public int getJDBCTypeCode() {

        // JDBC numbers happen to be the same as SQL
        return typeCode;
    }

    public String getJDBCClassName() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return "java.sql.Date";

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return "java.sql.Time";

            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return "java.sql.Timestamp";

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Integer getJDBCPrecision() {
        return this.displaySize();
    }

    public int getSQLGenericTypeCode() {
        return Types.SQL_DATETIME;
    }

    public String getNameString() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return Tokens.T_DATE;

            case Types.SQL_TIME :
                return Tokens.T_TIME;

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return Tokens.T_TIME + ' ' + Tokens.T_WITH + ' '
                       + Tokens.T_TIME + ' ' + Tokens.T_ZONE;

            case Types.SQL_TIMESTAMP :
                return Tokens.T_TIMESTAMP;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return Tokens.T_TIMESTAMP + ' ' + Tokens.T_WITH + ' '
                       + Tokens.T_TIME + ' ' + Tokens.T_ZONE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public String getDefinition() {

        if (scale == DTIType.defaultTimeFractionPrecision) {
            return getNameString();
        }

        String token;

        switch (typeCode) {

            case Types.SQL_DATE :
                return Tokens.T_DATE;

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                if (scale == DTIType.defaultTimeFractionPrecision) {
                    return getNameString();
                }

                token = Tokens.T_TIME;
                break;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                if (scale == DTIType.defaultTimestampFractionPrecision) {
                    return getNameString();
                }

                token = Tokens.T_TIMESTAMP;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(token);
        sb.append('(');
        sb.append(scale);
        sb.append(')');

        if (withTimeZone) {
            sb.append(' ' + Tokens.T_WITH + ' ' + Tokens.T_TIME + ' '
                      + Tokens.T_ZONE);
        }

        return sb.toString();
    }

    public boolean isDateTimeType() {
        return true;
    }

    public boolean isDateTimeTypeWithZone() {
        return withTimeZone;
    }

    public boolean acceptsFractionalPrecision() {
        return typeCode != Types.SQL_DATE;
    }

    public Type getAggregateType(Type other) {

        // DATE with DATE returned here
        if (typeCode == other.typeCode) {
            return scale >= other.scale ? this
                                        : other;
        }

        if (other.typeCode == Types.SQL_ALL_TYPES) {
            return this;
        }

        if (other.isCharacterType()) {
            return other.getAggregateType(this);
        }

        if (!other.isDateTimeType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        DateTimeType otherType = (DateTimeType) other;

        // DATE with TIME caught here
        if (otherType.startIntervalType > endIntervalType
                || startIntervalType > otherType.endIntervalType) {
            throw Error.error(ErrorCode.X_42562);
        }

        int     newType = typeCode;
        int     scale   = this.scale > otherType.scale ? this.scale
                                                       : otherType.scale;
        boolean zone    = withTimeZone || otherType.withTimeZone;
        int startType = otherType.startIntervalType > startIntervalType
                        ? startIntervalType
                        : otherType.startIntervalType;

        if (startType == Types.SQL_INTERVAL_HOUR) {
            newType = zone ? Types.SQL_TIME_WITH_TIME_ZONE
                           : Types.SQL_TIME;
        } else {
            newType = zone ? Types.SQL_TIMESTAMP_WITH_TIME_ZONE
                           : Types.SQL_TIMESTAMP;
        }

        return getDateTimeType(newType, scale);
    }

    public Type getCombinedType(Type other, int operation) {

        switch (operation) {

            case OpTypes.EQUAL :
            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.SMALLER_EQUAL :
            case OpTypes.SMALLER :
            case OpTypes.NOT_EQUAL : {
                if (typeCode == other.typeCode) {
                    return this;
                }

                if (other.typeCode == Types.SQL_ALL_TYPES) {
                    return this;
                }

                if (!other.isDateTimeType()) {
                    throw Error.error(ErrorCode.X_42562);
                }

                DateTimeType otherType = (DateTimeType) other;

                // DATE with TIME caught here
                if (otherType.startIntervalType > endIntervalType
                        || startIntervalType > otherType.endIntervalType) {
                    throw Error.error(ErrorCode.X_42562);
                }

                int     newType = typeCode;
                int     scale   = this.scale > otherType.scale ? this.scale
                                                               : otherType
                                                                   .scale;
                boolean zone    = withTimeZone || otherType.withTimeZone;
                int startType = otherType.startIntervalType
                                > startIntervalType ? startIntervalType
                                                    : otherType
                                                        .startIntervalType;

                if (startType == Types.SQL_INTERVAL_HOUR) {
                    newType = zone ? Types.SQL_TIME_WITH_TIME_ZONE
                                   : Types.SQL_TIME;
                } else {
                    newType = zone ? Types.SQL_TIMESTAMP_WITH_TIME_ZONE
                                   : Types.SQL_TIMESTAMP;
                }

                return getDateTimeType(newType, scale);
            }
            case OpTypes.ADD :
            case OpTypes.SUBTRACT :
                if (other.isIntervalType()) {
                    if (typeCode != Types.SQL_DATE && other.scale > scale) {
                        return getDateTimeType(typeCode, other.scale);
                    }

                    return this;
                }
                break;

            default :
        }

        throw Error.error(ErrorCode.X_42562);
    }

    public int compare(Object a, Object b) {

        long diff;

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

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE : {
                diff = ((TimeData) a).getSeconds()
                       - ((TimeData) b).getSeconds();

                if (diff == 0) {
                    diff = ((TimeData) a).getNanos()
                           - ((TimeData) b).getNanos();
                }

                return diff == 0 ? 0
                                 : diff > 0 ? 1
                                            : -1;
            }
            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                diff = ((TimestampData) a).getSeconds()
                       - ((TimestampData) b).getSeconds();

                if (diff == 0) {
                    diff = ((TimestampData) a).getNanos()
                           - ((TimestampData) b).getNanos();
                }

                return diff == 0 ? 0
                                 : diff > 0 ? 1
                                            : -1;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (scale == maxFractionPrecision) {
            return a;
        }

        switch (typeCode) {

            case Types.SQL_DATE :
                return a;

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME : {
                TimeData ti       = (TimeData) a;
                int      nanos    = ti.getNanos();
                int      newNanos = scaleNanos(nanos);

                if (newNanos == nanos) {
                    return ti;
                }

                return new TimeData(ti.getSeconds(), newNanos, ti.getZone());
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP : {
                TimestampData ts       = (TimestampData) a;
                int           nanos    = ts.getNanos();
                int           newNanos = scaleNanos(nanos);

                if (newNanos == nanos) {
                    return ts;
                }

                return new TimestampData(ts.getSeconds(), newNanos,
                                         ts.getZone());
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    int scaleNanos(int nanos) {

        int divisor = nanoScaleFactors[scale];

        return (nanos / divisor) * divisor;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return a;
        }

        switch (otherType.typeCode) {

            // A VoltDB extension to enable integer-to-timestamp conversion
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                // Assuming time provided is UTC. Can't use Timestamp.toString() since
                // it formats for the local timezone.
                // DateFormatUtils.formatUTC() provides a UTC-formatted time string,
                // but has a bug that fails to add leading zeros to the milliseconds.
                // Work around the bug by splicing in correctly-formatted milliseconds.
                long ts = Long.parseLong(a.toString());
                Date d = new Date(ts);
                String utc = org.apache.commons.lang3.time.DateFormatUtils.formatUTC(d, "yyyy-MM-dd HH:mm:ss");
                a = String.format("%s.%03d", utc, ts % 1000);
                // $FALL-THROUGH$
            // End of VoltDB extension
            case Types.SQL_CLOB :
                a = a.toString();

            //fall through
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                switch (this.typeCode) {

                    case Types.SQL_DATE :
                    case Types.SQL_TIME_WITH_TIME_ZONE :
                    case Types.SQL_TIME :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                    case Types.SQL_TIMESTAMP : {
                        a = session.getScanner().convertToDatetimeInterval(
                            (String) a, this);

                        return convertToTypeLimits(session, a);
                    }
                }
                break;

            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                break;

            default :
                throw Error.error(ErrorCode.X_42561);
        }

        switch (this.typeCode) {

            case Types.SQL_DATE :
                switch (otherType.typeCode) {

                    case Types.SQL_DATE :
                        return a;

                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        long seconds = ((TimestampData) a).getSeconds()
                                       + ((TimestampData) a).getZone();
                        long l = HsqlDateTime.getNormalisedDate(seconds
                            * 1000);

                        return new TimestampData(l / 1000);
                    }
                    case Types.SQL_TIMESTAMP : {
                        long l = HsqlDateTime.getNormalisedDate(
                            ((TimestampData) a).getSeconds() * 1000);

                        return new TimestampData(l / 1000);
                    }
                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIME_WITH_TIME_ZONE :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_TIME : {
                        TimeData ti = (TimeData) a;

                        return new TimeData(
                            ti.getSeconds() - session.getZoneSeconds(),
                            scaleNanos(ti.getNanos()),
                            session.getZoneSeconds());
                    }
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        TimestampData ts = (TimestampData) a;
                        long seconds =
                            HsqlDateTime.convertToNormalisedTime(
                                ts.getSeconds() * 1000) / 1000;

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.getNanos()),
                                            ts.getZone());
                    }
                    case Types.SQL_TIMESTAMP : {
                        TimestampData ts = (TimestampData) a;
                        long seconds = ts.getSeconds()
                                       - session.getZoneSeconds();

                        seconds =
                            HsqlDateTime.convertToNormalisedTime(
                                seconds * 1000) / 1000;

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.getNanos()),
                                            session.getZoneSeconds());
                    }
                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIME :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE : {
                        TimeData ti = (TimeData) a;

                        return new TimeData(ti.getSeconds() + ti.getZone(),
                                            scaleNanos(ti.getNanos()), 0);
                    }
                    case Types.SQL_TIME :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        TimestampData ts      = (TimestampData) a;
                        long          seconds = ts.getSeconds() + ts.getZone();

                        seconds =
                            HsqlDateTime.convertToNormalisedTime(
                                seconds * 1000) / 1000;

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.getNanos()), 0);
                    }
                    case Types.SQL_TIMESTAMP :
                        TimestampData ts = (TimestampData) a;
                        long seconds =
                            HsqlDateTime.convertToNormalisedTime(
                                ts.getSeconds() * 1000) / 1000;

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.getNanos()));

                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE : {
                        TimeData ti = (TimeData) a;
                        long seconds = session.getCurrentDate().getSeconds()
                                       + ti.getSeconds();

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.getNanos()),
                                                 ti.getZone());
                    }
                    case Types.SQL_TIME : {
                        TimeData ti = (TimeData) a;
                        long seconds = session.getCurrentDate().getSeconds()
                                       + ti.getSeconds()
                                       - session.getZoneSeconds();

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.getNanos()),
                                                 session.getZoneSeconds());
                    }
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_TIMESTAMP : {
                        TimestampData ts = (TimestampData) a;
                        long seconds = ts.getSeconds()
                                       - session.getZoneSeconds();

                        return new TimestampData(seconds,
                                                 scaleNanos(ts.getNanos()),
                                                 session.getZoneSeconds());
                    }
                    case Types.SQL_DATE : {
                        TimestampData ts = (TimestampData) a;

                        return new TimestampData(ts.getSeconds(),
                                                 session.getZoneSeconds());
                    }
                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIMESTAMP :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE : {
                        TimeData ti = (TimeData) a;
                        long seconds = session.getCurrentDate().getSeconds()
                                       + ti.getSeconds()
                                       - session.getZoneSeconds();

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.getNanos()),
                                                 session.getZoneSeconds());
                    }
                    case Types.SQL_TIME : {
                        TimeData ti = (TimeData) a;
                        long seconds = session.getCurrentDate().getSeconds()
                                       + ti.getSeconds();

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.getNanos()));
                    }
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        TimestampData ts      = (TimestampData) a;
                        long          seconds = ts.getSeconds() + ts.getZone();

                        return new TimestampData(seconds,
                                                 scaleNanos(ts.getNanos()));
                    }
                    case Types.SQL_TIMESTAMP :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_DATE :
                        return a;

                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {
        throw Error.error(ErrorCode.X_42561);
    }

    /** @todo - do the time zone */
    public Object convertJavaToSQL(SessionInterface session, Object a) {

        switch (typeCode) {

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                if (a instanceof java.sql.Date) {
                    break;
                }

                if (a instanceof java.util.Date) {
                    long seconds =
                        HsqlDateTime.getNormalisedTime(
                            ((java.util.Date) a).getTime()) / 1000;;
                    int zoneSeconds;
                    int nanos = 0;

                    if (this.isDateTimeTypeWithZone()) {
                        zoneSeconds = session.getZoneSeconds();
                    } else {
                        zoneSeconds = 0;
                        seconds     += session.getZoneSeconds();
                    }

                    if (a instanceof java.sql.Timestamp) {
                        nanos = ((java.sql.Timestamp) a).getNanos();
                        nanos = this.normaliseFraction(nanos, (int) scale);
                    }

                    return new TimeData((int) seconds, nanos, zoneSeconds);
                }
                break;

            case Types.SQL_DATE : {
                if (a instanceof java.sql.Time) {
                    break;
                }

                if (a instanceof java.util.Date) {
                    long nanos = ((java.util.Date) a).getTime()
                                 + session.getZoneSeconds() * 1000;

                    return new TimestampData(
                        HsqlDateTime.getNormalisedDate(nanos) / 1000);
                }

                break;
            }
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                if (a instanceof java.sql.Time) {
                    break;
                }

                if (a instanceof java.util.Date) {
                    long seconds = ((java.util.Date) a).getTime() / 1000;
                    int  zoneSeconds;
                    int  nanos = 0;

                    if (this.isDateTimeTypeWithZone()) {
                        zoneSeconds = session.getZoneSeconds();
                    } else {
                        zoneSeconds = 0;
                        seconds     += session.getZoneSeconds();
                    }

                    if (a instanceof java.sql.Timestamp) {
                        nanos = ((java.sql.Timestamp) a).getNanos();
                        nanos = this.normaliseFraction(nanos, (int) scale);
                    }

                    return new TimestampData(seconds, nanos, zoneSeconds);
                }

                break;
            }
        }

        throw Error.error(ErrorCode.X_42561);
    }

    /*
                return (Time) Type.SQL_TIME.getJavaDateObject(session, t, 0);
        java.sql.Timestamp value = new java.sql.Timestamp(
            (((TimestampData) o).getSeconds() - ((TimestampData) o)
                .getZone()) * 1000);
    */
    public Object convertSQLToJavaGMT(SessionInterface session, Object a) {

        long millis;

        switch (typeCode) {

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                millis = ((TimeData) a).getSeconds() * 1000;

                return new java.sql.Time(millis);

            case Types.SQL_DATE :
                millis = ((TimestampData) a).getSeconds() * 1000;

                return new java.sql.Date(millis);

            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                millis = ((TimestampData) a).getSeconds() * 1000;

                java.sql.Timestamp value = new java.sql.Timestamp(millis);

                value.setNanos(((TimestampData) a).getNanos());

                return value;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {

        switch (typeCode) {

            case Types.SQL_TIME : {
                int seconds = ((TimeData) a).getSeconds()
                              - session.getZoneSeconds();

                seconds = normaliseTime(seconds);

                return new java.sql.Time(seconds * 1000);
            }
            case Types.SQL_TIME_WITH_TIME_ZONE : {
                int seconds = ((TimeData) a).getSeconds();

                return new java.sql.Time(seconds * 1000);
            }
            case Types.SQL_DATE : {
                long seconds = ((TimestampData) a).getSeconds()
                               - session.getZoneSeconds();

                return new java.sql.Date(seconds * 1000);
            }
            case Types.SQL_TIMESTAMP : {
                long seconds = ((TimestampData) a).getSeconds()
                               - session.getZoneSeconds();
                java.sql.Timestamp value = new java.sql.Timestamp(seconds
                    * 1000);

                value.setNanos(((TimestampData) a).getNanos());

                return value;
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                long seconds = ((TimestampData) a).getSeconds();
                java.sql.Timestamp value = new java.sql.Timestamp(seconds
                    * 1000);

                value.setNanos(((TimestampData) a).getNanos());

                return value;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public static int normaliseTime(int seconds) {

        while (seconds < 0) {
            seconds += 24 * 60 * 60;
        }

        if (seconds > 24 * 60 * 60) {
            seconds %= 24 * 60 * 60;
        }

        return seconds;
    }

    public String convertToString(Object a) {

        boolean      zone = false;
        String       s;
        StringBuffer sb;

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_DATE :
                return HsqlDateTime.getDateString(
                    ((TimestampData) a).getSeconds());

            case Types.SQL_TIME_WITH_TIME_ZONE :
                zone = true;

            // $FALL-THROUGH$
            case Types.SQL_TIME : {
                TimeData t       = (TimeData) a;
                int      seconds = normaliseTime(t.getSeconds() + t.getZone());

                s = intervalSecondToString(seconds, t.getNanos(), false);

                if (!zone) {
                    return s;
                }

                sb = new StringBuffer(s);
                s = Type.SQL_INTERVAL_HOUR_TO_MINUTE.intervalSecondToString(
                    ((TimeData) a).getZone(), 0, true);

                sb.append(s);

                return sb.toString();
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                zone = true;

            // $FALL-THROUGH$
            case Types.SQL_TIMESTAMP : {
                TimestampData ts = (TimestampData) a;

                sb = new StringBuffer();

                HsqlDateTime.getTimestampString(sb,
                                                ts.getSeconds()
                                                + ts.getZone(), ts.getNanos(),
                                                    scale);

                if (!zone) {
                    return sb.toString();
                }

                s = Type.SQL_INTERVAL_HOUR_TO_MINUTE.intervalSecondToString(
                    ((TimestampData) a).getZone(), 0, true);

                sb.append(s);

                return sb.toString();
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        StringBuffer sb = new StringBuffer(32);

        switch (typeCode) {

            case Types.SQL_DATE :
                sb.append(Tokens.T_DATE);
                break;

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                sb.append(Tokens.T_TIME);
                break;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                sb.append(Tokens.T_TIMESTAMP);
                break;
        }

        sb.append(StringConverter.toQuotedString(convertToString(a), '\'',
                false));

        return sb.toString();
    }

    public boolean canConvertFrom(Type otherType) {

        if (otherType.typeCode == Types.SQL_ALL_TYPES) {
            return true;
        }

        if (otherType.isCharacterType()) {
            return true;
        }

        if (!otherType.isDateTimeType()) {
            return false;
        }

        if (otherType.typeCode == Types.SQL_DATE) {
            return typeCode != Types.SQL_TIME;
        } else if (otherType.typeCode == Types.SQL_TIME) {
            return typeCode != Types.SQL_DATE;
        }

        return true;
    }

    public Object add(Object a, Object b, Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            /** @todo -  range checks for units added */
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                if (b instanceof IntervalMonthData) {
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "DateTimeType");
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimeData) a,
                                      (int) ((IntervalSecondData) b).units,
                                      ((IntervalSecondData) b).nanos);
                }
                break;

            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                if (b instanceof IntervalMonthData) {
                    return addMonths((TimestampData) a,
                                     (int) ((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimestampData) a,
                                      (int) ((IntervalSecondData) b).units,
                                      ((IntervalSecondData) b).nanos);
                }
                break;

            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
    }

    public Object subtract(Object a, Object b, Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                if (b instanceof IntervalMonthData) {
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "DateTimeType");
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimeData) a,
                                      -(int) ((IntervalSecondData) b).units,
                                      -((IntervalSecondData) b).nanos);
                }
                break;

            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                if (b instanceof IntervalMonthData) {
                    return addMonths((TimestampData) a,
                                     -(int) ((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimestampData) a,
                                      -(int) ((IntervalSecondData) b).units,
                                      -((IntervalSecondData) b).nanos);
                }
                break;

            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
    }

    public boolean equals(Object other) {

        if (other instanceof Type) {
            return super.equals(other)
                   && ((DateTimeType) other).withTimeZone == withTimeZone;
        }

        return false;
    }

    public int getPart(Session session, Object dateTime, int part) {

        int calendarPart;
        int increment = 0;
        int divisor   = 1;

        switch (part) {

            case Types.SQL_INTERVAL_YEAR :
                calendarPart = Calendar.YEAR;
                break;

            case Types.SQL_INTERVAL_MONTH :
                increment    = 1;
                calendarPart = Calendar.MONTH;
                break;

            case Types.SQL_INTERVAL_DAY :
            case DAY_OF_MONTH :
                calendarPart = Calendar.DAY_OF_MONTH;
                break;

            case Types.SQL_INTERVAL_HOUR :
                calendarPart = Calendar.HOUR_OF_DAY;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                calendarPart = Calendar.MINUTE;
                break;

            case Types.SQL_INTERVAL_SECOND :
                calendarPart = Calendar.SECOND;
                break;

            case DAY_OF_WEEK :
                calendarPart = Calendar.DAY_OF_WEEK;
                break;

            case WEEK_OF_YEAR :
                calendarPart = Calendar.WEEK_OF_YEAR;
                break;

            case SECONDS_MIDNIGHT : {
                if (typeCode == Types.SQL_TIME
                        || typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {}
                else {
                    try {
                        Type target = withTimeZone
                                      ? Type.SQL_TIME_WITH_TIME_ZONE
                                      : Type.SQL_TIME;

                        dateTime = target.castToType(session, dateTime, this);
                    } catch (HsqlException e) {}
                }

                return ((TimeData) dateTime).getSeconds();
            }
            case TIMEZONE_HOUR :
                if (typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    return ((TimestampData) dateTime).getZone() / 3600;
                } else {
                    return ((TimeData) dateTime).getZone() / 3600;
                }
            case TIMEZONE_MINUTE :
                if (typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    return ((TimestampData) dateTime).getZone() / 60 % 60;
                } else {
                    return ((TimeData) dateTime).getZone() / 60 % 60;
                }
            case QUARTER :
                increment    = 1;
                divisor      = 3;
                calendarPart = Calendar.MONTH;
                break;

            case DAY_OF_YEAR :
                calendarPart = Calendar.DAY_OF_YEAR;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "IntervalType - " + part);
        }

        long millis;

        if (typeCode == Types.SQL_TIME
                || typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
            millis =
                (((TimeData) dateTime).getSeconds() + ((TimeData) dateTime)
                    .getZone()) * 1000;
        } else {
            millis =
                (((TimestampData) dateTime)
                    .getSeconds() + ((TimestampData) dateTime).getZone()) * 1000;
        }

        return HsqlDateTime.getDateTimePart(millis, calendarPart) / divisor
               + increment;
    }

    public BigDecimal getSecondPart(Object dateTime) {

        long seconds = getPart(null, dateTime, Types.SQL_INTERVAL_SECOND);
        int  nanos   = 0;

        if (typeCode == Types.SQL_TIMESTAMP) {
            nanos = ((TimestampData) dateTime).getNanos();
        } else if (typeCode == Types.SQL_TIME) {
            nanos = ((TimeData) dateTime).getNanos();
        }

        return getSecondPart(seconds, nanos);
    }

    public String getPartString(Session session, Object dateTime, int part) {

        String javaPattern = "";

        switch (part) {

            case DAY_NAME :
                javaPattern = "EEEE";
                break;

            case MONTH_NAME :
                javaPattern = "MMMM";
                break;
        }

        SimpleDateFormat format = session.getSimpleDateFormatGMT();

        try {
            format.applyPattern(javaPattern);
        } catch (Exception e) {}

        Date date = (Date) convertSQLToJavaGMT(session, dateTime);

        return format.format(date);
    }

    public Object getValue(long seconds, int nanos, int zoneSeconds) {

        switch (typeCode) {

            case Types.SQL_DATE :
                seconds =
                    HsqlDateTime.getNormalisedDate(
                        (seconds + zoneSeconds) * 1000) / 1000;

                return new TimestampData(seconds);

            case Types.SQL_TIME_WITH_TIME_ZONE :
                seconds = HsqlDateTime.getNormalisedDate(seconds * 1000)
                          / 1000;

                return new TimeData((int) seconds, nanos, zoneSeconds);

            case Types.SQL_TIME :
                seconds =
                    HsqlDateTime.getNormalisedTime(
                        (seconds + zoneSeconds) * 1000) / 1000;

                return new TimeData((int) seconds, nanos);

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return new TimestampData(seconds, nanos, zoneSeconds);

            case Types.SQL_TIMESTAMP :
                return new TimestampData(seconds + zoneSeconds, nanos);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public static DateTimeType getDateTimeType(int type, int scale) {

        if (scale > DTIType.maxFractionPrecision) {
            throw Error.error(ErrorCode.X_42592);
        }

        switch (type) {

            case Types.SQL_DATE :
                return SQL_DATE;

            case Types.SQL_TIME :
                if (scale != DTIType.defaultTimeFractionPrecision) {
                    return new DateTimeType(Types.SQL_TIME, type, scale);
                }

                return SQL_TIME;

            case Types.SQL_TIME_WITH_TIME_ZONE :
                if (scale != DTIType.defaultTimeFractionPrecision) {
                    return new DateTimeType(Types.SQL_TIME, type, scale);
                }

                return SQL_TIME_WITH_TIME_ZONE;

            case Types.SQL_TIMESTAMP :
                if (scale != DTIType.defaultTimestampFractionPrecision) {
                    return new DateTimeType(Types.SQL_TIMESTAMP, type, scale);
                }

                return SQL_TIMESTAMP;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                if (scale != DTIType.defaultTimestampFractionPrecision) {
                    return new DateTimeType(Types.SQL_TIMESTAMP, type, scale);
                }

                return SQL_TIMESTAMP_WITH_TIME_ZONE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Object changeZone(Object a, Type otherType, int targetZone,
                             int localZone) {

        if (a == null) {
            return null;
        }

        if (otherType.typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE
                || otherType.typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
            localZone = 0;
        }

        if (targetZone > DTIType.timezoneSecondsLimit
                || -targetZone > DTIType.timezoneSecondsLimit) {
            throw Error.error(ErrorCode.X_22009);
        }

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE : {
                TimeData value = (TimeData) a;

                if (localZone != 0 || value.zone != targetZone) {
                    return new TimeData(value.getSeconds() - localZone,
                                        value.getNanos(), targetZone);
                }

                break;
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                TimestampData value = (TimestampData) a;

                if (localZone != 0 || value.zone != targetZone) {
                    return new TimestampData(value.getSeconds() - localZone,
                                             value.getNanos(), targetZone);
                }

                break;
            }
        }

        return a;
    }

    public boolean canAdd(IntervalType other) {
        return other.startPartIndex >= startPartIndex
               && other.endPartIndex <= endPartIndex;
    }

    public static Boolean overlaps(Session session, Object[] a, Type[] ta,
                                   Object[] b, Type[] tb) {

        if (a == null || b == null) {
            return null;
        }

        if (a[0] == null || b[0] == null) {
            return null;
        }

        if (a[1] == null) {
            a[1] = a[0];
        }

        if (b[1] == null) {
            b[1] = b[0];
        }

        Type commonType = ta[0].getCombinedType(tb[0], OpTypes.EQUAL);

        a[0] = commonType.castToType(session, a[0], ta[0]);
        b[0] = commonType.castToType(session, b[0], tb[0]);

        if (ta[1].isIntervalType()) {
            a[1] = commonType.add(a[0], a[1], ta[1]);
        } else {
            a[1] = commonType.castToType(session, a[1], ta[1]);
        }

        if (tb[1].isIntervalType()) {
            b[1] = commonType.add(b[0], b[1], tb[1]);
        } else {
            b[1] = commonType.castToType(session, b[1], tb[1]);
        }

        if (commonType.compare(a[0], a[1]) > 0) {
            Object temp = a[0];

            a[0] = a[1];
            a[1] = temp;
        }

        if (commonType.compare(b[0], b[1]) > 0) {
            Object temp = b[0];

            b[0] = b[1];
            b[1] = temp;
        }

        if (commonType.compare(a[0], b[0]) > 0) {
            Object[] temp = a;

            a = b;
            b = temp;
        }

        if (commonType.compare(a[1], b[0]) > 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    //
    public static int subtractMonths(TimestampData a, TimestampData b,
                                     boolean isYear) {

        synchronized (HsqlDateTime.tempCalGMT) {
            boolean negate = false;

            if (b.getSeconds() > a.getSeconds()) {
                negate = true;

                TimestampData temp = a;

                a = b;
                b = temp;
            }

            HsqlDateTime.setTimeInMillis(HsqlDateTime.tempCalGMT,
                                         a.getSeconds() * 1000);

            int months = HsqlDateTime.tempCalGMT.get(Calendar.MONTH);
            int years  = HsqlDateTime.tempCalGMT.get(Calendar.YEAR);

            HsqlDateTime.setTimeInMillis(HsqlDateTime.tempCalGMT,
                                         b.getSeconds() * 1000);

            months -= HsqlDateTime.tempCalGMT.get(Calendar.MONTH);
            years  -= HsqlDateTime.tempCalGMT.get(Calendar.YEAR);

            if (isYear) {
                months = years * 12;
            } else {
                if (months < 0) {
                    months += 12;

                    years--;
                }

                months += years * 12;

                if (negate) {
                    months = -months;
                }
            }

            return months;
        }
    }

    /** @todo - overflow */
    public static TimeData addSeconds(TimeData source, int seconds,
                                      int nanos) {

        nanos   += source.getNanos();
        seconds += nanos / limitNanoseconds;
        nanos   %= limitNanoseconds;

        if (nanos < 0) {
            nanos += DTIType.limitNanoseconds;

            seconds--;
        }

        seconds += source.getSeconds();
        seconds %= (24 * 60 * 60);

        TimeData ti = new TimeData(seconds, nanos, source.getZone());

        return ti;
    }

    /** @todo - overflow */
    public static TimestampData addMonths(TimestampData source, int months) {

        int n = source.getNanos();

        synchronized (HsqlDateTime.tempCalGMT) {
            HsqlDateTime.setTimeInMillis(HsqlDateTime.tempCalGMT,
                                         source.getSeconds() * 1000);
            HsqlDateTime.tempCalGMT.add(Calendar.MONTH, months);

            TimestampData ts =
                new TimestampData(HsqlDateTime.tempCalGMT.getTimeInMillis()
                                  / 1000, n, source.getZone());

            return ts;
        }
    }

    /** @todo - overflow */
    public static TimestampData addSeconds(TimestampData source, int seconds,
                                           int nanos) {

        nanos   += source.getNanos();
        seconds += nanos / limitNanoseconds;
        nanos   %= limitNanoseconds;

        if (nanos < 0) {
            nanos += limitNanoseconds;

            seconds--;
        }

        long newSeconds = source.getSeconds() + seconds;
        TimestampData ts = new TimestampData(newSeconds, nanos,
                                             source.getZone());

        return ts;
    }
}
