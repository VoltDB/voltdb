/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.types;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json_voltpatches.JSONString;
import org.voltdb.common.Constants;

/**
 * Represent a microsecond-accurate VoltDB timestamp type.
 */
public class TimestampType implements JSONString, Comparable<TimestampType> {
    /**
     * Create a TimestampType from microseconds from epoch.
     * @param timestamp microseconds since epoch.
     */
    public TimestampType(long timestamp) {
        m_usecs = (short) (timestamp % 1000);
        long millis = (timestamp - m_usecs) / 1000;
        m_date = new Date(millis);
    }

    /**
     * Create a TimestampType from a Java Date class.
     * Microseconds will be rounded to zero.
     * @param date Java Date instance.
     */
    public TimestampType(Date date) {
        m_usecs = 0;
        m_date = (Date) date.clone();
    }

    private static long microsFromJDBCformat(String param){
        java.sql.Timestamp sqlTS;
        if (param.length() == 10) {
            sqlTS = java.sql.Timestamp.valueOf(param + " 00:00:00.000");
        }
        else {
            sqlTS = java.sql.Timestamp.valueOf(param);
        }

        final long timeInMillis = sqlTS.getTime();
        final long fractionalSecondsInNanos = sqlTS.getNanos();
        // Fractional microseconds would get truncated so flag them as an error.
        if ((fractionalSecondsInNanos % 1000) != 0) {
            throw new IllegalArgumentException("Can't convert from String to TimestampType with fractional microseconds");
        }
        // Milliseconds would be doubly counted as millions of nanos if they weren't truncated out via %.
        return (timeInMillis * 1000) + ((fractionalSecondsInNanos % 1000000)/1000);
    }

    /**
     * Given a string parseable by the JDBC Timestamp parser, return the fractional component
     * in milliseconds.
     *
     * @param param A timstamp in string format that is parseable by JDBC.
     * @return The fraction of a second portion of this timestamp expressed in milliseconds.
     * @throws IllegalArgumentException if the timestamp uses higher than millisecond precision.
     */
    public static long millisFromJDBCformat(String param) {
        java.sql.Timestamp sqlTS = java.sql.Timestamp.valueOf(param);
        final long fractionalSecondsInNanos = sqlTS.getNanos();
        // Fractional milliseconds would get truncated so flag them as an error.
        if ((fractionalSecondsInNanos % 1000000) != 0) {
            throw new IllegalArgumentException("Can't convert from String to Date with fractional milliseconds");
        }
        return sqlTS.getTime();
    }

    /**
     * Construct from a timestamp string in YYYY-MM-DD-SS.sss format.
     * This is typically used for reading CSV data or data output
     * from {@link java.sql.Timestamp}'s string format.
     *
     * @param param A string in YYYY-MM-DD-SS.sss format.
     */
    public TimestampType(String param) {
        this(microsFromJDBCformat(param));
    }

    /**
     * Create a TimestampType instance for the current time.
     */
    public TimestampType() {
        m_usecs = 0;
        m_date = new Date();
    }

    /**
     * Read the microsecond in time stored by this timestamp.
     * @return microseconds
     */
    public long getTime() {
        long millis = m_date.getTime();
        return millis * 1000 + m_usecs;
    }

    /**
     * Get the microsecond portion of this timestamp
     * @return Microsecond portion of timestamp as a short
     */
    public short getUSec() {
        return m_usecs;
    }

    /**
     * Equality.
     * @return true if equal.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TimestampType))
            return false;

        TimestampType ts = (TimestampType)o;
        if (!ts.m_date.equals(this.m_date))
            return false;

        if (!(ts.m_usecs == this.m_usecs))
            return false;

        return true;
    }

    /**
     * toString for debugging and printing VoltTables
     */
    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        Date dateToMillis = m_date;
        short usecs = m_usecs;
        if (usecs < 0) {
            // Negative usecs can occur for dates before 1970.
            // To be expressed as positive decimals, they must "borrow" a milli from the date
            // and convert it to 1000 micros.
            dateToMillis.setTime(dateToMillis.getTime()-1);
            usecs += 1000;
        }
        assert(usecs >= 0);
        String format = sdf.format(dateToMillis);
        // zero-pad so 1 or 2 digit usecs get appended correctly
        return format + String.format("%03d", usecs);
    }

    /**
     * Hashcode with the same uniqueness as a Java Date.
     */
    @Override
    public int hashCode() {
        long usec = this.getTime();
        return (int) usec ^ (int) (usec >> 32);
    }


    /**
     * CompareTo - to mimic Java Date
     */
    @Override
    public int compareTo(TimestampType dateval) {
        int comp = m_date.compareTo(dateval.m_date);
        if (comp == 0) {
            return m_usecs - dateval.m_usecs;
        }
        else {
            return comp;
        }
    }

    /**
     * Retrieve a copy of the approximate Java date.
     * The returned date is a copy; this object will not be affected by
     * modifications of the returned instance.
     * @return Clone of underlying Date object.
     */
    public Date asApproximateJavaDate() {
        return (Date) m_date.clone();
    }

    /**
     * Retrieve a copy of the Java date for a TimeStamp with millisecond granularity.
     * The returned date is a copy; this object will not be affected by
     * modifications of the returned instance.
     * @return Clone of underlying Date object.
     */
    public Date asExactJavaDate() {
        if (m_usecs != 0) {
            throw new RuntimeException("Can't convert to java Date from TimestampType with fractional milliseconds");
        }
        return (Date) m_date.clone();
    }

    /**
     * Retrieve a properly typed copy of the Java date for a TimeStamp with millisecond granularity.
     * The returned date is a copy; this object will not be affected by
     * modifications of the returned instance.
     * @return specifically typed copy of underlying Date object.
     */
    public java.sql.Date asExactJavaSqlDate() {
        if (m_usecs != 0) {
            throw new RuntimeException("Can't convert to sql Date from TimestampType with fractional milliseconds");
        }
        return new java.sql.Date(m_date.getTime());
    }

    /**
     * Retrieve a properly typed copy of the Java Timestamp for the VoltDB TimeStamp.
     * The returned Timestamp is a copy; this object will not be affected by
     * modifications of the returned instance.
     * @return reformatted Timestamp expressed internally as 1000s of nanoseconds.
     */
    public java.sql.Timestamp asJavaTimestamp() {
        java.sql.Timestamp result = new java.sql.Timestamp(m_date.getTime());
        result.setNanos(result.getNanos() + m_usecs * 1000);
        return result;
    }

    @Override
    public String toJSONString() {
        return String.valueOf(getTime());
    }

    private final Date m_date;     // stores milliseconds from epoch.
    private final short m_usecs;   // stores microsecond within date's millisecond.
}
