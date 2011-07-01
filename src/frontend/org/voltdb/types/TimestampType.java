/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.types;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json_voltpatches.JSONString;

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
     * @param Java Date instance.
     */
    public TimestampType(Date date) {
        m_usecs = 0;
        m_date = (Date) date.clone();
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String format = sdf.format(m_date);
        return format + "." + m_usecs;
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

    @Override
    public String toJSONString() {
        return String.valueOf(getTime());
    }

    private final Date m_date;     // stores milliseconds from epoch.
    private final short m_usecs;   // stores microsecond within date's millisecond.
}
