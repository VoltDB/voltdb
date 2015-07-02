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

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;

/**
 * Implementation of data item for INTERVAL MONTH.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class IntervalMonthData {

    public final int units;

    public static IntervalMonthData newInterval(double value, int typeCode) {

        int index = DTIType.intervalIndexMap.get(typeCode);

        value *= DTIType.yearToSecondFactors[index];

        return new IntervalMonthData((long) value);
    }

    public static IntervalMonthData newIntervalYear(long years,
            IntervalType type) {
        return new IntervalMonthData(years * 12, type);
    }

    public static IntervalMonthData newIntervalMonth(long months,
            IntervalType type) {
        return new IntervalMonthData(months, type);
    }

    public IntervalMonthData(long months, IntervalType type) {

        if (months >= type.getIntervalValueLimit()) {
            throw Error.error(ErrorCode.X_22006);
        }

        if (type.typeCode == Types.SQL_INTERVAL_YEAR) {
            months -= (months % 12);
        }

        this.units = (int) months;
    }

    public IntervalMonthData(long months) {
        this.units = (int) months;
    }

    public boolean equals(Object other) {

        if (other instanceof IntervalMonthData) {
            return units == ((IntervalMonthData) other).units;
        }

        return false;
    }

    public int hashCode() {
        return (int) units;
    }

    public int compareTo(IntervalMonthData b) {

        if (units > b.units) {
            return 1;
        } else if (units < b.units) {
            return -1;
        } else {
            return 0;
        }
    }

    public long getMonths() {
        return units;
    }

    public String toString() {
        return Type.SQL_INTERVAL_MONTH_MAX_PRECISION.convertToString(this);
    }
}
