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

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;

/**
 * Implementation of data item for INTERVAL SECOND.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class IntervalSecondData {

    public final long units;
    public final int  nanos;

    public static IntervalSecondData newIntervalDay(long days,
            IntervalType type) {
        return new IntervalSecondData(days * 24 * 60 * 60, 0, type);
    }

    public static IntervalSecondData newIntervalHour(long hours,
            IntervalType type) {
        return new IntervalSecondData(hours * 60 * 60, 0, type);
    }

    public static IntervalSecondData newIntervalMinute(long minutes,
            IntervalType type) {
        return new IntervalSecondData(minutes * 60, 0, type);
    }

    public static IntervalSecondData newIntervalSeconds(long seconds,
            IntervalType type) {
        return new IntervalSecondData(seconds, 0, type);
    }

    public IntervalSecondData(long seconds, int nanos, IntervalType type) {

        if (seconds >= type.getIntervalValueLimit()) {
            throw Error.error(ErrorCode.X_22015);
        }

        this.units = seconds;
        this.nanos = nanos;
    }

    public IntervalSecondData(long seconds, int nanos) {
        this.units = seconds;
        this.nanos = nanos;
    }

    /**
     * normalise is a marker, values are always normalised
     */
    public IntervalSecondData(long seconds, long nanos, IntervalType type,
                              boolean normalise) {

        if (nanos >= DTIType.limitNanoseconds) {
            long carry = nanos / DTIType.limitNanoseconds;

            nanos   = nanos % DTIType.limitNanoseconds;
            seconds += carry;
        } else if (nanos <= -DTIType.limitNanoseconds) {
            long carry = -nanos / DTIType.limitNanoseconds;

            nanos   = -(-nanos % DTIType.limitNanoseconds);
            seconds -= carry;
        }

        int scaleFactor = DTIType.nanoScaleFactors[type.scale];

        nanos /= scaleFactor;
        nanos *= scaleFactor;

        if (seconds > 0 && nanos < 0) {
            nanos += DTIType.limitNanoseconds;

            seconds--;
        } else if (seconds < 0 && nanos > 0) {
            nanos -= DTIType.limitNanoseconds;

            seconds++;
        }

        scaleFactor = DTIType.yearToSecondFactors[type.endPartIndex];
        seconds     /= scaleFactor;
        seconds     *= scaleFactor;

        if (seconds >= type.getIntervalValueLimit()) {
            throw Error.error(ErrorCode.X_22015);
        }

        this.units = seconds;
        this.nanos = (int) nanos;
    }

    public boolean equals(Object other) {

        if (other instanceof IntervalSecondData) {
            return units == ((IntervalSecondData) other).units
                   && nanos == ((IntervalSecondData) other).nanos;
        }

        return false;
    }

    public int hashCode() {
        return (int) units ^ nanos;
    }

    public int compareTo(IntervalSecondData b) {

        long diff = units - b.units;

        if (diff == 0) {
            diff = nanos - b.nanos;

            if (diff == 0) {
                return 0;
            }
        }

        return diff > 0 ? 1
                        : -1;
    }

    public long getSeconds() {
        return units;
    }

    public int getNanos() {
        return nanos;
    }

    public String toString() {
        throw Error.runtimeError(ErrorCode.U_S0500, "IntervalSecondData");
    }
}
