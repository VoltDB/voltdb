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

/**
 * Implementation of data item for TIME.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class TimeData {

    final int zone;
    final int seconds;
    final int nanos;

    public TimeData(int seconds, int nanos, int zoneSeconds) {

        while (seconds < 0) {
            seconds += 24 * 60 * 60;
        }

        if (seconds > 24 * 60 * 60) {
            seconds %= 24 * 60 * 60;
        }

        this.zone    = zoneSeconds;
        this.seconds = seconds;
        this.nanos   = nanos;
    }

    public TimeData(int seconds, int nanos) {
        this (seconds, nanos, 0);
    }

    public int getSeconds() {
        return seconds;
    }

    public int getNanos() {
        return nanos;
    }

    public int getZone() {
        return zone;
    }

    public boolean equals(Object other) {

        if (other instanceof TimeData) {
            return seconds == ((TimeData) other).seconds
                   && nanos == ((TimeData) other).nanos
                   && zone ==  ((TimeData) other).zone ;
        }

        return false;
    }

    public int hashCode() {
        return seconds ^ nanos;
    }

    public int compareTo(TimeData b) {

        long diff = seconds - b.seconds;

        if (diff == 0) {
            diff = nanos - b.nanos;

            if (diff == 0) {
                return 0;
            }
        }

        return diff > 0 ? 1
                        : -1;
    }
}
