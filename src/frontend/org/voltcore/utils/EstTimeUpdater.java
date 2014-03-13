/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltcore.utils;

public class EstTimeUpdater {
    //Report inconsistent update frequency at most every sixty seconds
    public static final long maxErrorReportInterval = 60 * 1000;
    //Warn if estimated time upates are > 2 seconds apart (should be at most five millis)
    public static final long maxTolerableUpdateDelta = 2000;
    public static long lastErrorReport = System.currentTimeMillis() - maxErrorReportInterval;

    public static Long update(final long now) {
        final long estNow = EstTime.m_now.get();
        if (estNow == now) {
            return null;
        }
        EstTime.m_now.lazySet(now);
        /*
         * Check if updating the estimated time was especially tardy.
         * I am concerned that the thread responsible for updating the estimated
         * time might be blocking on something and want to be able to log if
         * that happens
         */
        if (now - estNow > 2000) {
            /*
             * Only report the error every 60 seconds to cut down on log spam
             */
            if (lastErrorReport > now) {
                //Time moves backwards on occasion, check and reset
                lastErrorReport = now;
            }
            if (now - lastErrorReport > maxErrorReportInterval) {
                lastErrorReport = now;
                return now - estNow;
            }
        }
        return null;
    }
}
