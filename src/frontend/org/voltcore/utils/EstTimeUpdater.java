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

package org.voltcore.utils;

import org.voltcore.logging.VoltLogger;

public class EstTimeUpdater {
    //Report inconsistent update frequency at most every sixty seconds
    public static final long maxErrorReportInterval = 60 * 1000;
    public static long lastErrorReport = System.currentTimeMillis() - maxErrorReportInterval;

    public static final int ESTIMATED_TIME_UPDATE_FREQUENCY = Integer.getInteger("ESTIMATED_TIME_UPDATE_FREQUENCY", 5);
    public static final int ESTIMATED_TIME_WARN_INTERVAL = Integer.getInteger("ESTIMATED_TIME_WARN_INTERVAL", 2000);

    public static volatile boolean pause = false;

    private static final Thread updater = new Thread("Estimated Time Updater") {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(ESTIMATED_TIME_UPDATE_FREQUENCY);
                } catch (InterruptedException e) {}
                if (pause) continue;
                Long delta = EstTimeUpdater.update(System.currentTimeMillis());
                if ( delta != null ) {
                    new VoltLogger("HOST").info(delta +" estimated time update.");
                }
            }
        }
    };

    static {
        updater.setDaemon(true);
        updater.start();
    }

    /**
     * Don't call this unless you have paused the updater and intend to update yourself
     * @param now
     * @return
     */
    public static Long update(final long now) {
        final long estNow = EstTime.m_now;
        if (estNow == now) {
            return null;
        }

        EstTime.m_now = now;

        /*
         * Check if updating the estimated time was especially tardy.
         * I am concerned that the thread responsible for updating the estimated
         * time might be blocking on something and want to be able to log if
         * that happens
         */
        if (now - estNow > ESTIMATED_TIME_WARN_INTERVAL) {
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
