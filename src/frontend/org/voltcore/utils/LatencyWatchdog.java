/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;


public class LatencyWatchdog extends Thread {

    private static final VoltLogger LOG = new VoltLogger("HOST");

    private static ThreadLocal<AtomicLong> sLatencyVal = new ThreadLocal<AtomicLong>() {
        @Override
        public AtomicLong initialValue() {
            final AtomicLong retval = new AtomicLong();
            sLatencyMap.put(Thread.currentThread(), retval);
            return retval;
        }
    };

    private static ConcurrentHashMap<Thread, AtomicLong> sLatencyMap = new ConcurrentHashMap<Thread, AtomicLong>();

    private static final long WATCHDOG_THRESHOLD = Long.getLong("WATCHDOG_THRESHOLD", 100);  /* milliseconds */

    private static final long WAKEUP_INTERVAL = Long.getLong("WAKEUP_INTERVAL", 25); /* milliseconds */

    private static final long MIN_LOG_INTERVAL_SEC = Long.getLong("MIN_LOG_INTERVAL", 10); /* seconds */

    static LatencyWatchdog sWatchdog;

    public static final boolean sEnable = Boolean.getBoolean("ENABLE_LATENCY_WATCHDOG");  /* Compiler will eliminate code surrounded by its scope when turn it off */

    static {
        if (sEnable) {
            sWatchdog = new LatencyWatchdog();
            sWatchdog.start();
        }
    }

    /**
     * Update latency watchdog time stamp for current thread.
     * Keep this method small so inlining and elimination can work their magic
     */
    public static void pet() {
        if (!sEnable)
            return;
        petImpl();
    }

    private static void petImpl() {
        sLatencyVal.get().lazySet(System.currentTimeMillis());
    }

    /**
     * The watchdog thread will be invoked every WAKEUP_INTERVAL time, to check if any thread that be monitored
     * has not updated its time stamp more than WATCHDOG_THRESHOLD millisecond. Same stack trace messages are
     * rate limited by MIN_LOG_INTERVAL.
     */
    @Override
    public void run() {
        Thread.currentThread().setName("Latency Watchdog");
        LOG.info(String.format("Latency Watchdog enabled -- threshold:%d(ms) " +
                               "wakeup_interval:%d(ms) min_log_interval:%d(sec)\n",
                               WATCHDOG_THRESHOLD, WAKEUP_INTERVAL, MIN_LOG_INTERVAL_SEC));
        while (true) {
            for (Entry<Thread, AtomicLong> entry : sLatencyMap.entrySet()) {
                Thread t = entry.getKey();
                long timestamp = entry.getValue().get();
                long now = System.currentTimeMillis();
                if ((now - timestamp > WATCHDOG_THRESHOLD) && t.getState() != Thread.State.TERMINATED) {
                    StringBuilder sb = new StringBuilder();
                    String format = t.getName() + " has been delayed for more than " + WATCHDOG_THRESHOLD + " milliseconds\n %s";
                    for (StackTraceElement ste : t.getStackTrace()) {
                        sb.append(ste);
                        sb.append("\n");
                    }
                    LOG.rateLimitedInfo(MIN_LOG_INTERVAL_SEC, format, sb.toString());
                }
            }
            try {
                Thread.sleep(WAKEUP_INTERVAL);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
