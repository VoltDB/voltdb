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

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;


public class LatencyWatchdog extends Thread {

    private static final VoltLogger LOG = new VoltLogger("LatencyWatchdog");

    private static ConcurrentHashMap<Thread, AtomicLong> sLatencyMap = new ConcurrentHashMap<Thread, AtomicLong>();

    private static final long WATCHDOG_THRESHOLD = Long.getLong("WATCHDOG_THRESHOLD", 100);  /* millisecond, same below */

    private static final long WAKEUP_INTERVAL = Long.getLong("WAKEUP_INTERVAL", 25);

    private static final long MIN_LOG_INTERVAL = Long.getLong("MIN_LOG_INTERVAL", 10 * 1000);

    static LatencyWatchdog sWatchdog;

    public static final boolean sEnable = Boolean.getBoolean("ENABLE_LATENCY_WATCHDOG");  /* Compiler will eliminate code surrounded by its scope when turn it off */

    static {
        if (sEnable) {
            sWatchdog = new LatencyWatchdog();
            sWatchdog.start();
        }
    }

    /**
     * Make sure every pet() invocation was surrounded by if (isEnable()) block
     */
    public static boolean isEnable() {
        return sEnable;
    }

    /**
     * Update latency watchdog time stamp for current thread.
     */
    public static void pet() {
        if (!sEnable)
            return;

        Thread thread = Thread.currentThread();
        AtomicLong oldVal = sLatencyMap.get(thread);
        if (oldVal == null) {
            sLatencyMap.put(thread, new AtomicLong(System.currentTimeMillis()));
        } else {
            oldVal.lazySet(System.currentTimeMillis());
        }
    }

    /**
     * The watchdog thread will be invoked every WAKEUP_INTERVAL time, to check if any thread that be monitored
     * has not updated its time stamp more than WATCHDOG_THRESHOLD millisecond. Same stack trace messages are
     * rate limited by MIN_LOG_INTERVAL.
     */
    @Override
    public void run() {
        Thread.currentThread().setName("Latency Watchdog");
        System.out.printf("Latency Watchdog enabled -- threshold:%d(ms) wakeup_interval:%d(ms) min_log_interval:%d(ms)\n",
                WATCHDOG_THRESHOLD, WAKEUP_INTERVAL, MIN_LOG_INTERVAL);
        while (true) {
            for (Entry<Thread, AtomicLong> entry : sLatencyMap.entrySet()) {
                Thread t = entry.getKey();
                long timestamp = entry.getValue().get();
                long now = System.currentTimeMillis();
                if ((now - timestamp > WATCHDOG_THRESHOLD) && t.getState() != Thread.State.TERMINATED) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(t.getName() + " has been delayed for more than " + WATCHDOG_THRESHOLD + " milliseconds\n");
                    for (StackTraceElement ste : t.getStackTrace()) {
                        sb.append(ste);
                        sb.append("\n");
                    }
                    RateLimitedLogger.tryLogForMessage(sb.toString(), now, MIN_LOG_INTERVAL, TimeUnit.MILLISECONDS, LOG, Level.ERROR);
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
