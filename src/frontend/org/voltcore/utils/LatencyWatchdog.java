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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;


public class LatencyWatchdog extends Thread {

    private static final VoltLogger LOG = new VoltLogger("LatencyWatchdog");

    private static ConcurrentHashMap<Thread, WatchdogTimestamp> sLatencyMap = new ConcurrentHashMap<Thread, WatchdogTimestamp>();

    private static final long WATCHDOG_THRESHOLD = 100;  /* millisecond, same below */

    static final long WAKEUP_INTERVAL = 25;

    private static final long MIN_LOG_INTERVAL = 10;   /* second */

    static LatencyWatchdog sWatchdog;

    public static final boolean sEnable = Boolean.getBoolean("ENABLE_LATENCY_WATCHDOG");  /* Compiler will eliminate the code within its scope when turn it off */

    static {
        if (sEnable) {
            sWatchdog = new LatencyWatchdog();
            sWatchdog.start();
        }
    }

    static class WatchdogTimestamp {
        private Thread m_thread;
        private long m_timestamp;

        public WatchdogTimestamp(Thread t, long timestamp) {
            m_thread = t;
            m_timestamp = timestamp;
        }

        public Thread getThread() {
            return m_thread;
        }

        public Long getTimestamp() {
            return m_timestamp;
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
        sLatencyMap.put(thread, new WatchdogTimestamp(thread, System.currentTimeMillis()));
    }

    /**
     * The watchdog thread will be invoked every WAKEUP_INTERVAL time, to check if any thread that be monitored
     * has not updated its time stamp more than WATCHDOG_THRESHOLD millisecond. Rate limiter can be controlled
     * by setting different MIN_LOG_INTERVAL value.
     */
    @Override
    public void run() {
        Thread.currentThread().setName("Latency Watchdog");
        System.out.println("Latency Watchdog thread has been started.");
        while (true) {
            for (Iterator<WatchdogTimestamp> iter = sLatencyMap.values().iterator(); iter.hasNext();) {
                WatchdogTimestamp wt = iter.next();
                Thread t = wt.getThread();
                Long timestamp = wt.getTimestamp();
                long now = System.currentTimeMillis();
                if ((now - timestamp > WATCHDOG_THRESHOLD) && t.getState() != Thread.State.TERMINATED) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(t.getName() + " has been delayed for more than " + WATCHDOG_THRESHOLD + " milliseconds\n");
                    for (StackTraceElement ste : t.getStackTrace()) {
                        sb.append(ste);
                        sb.append("\n");
                    }
                    RateLimitedLogger.tryLogForMessage(sb.toString(), now, MIN_LOG_INTERVAL, TimeUnit.SECONDS, LOG, Level.DEBUG);
                }
                if (t.getState() == Thread.State.TERMINATED) {
                    iter.remove();
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
