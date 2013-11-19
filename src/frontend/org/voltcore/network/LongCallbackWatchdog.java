/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltcore.network;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.voltcore.logging.VoltLogger;

/**
 * Set up a watchdog thread that can print stack traces of any
 * network threads that have spent more than 1s in a network thread
 *
 */
public class LongCallbackWatchdog {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    public static final long TIMEOUT_NANOS = 100000000;

    //public static final boolean ENABLED = System.getProperties().contains("networkwatchdog");
    public static final boolean ENABLED = System.getenv().containsKey("NETWORKWATCHDOG");

    private static boolean m_watchDogStarted = false;

    // map of thread ids to nanosecond callback start times
    static ConcurrentSkipListMap<Long, Long> m_timersForActiveCallbacks = new ConcurrentSkipListMap<Long, Long>();

    public static long startTiming() {
        startWatchDogThreadIfNeeded();
        long threadId = Thread.currentThread().getId();
        long now = System.nanoTime();
        m_timersForActiveCallbacks.put(threadId, now);
        return threadId;
    }

    public static void stopTiming(long callId) {
        long threadId = Thread.currentThread().getId();
        m_timersForActiveCallbacks.remove(threadId);
    }

    static void logThreadsStackTrace(long threadId, long duration) {
        Thread[] allThreads = new Thread[Thread.activeCount() * 4];
        int threadCount = Thread.enumerate(allThreads);
        for (int i = 0; i < threadCount; i++) {
            Thread t = allThreads[i];
            if (t.getId() == threadId) {
                StackTraceElement stackTrace[] = t.getStackTrace();

                String errmsg = "Thread \"%s\" with id %d has spent %f.2 seconds in a callback as of last check.";
                errmsg = String.format(errmsg, t.getName(), threadId, duration / 1000000000.0);
                hostLog.warn(errmsg);

                String stkmsg = "Current stack trace of thread with long callback:\n";
                for (StackTraceElement ste : stackTrace) {
                    stkmsg += "  " + ste.toString() + "\n";
                }
                hostLog.warn(stkmsg);

                return;
            }
        }
    }

    public static void startWatchDogThreadIfNeeded() {
        if (m_watchDogStarted) return;

        consoleLog.info("Netowrk callback watchdog thread enabled - Starting up...");

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    // loop forever, as this is a daemon thread
                    while (true) {
                        try { Thread.sleep(100); } catch (InterruptedException e1) {}

                        long now = System.nanoTime();

                        // iterate over all known callbacks and check for stragglers to log
                        for (Entry<Long, Long> e : m_timersForActiveCallbacks.entrySet()) {
                            if ((now - e.getValue()) > TIMEOUT_NANOS) {
                                logThreadsStackTrace(e.getKey(), now - e.getValue());
                            }
                        }
                    }
                }
                catch (Exception e) {
                    consoleLog.warn("Netowrk callback watchdog thread stopping for exception.", e);
                }
            }
        };

        // make this thread less likely to be preempted
        int currentPriority = Thread.currentThread().getPriority();
        t.setPriority(currentPriority + 1);

        // for debugging
        t.setName("LongVoltNetworkCallbackWatchdog");

        // allow the process to exit without this thread being explicitly stopped
        t.setDaemon(true);
        t.start();

        consoleLog.info("Netowrk callback watchdog thread started.");

        m_watchDogStarted = true;
    }
}
