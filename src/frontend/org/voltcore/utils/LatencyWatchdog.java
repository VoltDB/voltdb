package org.voltcore.utils;

import java.util.HashMap;
import java.util.Map;


public class LatencyWatchdog {
    static Map<Thread, Long> m_latencyMap = new HashMap<Thread, Long>();
    static final long WATCHDOG_DELAY = 50;
    static final long RATE_LIMITED_INTERVAL = 5;
    static public final boolean m_enable = true;  /* Compiler will eliminate the code within its scope when turn off */

    static class WatchdogCallback implements Runnable {
        final Thread m_thread;

        WatchdogCallback(Thread t) {
            m_thread = t;
        }

        @Override
        public void run() {
            long timestamp = m_latencyMap.get(m_thread);
            long interval = System.currentTimeMillis() - timestamp;
            if (interval > WATCHDOG_DELAY) {
                System.out.printf("Thread [%s] has been delayed for more than %d milliseconds\n", m_thread.getName(), interval);
                for (StackTraceElement ste : m_thread.getStackTrace()) {
                    System.out.println(ste);
                }
            }
        }
    }

    public static void pet(Thread t) {
        if (!m_enable)
            return;

        // create a watchdog then feed it
        m_latencyMap.put(t, System.currentTimeMillis());
        Thread thread = new Thread(new WatchdogCallback(t), t.getName() + "-Watchdog");
        thread.start();
    }

}
