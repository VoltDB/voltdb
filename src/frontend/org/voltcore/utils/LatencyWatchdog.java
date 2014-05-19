package org.voltcore.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class LatencyWatchdog {
    static Map<Thread, Long> m_latencyMap = new HashMap<Thread, Long>();
    static final long WATCHDOG_DELAY = 50;
    static final long RATE_LIMITED_INTERVAL = 5;
    static public final boolean m_enable = true;  /* Compiler will eliminate the code within its scope when turn off */
    static ScheduledThreadPoolExecutor executor = CoreUtils.getScheduledThreadPoolExecutor("Latency Watchdog Executor", 10, CoreUtils.SMALL_STACK_SIZE);

    static class WatchdogCallback implements Runnable {
        final Thread m_thread;
        static int count = 0;

        WatchdogCallback(Thread t) {
            m_thread = t;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Latency Watchdog - " + m_thread.getName());
            long timestamp = m_latencyMap.get(m_thread);
            long interval = System.currentTimeMillis() - timestamp;
            if (interval > WATCHDOG_DELAY && count++ < 10) {
                System.out.printf("Thread [%s] has been delayed for more than %d milliseconds\n", m_thread.getName(), interval);
                for (StackTraceElement ste : m_thread.getStackTrace()) {
                    System.out.println(ste);
                }
            }
            executor.scheduleWithFixedDelay(new WatchdogCallback(m_thread), WATCHDOG_DELAY, WATCHDOG_DELAY, TimeUnit.MILLISECONDS);
        }
    }

    public static void pet(Thread t) {
        if (!m_enable)
            return;

        if (m_latencyMap.containsKey(t)) {
            // feed it
            m_latencyMap.put(t, System.currentTimeMillis());
        } else {
            // create a watchdog then feed it
            m_latencyMap.put(t, System.currentTimeMillis());
            executor.scheduleWithFixedDelay(new WatchdogCallback(t), WATCHDOG_DELAY, WATCHDOG_DELAY, TimeUnit.MILLISECONDS);
        }
    }

}
