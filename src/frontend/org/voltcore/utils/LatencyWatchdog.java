package org.voltcore.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;


public class LatencyWatchdog {

    private static final VoltLogger LOG = new VoltLogger("LatencyWatchdog");

    static volatile Map<Thread, Long> m_latencyMap = new HashMap<Thread, Long>();
    static final long WATCHDOG_DELAY = 50;
    static final long MIN_LOG_INTERVAL = 5 * 1000; /* millisecond */
    static public final boolean m_enable = true;  /* Compiler will eliminate the code within its scope when turn off */
    static ScheduledThreadPoolExecutor executor = CoreUtils.getScheduledThreadPoolExecutor("Latency Watchdog Executor", 10, CoreUtils.SMALL_STACK_SIZE);
    static volatile long m_lastLogTime = 0;

    public static boolean isEnable() {
        return m_enable;
    }

    static class WatchdogCallback implements Runnable {
        final Thread m_thread;

        WatchdogCallback(Thread t) {
            m_thread = t;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Latency Watchdog - " + m_thread.getName());
            long timestamp = m_latencyMap.get(m_thread);
            long now = System.currentTimeMillis();
            if ((now - timestamp > WATCHDOG_DELAY) && (now - m_lastLogTime > MIN_LOG_INTERVAL) && m_thread.getState() != Thread.State.TERMINATED ) {
                LOG.info("Thread " + m_thread.getName() + " has been delay for " + (now - timestamp) + " milliseconds" );
                m_lastLogTime = now;
                for (StackTraceElement ste : m_thread.getStackTrace()) {
                    LOG.info(ste);
                }
            }
            //executor.scheduleWithFixedDelay(new WatchdogCallback(m_thread), WATCHDOG_DELAY, WATCHDOG_DELAY, TimeUnit.MILLISECONDS);
        }
    }

    public static void pet() {
        if (!m_enable)
            return;

        Thread t = Thread.currentThread();
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
