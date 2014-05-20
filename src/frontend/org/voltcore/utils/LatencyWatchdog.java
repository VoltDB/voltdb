package org.voltcore.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;


public class LatencyWatchdog {

    private static final VoltLogger LOG = new VoltLogger("LatencyWatchdog");

    private static volatile Map<Thread, Long> m_latencyMap = new HashMap<Thread, Long>();

    private static ScheduledThreadPoolExecutor executor = CoreUtils.getScheduledThreadPoolExecutor("Latency Watchdog Executor", 10, CoreUtils.SMALL_STACK_SIZE);

    public static final boolean m_enable = true;  /* Compiler will eliminate the code within its scope when turn off */

    private static final long WATCHDOG_DELAY = 50;

    private static final long MIN_LOG_INTERVAL = 5 * 1000; /* millisecond */

    private static volatile long m_lastLogTime = 0;

    public static boolean isEnable() {
        return m_enable;
    }

    public static void pet() {
        if (!m_enable)
            return;

        final Thread t = Thread.currentThread();
        if (m_latencyMap.containsKey(t)) {
            // feed it
            m_latencyMap.put(t, System.currentTimeMillis());
        } else {
            // create a watchdog then feed it
            m_latencyMap.put(t, System.currentTimeMillis());
            executor.scheduleWithFixedDelay(new Runnable() {

                @Override
                public synchronized void run() {
                    long timestamp = m_latencyMap.get(t);
                    long now = System.currentTimeMillis();
                    if ((now - timestamp > WATCHDOG_DELAY) && (now - m_lastLogTime > MIN_LOG_INTERVAL) && t.getState() != Thread.State.TERMINATED ) {
                        LOG.info(t.getName() + " has been delay for " + (now - timestamp) + " milliseconds" );
                        m_lastLogTime = now;
                        for (StackTraceElement ste : t.getStackTrace()) {
                            LOG.info(ste);
                        }
                    }
                }

            }, WATCHDOG_DELAY, WATCHDOG_DELAY, TimeUnit.MILLISECONDS);
        }
    }

}
