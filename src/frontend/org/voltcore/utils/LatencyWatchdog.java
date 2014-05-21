package org.voltcore.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;


public class LatencyWatchdog {

    private static final VoltLogger LOG = new VoltLogger("LatencyWatchdog");

    private static LinkedHashMap<Thread, Pair<Thread, Long>> sLatencyMap = new LinkedHashMap<Thread, Pair<Thread, Long>>();

    private static HashMap<Thread, Long> sLastLogTime = new HashMap<Thread, Long>();

    private static ScheduledThreadPoolExecutor executor = CoreUtils.getScheduledThreadPoolExecutor("Latency Watchdog Executor", 1, CoreUtils.SMALL_STACK_SIZE);

    public static final boolean m_enable = true;  /* Compiler will eliminate the code within its scope when turn it off */

    private static final long WATCHDOG_THRESHOLD = 50;  /* millisecond */

    static final long WAKEUP_INTERVAL = 50;

    private static final long MIN_LOG_INTERVAL = 10 * 1000; /* 10 seconds */

    static boolean sOnlyExecuteOnce = false;

    public static boolean isEnable() {
        return m_enable;
    }

    public static void pet() {
        if (!m_enable)
            return;

        Thread thread = Thread.currentThread();
        sLatencyMap.put(thread, new Pair<Thread, Long>(thread, System.currentTimeMillis()));

        if (!sOnlyExecuteOnce) {
            sOnlyExecuteOnce = true;
            executor.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {

                    for (Iterator<Pair<Thread, Long>> iter = sLatencyMap.values().iterator(); iter.hasNext();) {
                        Pair<Thread, Long> pair = iter.next();
                        Thread t = pair.getFirst();
                        Long timestamp = pair.getSecond();
                        long now = System.currentTimeMillis();
                        if ((now - timestamp > WATCHDOG_THRESHOLD) && (now - sLastLogTime.get(t) > MIN_LOG_INTERVAL) && t.getState() != Thread.State.TERMINATED ) {
                            LOG.info(t.getName() + " has been delay for " + (now - timestamp) + " milliseconds" );
                            for (StackTraceElement ste : t.getStackTrace()) {
                                LOG.info(ste);
                            }
                            sLastLogTime.put(t, now);
                            sLatencyMap.put(t, new Pair<Thread, Long>(t, now));
                        }
                        if (t.getState() == Thread.State.TERMINATED) {
                            iter.remove();
                        }
                    }
                }

            }, WAKEUP_INTERVAL, WAKEUP_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }
}
