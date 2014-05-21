package org.voltcore.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.voltcore.logging.VoltLogger;


public class LatencyWatchdog extends Thread {

    private static final VoltLogger LOG = new VoltLogger("LatencyWatchdog");

    private static LinkedHashMap<Thread, Pair<Thread, Long>> sLatencyMap = new LinkedHashMap<Thread, Pair<Thread, Long>>();

    private static HashMap<Thread, Long> sLastLogTime = new HashMap<Thread, Long>();

    private boolean m_shouldStop = false;

    public static final boolean m_enable = true;  /* Compiler will eliminate the code within its scope when turn it off */

    private static final long WATCHDOG_THRESHOLD = 50;  /* millisecond, same below */

    static final long WAKEUP_INTERVAL = 25;

    private static final long MIN_LOG_INTERVAL = 10 * 1000;

    static LatencyWatchdog sWatchdog;

    public static boolean isEnable() {
        return m_enable;
    }

    public static LatencyWatchdog getInstance() {
        if (sWatchdog == null) {
            sWatchdog = new LatencyWatchdog();
        }
        return sWatchdog;
    }

    public static void pet() {
        if (!m_enable)
            return;

        if (sWatchdog == null) {
            getInstance().start();
        }
        Thread thread = Thread.currentThread();
        sLatencyMap.put(thread, new Pair<Thread, Long>(thread, System.currentTimeMillis()));
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Latency Watchdog");
        while (!m_shouldStop) {

            for (Iterator<Pair<Thread, Long>> iter = sLatencyMap.values().iterator(); iter.hasNext();) {
                Pair<Thread, Long> pair = iter.next();
                Thread t = pair.getFirst();
                Long timestamp = pair.getSecond();
                long now = System.currentTimeMillis();
                if ((now - timestamp > WATCHDOG_THRESHOLD) &&
                        t.getState() != Thread.State.TERMINATED &&
                        (sLastLogTime.get(t) == null || (now - sLastLogTime.get(t)) > MIN_LOG_INTERVAL)) {

                    LOG.info(t.getName() + " has been delayed for " + (now - timestamp) + " milliseconds" );
                    for (StackTraceElement ste : t.getStackTrace()) {
                        LOG.info(ste);
                    }
                    sLastLogTime.put(t, now);
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

    public void shutdown() {
        m_shouldStop = true;
    }
}
