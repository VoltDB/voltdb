package org.voltcore.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltdb.VoltDB;

public class LatencyWatchdog {
    static Map<Thread, Long> m_latencyMap = new HashMap<Thread, Long>();
    static final long WATCHDOG_DELAY = 50;
    static final long RATE_LIMITED_INTERVAL = 5;
    static public final boolean m_enable = true;  /* Compiler will eliminate the code within its scope when turn off */

    public static void pet(final Thread t) {
        if (!m_enable)
            return;
        m_latencyMap.put(t, System.currentTimeMillis());
        VoltDB.instance().scheduleWork(new Runnable() {

            @Override
            public void run() {
                long timestamp = m_latencyMap.get(t);
                if (System.nanoTime() - timestamp > WATCHDOG_DELAY) {
                    //StringBuilder sb = new StringBuilder();
                    System.out.printf("Thread [%s] has been delayed for more than %d milliseconds\n", t.getName(), WATCHDOG_DELAY);
                    for (StackTraceElement ste : t.getStackTrace()) {
                        System.out.println(ste);
                    }
                }
//                VoltDB.instance().scheduleWork(this,WATCHDOG_DELAY, WATCHDOG_DELAY, TimeUnit.MILLISECONDS);
            }
        }, WATCHDOG_DELAY, WATCHDOG_DELAY, TimeUnit.MILLISECONDS);
    }
}
