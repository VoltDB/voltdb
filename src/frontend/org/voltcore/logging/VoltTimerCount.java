/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltcore.logging;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Timer Counter to evaluate efficiency of the code. It reports stats into the log file every
 * LOGGER_INTERVAL_SEC seconds. After stats are reported, instrumentation is cleared to accumulate stats for the next
 * time interval. It might be easier to insert this class into code instead of trying to accumulate stats into a
 * voltDB table. It is especially true when we deal with some internal classes and processes.
 *
 * The typical usage of the class is as follows.
 *
 * static VoltTimerCount mycounter = VoltTimerCount.newVoltCounter("<counterName>", "myProperty");
 *
 * public void foo() {
 *     // start timer here
 *     VoltTimerCount.Timestamp start = mycounter.getStart();
 *
 *     <do your stuff here>
 *
 *     mycounter.count(start,value);
 * }
 *
 * In this case, the system will report how many times and how long the code spend in <do your stuff here>.
 * Also, it will report the Max, Min, and Average value for "myProperty".
 * In addition, one can access the number of times a particular code is processed with the call to
 *
 *     mycounter.count();
 *
 * Note that here the starting timestamp is missing
 *
 * The timers are enabled only when "PERF" logger is set at the DEBUG level. In addition, one can fine-tune the particular timers
 * with the use of environmental variable
 *
 *     VOLT_PERF_TIMER=<comma_separated_list_of_timers>
 *
 * The comparison is made to confirm that the timer label starts with one of the strings in VOLT_PERF_TIMER env.
 * If PERF_TIMER is not set but PERF logger is set as debug - all timers are enabled.
 *
 */
public class VoltTimerCount {

    private static final VoltLogger log = new VoltLogger("PERF") ;

    private static final String PERF_TIMER_ENV = System.getenv("VOLT_PERF_TIMER");
    private static final boolean loggingEnabled = log.isDebugEnabled() && (PERF_TIMER_ENV == null || !PERF_TIMER_ENV.trim().isEmpty());
    private static final List<String> labelsInd = PERF_TIMER_ENV == null ? new ArrayList<>() :
                                                               Arrays.asList(PERF_TIMER_ENV.trim().split(",",-1));

    // map for all performance Instrumentation
    private static final ConcurrentHashMap<String, VoltTimerCount> instrumentationMap = new ConcurrentHashMap<>();

    private static final ExecutorService loggingExecutor =
        Executors.newSingleThreadExecutor((r) -> {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        );

    private static Future<?> loggingFuture = null;

    private static volatile boolean started = false;

    public static final long LOGGER_INTERVAL_SEC = 10;
    private static final long LOGGER_INTERVAL_MSEC = TimeUnit.SECONDS.toMillis(LOGGER_INTERVAL_SEC);

    // instrumentation label
    private final String label;

    private boolean enabled = false;

    // performance counter
    private AtomicLong count = new AtomicLong(0);
    private AtomicLong totalTime = new AtomicLong(0);
    private AtomicLong maxTime = new AtomicLong(0);

    // property names
    private List<TimedProperty> properties;

    // start time of operation
    public static class Timestamp {
        public long timestamp;

        private Timestamp() {
            timestamp = System.nanoTime();
        }
    }

    private static class TimedProperty {
        String propLabel;
        AtomicLong total;
        AtomicLong max;
        AtomicLong min;

        private TimedProperty(String propLabel){
            this.propLabel = propLabel;
            total = new AtomicLong(0);
            min = new AtomicLong(Long.MAX_VALUE);
            max = new AtomicLong(Long.MIN_VALUE);
        }
    }

    /**
     * Factory method to get the start time for Performance Instrument
     * @return the start time which is needed for instrumentation.
     */
    public Timestamp getStart() {
        return enabled ? new Timestamp() : null;
    }

    public boolean isEnabled() {
        return enabled;
    }

    private static class Dumper implements Runnable {
        @Override
        public void run() {
            log.debug("Performance instrumentation dumper starts");

            while (true) {
                try {
                    // dump start at exactly interval since epoch
                    Thread.sleep(LOGGER_INTERVAL_MSEC);
                    dumpAll();

                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    log.error("error when dump performance instrumentation", e);
                }
            }

            log.debug("Performance instrumentation dumper ends");
        }
    }

    // start periodical dump thread
    public static void start() {
        if (!started) {
            synchronized (loggingExecutor) {
                if (!started) {
                    if (loggingEnabled) {
                        loggingFuture = loggingExecutor.submit(new Dumper());
                    }
                    started = true;
                }
            }
        }
    }

    // stop periodical dump thread
    public static void stop() {
        synchronized (loggingExecutor) {
            if( started ) {
                if (loggingFuture != null) {
                    loggingFuture.cancel(true);
                }
                loggingExecutor.shutdown();
            }
        }
    }

    // dump all performance counters
    private static void dumpAll() {
        if (loggingEnabled) {
            instrumentationMap.forEachValue(Long.MAX_VALUE,
                    v -> {
                        v.dump();
                        v.reset();
                    });
        }
    }

    /**
     * Factory method to create a new PerformInstrument
     * @param name of the performance instrument
     * @param propertyNames label for properties that can be evaluated with this PerformInstrument.
     * @return a new instance of thePerformInstrument object
     */
    public static VoltTimerCount newVoltCounter(String name, String... propertyNames) {
        VoltTimerCount existing = instrumentationMap.get(name);
        if( existing != null ) {
            return existing;
        }
        else {
            VoltTimerCount newPerf = new VoltTimerCount(name, propertyNames);
            existing = instrumentationMap.putIfAbsent(name, newPerf);
            if (existing != null) {
                return existing;
            }
            log.debug("add PerformanceCounter " + newPerf.label);
            if (loggingEnabled) {
                start();
            }
            return newPerf;
        }
    }

    private VoltTimerCount(String label, String... propertyNames) {
        this.label = label;
        if( loggingEnabled ) {
            enabled = PERF_TIMER_ENV == null;
            if (!enabled) {
                for (String ind : labelsInd) {
                    if (this.label.startsWith(ind)) {
                        enabled = true;
                        break;
                    }
                }
            }
            if (enabled && propertyNames.length > 0) {
                properties = new ArrayList<>(propertyNames.length);
                for (String propertyName : propertyNames) {
                    this.properties.add(new TimedProperty(propertyName));
                }
            }
        }
    }

    /**
     * Count the occurrence of this code without measuring the time efficiency.
     * This method is used to find haw many times a particular code was used.
     */
    public void count() {
        if (enabled) {
            count.incrementAndGet();
        }
    }

    /**
     * Count the occurrence of the event and a particular property without evaluating the time spent in the code.
     * @param properties values for the properties for this PerformInstrument.
     */
    public void count(long... properties) {
        if (enabled) {
            count.incrementAndGet();
            assert(properties.length == this.properties.size());
            for (int i = 0; i < properties.length; i++) {
                long property = properties[i];
                TimedProperty prop = this.properties.get(i);
                prop.total.addAndGet(property);
                prop.max.updateAndGet(prev->Math.max(prev,property));
                prop.min.updateAndGet(prev->Math.min(prev,property));
            }
        }
    }

    /**
     * Evaluate the time spent between startTime and now.
     * @param startTime the start time for this count()
     */
    public void count(Timestamp startTime) {
        if( startTime != null) {
            countIntern(startTime.timestamp);
        }
    }

    /**
     * Evaluate the time spent between startTime and now including updating the value for the custom properties.
     * @param startTime start time for this timer.
     * @param properties values for the properties for the PerformInstrument
     */
    public void count(Timestamp startTime, long... properties) {
        if( startTime != null ) {
            countIntern(startTime.timestamp, properties);
        }
    }

    private void countIntern(long startTime) {
        if (enabled) {
            long deltaTime = System.nanoTime() - startTime;
            totalTime.addAndGet(deltaTime);
            maxTime.updateAndGet(prev -> Math.max(prev, deltaTime));
            count.incrementAndGet();
        }
    }

    private void countIntern(long startTime, long... properties) {
        if (enabled) {
            countIntern(startTime);
            assert(properties.length == this.properties.size());
            for (int i = 0; i < properties.length; i++) {
                long property = properties[i];
                TimedProperty prop = this.properties.get(i);
                prop.total.addAndGet(property);
                prop.max.updateAndGet(prev->Math.max(prev,property));
                prop.min.updateAndGet(prev->Math.min(prev,property));
            }
        }
    }

    private void dump() {
        long count = this.count.get();
        if (count > 0) {
            long averageTime = totalTime.get() / count;
            StringBuilder builder = new StringBuilder();
            builder.append(System.currentTimeMillis())
                .append(" : ").append(label)
                .append(" count ").append(count)
                .append(" total ").append(totalTime.get()/1000)
                .append(" average ").append(String.format("%.2f",averageTime/1000.0))
                .append(" max ").append(maxTime.get()/1000);

            if (properties != null) {
                for (int i = 0; i < properties.size(); i++) {
                    TimedProperty prop = properties.get(i);
                    double average = 1.0 * prop.total.get() / count;
                    builder.append(" ").append(prop.propLabel).append(" ")
                        .append(String.format("%.2f", average)).append(" ")
                        .append("min ").append(prop.min.get()).append(" ")
                        .append("max ").append(prop.max.get());
                }
            }
            log.debug(builder.toString());
        }
    }

    private void reset() {
        count.set(0);
        totalTime.set(0);
        maxTime.set(0);
        if (properties != null) {
            for (int i = 0; i < properties.size(); i++) {
                TimedProperty prop = properties.get(i);
                prop.total.set(0);
                prop.max.set(Long.MIN_VALUE);
                prop.min.set(Long.MAX_VALUE);
            }
        }
    }

    public String getLabel() {
        return this.label;
    }
}