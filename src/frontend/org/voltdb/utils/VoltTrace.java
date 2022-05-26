/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.utils;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google_voltpatches.common.collect.EvictingQueue;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 * Utility class to log Chrome Trace Event format trace messages into files.
 * Trace events are queued in a ring buffer. When the buffer is full, oldest
 * events will be removed to make room for new events. Events in the ring buffer
 * can be dumped to a file on user's request.
 *
 * This class is thread-safe.
 */
public class VoltTrace implements Runnable {

    /***********************************
     *   Static Variables of VoltTrace
     ***********************************/
    private static final VoltLogger s_logger = new VoltLogger("TRACER");
    private static volatile VoltTrace s_tracer;
    private static volatile TraceEventFilter s_tracingFilter = null;
    // Current process id. Used by all trace events.
    private static final int s_pid = CLibrary.getpid();

    /***********************************
     *   Member Variables of VoltTrace
     ***********************************/
    // Events from trace producers are put into this queue.
    // TraceFileWriter takes events from this queue and writes them to files.
    static final int QUEUE_SIZE = Integer.getInteger("VOLTTRACE_QUEUE_SIZE", 4096);
    private EvictingQueue<TraceEventBatch> m_traceEvents = EvictingQueue.create(QUEUE_SIZE);
    private EvictingQueue<TraceEventBatch> m_emptyQueue = EvictingQueue.create(QUEUE_SIZE);
    private final ListeningExecutorService m_writerThread = CoreUtils.getCachedSingleThreadExecutor("VoltTrace Writer", 1000);
    private volatile boolean m_shutdown = false;
    private volatile Runnable m_shutdownCallback = null;
    private volatile Set<Category> m_enabledCategories = ImmutableSet.of();
    private final LinkedTransferQueue<Runnable> m_work = new LinkedTransferQueue<>();

    // This is a singleton class, created through start() method below
    private VoltTrace() {}

    public enum Category {
        CI, MPI, MPSITE, SPI, SPSITE, EE, DRPRODUCER, DRCONSUMER
    }

    private static Map<Character, TraceEventType> s_typeMap = new HashMap<>();
    public enum TraceEventType {

        ASYNC_BEGIN('b'),
        ASYNC_END('e'),
        ASYNC_INSTANT('n'),
        CLOCK_SYNC('c'),
        COMPLETE('X'),
        CONTEXT(','),
        COUNTER('C'),
        DURATION_BEGIN('B'),
        DURATION_END('E'),
        FLOW_END('f'),
        FLOW_START('s'),
        FLOW_STEP('t'),
        INSTANT('i'),
        MARK('R'),
        MEMORY_DUMP_GLOBAL('V'),
        MEMORY_DUMP_PROCESS('v'),
        METADATA('M'),
        OBJECT_CREATED('N'),
        OBJECT_DESTROYED('D'),
        OBJECT_SNAPSHOT('O'),
        SAMPLE('P');

        private final char m_typeChar;

        TraceEventType(char typeChar) {
            m_typeChar = typeChar;
            s_typeMap.put(typeChar, this);
        }

        public char getTypeChar() {
            return m_typeChar;
        }

        public static TraceEventType fromTypeChar(char ch) {
            return s_typeMap.get(ch);
        }
    }

    /**
     * Trace event class annotated with JSON annotations to serialize events in the exact format
     * required by Chrome.
     */
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    public static class TraceEvent {
        private TraceEventType m_type;
        private String m_name;
        private Category m_category;
        private String m_id;
        private long m_tid;
        private long m_nanos;
        private double m_ts;
        private Object[] m_argsArr;
        private Map<String, String> m_args;

        // Empty constructor and setters for jackson deserialization for ease of testing
        public TraceEvent() {
        }

        public TraceEvent(TraceEventType type,
                          String name,
                          String asyncId,
                          Object... args) {
            m_type = type;
            m_name = name;
            m_id = asyncId;
            m_argsArr = args;
        }

        private void mapFromArgArray() {
            m_args = new HashMap<>();
            if (m_argsArr == null) {
                return;
            }

            for (int i=0; i<m_argsArr.length; i+=2) {
                if (i+1 == m_argsArr.length) {
                    break;
                }
                m_args.put(String.valueOf(m_argsArr[i]), String.valueOf(m_argsArr[i+1]));
            }
        }

        /**
         * Use the nanoTime of the first event for this file to set the sync time.
         * This is used to sync first event time on all volt nodes to zero and thus
         * make it easy to visualize multiple volt node traces.
         *
         * @param syncNanos
         */
        public void setSyncNanos(long syncNanos) {
            m_ts = (m_nanos - syncNanos)/1000.0;
        }

        @JsonIgnore
        public TraceEventType getType() {
            return m_type;
        }

        @JsonProperty("ph")
        public char getTypeChar() {
            return m_type.getTypeChar();
        }

        @JsonProperty("ph")
        public void setTypeChar(char ch) {
            m_type = TraceEventType.fromTypeChar(ch);
        }

        public String getName() {
            if (m_name != null) {
                return m_name;
            } else {
                return null;
            }
        }

        public void setName(String name) {
            m_name = name;
        }

        @JsonProperty("cat")
        public String getCategory() {
            if (m_category != null) {
                return m_category.name();
            } else {
                return null;
            }
        }

        @JsonProperty("cat")
        public void setCategory(Category cat) {
            m_category = cat;
        }

        public String getId() {
            return m_id;
        }

        public void setId(String id) {
            m_id = id;
        }

        public int getPid() {
            return s_pid;
        }

        public void setPid(int pid) {
        }

        public long getTid() {
            return m_tid;
        }

        public void setTid(long tid) {
            m_tid = tid;
        }

        @JsonIgnore
        public long getNanos() {
            return m_nanos;
        }

        @JsonIgnore
        public void setNanos(long nanos) {
            m_nanos = nanos;
        }

        /**
         * The event timestamp in microseconds.
         * @return
         */
        @JsonSerialize(using = CustomDoubleSerializer.class)
        public double getTs() {
            return m_ts;
        }

        public void setTs(long ts) {
            m_ts = ts;
        }

        public Map<String, String> getArgs() {
            if (m_args==null) {
                mapFromArgArray();
            }
            return m_args;
        }

        public void setArgs(Map<String, String> args) {
            m_args = args;
        }

        @Override
        public String toString() {
            return m_type + " " + m_name + " " + m_category + " " + m_id + " " + m_tid + " " + m_ts + " " +
                   m_nanos + " " + m_args;
        }
    }

    /**
     * Custom serializer to serialize doubles in an easily readable format in the trace file.
     */
    private static class CustomDoubleSerializer extends JsonSerializer<Double> {

        private DecimalFormat m_format = new DecimalFormat("#0.00");

        @Override
        public void serialize(Double value, JsonGenerator jsonGen, SerializerProvider sp)
        throws IOException {
            if (value == null) {
                jsonGen.writeNull();
            } else {
                jsonGen.writeNumber(m_format.format(value));
            }
        }
    }

    /**
     * Trace event filter
     * This is a singleton class.
     */
    public static class TraceEventFilter {
        // lazy initialization
        private static TraceEventFilter s_filter = null;

        private TraceEventFilter() {}

        // Return an instance of TraceEventFilter
        public static TraceEventFilter getFilterInstance() {
            if (s_filter == null) {
                s_filter = new TraceEventFilter();
            }
            return s_filter;
        }

        // m_filterTime (unit: microsecond) is a latency target set by the customers
        // It is used as the threshold to filter the trace events,
        // whose latencies are larger than the m_filterTime.
        private double m_filterTime;

        // Thread safe data structures
        // Hash map and linked deque (used as stack) to temporarily store the begin of a trace event
        private final static int INIT_CAPACITY = Integer.getInteger("VOLTTRACE_INIT_CAPACITY", 1024);
        private ConcurrentHashMap<String, Pair<TraceEventWrapper, Long>> m_asyncEvents = new ConcurrentHashMap<>(INIT_CAPACITY);
        // The concurrentLinkedDeque is used as stack to deal with nested duration begin and end events
        // The end events always match the nearest begin event
        // For example, we have a serias of nested duration events: B1, B2, E2, B3, E3, E1
        // We push begin events in the stack
        // When there comes an end event, the top event on the stack is the matched begin event and we pop it from stack
        private ConcurrentLinkedDeque<Pair<TraceEventWrapper, Long>> m_durationEvents = new ConcurrentLinkedDeque<>();

        public void setThreshold(double time) {
            m_filterTime = time;
        }

        public double getThreshold() {
            return m_filterTime;
        }

        // Put the trace events of DURATION_BEGIN or ASYNC_BGEIN type in the filter
        public void put(TraceEventType eventType, TraceEventWrapper beginWrapper, long beginTime, String eventId) {
            if (TraceEventType.DURATION_BEGIN.equals(eventType)) {
                m_durationEvents.addLast(Pair.of(beginWrapper, beginTime));
                return;
            }
            if (TraceEventType.ASYNC_BEGIN.equals(eventType)) {
                m_asyncEvents.put(eventId, Pair.of(beginWrapper, beginTime));
                return;
            }
            return;
        }

         // Check whether a trace event of DURATION_END or ASYNC_END type
         // has a matched begin event such that the latency is above the threshold (m_filterTime).
         // If so, return the trace event wrapper. Otherwise, return null.
        public TraceEventWrapper get(TraceEventType eventType, long endTime, String eventId) {
            final Pair<TraceEventWrapper, Long> p;
            if (TraceEventType.DURATION_END.equals(eventType)) {
                assert eventId == null;
                p = m_durationEvents.pollLast();
            } else if (TraceEventType.ASYNC_END.equals(eventType)) {
                p = m_asyncEvents.remove(eventId);
            } else {
                p = null;
            }
            if (p != null) {
                double diff = (double)(endTime - p.getSecond()) / 1000 - m_filterTime;
                if (diff >= 0) {
                    return p.getFirst();
                }
            }
            return null;
        }

        // Clear the trace events batch buffer after dump
        public void clear() {
            m_asyncEvents.clear();
            m_durationEvents.clear();
        }
    }

    /**
     * Creates and starts a filter. If one already exists, this is a no-op.
     * Synchronized to prevent multiple threads enabling it at the same time.
     */
    private static synchronized void createFilter() throws IOException {
        if (s_tracingFilter == null) {
            final TraceEventFilter filter = TraceEventFilter.getFilterInstance();
            s_tracingFilter = filter;
        }
    }

    public static void turnOnFilter(double time) throws IOException {
        if (s_tracingFilter == null) {
            createFilter();
        }
        s_tracingFilter.setThreshold(time);
    }

    public static void turnOffFilter() {
        if (s_tracingFilter != null) {
            s_tracingFilter.clear();
            s_tracingFilter = null;
        }
    }

    public static boolean isFilterOn() {
        return s_tracingFilter != null;
    }

    public static double getFilterTime() {
        if (s_tracingFilter == null) {
            return 0;
        }
        return s_tracingFilter.getThreshold();
    }

    /**
     * Represents a batch of trace events that belong to the same category and thread.
     */
    public static class TraceEventBatch {
        private final Category m_cat;
        private final long m_tid;

        private LinkedList<TraceEventWrapper> m_events = new LinkedList<>();

        public TraceEventBatch(Category cat) {
            m_cat = cat;
            m_tid = Thread.currentThread().getId();
        }

        /**
         * Add the event into the ring buffer. The event will stay in the buffer
         * unless user instruct to write the queue to file. If the ring buffer is
         * full, old events will be removed to make room for new events.
         * @param s A supplier of the trace event. You can use Java 8's lambda
         *          function to delay materializing the parameters in the event
         *          because they could be expensive. If the event is never written
         *          to file, the parameters will never be materialized.
         */
        public TraceEventBatch add(Supplier<TraceEvent> s) {
            TraceEventWrapper eventWrapper = new TraceEventWrapper(s);

            if (!isFilterOn()) {
                m_events.add(eventWrapper);
                return this;
            }

            filterTraceEvents(eventWrapper);
            return this;
        }

        private void filterTraceEvents(TraceEventWrapper eventWrapper) {
            TraceEvent e = eventWrapper.get(m_cat, m_tid);
            TraceEventType eventType = e.getType();

            // Skip trace events without a duration of time
            if (TraceEventType.METADATA.equals(eventType) || TraceEventType.ASYNC_INSTANT.equals(eventType) || TraceEventType.INSTANT.equals(eventType)) {
                return;
            }

            String eventId = e.getId();
            long eventTs = e.getNanos();
            // Check trace events with a duration of time
            if (TraceEventType.DURATION_BEGIN.equals(eventType) || TraceEventType.ASYNC_BEGIN.equals(eventType)) {
                s_tracingFilter.put(eventType, eventWrapper, eventTs, eventId);
            } else if (TraceEventType.DURATION_END.equals(eventType) || TraceEventType.ASYNC_END.equals(eventType)) {
                TraceEventWrapper beginWrapper = s_tracingFilter.get(eventType, eventTs, eventId);
                if (beginWrapper != null) {
                    m_events.add(beginWrapper);
                    m_events.add(eventWrapper);
                    // add filtered tracer events to buffer
                    final VoltTrace tracer = s_tracer;
                    tracer.queueEvent(this);
                }
            }
            return;
        }

        protected TraceEvent nextEvent() {
            final TraceEventWrapper wrapper = m_events.poll();
            if (wrapper != null) {
                return wrapper.get(m_cat, m_tid);
            } else {
                return null;
            }
        }
    }

    /**
     * Wraps around the event supplier so that we can capture timestamp
     * at the time of the log.
     */
    private static class TraceEventWrapper {
        private final long m_ts = System.nanoTime();
        private final Supplier<TraceEvent> m_event;

        public TraceEventWrapper(Supplier<TraceEvent> event) {
            m_event = event;
        }

        public TraceEvent get(Category cat, long tid) {
            final TraceEvent event = m_event.get();
            event.setCategory(cat);
            event.setTid(tid);
            event.setNanos(m_ts);
            return event;
        }
    }

    private boolean isCategoryEnabled(Category cat) {
        return m_enabledCategories.contains(cat);
    }

    private void queueEvent(TraceEventBatch s) {
        m_work.offer(() -> m_traceEvents.offer(s));
        // If queue is full, drop oldest events
    }

    private ListenableFuture<?> dumpEvents(File path) {
        if (m_emptyQueue == null || m_traceEvents.isEmpty()) {
            return null;
        }

        final EvictingQueue<TraceEventBatch> writeQueue = m_traceEvents;
        m_traceEvents = m_emptyQueue;
        m_emptyQueue = null;

        final ListenableFuture<?> future = m_writerThread.submit(new TraceFileWriter(path, writeQueue));
        future.addListener(() -> m_work.offer(() -> m_emptyQueue = writeQueue), CoreUtils.SAMETHREADEXECUTOR);
        return future;
    }

    /**
     * Write the events in the queue to file.
     * @param logDir The directory to write the file to.
     * @return The file path if successfully written, or null if the there is
     * already a write in progress.
     */
    private String write(String logDir) throws IOException, ExecutionException, InterruptedException {
        final File file = new File(logDir, "trace_" + System.currentTimeMillis() + ".json.gz");

        if (file.exists()) {
            throw new IOException("Trace file " + file.getAbsolutePath() + " already exists");
        }
        if (!file.getParentFile().canWrite() || !file.getParentFile().canExecute()) {
            throw new IOException("Trace file " + file.getAbsolutePath() + " is not writable");
        }

        SettableFuture<Future<?>> f = SettableFuture.create();
        m_work.offer(() -> f.set(dumpEvents(file)));
        final Future<?> writeFuture = f.get();
        if (writeFuture != null) {
            writeFuture.get(); // Wait for the write to finish without blocking new events
            return file.getAbsolutePath();
        } else {
            // A write is already in progress, ignore this request
            return null;
        }
    }

    @Override
    public void run() {
        while (!m_shutdown) {
            try {
                final Runnable work = m_work.take();
                if (work != null) {
                    work.run();
                }
            } catch (Throwable t) {}
        }
    }

    private void shutdown() {
        if (m_shutdownCallback != null) {
            try {
                m_shutdownCallback.run();
            } catch (Throwable t) {}
        }
        m_shutdown = true;
    }

    /**
     * Create a trace event batch for the given category. The events that go
     * into this batch should all originate from the same thread.
     * @param cat The category to write the events to.
     * @return The batch object to add events to, or null if trace logging for
     * the category is not enabled.
     */
    public static TraceEventBatch log(Category cat) {
        final VoltTrace tracer = s_tracer;
        if (tracer != null && tracer.isCategoryEnabled(cat)) {
            final TraceEventBatch batch = new TraceEventBatch(cat);
            if (!isFilterOn()) {
                tracer.queueEvent(batch);
            }
            return batch;
        } else {
            return null;
        }
    }

    /**
     * Creates a metadata trace event. This method does not queue the
     * event. Call {@link TraceEventBatch#add(Supplier)} to queue the event.
     */
    public static TraceEvent meta(String name, Object... args) {
        return new TraceEvent(TraceEventType.METADATA, name, null, args);
    }

    /**
     * Creates an instant trace event. This method does not queue the
     * event. Call {@link TraceEventBatch#add(Supplier)} to queue the event.
     */
    public static TraceEvent instant(String name, Object... args) {
        return new TraceEvent(TraceEventType.INSTANT, name, null, args);
    }

    /**
     * Creates a begin duration trace event. This method does not queue the
     * event. Call {@link TraceEventBatch#add(Supplier)} to queue the event.
     */
    public static TraceEvent beginDuration(String name, Object... args) {
        return new TraceEvent(TraceEventType.DURATION_BEGIN, name, null, args);
    }

    /**
     * Creates an end duration trace event. This method does not queue the
     * event. Call {@link TraceEventBatch#add(Supplier)} to queue the event.
     */
    public static TraceEvent endDuration(Object... args) {
        return new TraceEvent(TraceEventType.DURATION_END, null, null, args);
    }

    /**
     * Creates a begin async trace event. This method does not queue the
     * event. Call {@link TraceEventBatch#add(Supplier)} to queue the event.
     */
    public static TraceEvent beginAsync(String name, Object id, Object... args) {
        return new TraceEvent(TraceEventType.ASYNC_BEGIN, name, String.valueOf(id), args);
    }

    /**
     * Creates an end async trace event. This method does not queue the
     * event. Call {@link TraceEventBatch#add(Supplier)} to queue the event.
     */
    public static TraceEvent endAsync(String name, Object id, Object... args) {
        return new TraceEvent(TraceEventType.ASYNC_END, name, String.valueOf(id), args);
    }

    /**
     * Creates an async instant trace event. This method does not queue the
     * event. Call {@link TraceEventBatch#add(Supplier)} to queue the event.
     */
    public static TraceEvent instantAsync(String name, Object id, Object... args) {
        return new TraceEvent(TraceEventType.ASYNC_INSTANT, name, String.valueOf(id), args);
    }

    /**
     * Close all open files and wait for shutdown.
     * @param logDir           The directory to write the trace events to, null to skip writing to file.
     * @param timeOutMillis    Timeout in milliseconds. Negative to not wait
     * @return The path to the trace file if written, or null if a write is already in progress.
     */
    public static String closeAllAndShutdown(String logDir, long timeOutMillis) throws IOException {
        String path = null;
        final VoltTrace tracer = s_tracer;

        if (tracer != null) {
            if (logDir != null) {
                path = dump(logDir);
            }

            s_tracer = null;

            if (timeOutMillis >= 0) {
                try {
                    tracer.m_writerThread.shutdownNow();
                    tracer.m_writerThread.awaitTermination(timeOutMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                }
            }

            tracer.shutdown();
        }

        return path;
    }

    /**
     * Creates and starts a new tracer. If one already exists, this is a
     * no-op. Synchronized to prevent multiple threads enabling it at the same
     * time.
     */
    private static synchronized void start() throws IOException {
        if (s_tracer == null) {
            final VoltTrace tracer = new VoltTrace();
            final Thread thread = new Thread(tracer);
            thread.setDaemon(true);
            thread.start();
            s_tracer = tracer;
        }
    }

    /**
     * Write all trace events in the queue to file.
     * @return The file path if written successfully, or null if a write is already in progress.
     */
    public static String dump(String logDir) throws IOException {
        String path = null;
        final VoltTrace tracer = s_tracer;

        if (tracer != null) {
            final File dir = new File(logDir);
            if (!dir.getParentFile().canWrite() || !dir.getParentFile().canExecute()) {
                throw new IOException("Trace log parent directory " + dir.getParentFile().getAbsolutePath() +
                                      " is not writable");
            }
            if (!dir.exists()) {
                if (!dir.mkdir()) {
                    throw new IOException("Failed to create trace log directory " + dir.getAbsolutePath());
                }
            }

            try {
                path = tracer.write(logDir);
            } catch (Exception e) {
                s_logger.info("Unable to write trace file: " + e.getMessage(), e);
            }
        }

        // Clear the containers used in trace event filtering after dump
        if (isFilterOn()) {
            s_tracingFilter.clear();
        }

        return path;
    }

    /**
     * Enable the given categories. If the tracer is not running at the moment,
     * create a new one.
     * @param categories The categories to enable. If some of them are enabled
     * already, skip those.
     * @throws IOException
     */
    public static void enableCategories(Category... categories) throws IOException {
        if (s_tracer == null) {
            start();
        }
        final VoltTrace tracer = s_tracer;
        assert tracer != null;

        final ImmutableSet.Builder<Category> builder = ImmutableSet.builder();
        builder.addAll(tracer.m_enabledCategories);
        builder.addAll(Arrays.asList(categories));
        tracer.m_enabledCategories = builder.build();
    }

    /**
     * Disable the given categories. If the tracer has no enabled category after
     * this call, shutdown the tracer.
     * @param categories The categories to disable. If some of them are disabled
     * already, skip those.
     */
    public static void disableCategories(Category... categories) {
        final VoltTrace tracer = s_tracer;

        if (tracer == null) {
            return;
        }

        final List<Category> toDisable = Arrays.asList(categories);
        final ImmutableSet.Builder<Category> builder = ImmutableSet.builder();
        for (Category enabledCategory : tracer.m_enabledCategories) {
            if (!toDisable.contains(enabledCategory)) {
                builder.add(enabledCategory);
            }
        }
        final ImmutableSet<Category> enabledCategories = builder.build();

        if (enabledCategories.isEmpty()) {
            // All categories disabled, shutdown tracer
            try {
                closeAllAndShutdown(null, 0);
            } catch (IOException e) {}
        } else {
            tracer.m_enabledCategories = enabledCategories;
        }
    }

    /**
     * Disable all categories and shutdown the tracer.
     */
    public static void disableAllCategories() {
        final VoltTrace tracer = s_tracer;

        if (tracer == null) {
            return;
        }
        // clear the set of enabled categories
        final ImmutableSet.Builder<Category> builder = ImmutableSet.builder();
        tracer.m_enabledCategories = builder.build();

        try {
            closeAllAndShutdown(null, 0);
        } catch (IOException e) {}
    }

    /**
     * @return The categories currently enabled, or null if none.
     */
    public static Collection<Category> enabledCategories() {
        final VoltTrace tracer = s_tracer;

        if (tracer == null) {
            return Collections.emptyList();
        }

        return tracer.m_enabledCategories;
    }
}
