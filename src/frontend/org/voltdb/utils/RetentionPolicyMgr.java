/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.BinaryDeque.RetentionPolicyType;

import com.google.common.collect.ImmutableMap;

/**
 *  Manager class that is the entry point for adding a time based or size based retention policy to a PBD.
 */
public class RetentionPolicyMgr {
    private static final VoltLogger LOG = new VoltLogger("HOST");

    public static class RetentionLimitException extends Exception {
        private static final long serialVersionUID = 1L;
        RetentionLimitException() { super(); }
        RetentionLimitException(String s) { super(s); }
    }

    // A map of time configuration qualifiers to millisecond value
    private static final Map<String, Long> s_timeLimitConverter;
    static {
        ImmutableMap.Builder<String, Long>bldr = ImmutableMap.builder();
        bldr.put("ss", 1000L);
        bldr.put("mn", 60_000L);
        bldr.put("hr", 60L * 60_000L);
        bldr.put("dy", 24L * 60L * 60_000L);
        bldr.put("wk", 7L * 24L * 60L * 60_000L);
        bldr.put("mo", 30L * 24L * 60L * 60_000L);
        bldr.put("yr", 365L * 24L * 60L * 60_000L);
        s_timeLimitConverter = bldr.build();
    }

    // A map of byte configuration qualifiers to bytes value
    private static final Map<String, Long> s_byteLimitConverter;
    static {
        ImmutableMap.Builder<String, Long>bldr = ImmutableMap.builder();
        bldr.put("mb", 1024L * 1024L);
        bldr.put("gb", 1024L * 1024L * 1024L);
        s_byteLimitConverter = bldr.build();
    }
    private static long s_minBytesLimitMb = 64;

    private final ScheduledThreadPoolExecutor m_scheduler;
    private final Map<PersistentBinaryDeque<?>.ReadCursor, ScheduledFuture<?>> m_futures = new HashMap<>();

    RetentionPolicyMgr(int numThreads) {
        m_scheduler = new ScheduledThreadPoolExecutor(numThreads);
        LOG.info("Initialized PBD with " + numThreads + " threads to enforce retention policy");
    }

    void updateThreadPoolSize(int numThreads) {
        if (numThreads != m_scheduler.getCorePoolSize()) {
            m_scheduler.setCorePoolSize(numThreads);
            LOG.info("Updated PBD to use " + numThreads + " threads to enforce retention policy");
        }
    }

    public int getRetentionThreadPoolSize() {
        return m_scheduler.getCorePoolSize();
    }

    public PBDRetentionPolicy addRetentionPolicy(RetentionPolicyType policyType, PersistentBinaryDeque<?> pbd, Object... params) {
        switch(policyType) {
        case TIME_MS:
            return addTimeBasedRetentionPolicy(pbd, params);
        case MAX_BYTES:
            return addMaxBytesRetentionPolicy(pbd, params);
        }

        throw new RuntimeException("Invalid retention policy type" + policyType);
    }

    private TimeBasedRetentionPolicy addTimeBasedRetentionPolicy(PersistentBinaryDeque<?> pbd, Object... params) {
        assert(params.length == 1);
        assert(params[0]!=null && params[0] instanceof Long);
        long retainMillis = ((Long) params[0]).longValue();
        assert (retainMillis > 0);
        return new TimeBasedRetentionPolicy(pbd, retainMillis);
    }

    private MaxBytesRetentionPolicy addMaxBytesRetentionPolicy(PersistentBinaryDeque<?> pbd, Object... params) {
        assert(params.length == 1);
        assert(params[0]!=null && params[0] instanceof Long);
        long maxBytes = ((Long) params[0]).longValue();
        assert (maxBytes > 0);
        return new MaxBytesRetentionPolicy(pbd, maxBytes);
    }

    /*
     * All tasks that delete segments based on time must be scheduled through this synchronized method.
     */
    synchronized void scheduleTaskFor(PersistentBinaryDeque<?>.ReadCursor reader, Runnable runnable, long delay) {
        if (!m_futures.containsKey(reader)) { // only schedule if one doesn't exist already in queue
            m_futures.put(reader, m_scheduler.schedule(runnable, delay, TimeUnit.MILLISECONDS));
        }
    }

    synchronized void replaceTaskFor(PersistentBinaryDeque<?>.ReadCursor reader, Runnable runnable, long delay) {
        ScheduledFuture<?> old = m_futures.put(reader, m_scheduler.schedule(runnable, delay, TimeUnit.MILLISECONDS));
        if (old != null) {
            old.cancel(false);
        }
    }

    /*
     * Removal of futures must be done through this synchronized method.
     */
    synchronized void removeTaskFuture(PersistentBinaryDeque<?>.ReadCursor reader) {
        m_futures.remove(reader);
    }

    /**
     * Abstract PBDRetentionPolicy class that uses this mgr class for scheduling executions.
     */
    abstract class AbstractRetentionPolicy implements PBDRetentionPolicy {

        protected final PersistentBinaryDeque<?> m_pbd;
        protected PersistentBinaryDeque<?>.ReadCursor m_reader;

        public AbstractRetentionPolicy(PersistentBinaryDeque<?> pbd) {
            m_pbd = pbd;
        }

        /**
         * Does some common operations and calls the actual retention code
         */
        protected void deleteOldSegments(PersistentBinaryDeque<?>.ReadCursor reader) {
            try {
                removeTaskFuture(reader);
                if (!reader.isOpen()) {
                    return;
                }

                // Most of the time this is a no-op.
                // But if data was added (gap filling) in a segment before the
                // current retention point, we need to seek to the beginning
                reader.seekToFirstSegment();

                // Now delete
                executeRetention(reader);
            } catch(Throwable t) {
                handleExecutionError(reader, t);
            }
        }

        /**
         * Retention policy enforcement implementations must implement this
         * with the retention logic
         */
        protected abstract void executeRetention(PersistentBinaryDeque<?>.ReadCursor reader) throws IOException;

        /**
         * Implementation specific handling of any errors during {@link #executeRetention()}
         */
        protected abstract void handleExecutionError(PersistentBinaryDeque<?>.ReadCursor reader, Throwable t);

        @Override
        public void startPolicyEnforcement() throws IOException {
            if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                m_pbd.getUsageSpecificLog().debug("Starting retention policy enforcement for PBD " + m_pbd.getNonce() +
                        " using " + this.getClass().getName());
            }

            if (m_reader != null) { // retention is already active
                if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                    m_pbd.getUsageSpecificLog().debug("Retention policy for " + m_pbd.getNonce() + " is already active");
                }
                return;
            }

            m_reader = m_pbd.openForRead(getCursorId());
            scheduleRetentionTask(0);
        }

        @Override
        public void stopPolicyEnforcement() {
            removeTaskFuture(m_reader);
            m_reader = null;
            m_pbd.closeCursor(getCursorId());
        }

        @Override
        public boolean isPolicyEnforced() {
            return m_reader != null;
        }

        protected void scheduleRetentionTask(long delay) {
            PersistentBinaryDeque<?>.ReadCursor reader = m_reader;
            scheduleTaskFor(reader, () -> deleteOldSegments(reader), delay);
        }
    }

    /**
     * Class that enforces time based retention on a given given PBD.
     */
    class TimeBasedRetentionPolicy extends AbstractRetentionPolicy {
        private static final String CURSOR_NAME = "_TimeBasedRetention_";
        private static final long MIN_DELAY = 50; // minimum delay between schedules to avoid over scheduling

        private final long m_retainMillis;

        public TimeBasedRetentionPolicy(PersistentBinaryDeque<?> pbd, long retainMillis) {
            super(pbd);
            m_retainMillis = retainMillis;
        }

        @Override
        public String getCursorId() {
            return CURSOR_NAME;
        }

        @Override
        public void bytesAdded(long numBytes) {
            //Nothing to do
        }

        // This is called from synchronized PBD method, so calls to this should be serialized as well.
        @Override
        public void newSegmentAdded(long initialBytes) {
            if (!isPolicyEnforced()) {
                return;
            }

            if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                m_pbd.getUsageSpecificLog().debug("Processing newSegmentAdded for PBD " + m_pbd.getNonce());
            }
            scheduleRetentionTask(MIN_DELAY);
        }

        @Override
        public void finishedGapSegment() {
            if (!isPolicyEnforced()) {
                return;
            }

            if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                m_pbd.getUsageSpecificLog().debug("Processing finishedGapProcessing for PBD " + m_pbd.getNonce());
            }
            PersistentBinaryDeque<?>.ReadCursor reader = m_reader;
            replaceTaskFor(reader, () -> deleteOldSegments(reader), MIN_DELAY);
        }

        // Only executed in scheduler thread.
        // Normally only one of these tasks will be executing at one time for one PBD.
        // However, if a schedule request comes right after this task removes its task future,
        // a second one could get scheduled. Since PBD operations are synchronized,
        // the execution is thread-safe.
        @Override
        protected void executeRetention(PersistentBinaryDeque<?>.ReadCursor reader) {
            try {
                while (reader.isOpen() && reader.skipToNextSegmentIfOlder(m_retainMillis));
            } catch(IOException e) {
                m_pbd.getUsageSpecificLog().warn("Unexpected error trying to check for PBD segments to be deleted", e);
                // We will try this again when we get the next notification
                // that something has changed.
                return;
            }

            if (reader.isOpen() && !reader.isCurrentSegmentActive()) {
                long recordTime = reader.getSegmentTimestamp();
                if (recordTime == PBDSegment.INVALID_TIMESTAMP) { // cannot read last record timestamp
                    if (reader.getCurrentSegment() != null) {
                        m_pbd.getUsageSpecificLog().rateLimitedLog(60, Level.WARN, null,
                                "Could not get last record time for segment in PBD %s. This may prevent enforcing time-based retention",
                                m_pbd.getNonce());
                    }
                    scheduleRetentionTask(m_retainMillis);
                    return;
                }

                long timerDelay = m_retainMillis - (System.currentTimeMillis() - recordTime);
                if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                    m_pbd.getUsageSpecificLog().rateLimitedLog(60, Level.DEBUG, null,
                            "Scheduling time-based retention for %s in %d milliseconds",
                            m_pbd.getNonce(), Math.max(timerDelay,  MIN_DELAY));
                }
                scheduleRetentionTask(Math.max(timerDelay,  MIN_DELAY));
            }
        }

        @Override
        protected void handleExecutionError(PersistentBinaryDeque<?>.ReadCursor reader, Throwable t) {
            m_pbd.getUsageSpecificLog().error("Unexpected error running deleteOldSegements for pbd " + m_pbd.getNonce(), t);
        }
    }

    /**
     * Class that enforces number of bytes based retention policy on PBDs.
     */
    class MaxBytesRetentionPolicy extends AbstractRetentionPolicy {
        private static final String CURSOR_NAME = "_MaxBytesBasedRetention_";

        private final long m_maxBytes;
        private long m_sizeNeeded = Long.MAX_VALUE;

        public MaxBytesRetentionPolicy(PersistentBinaryDeque<?> pbd, long maxBytes) {
            super(pbd);
            m_maxBytes = maxBytes;
        }

        @Override
        public String getCursorId() {
            return CURSOR_NAME;
        }

        // Only executed in the retention policy thread.
        // There will not be concurrent executions of this most of the time because
        // of how we reset m_sizeNeeded to MAX_VALUE when we schedule this task.
        // When gaps are filled, there is a possibility of scheduling a second task, but because
        // of the synchronization on PBD object, this should not cause any problems due to race conditions
        @Override
        protected void executeRetention(PersistentBinaryDeque<?>.ReadCursor reader) throws IOException {
            while (reader.isOpen()) {
                long currValue = 0;
                synchronized(m_pbd) {
                    if ((currValue = reader.skipToNextSegmentIfBigger(m_maxBytes)) != 0) {
                        m_sizeNeeded = currValue;
                        break;
                    }
                }
            }
        }

        @Override
        protected void handleExecutionError(PersistentBinaryDeque<?>.ReadCursor reader, Throwable t) {
            if (t instanceof IOException && !reader.isOpen()) { // reader got closed. May be we stopped retention enforcement
                m_pbd.getUsageSpecificLog().debug("IOException running byte based retention on " + m_pbd.getNonce(), t);
            } else {
                m_pbd.getUsageSpecificLog().error("Unexpected error running byte based retention on " + m_pbd.getNonce(), t);
            }
            m_sizeNeeded = 4096; // Try running the policy again after 4K bytes are added. Arbitrary number of bytes here.
        }

        @Override
        public void newSegmentAdded(long initialBytes) {
            bytesAdded(initialBytes);
        }

        @Override
        public void bytesAdded(long numBytes) {
            if (!isPolicyEnforced()) {
                return;
            }

            m_sizeNeeded -= numBytes;
            if (!m_reader.isCurrentSegmentActive() && m_sizeNeeded <= 0) {
                m_sizeNeeded = Long.MAX_VALUE;
                scheduleRetentionTask(0);
            }
        }

        @Override
        public void finishedGapSegment() {
            if (!isPolicyEnforced()) {
                return;
            }

            m_sizeNeeded = Long.MAX_VALUE;
            scheduleRetentionTask(0);
        }
    }

    public static long parseTimeLimit(String limitStr) throws RetentionLimitException {
        return parseLimit(limitStr, s_timeLimitConverter);
    }

    public static long parseByteLimit(String limitStr) throws RetentionLimitException {
        long limit = parseLimit(limitStr, s_byteLimitConverter);
        long minLimit = s_minBytesLimitMb * s_byteLimitConverter.get("mb");
        if (limit < minLimit) {
            throw new RetentionLimitException("Size-based retention limit must be > " + s_minBytesLimitMb + " mb");
        }
        return limit;
    }

    // Parse a retention limit qualified by a 2-character qualifier and return its converted value
    private static long parseLimit(String limitStr, Map<String, Long> cvt) throws RetentionLimitException {
        if (StringUtils.isEmpty(limitStr)) {
            throw new RetentionLimitException("empty retention limit");
        }
        String parse = limitStr.trim().toLowerCase();
        if (parse.length() <= 2) {
            throw new RetentionLimitException("\"" + limitStr + "\" is too short for a retention limit");
        }
        String qualifier = parse.substring(parse.length() - 2);
        if (!cvt.keySet().contains(qualifier)) {
            throw new RetentionLimitException("\"" + qualifier + "\" is not a valid limit qualifier: "
                    + cvt.keySet() + " are the valid values");
        }
        String valStr = parse.substring(0, parse.length() - 2);
        long limit = 0;
        try {
            limit = Long.parseLong(valStr.trim());
            limit *= cvt.get(qualifier);
        }
        catch (NumberFormatException ex) {
            throw new RetentionLimitException("Failed to parse\"" + limitStr + "\": " + ex);
        }
        if (limit <= 0) {
            throw new RetentionLimitException("A retention limit must have a positive value");
        }
        return limit;
    }
}