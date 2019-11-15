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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltdb.utils.BinaryDeque.RetentionPolicyType;

/**
 *  Manager class that is the entry point for adding time based retention policy to PBDs.
 */
class RetentionPolicyMgr {
    // TODO: Is there a way to avoid the static?
    private static final RetentionPolicyMgr s_instance = new RetentionPolicyMgr();
    public static RetentionPolicyMgr getInstance() {
        return s_instance;
    }

    private ScheduledExecutorService m_scheduler = Executors.newScheduledThreadPool(2);
    private Map<String, ScheduledFuture<?>> m_futures = new HashMap<>();

    private RetentionPolicyMgr() {
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
        assert(params[0]!=null && params[0] instanceof Integer);
        int retainMillis = ((Integer) params[0]).intValue();
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
    synchronized void scheduleTaskFor(String nonce, Runnable runnable, long delay) {
        if (!m_futures.containsKey(nonce)) { // only schedule if one doesn't exist already in queue
            m_futures.put(nonce, m_scheduler.schedule(runnable, delay, TimeUnit.MILLISECONDS));
        }
    }

    /*
     * Removal of futures must be done through this synchronized method.
     */
    synchronized void removeTaskFuture(String nonce) {
        m_futures.remove(nonce);
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
         * Method that does the actual cleaning up of PBD segments.
         */
        protected abstract void deleteOldSegments();

        @Override
        public void startPolicyEnforcement() throws IOException {
            if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                m_pbd.getUsageSpecificLog().debug("Starting retention policy enforcement for PBD " + m_pbd.getNonce() +
                        " using " + this.getClass().getName());
            }

            m_reader = m_pbd.openForRead(getCursorId());
            scheduleTaskFor(m_pbd.getNonce(), this::deleteOldSegments, 0);
        }

        @Override
        public void stopPolicyEnforcement() {
            m_pbd.closeCursor(getCursorId());
            removeTaskFuture(m_pbd.getNonce());
        }
    }

    /**
     * Class that enforces time based retention on a given given PBD.
     */
    class TimeBasedRetentionPolicy extends AbstractRetentionPolicy {
        private static final String CURSOR_NAME = "_TimeBasedRetention_";
        private static final long MIN_DELAY = 50; // minimum delay between schedules to avoid over scheduling

        private final int m_retainMillis;

        public TimeBasedRetentionPolicy(PersistentBinaryDeque<?> pbd, int retainMillis) {
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
        public void newSegmentAdded() {
            if (!m_reader.isOpen()) { // not started yet
                return;
            }

            if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                m_pbd.getUsageSpecificLog().debug("Processing newSegmentAdded for PBD " + m_pbd.getNonce());
            }
            scheduleTaskFor(m_pbd.getNonce(), this::deleteOldSegments, MIN_DELAY);
        }

        // Only executed in scheduler thread.
        // Normally only one of these tasks will be executing at one time for one PBD.
        // However, if a schedule request comes right after this task removes its task future,
        // a second one could get scheduled. Since PBD operations are synchronized,
        // the execution is thread-safe.
        @Override
        protected void deleteOldSegments() {
            try {
                removeTaskFuture(m_pbd.getNonce()); // Remove future so that any callback after this point will queue a delete task.

                try {
                    while (m_reader.isOpen() && m_reader.skipToNextSegmentIfOlder(m_retainMillis));
                } catch(IOException e) {
                    m_pbd.getUsageSpecificLog().warn("Unexpected error trying to check for PBD segments to be deleted", e);
                    // We will try this again when we get the next notification
                    // that something has changed.
                    return;
                }

                if (m_reader.isOpen() && !m_reader.isCurrentSegmentActive()) {
                    long recordTime = m_reader.getSegmentLastRecordTimestamp();
                    if (recordTime == 0) { // cannot read last record timestamp
                        m_pbd.getUsageSpecificLog().rateLimitedLog(60, Level.WARN, null,
                                "Could not get last record time for segment in PBD %s. This may prevent enforcing time-based retention",
                                m_pbd.getNonce());
                        return;
                    }

                    long timerDelay = m_retainMillis - (System.currentTimeMillis() - recordTime);
                    if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                        m_pbd.getUsageSpecificLog().rateLimitedLog(60, Level.DEBUG, null,
                                "Scheduling time-based retention for %s in %d milliseconds",
                                m_pbd.getNonce(), Math.max(timerDelay,  MIN_DELAY));
                    }
                    scheduleTaskFor(m_pbd.getNonce(), this::deleteOldSegments, Math.max(timerDelay, MIN_DELAY));
                }
            } catch(Exception e) {
                m_pbd.getUsageSpecificLog().error("Unexpected error running deleteOldSegements for pbd " + m_pbd.getNonce(), e);
            }
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
        // There will not be concurrent executions of this because
        // of how we reset m_sizeNeeded to MAX_VALUE when we schedule this task.
        @Override
        protected void deleteOldSegments() {
            try {
                removeTaskFuture(m_pbd.getNonce());

                while (m_reader.isOpen()) {
                    long currValue = 0;
                    synchronized(m_pbd) {
                        if ((currValue = m_reader.skipToNextSegmentIfBigger(m_maxBytes)) != 0) {
                            m_sizeNeeded = currValue;
                            break;
                        }
                    }
                }
            } catch(Exception e) {
                if (e instanceof IOException && m_reader.isOpen()) {
                    m_pbd.getUsageSpecificLog().debug("IOException running byte based retention on " + m_pbd.getNonce(), e);
                }
                m_pbd.getUsageSpecificLog().error("Unexpected error running byte based retention on " + m_pbd.getNonce(), e);
                m_sizeNeeded = 4096; // Try running the policy again after 4K bytes are added. Arbitrary number of bytes here.
            }
        }

        @Override
        public void newSegmentAdded() {
            // Just adding a new segment does not increase the data size of the PBD. no-op.
        }

        @Override
        public void bytesAdded(long numBytes) {
            if (!m_reader.isOpen()) {
                return;
            }

            m_sizeNeeded -= numBytes;
            if (!m_reader.isCurrentSegmentActive() && m_sizeNeeded <= 0) {
                m_sizeNeeded = Long.MAX_VALUE;
                scheduleTaskFor(m_pbd.getNonce(), this::deleteOldSegments, 0);
            }
        }
    }
}