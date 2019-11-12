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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;

/**
 *  Manager class that is the entry point for adding time based retention policy to PBDs.
 */
class TimeBasedRetentionPolicyMgr {
    private static final String CURSOR_NAME = "_TimeBasedRetention_";
    private static final long MIN_DELAY = 50; // minimum delay between schedules to avoid over scheduling

    // TODO: Is there a way to avoid the static?
    private static final TimeBasedRetentionPolicyMgr s_instance = new TimeBasedRetentionPolicyMgr();
    public static TimeBasedRetentionPolicyMgr getInstance() {
        return s_instance;
    }

    private ScheduledExecutorService m_scheduler = Executors.newScheduledThreadPool(2);
    private Map<String, ScheduledFuture<?>> m_futures = new HashMap<>();

    private TimeBasedRetentionPolicyMgr() {
    }

    public <E> TimeBasedRetentionPolicy<E> addTimeBasedRetentionPolicy(PersistentBinaryDeque<E> pbd, Object... params) {
        assert(params.length == 1);
        assert(params[0]!=null && params[0] instanceof Integer);
        int retainMillis = ((Integer) params[0]).intValue();
        assert (retainMillis > 0); //TODO: should be enforce a minimum?
        return new TimeBasedRetentionPolicy<E>(pbd, retainMillis);
    }

    /*
     * All tasks that delete segments based on time must be scheduled through this synchronized method.
     */
    private synchronized void scheduleTaskFor(String nonce, Runnable runnable, long delay) {
        if (!m_futures.containsKey(nonce)) { // only schedule if one doesn't exist already in queue
            m_futures.put(nonce, m_scheduler.schedule(runnable, delay, TimeUnit.MILLISECONDS));
        }
    }

    /*
     * Removal of futures must be done through this synchronized method.
     */
    private synchronized void removeTaskFuture(String nonce) {
        m_futures.remove(nonce);
    }

    /**
     * Class that enforces time based retention on a given given PBD.
     *
     * @param <E> the type of the extra metdata of the PBD
     */
    class TimeBasedRetentionPolicy<E> implements PBDRetentionPolicy {

        private final PersistentBinaryDeque<E> m_pbd;
        private final int m_retainMillis;
        private BinaryDequeReader<E> m_reader;

        public TimeBasedRetentionPolicy(PersistentBinaryDeque<E> pbd, int retainMillis) {
            m_pbd = pbd;
            m_retainMillis = retainMillis;
        }

        @Override
        public String getCursorId() {
            return CURSOR_NAME;
        }

        @Override
        public void startPolicyEnforcement() throws IOException {
            if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                m_pbd.getUsageSpecificLog().debug("Starting time based retention policy enforcement with retainMillis=" + m_retainMillis +
                        " for PBD " + m_pbd.getNonce());
            }

            m_reader = m_pbd.openForRead(CURSOR_NAME);
            scheduleTaskFor(m_pbd.getNonce(), this::deleteOldSegments, 0);
        }

        // This is called from synchronized PBD method, so calls to this should be serialized as well.
        @Override
        public void newSegmentAdded() {
            if (!m_pbd.isCursorOpen(CURSOR_NAME)) { // not started yet
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
        private void deleteOldSegments() {
            try {
                removeTaskFuture(m_pbd.getNonce()); // Remove future so that any callback after this point will queue a delete task.

                try {
                    while (m_pbd.isCursorOpen(CURSOR_NAME) && m_reader.skipToNextSegmentIfOlder(m_retainMillis));
                } catch(IOException e) {
                    m_pbd.getUsageSpecificLog().warn("Unexpected error trying to check for PBD segments to be deleted", e);
                    // We will try this again when we get the next notification
                    // that something has changed.
                    return;
                }

                if (m_pbd.isCursorOpen(CURSOR_NAME) && !m_reader.isCurrentSegmentActive()) {
                    long recordTime = m_reader.getSegmentLastRecordTimestamp();
                    if (recordTime == 0) { // cannot read last record timestamp
                        m_pbd.getUsageSpecificLog().rateLimitedLog(60, Level.WARN, null,
                                "Could not get last record time for segment in PBD %s. This may prevent enforcing time-based retention",
                                m_pbd.getNonce());
                        return;
                    }

                    long timerDelay = m_retainMillis - (System.currentTimeMillis() - recordTime);
                    try {
                        if (m_pbd.getUsageSpecificLog().isDebugEnabled()) {
                            m_pbd.getUsageSpecificLog().rateLimitedLog(60, Level.DEBUG, null,
                                    "Scheduling time-based retention for %s in %d milliseconds",
                                    m_pbd.getNonce(), Math.max(timerDelay,  MIN_DELAY));
                        }
                        scheduleTaskFor(m_pbd.getNonce(), this::deleteOldSegments, Math.max(timerDelay, MIN_DELAY));
                    } catch(RejectedExecutionException e) {
                        // Executor service has been shutdown already.
                    }
                }
            } catch(Exception e) {
                m_pbd.getUsageSpecificLog().error("Unexpected error running deleteOldSegements for pbd " + m_pbd.getNonce(), e);
            }
        }

        @Override
        public void stopPolicyEnforcement() {
            m_pbd.closeCursor(CURSOR_NAME);
            removeTaskFuture(m_pbd.getNonce());
        }
    }

}