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

package org.voltdb.iv2;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.rejoin.TaskLog;

/**
 * Runs the tick operation against the EE
 */
public class TickProducer extends SiteTasker implements Runnable {
    protected final VoltLogger m_logger;
    protected final long m_procedureLogThreshold;
    protected final SiteTaskerQueue m_taskQueue;
    protected final long SUPPRESS_INTERVAL = 60; // 60 seconds
    protected final int m_partitionId;
    protected final long m_siteId;

    private ScheduledFuture<?> m_scheduledTick;
    private long m_previousTaskTimestamp = -1;
    private long m_previousTaskPeekTime = -1;
    private long m_scheduledTickInterval = 1000;

    private static final String GENERAL_TEMPLATE = "A process (procedure, fragment, or operational task) is taking a long time "
            + "-- over %d seconds -- and blocking the queue for site %d (%s). "
            + "No other jobs will be executed until it completes. %s";

    private static final String PROCEDURE_TEMPLATE = "The procedure %s is taking a long time "
            + "-- over %d seconds -- and blocking the queue for site %d (%s). "
            + "No other jobs will be executed until it completes. %s";

    private final AtomicReference<String> m_procedureName = new AtomicReference<>();

    TickProducer(SiteTaskerQueue taskQueue, long siteId) {
        m_taskQueue = taskQueue;
        m_logger = new VoltLogger("HOST");
        m_partitionId = taskQueue.getPartitionId();
        // get warning threshold from deployment
        // convert to nano seconds (default 10s)
        m_procedureLogThreshold = 1_000_000L * VoltDB.instance()
                                .getCatalogContext()
                                .getDeployment()
                                .getSystemsettings()
                                .getProcedure()
                                .getLoginfo();
        m_siteId = siteId;
        m_scheduledTickInterval = VoltDB.instance()
                .getCatalogContext()
                .getDeployment()
                .getSystemsettings()
                .getFlushinterval().getMinimum();

        m_scheduledTick = VoltDB.instance().schedulePriorityWork(
                this,
                m_scheduledTickInterval,
                m_scheduledTickInterval,
                TimeUnit.MILLISECONDS);
    }

    void cancel() {
        m_scheduledTick.cancel(true);
    }

    void changeTickInterval(long newInterval) {
        if (newInterval != m_scheduledTickInterval) {
            m_scheduledTick.cancel(false);
            m_scheduledTick = VoltDB.instance().schedulePriorityWork(
                    this,
                    Math.min(m_scheduledTickInterval, newInterval),
                    newInterval,
                    TimeUnit.MILLISECONDS);
            m_scheduledTickInterval = newInterval;
        }
    }

    // Runnable.run() schedules execution.
    // Checks whether current task has been running for a long time, inferred
    // by checking whether the head of the queue has been waiting at the
    // head of the queue for a long time.
    //
    // This routine is carefully sequenced so that we read the name of a
    // possibly-long-running proc first of all. If the head of queue changes
    // after that, then we know that proc is no longer long-running, and we
    // won't use the name. The reverse order would risk getting a wrong name.
    @Override
    public void run() {
        String procName = getProcedure();
        m_taskQueue.offer(this);
        SiteTasker task = m_taskQueue.peek();
        long currentTime = System.nanoTime();
        long headOfQueueOfferTime;
        if (task != null) {
            // The task could be us, could be some other task
            headOfQueueOfferTime = task.getQueueOfferTime();
        } else {
            // The tick task we just offered is already running on the site
            headOfQueueOfferTime = currentTime;
        }
        if (headOfQueueOfferTime != m_previousTaskTimestamp) {
            // New head of queue, therefore current task must have changed
            m_previousTaskTimestamp = headOfQueueOfferTime;
            m_previousTaskPeekTime = currentTime;
        } else {
            // Wait time is calculated on when we first saw it at head.
            long waitTime = currentTime - m_previousTaskPeekTime;
            if (waitTime >= m_procedureLogThreshold) {
                // Head of queue is blocked by the executing proc.
                logTimeout(procName, waitTime/1_000_000_000L, task);
            }
        }
    }

    // Log message for a long-executing task. Identifies procedure by name
    // when possible, otherwise logs a generalized message.
    private void logTimeout(String procName, long waitSecs, SiteTasker task) {
        String site = CoreUtils.hsIdToString(m_siteId);
        String taskInfo = "";
        if (m_logger.isDebugEnabled() && task != null) {
            taskInfo = " Task Info: " + task.getTaskInfo();
        }
        if (procName == null) {
            m_logger.rateLimitedWarn(SUPPRESS_INTERVAL, GENERAL_TEMPLATE,
                    waitSecs, m_partitionId, site, taskInfo);
        } else {
            m_logger.rateLimitedWarn(SUPPRESS_INTERVAL, PROCEDURE_TEMPLATE,
                    procName, waitSecs, m_partitionId, site, taskInfo);
        }
    }

    @Override
    public void run(final SiteProcedureConnection siteConnection) {
        siteConnection.tick();
    }

    @Override
    public void runForRejoin(final SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException {
        siteConnection.tick();
    }

    // These are used to record the name of the
    // currently-executing procedure task, if any.

    public void setupProcedure(String procedureName) {
        m_procedureName.set(procedureName);
    }

    public void completeProcedure()  {
        m_procedureName.set(null);
    }

    protected String getProcedure() {
        return m_procedureName.get();
    }
}
