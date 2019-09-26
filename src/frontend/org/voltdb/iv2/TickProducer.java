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

package org.voltdb.iv2;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.rejoin.TaskLog;

/**
 * Runs the tick operation against the EE
 */
public class TickProducer extends SiteTasker implements Runnable
{
    private final SiteTaskerQueue m_taskQueue;
    private ScheduledFuture<?> m_scheduledTick;
    private final long m_procedureLogThreshold;
    private final long SUPPRESS_INTERVAL = 60; // 60 seconds
    private VoltLogger m_logger;
    private int m_partitionId;
    private final long m_siteId;
    private long m_previousTaskTimestamp = -1;
    private long m_previousTaskPeekTime = -1;
    private long m_scheduledTickInterval = 1000;

    private static String TICK_MESSAGE = " A process (procedure, fragment, or operational task) is taking a long time "
            + "-- over %d seconds -- and blocking the queue for site %d (%s) "
            + "No other jobs will be executed until that process completes.";

    public TickProducer(SiteTaskerQueue taskQueue, long siteId)
    {
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

    public void changeTickInterval(long newInterval) {
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

    // Runnable.run() schedules execution
    @Override
    public void run()
    {
        m_taskQueue.offer(this);
        // check if previous task is running for more than threshold
        SiteTasker task = m_taskQueue.peek();
        long currentTime = System.nanoTime();
        long headOfQueueOfferTime;
        if (task != null) {
            headOfQueueOfferTime = task.getQueueOfferTime();
        } else {
            headOfQueueOfferTime = currentTime;
        }
        if (headOfQueueOfferTime != m_previousTaskTimestamp) {
            m_previousTaskTimestamp = headOfQueueOfferTime;
            m_previousTaskPeekTime = currentTime;
        } else if (currentTime - m_previousTaskPeekTime >= m_procedureLogThreshold) {
            long waitTime = (currentTime - m_previousTaskPeekTime)/1_000_000_000L; // in seconds
            if (m_logger.isDebugEnabled()) {
                String taskInfo = (task == null) ? "" : " Task Info: " + task.getTaskInfo();
                m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.DEBUG, null, TICK_MESSAGE + taskInfo, waitTime, m_partitionId, CoreUtils.hsIdToString(m_siteId));
                m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.DEBUG, null, "Site:" + CoreUtils.hsIdToString(m_siteId) + " " + m_taskQueue.toString());
            } else {
                m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.INFO, null, TICK_MESSAGE, waitTime, m_partitionId, CoreUtils.hsIdToString(m_siteId));
            }
        }
    }

    @Override
    public void run(final SiteProcedureConnection siteConnection)
    {
        siteConnection.tick();
    }

    @Override
    public void runForRejoin(final SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        siteConnection.tick();
    }
}
