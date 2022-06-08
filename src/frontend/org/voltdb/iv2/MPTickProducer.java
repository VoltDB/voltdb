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

package org.voltdb.iv2;

import java.io.IOException;

import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.rejoin.TaskLog;

/**
 * No-op tick producer for the MP site, allowing monitoring MP execution times.
 * <p>
 * This class evaluates the timeouts differently than {@link TickProducer}, because
 * the MP Site doesn't queue tasks to the {@link SiteTaskerQueue} when an MP is executing:
 * instead, subsequent tasks are queued in the backlog. This class evaluates timeouts on
 * the last execution of the tick itself.
 */
public class MPTickProducer extends TickProducer {

    private volatile long m_lastTickTime = -1;

    private static final String GENERAL_TEMPLATE = "A multipartition process (procedure, fragment, or operational task) is taking a long time "
            + "-- over %d seconds -- and blocking the MPI queue on host id %d. "
            + "No other jobs will be executed until it completes.";

    private static final String PROCEDURE_TEMPLATE = "The multipartition procedure %s is taking a long time "
            + "-- over %d seconds -- and blocking the MPI queue on host id %d. "
            + "No other jobs will be executed until it completes.";

    MPTickProducer(SiteTaskerQueue taskQueue, long siteId) {
        super(taskQueue, siteId);
    }

    @Override
    public void run() {
        String procName = super.getProcedure();
        m_taskQueue.offer(this);
        long waitTime = System.nanoTime() - m_lastTickTime;
        if (m_lastTickTime != -1 && waitTime >= m_procedureLogThreshold) {
            logTimeout(procName, waitTime / 1_000_000_000L);
        }
    }

    private void logTimeout(String procName, long waitSecs) {
        int host = CoreUtils. getHostIdFromHSId(m_siteId);
        if (procName == null) {
            m_logger.rateLimitedWarn(SUPPRESS_INTERVAL, GENERAL_TEMPLATE, waitSecs, host);
        } else {
            m_logger.rateLimitedWarn(SUPPRESS_INTERVAL, PROCEDURE_TEMPLATE, procName, waitSecs, host);
        }
    }

    @Override
    public void run(final SiteProcedureConnection siteConnection) {
        // no-op
        m_lastTickTime = System.nanoTime();
    }

    @Override
    public void runForRejoin(final SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException {
        // no-op
        m_lastTickTime = System.nanoTime();
    }
}
