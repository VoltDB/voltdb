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
package org.voltdb.importclient.kafka.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Track a number of async procedures still in flight.
 * Used to report the number of work items consumed as a statistics.
 */
final public class PendingWorkTracker {

    public static final int SHUTDOWN_WAIT_TIME = Integer.getInteger("KAFKA_IMPORTER_MAX_SHUTDOWN_WAIT_TIME_SECONDS", 60);

    private volatile long m_workProduced = 0;
    private final LongAdder m_workConsumed = new LongAdder();

    public void produceWork() {
        m_workProduced++;
    }

    public void consumeWork() {
        m_workConsumed.increment();
    }

    public boolean waitForWorkToFinish() {
        final int attemptIntervalMs = 100;
        final int maxAttempts = (int) (TimeUnit.SECONDS.toMillis(SHUTDOWN_WAIT_TIME) / attemptIntervalMs);
        int attemptCount = 0;
        while (m_workProduced != m_workConsumed.longValue() && attemptCount < maxAttempts) {
            try {
                Thread.sleep(attemptIntervalMs);
            } catch (InterruptedException unexpected) {
            }
            attemptCount++;
        }
        return m_workProduced == m_workConsumed.longValue();
    }

    public long getCallbackCount() {
        return m_workConsumed.longValue();
    }

    @Override
    public String toString() {
        return "produced/consumed:" + m_workProduced + "/" + m_workConsumed.longValue();
    }
}
