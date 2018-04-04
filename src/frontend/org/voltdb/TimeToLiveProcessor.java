/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
package org.voltdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;

//schedule and process time-to-live feature via @LowImpactDelete. The host with smallest host id
//will get the task done.
public class TimeToLiveProcessor {

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private ScheduledExecutorService m_timeToLiveExecutor;

    private final int m_hostId;
    private final HostMessenger m_messenger ;
    private final ClientInterface m_interface;

    public TimeToLiveProcessor(int hostId, HostMessenger hostMessenger, ClientInterface clientInterface) {
        m_hostId = hostId;
        m_messenger = hostMessenger;
        m_interface = clientInterface;
    }

    public void scheduleTimeToLiveTasks(NavigableSet<Table> ttlTables) {
        List<Integer> liveHostIds = new ArrayList<Integer>(m_messenger.getLiveHostIds());
        Collections.sort(liveHostIds);

        //if the host id is not the smallest or no TTL table, then shutdown the task if it is running.
        if (m_hostId != liveHostIds.get(0) || ttlTables == null || ttlTables.isEmpty()) {
            //shutdown if running
            if (m_timeToLiveExecutor != null) {
                m_timeToLiveExecutor.shutdown();
                hostLog.info("Time to live task has been shutdown.");
            }
            m_timeToLiveExecutor = null;
            return;
        }

        //schedule all TTL tasks.
        m_timeToLiveExecutor = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("TimeToLive"));
        final int delay = Integer.getInteger("TIME_TO_LIVE_DELAY", 30);
        final int chunkSize = Integer.getInteger("TIME_TO_LIVE_CHUNK_SIZE", 1000);
        final int timeout = Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2000);

        for (Table t : ttlTables) {
            TimeToLive ttl = t.getTimetolive().add("ttl");
            m_timeToLiveExecutor.scheduleAtFixedRate(
                    () -> {m_interface.runTimeToLive(
                            t.getTypeName(), ttl.getTtlcolumn().getName(), ttl.getTtlvalue(), chunkSize, timeout);},
                    delay, ttl.getTtlvalue(), getTimeUnit(ttl.getTtlunit()));

        }
        hostLog.info("Time to live task is started.");
    }

    public void shutDown() {
        if (m_timeToLiveExecutor != null) {
            m_timeToLiveExecutor.shutdown();
            m_timeToLiveExecutor = null;
        }
    }

    private static TimeUnit getTimeUnit(String timeUnit) {
        if ("MINUTE".equals(timeUnit)) {
            return TimeUnit.MINUTES;
        }
        if ("HOUR".equals(timeUnit)) {
            return TimeUnit.HOURS;
        }
        if ("DAY".equals(timeUnit)) {
            return TimeUnit.DAYS;
        }
        return TimeUnit.SECONDS;
    }
}
