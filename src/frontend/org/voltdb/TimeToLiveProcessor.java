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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.utils.CatalogUtil;

//schedule and process time-to-live feature via @LowImpactDelete. The host with smallest host id
//will get the task done.
public class TimeToLiveProcessor {

    public static class TimeToLiveStats {
        final String tableName;
        long rowsDeleted = 0L;
        long rowsLeft = 0L;
        public TimeToLiveStats(String tableName) {
            this.tableName = tableName;
        }
        public void update(long deleted, long rowRemaining) {
            rowsDeleted += deleted;
            rowsLeft = rowRemaining;
        }
        @Override
        public String toString() {
            return String.format("TTL stats on table %s: tuples deleted %d, tuples remaining %d", tableName, rowsDeleted, rowsLeft);
        }
    }

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private ScheduledExecutorService m_timeToLiveExecutor;

    private final int m_hostId;
    private final HostMessenger m_messenger ;
    private final ClientInterface m_interface;

    private final Map<String, TimeToLiveStats> m_stats = new HashMap<>();

    public TimeToLiveProcessor(int hostId, HostMessenger hostMessenger, ClientInterface clientInterface) {
        m_hostId = hostId;
        m_messenger = hostMessenger;
        m_interface = clientInterface;
    }

    /**
     * schedule TTL tasks per configurations
     * @param ttlTables A list of tables for TTL
     */
    public void scheduleTimeToLiveTasks(NavigableSet<Table> ttlTables) {

        //shutdown the execution tasks if they are running and reschedule them if needed
        //could be smarter here: only shutdown those dropped TTL and rescheudle updated ones
        shutDown();

        if (ttlTables == null || ttlTables.isEmpty()) return;
        //if the host id is not the smallest or no TTL table, then shutdown the task if it is running.
        List<Integer> liveHostIds = new ArrayList<Integer>(m_messenger.getLiveHostIds());
        Collections.sort(liveHostIds);
        if (m_hostId != liveHostIds.get(0)) return;

        //schedule all TTL tasks.
        m_timeToLiveExecutor = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("TimeToLive"));
        final int delay = Integer.getInteger("TIME_TO_LIVE_DELAY", 5);
        final int interval = Integer.getInteger("TIME_TO_LIVE_INTERVAL", 5);
        final int chunkSize = Integer.getInteger("TIME_TO_LIVE_CHUNK_SIZE", 1000);
        final int timeout = Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2000);

        String info = "TTL task is started for table %s, column %s";
        for (Table t : ttlTables) {
            TimeToLive ttl = t.getTimetolive().get(TimeToLiveVoltDB.TTL_NAME);
            if (!CatalogUtil.isColumnIndexed(t, ttl.getTtlcolumn())) {
                hostLog.warn("An index is missing on column " + t.getTypeName() + "." + ttl.getTtlcolumn().getName() + " for TTL");
                continue;
            }

            TimeToLiveStats stats = m_stats.get(t.getTypeName());
            if (stats == null) {
                stats =  new TimeToLiveStats(t.getTypeName());
                m_stats.put(t.getTypeName(), stats);
            }
            m_timeToLiveExecutor.scheduleAtFixedRate(
                    () -> {m_interface.runTimeToLive(
                            t.getTypeName(), ttl.getTtlcolumn().getName(),
                            transformValue(ttl.getTtlcolumn(), ttl.getTtlunit(), ttl.getTtlvalue()),
                            chunkSize, timeout,
                            m_stats.get(t.getTypeName()));},
                    delay, interval, TimeUnit.SECONDS);

            hostLog.info(String.format(info, t.getTypeName(), ttl.getTtlcolumn().getName()));
        }
    }

    public void shutDown() {
        if (m_timeToLiveExecutor != null) {
            m_timeToLiveExecutor.shutdown();
            m_timeToLiveExecutor = null;
        }
    }

    private long transformValue(Column col, String unit, int value) {
        if (VoltType.get((byte)col.getType()) != VoltType.TIMESTAMP) {
            return value;
        }
        TimeUnit timeUnit = TimeUnit.SECONDS;
        if ("MINUTE".equalsIgnoreCase(unit)) {
            timeUnit = TimeUnit.MINUTES;
        } else if ("HOUR".equalsIgnoreCase(unit)) {
            timeUnit = TimeUnit.HOURS;
        }else if ("DAY".equalsIgnoreCase(unit)) {
            timeUnit =  TimeUnit.DAYS;
        }
        return ((System.currentTimeMillis() - timeUnit.toMillis(value)) * 1000);
    }
}
