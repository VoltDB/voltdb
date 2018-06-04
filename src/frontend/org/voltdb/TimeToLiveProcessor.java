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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.utils.CatalogUtil;

//schedule and process time-to-live feature via @LowImpactDelete. The host with smallest host id
//will get the task done.
public class TimeToLiveProcessor extends StatsSource{

    static final int DELAY = Integer.getInteger("TIME_TO_LIVE_DELAY", 0);
    static final int INTERVAL = Integer.getInteger("TIME_TO_LIVE_INTERVAL", 5);
    static final int CHUNK_SIZE = Integer.getInteger("TIME_TO_LIVE_CHUNK_SIZE", 1000);
    static final int TIMEOUT = Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2000);

    public static class TTLStats {
        final String tableName;
        long rowsLeft = 0L;
        long rowsDeleted = 0L;
        Timestamp ts;
        public TTLStats(String tableName) {
            this.tableName = tableName;
        }
        public void update(long rowDeleted, long rowsLeft) {
            this.rowsDeleted = rowDeleted;
            this.rowsLeft = rowsLeft;
            ts = new Timestamp(System.currentTimeMillis());
        }
        @Override
        public String toString() {
            return String.format("TTL stats on table %s: tuples deleted %d, tuples remaining %d", tableName, rowsDeleted, rowsLeft);
        }
    }

    public static class TTLTask implements Runnable {

        final String tableName;
        AtomicReference<TimeToLive> ttlRef;
        final ClientInterface cl;
        final TTLStats stats;
        AtomicBoolean canceled = new AtomicBoolean(false);
        final int m_hostId;
        public TTLTask(String table, TimeToLive timeToLive, ClientInterface clientInterface, TTLStats ttlStats, int hostId) {
            tableName = table;
            ttlRef = new AtomicReference<>(timeToLive);
            cl= clientInterface;
            stats = ttlStats;
            m_hostId = hostId;
        }
        @Override
        public void run() {
            if (!canceled.get()) {
                TimeToLive ttl = ttlRef.get();
                cl.runTimeToLive(tableName, ttl.getTtlcolumn().getName(),
                        transformValue(ttl), CHUNK_SIZE, TIMEOUT, stats);
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("Executing ttl on table " + tableName +  " host:" + m_hostId);
                }
            }
        }

        public void stop() {
            canceled.set(true);
        }

        public void updateTask(TimeToLive updatedTTL) {
            ttlRef.compareAndSet(ttlRef.get(), updatedTTL);
        }

        private long transformValue(TimeToLive ttl) {
            if (VoltType.get((byte)ttl.getTtlcolumn().getType()) != VoltType.TIMESTAMP) {
                return ttl.getTtlvalue();
            }
            TimeUnit timeUnit = TimeUnit.SECONDS;
            if ("MINUTE".equalsIgnoreCase(ttl.getTtlunit())) {
                timeUnit = TimeUnit.MINUTES;
            } else if ("HOUR".equalsIgnoreCase(ttl.getTtlunit())) {
                timeUnit = TimeUnit.HOURS;
            }else if ("DAY".equalsIgnoreCase(ttl.getTtlunit())) {
                timeUnit = TimeUnit.DAYS;
            }
            return ((System.currentTimeMillis() - timeUnit.toMillis(ttl.getTtlvalue())) * 1000);
        }
    }

    private static class DummyIterator implements Iterator<Object> {
        private final Iterator<String> i;

        private DummyIterator(Iterator<String> i) {
            this.i = i;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public Object next() {
            return i.next();
        }

        @Override
        public void remove() {
            i.remove();
        }
    }
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private ScheduledThreadPoolExecutor m_timeToLiveExecutor;

    private final int m_hostId;
    private final HostMessenger m_messenger ;
    private final ClientInterface m_interface;
    private final Map<String, TTLTask> m_tasks = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> m_futures = new HashMap<>();
    private final Map<String, TTLStats> m_stats = new HashMap<>();

    public TimeToLiveProcessor(int hostId, HostMessenger hostMessenger, ClientInterface clientInterface) {
        super(false);
        m_hostId = hostId;
        m_messenger = hostMessenger;
        m_interface = clientInterface;
    }

    /**
     * schedule TTL tasks per configurations
     * @param ttlTables A list of tables for TTL
     */
    public void scheduleTimeToLiveTasks(Map<String, Table> ttlTables) {

        //if the host id is not the smallest or no TTL table, then shutdown the task if it is running.
        List<Integer> liveHostIds = new ArrayList<Integer>(m_messenger.getLiveHostIds());
        Collections.sort(liveHostIds);
        if (m_hostId != liveHostIds.get(0)) {
            return;
        }

        if (m_timeToLiveExecutor == null && !ttlTables.isEmpty()) {
            m_timeToLiveExecutor = CoreUtils.getScheduledThreadPoolExecutor("TimeToLive", 1, CoreUtils.SMALL_STACK_SIZE);
            m_timeToLiveExecutor.setRemoveOnCancelPolicy(true);
        }

        //remove dropped TTL tasks
        String info = "TTL task for table %s";
        Iterator<Map.Entry<String, TTLTask>> it = m_tasks.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, TTLTask> task = it.next();
            if (!ttlTables.containsKey(task.getKey())) {
                task.getValue().stop();
                it.remove();
                m_stats.remove(task.getKey());
                hostLog.info(String.format(info + " has been dropped.", task.getKey()));
            }
        }

        Iterator<Map.Entry<String, ScheduledFuture<?>>> fut = m_futures.entrySet().iterator();
        while(fut.hasNext()) {
            Map.Entry<String, ScheduledFuture<?>> task = fut.next();
            if (!ttlTables.containsKey(task.getKey())) {
                task.getValue().cancel(true);
                fut.remove();
            }
        }

        for (Table t : ttlTables.values()) {
            TimeToLive ttl = t.getTimetolive().get(TimeToLiveVoltDB.TTL_NAME);
            if (!CatalogUtil.isColumnIndexed(t, ttl.getTtlcolumn())) {
                hostLog.warn("An index is missing on column " + t.getTypeName() + "." + ttl.getTtlcolumn().getName() +
                        " for TTL. No records will be purged until an index is added.");
                continue;
            }
            TTLTask task = m_tasks.get(t.getTypeName());
            if (task == null) {
                TTLStats stats = m_stats.get(t.getTypeName());
                if (stats == null) {
                    stats = new TTLStats(t.getTypeName());
                    m_stats.put(t.getTypeName(), stats);
                }
                task = new TTLTask(t.getTypeName(), ttl, m_interface, stats, m_hostId);
                m_tasks.put(t.getTypeName(), task);
                m_futures.put(t.getTypeName(), m_timeToLiveExecutor.scheduleAtFixedRate(task, DELAY, INTERVAL, TimeUnit.SECONDS));
                hostLog.info(String.format(info + " has been scheduled.", t.getTypeName()));
            } else {
                task.updateTask(ttl);
                hostLog.info(String.format(info + " has been updated.", t.getTypeName()));
            }
        }
    }

    public void shutDown() {
        for (Map.Entry<String, ScheduledFuture<?>> fut: m_futures.entrySet()) {
            fut.getValue().cancel(true);
            hostLog.info("Removing ttl task on this host for " + fut.getKey());
        }
        if (m_timeToLiveExecutor != null) {
            try {
                m_timeToLiveExecutor.shutdown();
            } catch (Exception e) {
                hostLog.warn("Time to live execution shutdown", e);
            }
            m_timeToLiveExecutor = null;
        }
        m_tasks.clear();
        m_futures.clear();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        columns.add(new ColumnInfo("TIMESTAMP", VoltType.TIMESTAMP));
        columns.add(new ColumnInfo("TABLE", VoltType.STRING));
        columns.add(new ColumnInfo("ROWS_DELETED", VoltType.INTEGER));
        columns.add(new ColumnInfo("ROWS_LEFT", VoltType.INTEGER));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        Set<String> stats = new HashSet<>();
        stats.addAll(m_stats.keySet());
        return new DummyIterator(stats.iterator());
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        TTLStats stats = m_stats.get(rowKey);
        if (stats != null) {
            rowValues[columnNameToIndex.get("TIMESTAMP")] = stats.ts;
            rowValues[columnNameToIndex.get("TABLE")] = rowKey;
            rowValues[columnNameToIndex.get("ROWS_DELETED")] = stats.rowsDeleted;
            rowValues[columnNameToIndex.get("ROWS_LEFT")] = stats.rowsLeft;
        }
    }
}
