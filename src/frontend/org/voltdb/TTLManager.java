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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.utils.CatalogUtil;

//schedule and process time-to-live feature via @LowImpactDelete. The host with smallest host id
//will get the task done.
public class TTLManager extends StatsSource{

    //exception is thrown if  DR consumer gets a chunk of larger than 50MB
    //DRTupleStream.cpp
    public static final String DR_LIMIT_MSG = "bytes exceeds max DR Buffer size";
    static final int DELAY = Integer.getInteger("TIME_TO_LIVE_DELAY", 0) * 1000;
    static final int INTERVAL = Integer.getInteger("TIME_TO_LIVE_INTERVAL", 1) * 1000;
    static final int CHUNK_SIZE = Integer.getInteger("TIME_TO_LIVE_CHUNK_SIZE", 1000);
    static final int TIMEOUT = Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2000);

    public static class TTLStats {
        final String tableName;
        long rowsLeft = 0L;

        //Total rows deleted on this TTL control. The total count
        //will be reset if this node fails and another node takes over
        //TTL control
        long rowsDeleted = 0L;
        long rowsLastDeleted = 0L;
        Timestamp ts;
        public TTLStats(String tableName) {
            this.tableName = tableName;
        }
        public void update(long rowDeleted, long rowsLeft, long lastExecutionTimestamp) {
            this.rowsLastDeleted = rowDeleted;
            this.rowsLeft = rowsLeft;
            this.rowsDeleted += rowDeleted;
            ts = new Timestamp(lastExecutionTimestamp);
        }
        @Override
        public String toString() {
            return String.format("TTL stats on table %s: tuples deleted %d, tuples remaining %d", tableName, rowsDeleted, rowsLeft);
        }
    }

    public class TTLTask implements Runnable {

        final String tableName;
        AtomicReference<TimeToLive> ttlRef;
        final TTLStats stats;
        AtomicBoolean canceled = new AtomicBoolean(false);
        public TTLTask(String table, TimeToLive timeToLive, TTLStats ttlStats) {
            tableName = table;
            ttlRef = new AtomicReference<>(timeToLive);
            stats = ttlStats;
        }
        @Override
        public void run() {

            //do not run TTL when cluster is paused to allow proper draining of stream and dr buffer
            final VoltDBInterface voltdb = VoltDB.instance();
            if (voltdb.getMode() != OperationMode.RUNNING) {
                return;
            }
            ClientInterface cl = voltdb.getClientInterface();
            if (!canceled.get() && cl != null && cl.isAcceptingConnections()) {
                TimeToLive ttl = ttlRef.get();
                cl.runTimeToLive(ttl.getTtlcolumn().getName(),
                        transformValue(ttl), CHUNK_SIZE, TIMEOUT, this);
            }
        }

        public void cancel() {
            canceled.set(true);
            ScheduledFuture<?> fut = m_futures.get(tableName);
            if (fut != null) {
                fut.cancel(true);
                m_futures.remove(tableName);
            }
        }

        public void updateTask(TimeToLive updatedTTL) {
            ttlRef.compareAndSet(ttlRef.get(), updatedTTL);
        }

        private long transformValue(TimeToLive ttl) {
            if (VoltType.get((byte)ttl.getTtlcolumn().getType()) != VoltType.TIMESTAMP) {
                return ttl.getTtlvalue();
            }
            TimeUnit timeUnit = TimeUnit.SECONDS;
            if(!ttl.getTtlunit().isEmpty()) {
                final char frequencyUnit = ttl.getTtlunit().toLowerCase().charAt(0);
                switch (frequencyUnit) {
                case 'm':
                    timeUnit = TimeUnit.MINUTES;
                    break;
                case 'h':
                    timeUnit = TimeUnit.HOURS;
                    break;
                case 'd':
                    timeUnit = TimeUnit.DAYS;
                    break;
                default:
                    timeUnit = TimeUnit.SECONDS;
                }
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
    private static volatile TTLManager m_self;
    private final Map<String, TTLTask> m_tasks = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> m_futures = new ConcurrentHashMap<>();
    private final Map<String, TTLStats> m_stats = new ConcurrentHashMap<>();

    public static void initialze() {
        if (m_self == null) {
            synchronized (TTLManager.class) {
                if (m_self == null) {
                    m_self = new TTLManager();
                }
            }
        }
    }

    private TTLManager() {
        super(false);
    }

    public static TTLManager instance() {
        //could be called before initialized.
        initialze();
        return m_self;
    }

    /**
     * schedule TTL tasks per configurations
     * @param ttlTables A list of tables for TTL
     */
    public void scheduleTTLTasks() {

        //do not trigger TTL on replica clusters
        RealVoltDB db = (RealVoltDB)VoltDB.instance();
        if (db.getReplicationRole() == ReplicationRole.REPLICA) {
            shutDown();
            return;
        }

        Map<String, Table> ttlTables = CatalogUtil.getTimeToLiveTables(db.m_catalogContext.database);
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
                task.getValue().cancel();
                it.remove();
                m_stats.remove(task.getKey());
                hostLog.info(String.format(info + " has been dropped.", task.getKey()));
            }
        }

        //random initial delay
        final Random random = new Random();
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
                task = new TTLTask(t.getTypeName(), ttl, stats);
                m_tasks.put(t.getTypeName(), task);
                m_futures.put(t.getTypeName(), m_timeToLiveExecutor.scheduleAtFixedRate(task, DELAY + random.nextInt(INTERVAL), INTERVAL, TimeUnit.MILLISECONDS));
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
        columns.add(new ColumnInfo("TIMESTAMP", VoltType.BIGINT));
        columns.add(new ColumnInfo("TABLE_NAME", VoltType.STRING));
        columns.add(new ColumnInfo("ROWS_DELETED", VoltType.BIGINT));
        columns.add(new ColumnInfo("ROWS_DELETED_LAST_ROUND", VoltType.BIGINT));
        columns.add(new ColumnInfo("ROWS_REMAINING", VoltType.BIGINT));
        columns.add(new ColumnInfo("LAST_DELETE_TIMESTAMP", VoltType.TIMESTAMP));
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
            rowValues[columnNameToIndex.get("TIMESTAMP")] = System.currentTimeMillis();
            rowValues[columnNameToIndex.get("TABLE_NAME")] = rowKey;
            rowValues[columnNameToIndex.get("ROWS_DELETED")] = stats.rowsDeleted;
            rowValues[columnNameToIndex.get("ROWS_DELETED_LAST_ROUND")] = stats.rowsLastDeleted;
            rowValues[columnNameToIndex.get("ROWS_REMAINING")] = stats.rowsLeft;
            rowValues[columnNameToIndex.get("LAST_DELETE_TIMESTAMP")] = stats.ts;
        }
    }
}
