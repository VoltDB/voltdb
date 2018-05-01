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

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.base.Charsets;

//schedule and process time-to-live feature via @LowImpactDelete. The host with smallest host id
//will get the task done.
public class TimeToLiveProcessor extends StatsSource{

    // JSON KEYS FOR SERIALIZATION
    static final String JSON_TABLE_KEY = "table";
    static final String JSON_ROWS_DELETED_KEY = "rowsDeleted";
    static final String JSON_ROUNDS_KEY = "rounds";
    static final String JSON_DELETED_LAST_ROUND_KEY = "rowsDeletedLastRound";
    static final String JSON_ROWS_LEFT_KEY = "rowsLeft";
    static final String JSON_TTL_STATS_KEY = "ttlStats";

    static final int DELAY = Integer.getInteger("TIME_TO_LIVE_DELAY", 0);
    static final int INTERVAL = Integer.getInteger("TIME_TO_LIVE_INTERVAL", 5);
    static final int CHUNK_SIZE = Integer.getInteger("TIME_TO_LIVE_CHUNK_SIZE", 1000);
    static final int TIMEOUT = Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2000);

    public static class TTLStats implements JSONString {
        final String tableName;
        long totalRowsDeleted = 0L;
        long rowsLeft = 0L;
        long rowsDeletedLastRound = 0L;
        long rounds = 0L;
        public TTLStats(String tableName) {
            this.tableName = tableName;
        }
        public void update(long deleted, long rowRemaining, long deletedLastRound, long round) {
            totalRowsDeleted += deleted;
            if (deleted > 0) {
                rowsDeletedLastRound = deletedLastRound;
                rounds += round;
                rowsLeft = rowRemaining;
                updateTTLStats(this);
            } else if (rowsLeft != rowRemaining){
                rowsLeft = rowRemaining;
                updateTTLStats(this);
            }
        }
        @Override
        public String toString() {
            return String.format("TTL stats on table %s: tuples deleted %d, tuples remaining %d", tableName, totalRowsDeleted, rowsLeft);
        }

        @Override
        public String toJSONString() {
            JSONStringer js = new JSONStringer();
            try {
                js.object();
                js.keySymbolValuePair(JSON_TABLE_KEY, tableName);
                js.keySymbolValuePair(JSON_ROWS_DELETED_KEY, totalRowsDeleted);
                js.keySymbolValuePair(JSON_ROUNDS_KEY, rounds);
                js.keySymbolValuePair(JSON_DELETED_LAST_ROUND_KEY, rowsDeletedLastRound);
                js.keySymbolValuePair(JSON_ROWS_LEFT_KEY, rowsLeft);
                js.endObject();
            } catch (JSONException e) {
                throw new RuntimeException("Failed to serialize a parameter set to JSON.", e);
            }
            return js.toString();
        }

        public static void updateTTLStats(TTLStats stats) {
            ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
            try {
                byte[] payload = stats.toJSONString().getBytes(Charsets.UTF_8);
                String path = ZKUtil.joinZKPath(VoltZK.ttl_statistics, stats.tableName);
                zk.setData(path, payload, -1);
            } catch (KeeperException | InterruptedException  e) {
                VoltDB.crashLocalVoltDB("Unable to update TTL stats to ZK, dying", true, e);
            }
        }

        public static TTLStats fromJSON(String jsonData) {
            try {
                JSONObject jsonObj = new JSONObject(jsonData);
                TTLStats stats = new TTLStats(jsonObj.getString(JSON_TABLE_KEY));
                stats.totalRowsDeleted = jsonObj.getLong(JSON_ROWS_DELETED_KEY);
                stats.rounds = jsonObj.getLong(JSON_ROUNDS_KEY);
                stats.rowsDeletedLastRound = jsonObj.getLong(JSON_DELETED_LAST_ROUND_KEY);
                stats.rowsLeft = jsonObj.getLong(JSON_ROWS_LEFT_KEY);
                return stats;
            } catch (JSONException e) {
            }
            return null;
        }
    }

    public static class TTLTask implements Runnable {

        final String tableName;
        AtomicReference<TimeToLive> ttlRef;
        final ClientInterface cl;
        final TTLStats stats;
        AtomicBoolean canceled = new AtomicBoolean(false);
        public TTLTask(String table, TimeToLive timeToLive, ClientInterface clientInterface, TTLStats ttlStats) {
            tableName = table;
            ttlRef = new AtomicReference<>(timeToLive);
            cl= clientInterface;
            stats = ttlStats;
        }
        @Override
        public void run() {
            if (!canceled.get()) {
                TimeToLive ttl = ttlRef.get();
                cl.runTimeToLive(tableName, ttl.getTtlcolumn().getName(),
                        transformValue(ttl), CHUNK_SIZE, TIMEOUT, stats);
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
            shutDown();
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
            if (ttlTables.isEmpty() || !ttlTables.containsKey(task.getKey())) {
                task.getValue().stop();
                it.remove();
                m_stats.remove(task.getKey());
                deleteTTLStatsNode(task.getKey());
                hostLog.info(String.format(info + " has been dropped.", task.getKey()));
            }
        }

        Iterator<Map.Entry<String, ScheduledFuture<?>>> fut = m_futures.entrySet().iterator();
        while(fut.hasNext()) {
            Map.Entry<String, ScheduledFuture<?>> task = fut.next();
            if (ttlTables.isEmpty() || !ttlTables.containsKey(task.getKey())) {
                task.getValue().cancel(false);
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
                TTLStats stats = new TTLStats(t.getTypeName());
                task = new TTLTask(t.getTypeName(), ttl, m_interface, stats);
                m_tasks.put(t.getTypeName(), task);
                m_stats.put(t.getTypeName(), stats);
                m_futures.put(t.getTypeName(), m_timeToLiveExecutor.scheduleAtFixedRate(task, DELAY, INTERVAL, TimeUnit.SECONDS));
                createTTLStatsNode(stats);
                hostLog.info(String.format(info + " has been scheduled.", t.getTypeName()));
            } else {
                task.updateTask(ttl);
                hostLog.info(String.format(info + " has been updated.", t.getTypeName()));
            }
        }
    }

    public void shutDown() {
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
        columns.add(new ColumnInfo("TABLE", VoltType.STRING));
        columns.add(new ColumnInfo("ROWS_DELETED", VoltType.INTEGER));
        columns.add(new ColumnInfo("ROUNDS", VoltType.INTEGER));
        columns.add(new ColumnInfo("ROWS_DELETED_LAST_ROUND", VoltType.INTEGER));
        columns.add(new ColumnInfo("ROWS_REMAINING", VoltType.INTEGER));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        readTTLStats();
        Set<String> stats = new HashSet<>();
        stats.addAll(m_stats.keySet());
        return new DummyIterator(stats.iterator());
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        TTLStats stats = m_stats.get(rowKey);
        if (stats != null) {
            rowValues[columnNameToIndex.get("TABLE")] = rowKey;
            rowValues[columnNameToIndex.get("ROWS_DELETED")] = stats.totalRowsDeleted;
            rowValues[columnNameToIndex.get("ROUNDS")] = stats.rounds;
            rowValues[columnNameToIndex.get("ROWS_DELETED_LAST_ROUND")] = stats.rowsDeletedLastRound;
            rowValues[columnNameToIndex.get("ROWS_REMAINING")] = stats.rowsLeft;
        }
    }

    private void createTTLStatsNode(TTLStats stats) {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        try {
            byte[] payload = stats.toJSONString().getBytes(Charsets.UTF_8);
            String path = ZKUtil.joinZKPath(VoltZK.ttl_statistics, stats.tableName);
            zk.create(path, payload, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                VoltDB.crashLocalVoltDB("Unable to create TTL stats for " + stats.tableName, true, e);
            }
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to create TTL stats for " + stats.tableName, true, e);
        }
    }

    private void deleteTTLStatsNode(String tableName) {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        try {
            String path = ZKUtil.joinZKPath(VoltZK.ttl_statistics, tableName);
            zk.delete(path, -1);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                hostLog.warn("Failed to remove TTL stats " + e.getMessage(), e);
            }
        } catch (InterruptedException e) {
            hostLog.warn("Failed to remove TTL stats " + e.getMessage(), e);
        }
    }

    private void readTTLStats() {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        m_stats.clear();
        try {
            List<String> tables = zk.getChildren(VoltZK.ttl_statistics, false);
            if (tables == null || tables.isEmpty()) {
                return;
            }
            for (String table : tables) {
                String path = ZKUtil.joinZKPath(VoltZK.ttl_statistics, table);
                byte[] data = zk.getData(path, false, null);
                String statData = new String(data, Charsets.UTF_8);
                TTLStats stats = TTLStats.fromJSON(statData);
                if (stats != null) {
                    m_stats.put(stats.tableName, stats);
                }
            }
        } catch (KeeperException | InterruptedException e) {
            hostLog.warn("Failed to read TTL stats " + e.getMessage(), e);
        }
    }
}
