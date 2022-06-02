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
package org.voltdb;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.iv2.MpTransactionState;
import org.voltdb.sysprocs.LowImpactDeleteNT.ResultTable;
import org.voltdb.sysprocs.MigrateRowsNT.MigrateResultTable;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.TimeUtils;

//schedule and process time-to-live feature via @LowImpactDeleteNT. The host with smallest host id
//will get the task done.
public class TTLManager extends StatsSource{

    public enum TTL {
        TIMESTAMP               (VoltType.BIGINT),
        TABLE_NAME              (VoltType.STRING),
        ROWS_DELETED            (VoltType.BIGINT),
        ROWS_DELETED_LAST_ROUND (VoltType.BIGINT),
        ROWS_REMAINING          (VoltType.BIGINT),
        LAST_DELETE_TIMESTAMP   (VoltType.TIMESTAMP);

        public final VoltType m_type;
        TTL(VoltType type) { m_type = type; }
    }

    //exception is thrown if  DR consumer gets a chunk of larger than 50MB
    //DRTupleStream.cpp
    public static final String DR_LIMIT_MSG = "bytes exceeds max DR Buffer size";
    static final int DELAY = Integer.getInteger("TIME_TO_LIVE_DELAY", 0) * 1000;
    static final int INTERVAL = Integer.getInteger("TIME_TO_LIVE_INTERVAL", 1000);
    static final int CHUNK_SIZE = Integer.getInteger("TIME_TO_LIVE_CHUNK_SIZE", 1000);
    static final int TIMEOUT = Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2000);
    public static final int NT_PROC_TIMEOUT = Integer.getInteger("NT_PROC_TIMEOUT", 1000 * 120);
    static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;

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
        final TTLStats stats;
        AtomicReference<TimeToLive> ttlRef;
        AtomicReference<Table> tableRef;
        AtomicBoolean canceled = new AtomicBoolean(false);
        public TTLTask(String tableName, TimeToLive timeToLive, Table table, TTLStats ttlStats) {
            this.tableName = tableName;
            ttlRef = new AtomicReference<>(timeToLive);
            tableRef = new AtomicReference<>(table);
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
                String stream = tableRef.get().getMigrationtarget();
                if (!StringUtil.isEmpty(stream)) {
                    migrate(cl, this);
                } else {
                    delete(cl, this);
                }
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

        public void updateTask(TimeToLive updatedTTL, Table updatedTable) {
            ttlRef.compareAndSet(ttlRef.get(), updatedTTL);
            tableRef.compareAndSet(tableRef.get(), updatedTable);
        }

        long getValue() {
            TimeToLive ttl = ttlRef.get();
            if (VoltType.get((byte)ttl.getTtlcolumn().getType()) != VoltType.TIMESTAMP) {
                return ttl.getTtlvalue();
            }
            TimeUnit timeUnit = TimeUnit.SECONDS;
            if (!ttl.getTtlunit().isEmpty()) {
                timeUnit = TimeUtils.convertTimeUnit(ttl.getTtlunit().substring(0,1));
                if (timeUnit == null) { // error, not one of smhd, so just ignore?
                    timeUnit = TimeUnit.SECONDS;
                }
            }
            return ((System.currentTimeMillis() - timeUnit.toMillis(ttl.getTtlvalue())) * 1000);
        }
        int getMaxFrequency() {
            return ttlRef.get().getMaxfrequency();
        }
        int getBatchSize() {
            return ttlRef.get().getBatchsize();
        }
        String getColumnName() {
            return ttlRef.get().getTtlcolumn().getName();
        }

        String getStream() {
            return tableRef.get().getMigrationtarget();
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
    private final Map<String, TTLTask> m_tasks = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> m_futures = new ConcurrentHashMap<>();
    private final Map<String, TTLStats> m_stats = new ConcurrentHashMap<>();

    TTLManager() {
        super(false);
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
            if (!checkTTLIndex(t, ttl.getTtlcolumn())) {
                //
                // NOTE: we let the TTL task be created in order to log rate limited error messages
                // repeatedly in the logs, complaining about the missing or incorrect index.
            }
            TTLTask task = m_tasks.get(t.getTypeName());
            if (task == null) {
                TTLStats stats = m_stats.get(t.getTypeName());
                if (!TableType.isPersistentMigrate(t.getTabletype()) && stats == null) {
                    stats = new TTLStats(t.getTypeName());
                    m_stats.put(t.getTypeName(), stats);
                }
                task = new TTLTask(t.getTypeName(), ttl, t, stats);
                m_tasks.put(t.getTypeName(), task);
                m_futures.put(t.getTypeName(),
                              m_timeToLiveExecutor.scheduleAtFixedRate(task,
                                      DELAY + random.nextInt(INTERVAL),
                                      INTERVAL, TimeUnit.MILLISECONDS));
                hostLog.info(String.format(info + " has been scheduled.", t.getTypeName()));
            } else {
                task.updateTask(ttl, t);
                hostLog.info(String.format(info + " has been updated.", t.getTypeName()));
            }
        }
    }

    private boolean checkTTLIndex(Table table, Column column) {
        boolean migrate = TableType.isPersistentMigrate(table.getTabletype());
        for (Index index : table.getIndexes()) {
            if (index.getTypeName().equals(HSQLInterface.AUTO_GEN_MATVIEW_IDX)) {
                // skip the views auto-generated index, which never has the migrate qualifiers
                continue;
            }

            for (ColumnRef colRef : index.getColumns()) {
                if (column.equals(colRef.getColumn())){
                    if (migrate && !index.getMigrating()) {
                        hostLog.warnFmt("The index on column %s.%s must be declared as \"CREATE INDEX %s ON %s (%s) WHERE NOT MIGRATING\"."
                                + " Until this is corrected, no records will be migrated.",
                                table.getTypeName(), column.getName(), index.getTypeName(), table.getTypeName(), column.getName());
                        return false;
                    }
                    return true;
                }
            }
        }

        if (migrate) {
            hostLog.warnFmt("An index is missing on column %s.%s for migrating."
                    + " It must be declared as \"CREATE INDEX myIndex ON %s (%s) WHERE NOT MIGRATING\"."
                    + " Until this is corrected, no records will be migrated.",
                    table.getTypeName(), column.getName(), table.getTypeName(), column.getName());
        } else {
            hostLog.warnFmt("An index is missing on column %s.%s for TTL. No records will be purged until an index is added.",
                    table.getTypeName(), column.getName());
        }
        return false;
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
        for (TTL col : TTL.values()) {
            columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
        };
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        Set<String> stats = new HashSet<>();
        stats.addAll(m_stats.keySet());
        return new DummyIterator(stats.iterator());
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        TTLStats stats = m_stats.get(rowKey);
        if (stats != null) {
            rowValues[TTL.TIMESTAMP.ordinal()] = System.currentTimeMillis();
            rowValues[TTL.TABLE_NAME.ordinal()] = rowKey;
            rowValues[TTL.ROWS_DELETED.ordinal()] = stats.rowsDeleted;
            rowValues[TTL.ROWS_DELETED_LAST_ROUND.ordinal()] = stats.rowsLastDeleted;
            rowValues[TTL.ROWS_REMAINING.ordinal()] = stats.rowsLeft;
            rowValues[TTL.LAST_DELETE_TIMESTAMP.ordinal()] = stats.ts;
            return TTL.values().length;
        }
        return 0;
    }

    protected void migrate(ClientInterface cl, TTLTask task) {
        CountDownLatch latch = new CountDownLatch(1);
        final ProcedureCallback cb = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse resp) throws Exception {
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    hostLog.warn(String.format("Fail to execute nibble export on table: %s, column: %s, status: %s",
                            task.tableName, task.getColumnName(), resp.getStatusString()));
                }
                else if (resp.getResults() != null && resp.getResults().length > 0) {
                    // Procedure call may succeed but response may carry a migration error
                    VoltTable t = resp.getResults()[0];
                    t.advanceRow();
                    long status = t.getLong(MigrateResultTable.STATUS);
                    if (status != ClientResponse.SUCCESS) {
                        String error = t.getString(ResultTable.MESSAGE);
                        if (t.wasNull()) {
                            error = null;
                        }
                        hostLog.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,
                                "Error migrating table %s: %s", task.tableName,
                                parseTTLError(error));
                    }
                }
                latch.countDown();
            }
        };
        cl.getDispatcher().getInternalAdapterNT().callProcedure(cl.getInternalUser(), true, NT_PROC_TIMEOUT, cb,
                "@MigrateRowsNT", new Object[] {task.tableName, task.getColumnName(), task.getValue(), "<=", task.getBatchSize(),
                        TIMEOUT, task.getMaxFrequency(), INTERVAL});
        try {
            latch.await(NT_PROC_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignored
        }
    }

    protected void delete(ClientInterface cl, TTLTask task) {
        CountDownLatch latch = new CountDownLatch(1);
        final ProcedureCallback cb = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse resp) throws Exception {
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    hostLog.warn(String.format("Fail to execute TTL on table: %s, column: %s, status: %s",
                            task.tableName, task.getColumnName(), resp.getStatusString()));
                }
                if (resp.getResults() != null && resp.getResults().length > 0) {
                    VoltTable t = resp.getResults()[0];
                    t.advanceRow();
                    String error = t.getString(ResultTable.MESSAGE);
                    if (!error.isEmpty()) {
                        String drLimitError = "";
                        if (error.indexOf(TTLManager.DR_LIMIT_MSG) > -1) {
                            // The buffer limit for a DR transaction is 50M. If over the limit,
                            // the transaction will be aborted. The same is true for nibble delete transaction.
                            // If hit this error, no more data can be deleted in this TTL table.
                            drLimitError = "The transaction exceeds DR Buffer Limit of "
                                    + MpTransactionState.DR_MAX_AGGREGATE_BUFFERSIZE
                                    + " TTL is disabled for the table. Please change BATCH_SIZE to a smaller value.";
                            task.cancel();
                            ScheduledFuture<?> fut = m_futures.get(task.tableName);
                            if (fut != null) {
                                fut.cancel(false);
                                m_futures.remove(task.tableName);
                            }
                        }
                        hostLog.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,
                                "Errors occured on TTL table %s: %s %s", task.tableName,
                                parseTTLError(error), drLimitError);
                    } else {
                        task.stats.update(t.getLong(ResultTable.ROWS_DELETED),
                                          t.getLong(ResultTable.ROWS_LEFT),
                                          t.getLong(ResultTable.LAST_DELETE_TIMESTAMP));
                    }
                }
                latch.countDown();
            }
        };
        cl.getDispatcher().getInternalAdapterNT().callProcedure(cl.getInternalUser(), true, NT_PROC_TIMEOUT, cb,
                "@LowImpactDeleteNT", new Object[] {task.tableName, task.getColumnName(), task.getValue(), "<=", task.getBatchSize(),
                        TIMEOUT, task.getMaxFrequency(), INTERVAL});
        try {
            latch.await(NT_PROC_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            hostLog.warn("TTL waiting interrupted" + e.getMessage());
        }
    }

    private String parseTTLError(String errorMsg) {
        String ret = "no reported error";
        if (StringUtils.isBlank(errorMsg)) {
            return ret;
        }
        ret = errorMsg;

        // Some of the reported errors may be JSON-serialized ClientResponseImpl objects
        try {
            JSONObject jsonObj = new JSONObject(errorMsg);
            String jsonMsg = jsonObj.getString("statusstring");
            if (!StringUtils.isBlank(jsonMsg)) {
                ret = jsonMsg;
            }
        }
        catch (Exception ex) {
            // Ignore any errors/exceptions occurring in this branch
        }

        // Keep only the first line if the message seems to contain a backtrace,
        // e.g. the most probable error on incorrect index for migrate:
        // "VOLTDB ERROR: USER ABORT Could not find migrating index. example: CREATE INDEX myindex ON ORDERS() WHERE NOT MIGRATING\n    at org.voltdb.sysprocs.MigrateRowsSP.run(MigrateRowsSP.java:28)"
        int idx = ret.indexOf('\n');
        if (idx != -1 && ret.contains("at org.")) {
            ret = ret.substring(0, idx);
        }

        return ret;
    }

}
