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

package org.voltdb.sysprocs;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hsqldb_voltpatches.lib.StringUtil;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterConverter;
import org.voltdb.VoltDB;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;

public class MigrateRowsNT extends VoltNTSystemProcedure {
    VoltLogger exportLog = new VoltLogger("EXPORT");

    private Table getValidatedTable(CatalogContext ctx, String tableName) {
        Table catTable = ctx.database.getTables().getIgnoreCase(tableName.trim());
        if (catTable == null) {
            throw new VoltAbortException(String.format("Table \"%s\" not found.", tableName));
        }
        return catTable;
    }

    private Column getValidatedColumn(Table table, String columnName) {
        Column column = table.getColumns().getIgnoreCase(columnName.trim());
        if (column == null) {
            throw new VoltAbortException(String.format("Column \"%s\" not found in table \"%s\".", columnName, table));
        }
        return column;
    }

    private Object getValidatedValue(VoltType type, String value) {
        try {
            return ParameterConverter.tryToMakeCompatible(type.classFromType(), value);
        } catch (Exception e) {
            throw new VoltAbortException(String.format("Unable to convert provided parameter value to column type: \"%s\".",
                    type.classFromType().getCanonicalName()));
        }
    }

    public static interface MigrateResultTable {
        public static final String ROWS_MIGRATED = "ROWS_MIGRATED";
        public static final String ROWS_LEFT = "ROWS_LEFT";
        public static final String MIGRATED_LAST_ROUND = "MIGRATED_LAST_ROUND";
        public static final String LAST_MIGRATED_TIMESTAMP = "LAST_MIGRATED_TIMESTAMP";
        public static final String STATUS = "STATUS";
        public static final String MESSAGE = "MESSAGE";
    }

    static class NibbleStatus {
        final AtomicLong rowsMoved;
        long rowsToBeMoved;
        long rowsJustMoved;
        String errorMessages;

        NibbleStatus(long rowsToBeMoved, long rowsJustMoved, String errorMessages) {
            this.rowsToBeMoved = rowsToBeMoved;
            this.rowsJustMoved = rowsJustMoved;
            rowsMoved = new AtomicLong(rowsJustMoved);
            this.errorMessages = errorMessages;
        }
    }

    NibbleStatus migrateRows(String tableName, String columnName, String comparisonOp,
            Object value, long chunksize, boolean isReplicated) {
        long rowsJustMoved = 0;
        long rowsToBeMoved = 0;
        int ONE = 1;
        VoltTable parameter = new VoltTable(new ColumnInfo[] {
                new ColumnInfo("col1", VoltType.typeFromObject(value)),
        });
        parameter.addRow(value);
        if (isReplicated) {
            try {
                CompletableFuture<ClientResponse> cf = callProcedure("@MigrateRowsMP", tableName,
                        columnName, comparisonOp, parameter, chunksize);
                ClientResponse cr;
                try {
                    cr = cf.get(ONE, TimeUnit.MINUTES);
                } catch (Exception e) {
                    return new NibbleStatus(-1, rowsJustMoved, "Received exception while waiting response back "
                            + "from nibble export system procedure:" + e.getMessage());
                }
                ClientResponseImpl cri = (ClientResponseImpl) cr;
                switch(cri.getStatus()) {
                case ClientResponse.SUCCESS:
                    VoltTable result = cri.getResults()[0];
                    result.advanceRow();
                    rowsJustMoved = result.getLong("MIGRATED_ROWS");
                    rowsToBeMoved = result.getLong("LEFT_ROWS");
                    break;
                case ClientResponse.RESPONSE_UNKNOWN:
                    break;
                default:
                    return new NibbleStatus(rowsToBeMoved, rowsJustMoved, cri.toJSONString());
                }
            } catch (Exception e) {
                return new NibbleStatus(rowsToBeMoved, rowsJustMoved, e.getMessage());
            }
        } else {
            CompletableFuture<ClientResponseWithPartitionKey[]> pf = null;
            try {
                pf = callAllPartitionProcedure("@MigrateRowsSP", tableName, columnName, comparisonOp, parameter, chunksize);
            } catch (Exception e) {
                return new NibbleStatus(rowsToBeMoved, rowsJustMoved, e.getMessage());
            }
            ClientResponseWithPartitionKey[] crs;
            try {
                crs = pf.get(ONE, TimeUnit.MINUTES);
            } catch (Exception e) {
                return new NibbleStatus(-1, rowsJustMoved, "Received exception while waiting response back "
                        + "from migrate rows system procedure:" + e.getMessage());
            }

            for (ClientResponseWithPartitionKey crwp : crs) {
                ClientResponseImpl cri = (ClientResponseImpl) crwp.response;
                switch (crwp.response.getStatus()) {
                case ClientResponse.SUCCESS:
                    VoltTable result = cri.getResults()[0];
                    result.advanceRow();
                    rowsJustMoved += result.getLong("MIGRATED_ROWS");
                    rowsToBeMoved += result.getLong("LEFT_ROWS");
                    break;
                case ClientResponse.RESPONSE_UNKNOWN:
                    // Could because node failure, nothing to do here I guess
                    break;
                default:
                    return new NibbleStatus(rowsToBeMoved, rowsJustMoved, cri.toJSONString());
                }
            }
        }
        return new NibbleStatus(rowsToBeMoved, rowsJustMoved, "");
    }

    public VoltTable run(String tableName, String columnName, String valueStr, String comparisonOp,
            long chunksize, long timeoutms, long maxFrequency, long interval) {

        if (exportLog.isTraceEnabled()) {
            exportLog.trace(String.format("Executing migrate rows, table %s, column %s, value %s, batchsize %d, frequency %d" ,
                    tableName, columnName, valueStr, chunksize, maxFrequency));
        }
        VoltTable returnTable = new VoltTable(
                   new ColumnInfo(MigrateResultTable.ROWS_MIGRATED,             VoltType.BIGINT),
                   new ColumnInfo(MigrateResultTable.ROWS_LEFT,                 VoltType.BIGINT),
                   new ColumnInfo(MigrateResultTable.MIGRATED_LAST_ROUND,       VoltType.BIGINT),
                   new ColumnInfo(MigrateResultTable.LAST_MIGRATED_TIMESTAMP,   VoltType.BIGINT),
                   new ColumnInfo(MigrateResultTable.STATUS,                    VoltType.BIGINT),
                   new ColumnInfo(MigrateResultTable.MESSAGE,                   VoltType.STRING));

        // collect all the validated info and metadata needed
        // these throw helpful errors if they run into problems
        CatalogContext ctx = VoltDB.instance().getCatalogContext();
        Table catTable = getValidatedTable(ctx, tableName);
        Column catColumn = getValidatedColumn(catTable, columnName);
        VoltType colType = VoltType.get((byte) catColumn.getType());
        Object value = getValidatedValue(colType, valueStr);

        // always run nibble delete at least once
        NibbleStatus status = migrateRows(tableName, columnName, comparisonOp,
                value, chunksize, catTable.getIsreplicated());
        long rowsToBeMoved = status.rowsToBeMoved;
        // If any partition receive failure, report the delete status plus the error message back.
        if (!StringUtil.isEmpty(status.errorMessages)) {
            returnTable.addRow(status.rowsJustMoved, rowsToBeMoved, status.rowsJustMoved, System.currentTimeMillis(),
                    ClientResponse.GRACEFUL_FAILURE, status.errorMessages);
            return returnTable;
        }
        // handle the case where we're jammed from the start (no rows deleted)
        if (status.rowsJustMoved == 0 && status.rowsToBeMoved > 0) {
            throw new VoltAbortException(String.format(
                    "While migrating tuples from table %s, found more tuples %d which "
                  + " still met the criteria. This is unexpected, but doesn't imply corrupt state.",
                    catTable.getTypeName(), rowsToBeMoved));
        }

        int attemptsLeft = (int)Math.min((long)Math.ceil((double)rowsToBeMoved/(double)chunksize), maxFrequency) - 1;
        // no more try
        if (attemptsLeft < 1) {
            returnTable.addRow(status.rowsJustMoved, rowsToBeMoved, status.rowsJustMoved, System.currentTimeMillis(),
                    ClientResponse.SUCCESS, "");
            return returnTable;
        }

        //spread additional deletes within the interval
        long delay = TimeUnit.SECONDS.toMillis(interval)/attemptsLeft;
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("MigrateRows"));
        CountDownLatch latch = new CountDownLatch(attemptsLeft);
        String[] errors = new String[attemptsLeft];
        Arrays.fill(errors, "");
        AtomicBoolean success = new AtomicBoolean(true);
        class ExportTask implements Runnable {
            final int attempt;
            public ExportTask(int attempt) {
                this.attempt = attempt;
            }
            @Override
            public void run() {
                NibbleStatus thisStatus = migrateRows(tableName, columnName, comparisonOp,
                        value, chunksize, catTable.getIsreplicated());
                if (!thisStatus.errorMessages.isEmpty()) {
                    errors[attempt-1] = thisStatus.errorMessages;
                    success.set(false);
                } else {
                    status.rowsMoved.addAndGet(thisStatus.rowsJustMoved);
                    if (attempt == attemptsLeft) {
                        status.rowsToBeMoved = thisStatus.rowsToBeMoved;
                        status.rowsJustMoved = thisStatus.rowsJustMoved;
                    }
                }
                latch.countDown();
            }
        }
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Migrate rows attempts left in this round:" + attemptsLeft + " on table " + tableName);
        }
        status.rowsToBeMoved = 0;
        status.rowsJustMoved = 0;
        int attempts = 1;
        while (attempts <= attemptsLeft) {
            ExportTask task = new ExportTask(attempts);
            es.schedule(task, delay * attempts, TimeUnit.MILLISECONDS);
            attempts++;
        }
        try {
            latch.await(timeoutms, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            exportLog.warn("Migrate rows interrupted" + e.getMessage());
        } finally {
            es.shutdownNow();
        }

        returnTable.addRow(status.rowsMoved, status.rowsToBeMoved, status.rowsJustMoved, System.currentTimeMillis(),
                success.get() ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE,
                success.get() ? "" : Arrays.toString(errors));
        return returnTable;
    }
}
