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

public class LowImpactDeleteNT extends VoltNTSystemProcedure {
    VoltLogger hostLog = new VoltLogger("HOST");

    public static enum ComparisonOperation {
        GT,
        LT,
        GTE,
        LTE,
        EQ;

        static ComparisonOperation fromString(String op) {
            op = op.trim();
            if (op.equals(">")) return GT;
            if (op.equals("<")) return LT;
            if (op.equals(">=")) return GTE;
            if (op.equals("<=")) return LTE;
            if (op.equals("==")) return EQ;

            throw new VoltAbortException("Invalid comparison operation: " + op);
        }

        public String toString() {
            switch (this) {
            case GT:
                return ">";
            case LT:
                return "<";
            case GTE:
                return ">=";
            case LTE:
                return "<=";
            case EQ:
                return "==";
            default:
                return null;
            }
        }
    }

    public static interface ResultTable {
        public static final String ROWS_DELETED = "ROWS_DELETED";
        public static final String ROWS_LEFT = "ROWS_LEFT";
        public static final String DELETED_LAST_ROUND = "DELETED_LAST_ROUND";
        public static final String LAST_DELETE_TIMESTAMP = "LAST_DELETE_TIMESTAMP";
        public static final String STATUS = "STATUS";
        public static final String MESSAGE = "MESSAGE";
    }

    Table getValidatedTable(CatalogContext ctx, String tableName) {
        tableName = tableName.trim();
        // Get the metadata object for the table in question from the global metadata/config
        // store, the Catalog.
        Table catTable = ctx.database.getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException(String.format("Table \"%s\" not found.", tableName));
        }
        return catTable;
    }

    Column getValidatedColumn(Table table, String columnName) {
        columnName = columnName.trim();
        // get the column
        Column column = table.getColumns().getIgnoreCase(columnName);
        if (column == null) {
            throw new VoltAbortException(String.format("Column \"%s\" not found in table \"%s\".", columnName, table));
        }
        return column;
    }

    Object getValidatedValue(VoltType type, String value) {
        // do this mostly just to see if it works
        try {
            return ParameterConverter.tryToMakeCompatible(type.classFromType(), value);
        }
        catch (Exception e) {
            throw new VoltAbortException(String.format("Unable to convert provided parameter value to column type: \"%s\".",
                    type.classFromType().getCanonicalName()));
        }
    }

    static class NibbleStatus {
        final AtomicLong rowsDeleted;
        long rowsLeft;
        long rowsJustDeleted;
        String errorMessages;

        NibbleStatus(long rowsLeft, long rowsJustDeleted, String errorMessages) {
            this.rowsLeft = rowsLeft;
            this.rowsJustDeleted = rowsJustDeleted;
            rowsDeleted = new AtomicLong(rowsJustDeleted);
            this.errorMessages = errorMessages;
        }
    }

    NibbleStatus runNibbleDeleteOperation(
            String tableName,
            String columnName,
            String comparisonOp,
            Object value,
            long chunksize,
            boolean isReplicated) {
        long rowsJustDeleted = 0;
        long rowsLeft = 0;
        int ONE = 1;
        VoltTable parameter = new VoltTable(new ColumnInfo[] {
                new ColumnInfo("col1", VoltType.typeFromObject(value)),
        });
        parameter.addRow(value);
        if (isReplicated) {
            try {
                CompletableFuture<ClientResponse> cf = callProcedure("@NibbleDeleteMP", tableName, columnName, comparisonOp, parameter, chunksize);
                ClientResponse cr;
                try {
                    cr = cf.get(ONE, TimeUnit.MINUTES);
                } catch (Exception e) {
                    return new NibbleStatus(-1, rowsJustDeleted, "TTL system procedure task failed after timeout (60 seconds)");
                }
                ClientResponseImpl cri = (ClientResponseImpl) cr;
                switch(cri.getStatus()) {
                case ClientResponse.SUCCESS:
                    VoltTable result = cri.getResults()[0];
                    result.advanceRow();
                    rowsJustDeleted = result.getLong("DELETED_ROWS");
                    rowsLeft = result.getLong("LEFT_ROWS");
                    break;
                case ClientResponse.RESPONSE_UNKNOWN:
                    // Could because node failure, nothing to do here I guess
                    break;
                default:
                    return new NibbleStatus(rowsLeft, rowsJustDeleted, cri.toJSONString());
                }
            } catch (Exception e) {
                return new NibbleStatus(rowsLeft, rowsJustDeleted, e.getMessage());
            }
        } else {
            // for single partitioned table, run the smart delete everywhere
            CompletableFuture<ClientResponseWithPartitionKey[]> pf = null;
            try {
                pf = callAllPartitionProcedure("@NibbleDeleteSP", tableName, columnName, comparisonOp, parameter, chunksize);
            } catch (Exception e) {
                return new NibbleStatus(rowsLeft, rowsJustDeleted, e.getMessage());
            }
            ClientResponseWithPartitionKey[] crs;
            try {
                crs = pf.get(ONE, TimeUnit.MINUTES);
            } catch (Exception e) {
                return new NibbleStatus(-1, rowsJustDeleted, "TTL system procedure task failed after timeout (60 seconds)");
            }

            for (ClientResponseWithPartitionKey crwp : crs) {
                ClientResponseImpl cri = (ClientResponseImpl) crwp.response;
                switch (crwp.response.getStatus()) {
                case ClientResponse.SUCCESS:
                    VoltTable result = cri.getResults()[0];
                    result.advanceRow();
                    rowsJustDeleted += result.getLong("DELETED_ROWS");
                    rowsLeft += result.getLong("LEFT_ROWS");
                    break;
                case ClientResponse.RESPONSE_UNKNOWN:
                    // Could because node failure, nothing to do here I guess
                    break;
                default:
                    return new NibbleStatus(rowsLeft, rowsJustDeleted, cri.toJSONString());
                }
            }
        }
        return new NibbleStatus(rowsLeft, rowsJustDeleted, "");
    }

    public VoltTable run(
            String tableName,
            String columnName,
            String valueStr,
            String comparisonOp,
            long chunksize,
            long timeoutms,
            long maxFrequency,
            long interval)
    {
        VoltTable returnTable = new VoltTable(new ColumnInfo(ResultTable.ROWS_DELETED, VoltType.BIGINT),
                                new ColumnInfo(ResultTable.ROWS_LEFT, VoltType.BIGINT),
                                new ColumnInfo(ResultTable.DELETED_LAST_ROUND, VoltType.BIGINT),
                                new ColumnInfo(ResultTable.LAST_DELETE_TIMESTAMP, VoltType.BIGINT),
                                new ColumnInfo(ResultTable.STATUS, VoltType.BIGINT),
                                new ColumnInfo(ResultTable.MESSAGE, VoltType.STRING));

        // collect all the validated info and metadata needed
        // these throw helpful errors if they run into problems
        CatalogContext ctx = VoltDB.instance().getCatalogContext();
        Table catTable = getValidatedTable(ctx, tableName);
        Column catColumn = getValidatedColumn(catTable, columnName);
        VoltType colType = VoltType.get((byte) catColumn.getType());
        Object value = getValidatedValue(colType, valueStr);

        // always run nibble delete at least once
        NibbleStatus status = runNibbleDeleteOperation(
                    tableName,
                    columnName,
                    comparisonOp,
                    value,
                    chunksize,
                    catTable.getIsreplicated());
        long rowsLeft = status.rowsLeft;
        // If any partition receive failure, report the delete status plus the error message back.
        if (!status.errorMessages.isEmpty()) {
            returnTable.addRow(status.rowsJustDeleted, rowsLeft, status.rowsJustDeleted, System.currentTimeMillis(),
                    ClientResponse.GRACEFUL_FAILURE, status.errorMessages);
            return returnTable;
        }
        // handle the case where we're jammed from the start (no rows deleted)
        if (status.rowsJustDeleted == 0 && status.rowsLeft > 0) {
            throw new VoltAbortException(String.format(
                    "While removing tuples from table %s, first delete deleted zero tuples while %d"
                    + " still met the criteria for delete. This is unexpected, but doesn't imply corrupt state.",
                    catTable.getTypeName(), rowsLeft));
        }

        int attemptsLeft = (int)Math.min((long)Math.ceil((double)rowsLeft/(double)chunksize), (maxFrequency-1));
        if (attemptsLeft == 0) {
            returnTable.addRow(status.rowsJustDeleted, rowsLeft, status.rowsJustDeleted, System.currentTimeMillis(),
                    ClientResponse.SUCCESS, "");
            return returnTable;
        }

        //spread additional deletes within the interval
        long delay = interval/attemptsLeft;
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("TTLDeleter"));
        CountDownLatch latch = new CountDownLatch(attemptsLeft);
        String[] errors = new String[attemptsLeft];
        Arrays.fill(errors, "");
        AtomicBoolean success = new AtomicBoolean(true);
        class DeleteTask implements Runnable {
            final int attempt;
            public DeleteTask(int attempt) {
                this.attempt = attempt;
            }
            @Override
            public void run() {
                NibbleStatus thisStatus = runNibbleDeleteOperation(
                        tableName,
                        columnName,
                        comparisonOp,
                        value,
                        chunksize,
                        catTable.getIsreplicated());
                if (!thisStatus.errorMessages.isEmpty()) {
                    errors[attempt] = thisStatus.errorMessages;
                    success.set(false);
                } else {
                    status.rowsDeleted.addAndGet(thisStatus.rowsJustDeleted);
                    if (attempt == (attemptsLeft-1)) {
                        status.rowsLeft = thisStatus.rowsLeft;
                        status.rowsJustDeleted = thisStatus.rowsJustDeleted;
                    }
                }
                latch.countDown();
            }
        }
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("ttl attempts left in this round:" + attemptsLeft + " on table " + tableName);
        }
        status.rowsLeft = 0;
        status.rowsJustDeleted = 0;
        int attempts = 0;
        while (attempts < attemptsLeft) {
            DeleteTask task = new DeleteTask(attempts);
            es.schedule(task, delay * attempts, TimeUnit.MILLISECONDS);
            attempts++;
        }
        try {
            latch.await(timeoutms, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            hostLog.warn("TTL interrupted" + e.getMessage());
        } finally {
            es.shutdownNow();
        }

        returnTable.addRow(
                status.rowsDeleted,
                status.rowsLeft,
                status.rowsJustDeleted,
                System.currentTimeMillis(),
                success.get() ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE,
                success.get() ? "" : Arrays.toString(errors));
        return returnTable;
    }
}
