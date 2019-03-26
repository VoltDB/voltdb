/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.TheHashinator;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientResponse;
import org.voltdb.iv2.MpInitiator;

public class MigrateRowsDeleterNT extends VoltNTSystemProcedure {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    static final long TIMEOUT = TimeUnit.MINUTES.toMillis(Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2));
    static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;

    static int getHashinatorPartitionKey(int partitionId) {
        VoltTable partitionKeys = TheHashinator.getPartitionKeys(VoltType.INTEGER);
        while (partitionKeys.advanceRow()) {
            if (partitionId == partitionKeys.getLong("PARTITION_ID")) {
                return (int)(partitionKeys.getLong("PARTITION_KEY"));
            }
        }
        return Integer.MIN_VALUE;
    }

    public VoltTable run(
            int partitionId,             // Partition parameter
            String tableName,            // Name of table that can have rows deleted
            long deletableTxnId,         // All rows with TxnIds before this can be deleted
            int maxRowCount)             // Maximum rows to be deleted that will fit in a DR buffer
    {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(String.format("Deleting migrated rows, batch size %d on table %s.txn id: %d",
                    maxRowCount, tableName, deletableTxnId));
        }

        VoltTable ret = new VoltTable(  new ColumnInfo("ROWS_LEFT", VoltType.BIGINT),
                                        new ColumnInfo("STATUS", VoltType.BIGINT),
                                        new ColumnInfo("MESSAGE", VoltType.STRING));
        long rowsRemaining = Long.MIN_VALUE;
        try {
            ClientResponse resp = deleteRows(partitionId, tableName, deletableTxnId, maxRowCount);
            if (resp.getStatus() == ClientResponse.TXN_MISPARTITIONED){
                resp = deleteRows(partitionId, tableName, deletableTxnId, maxRowCount);
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    exportLog.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,
                            "Errors on deleting migrated row on table %s: %s", tableName, resp.getStatusString());
                    ret.addRow((rowsRemaining != Long.MIN_VALUE) ? rowsRemaining : -1, ClientResponse.UNEXPECTED_FAILURE, resp.getAppStatusString());
                    return ret;
                }
            } else if (resp.getStatus() != ClientResponse.SUCCESS) {
                exportLog.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,
                        "Errors on deleting migrated row on table %s: %s", tableName, resp.getStatusString());
                ret.addRow((rowsRemaining != Long.MIN_VALUE) ? rowsRemaining : -1, ClientResponse.UNEXPECTED_FAILURE, resp.getAppStatusString());
                return ret;
            }
            VoltTable vt = resp.getResults()[0];
            if (vt.advanceRow()) {
                rowsRemaining = vt.getLong("RowsRemainingDeleted");
            }
            if (exportLog.isDebugEnabled()) {
                exportLog.debug(String.format("Deleting migrated rows, batch size %d, remaining %d on table %s.txn id: %d",
                        maxRowCount, rowsRemaining, tableName, deletableTxnId));
            }
            if (rowsRemaining > 0) {
                int attemptsLeft = (int)Math.ceil((double)rowsRemaining/(double)maxRowCount);
                ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("MigrateDeleter"));
                CountDownLatch latch = new CountDownLatch(attemptsLeft);
                String[] errors = new String[attemptsLeft];
                Arrays.fill(errors, "");
                class Task implements Runnable {
                    final int attempt;
                    public Task(int attempt) {
                        this.attempt = attempt;
                    }
                    @Override
                    public void run() {
                        try {
                            ClientResponse resp = deleteRows(partitionId, tableName, deletableTxnId, maxRowCount);
                            if (resp.getStatus() != ClientResponse.SUCCESS) {
                                errors[attempt] = resp.getStatusString();
                            }
                        } catch (Exception e) {
                            exportLog.warn("Migrate delete error:" + e.getMessage());
                        } finally {
                            latch.countDown();
                        }
                    }
                }
                int attempts = 0;
                final long delay = 20;
                while (attempts < attemptsLeft) {
                    Task task = new Task(attempts);
                    es.schedule(task, delay * attempts, TimeUnit.MILLISECONDS);
                    attempts++;
                }
                try {
                    latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    exportLog.warn("Migrate delete interrupted" + e.getMessage());
                } finally {
                    es.shutdownNow();
                }
            }
        } catch (Exception ex) {
            ret.addRow((rowsRemaining != Long.MAX_VALUE) ? rowsRemaining : -1, ClientResponse.UNEXPECTED_FAILURE, ex.getMessage());
            return ret;
        }
        ret.addRow(rowsRemaining, ClientResponse.SUCCESS, "");
        return ret;
    }

    private ClientResponse deleteRows(int partitionId, String tableName, long deletableTxnId, int maxRowCount) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<ClientResponse> cf = null;
        if (partitionId == MpInitiator.MP_INIT_PID) {
            cf = callProcedure("@MigrateRowsAcked_MP", tableName, deletableTxnId, maxRowCount);
        } else {
            cf = callProcedure("@MigrateRowsAcked_SP", getHashinatorPartitionKey(partitionId), tableName, deletableTxnId, maxRowCount);
        }
        return cf.get(5, TimeUnit.MINUTES);
    }
}
