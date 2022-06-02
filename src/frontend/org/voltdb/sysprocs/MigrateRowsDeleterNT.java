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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TTLManager;
import org.voltdb.TheHashinator;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;

public class MigrateRowsDeleterNT extends VoltNTSystemProcedure {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    static final long TIMEOUT = TimeUnit.MINUTES.toMillis(Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2));
    static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;
    public static final String ROWS_TO_BE_DELETED = "RowsRemainingDeleted";
    static int getHashinatorPartitionKey(int partitionId) {
        VoltTable partitionKeys = TheHashinator.getPartitionKeys(VoltType.INTEGER);
        while (partitionKeys.advanceRow()) {
            if (partitionId == partitionKeys.getLong("PARTITION_ID")) {
                return (int)(partitionKeys.getLong("PARTITION_KEY"));
            }
        }
        return Integer.MIN_VALUE;
    }

    /**
     *
     * @param partitionId Partition parameter
     * @param tableName Name of table that can have rows deleted
     * @param deletableTxnId All rows with this transaction or the first transaction before this can be deleted
     * @return
     */
    public VoltTable run(int partitionId, String tableName, long deletableTxnId)
    {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(String.format("Deleting migrated rows on table %s.txn id: %s",
                    tableName, TxnEgo.txnIdToString(deletableTxnId)));
        }

        VoltTable result = new VoltTable(  new ColumnInfo("STATUS", VoltType.BIGINT),
                                        new ColumnInfo("MESSAGE", VoltType.STRING));
        boolean rowsRemaining = true;
        try {
            while (rowsRemaining) {
                rowsRemaining = deleteRows(partitionId, tableName, deletableTxnId);
            }
        } catch (Exception ex) {
            result.addRow(ClientResponse.UNEXPECTED_FAILURE, ex.getMessage());
            return result;
        }
        result.addRow(ClientResponse.SUCCESS, "");
        return result;
    }

    private boolean deleteRows(int partitionId, String tableName, long deletableTxnId) throws Exception {
        CompletableFuture<ClientResponse> cf = null;
        if (partitionId == MpInitiator.MP_INIT_PID) {
            cf = callProcedure("@MigrateRowsAcked_MP", tableName, deletableTxnId);
        } else {
            cf = callProcedure("@MigrateRowsAcked_SP", getHashinatorPartitionKey(partitionId), tableName, deletableTxnId, partitionId);
        }
        ClientResponse resp = cf.get(TTLManager.NT_PROC_TIMEOUT, TimeUnit.MILLISECONDS);
        if (resp.getStatus() == ClientResponse.TXN_MISPARTITIONED){
            exportLog.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,
                    "Errors on deleting migrated row on table %s: %s", tableName, resp.getStatusString());
            // Update the hashinator and re-run the delete
            Pair<Long, byte[]> hashinator = ((ClientResponseImpl)resp).getMispartitionedResult();
            if (hashinator != null) {
                TheHashinator.updateHashinator(TheHashinator.getConfiguredHashinatorClass(),
                        hashinator.getFirst(), hashinator.getSecond(), false);
            }
            return true;
        } else if (resp.getStatus() != ClientResponse.SUCCESS) {
            exportLog.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,
                    "Errors on deleting migrated row on table %s: %s", tableName, resp.getStatusString());
            throw new VoltProcedure.VoltAbortException(resp.getAppStatusString());
        }
        VoltTable vt = resp.getResults()[0];
        while(vt.advanceRow()) {
            if (vt.getLong(ROWS_TO_BE_DELETED) == 1) {
                return true;
            }
        }
        return false;
    }
}
