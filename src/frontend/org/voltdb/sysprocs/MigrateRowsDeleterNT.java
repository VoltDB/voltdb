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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TheHashinator;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientResponse;
import org.voltdb.iv2.MpInitiator;

public class MigrateRowsDeleterNT extends VoltNTSystemProcedure {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    static final int TIMEOUT = Integer.getInteger("TIME_TO_LIVE_TIMEOUT", 2000);

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
        VoltTable ret = new VoltTable(  new ColumnInfo("ROWS_LEFT", VoltType.BIGINT),
                                        new ColumnInfo("STATUS", VoltType.BIGINT),
                                        new ColumnInfo("MESSAGE", VoltType.STRING));
        long rowsRemaining = Long.MAX_VALUE;
        long start = System.currentTimeMillis();
        try {
            while (rowsRemaining > 0 && (System.currentTimeMillis() - start) < TIMEOUT) {
                ClientResponse resp = deleteRows(partitionId, tableName, deletableTxnId, maxRowCount);
                if (resp.getStatus() == ClientResponse.SUCCESS) {
                    rowsRemaining = resp.getResults()[0].getLong("RowsRemainingDeleted");
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug(String.format("Deleting migrated rows, batch size %d, remaining %d on table %s.txn id: %d",
                                maxRowCount, rowsRemaining, tableName, deletableTxnId));
                    }
                } else {
                    ret.addRow((rowsRemaining != Long.MAX_VALUE) ? rowsRemaining : -1, ClientResponse.UNEXPECTED_FAILURE, resp.getAppStatusString());
                }
                System.out.println(String.format("@@Deleting migrated rows, %s, remaining %d on table %s.txn id: %d", resp.getAppStatusString(),
                        maxRowCount, rowsRemaining, tableName, deletableTxnId));
            }
        } catch (Exception ex) {
            ret.addRow((rowsRemaining != Long.MAX_VALUE) ? rowsRemaining : -1, ClientResponse.UNEXPECTED_FAILURE, ex.getMessage());
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
            ClientResponse cr = cf.get(1, TimeUnit.MINUTES);
            ClientResponseImpl cri = (ClientResponseImpl) cr;
            switch(cri.getStatus()) {
            case ClientResponse.TXN_MISPARTITIONED:
                cf = callProcedure("@MigrateRowsAcked_SP", getHashinatorPartitionKey(partitionId), tableName, deletableTxnId, maxRowCount);
                cr = cf.get(1, TimeUnit.MINUTES);
                return cr;
            default:
                return cr;
            }
    }
}
