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

package org.voltdb.sysprocs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterConverter;
import org.voltdb.TheHashinator;
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
import org.voltdb.iv2.MpInitiator;

public class LowImpactDelete extends VoltNTSystemProcedure {
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
        final long rowsLeft;
        final long rowsJustDeleted;
        final String errorMessages;

        NibbleStatus(long rowsLeft, long rowsJustDeleted, String errorMessages) {
            this.rowsLeft = rowsLeft;
            this.rowsJustDeleted = rowsJustDeleted;
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
        Map<Integer, String> errorMessages = new HashMap<>();
        if (isReplicated) {
            try {
                CompletableFuture<ClientResponse> cf = callProcedure("@NibbleDeleteMP", tableName, columnName, comparisonOp, parameter, chunksize);
                ClientResponse cr;
                try {
                    cr = cf.get(ONE, TimeUnit.MINUTES);
                } catch (Exception e) {
                    return new NibbleStatus(-1, rowsJustDeleted, "Received exception while waiting response back "
                            + "from smart delete system procedure:" + e.getMessage());
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
                    errorMessages.put(MpInitiator.MP_INIT_PID, cri.toJSONString());
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
                return new NibbleStatus(-1, rowsJustDeleted, "Received exception while waiting response back "
                        + "from smart delete system procedure:" + e.getMessage());
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
                    int partitionId = TheHashinator.getPartitionForParameter(VoltType.INTEGER, crwp.partitionKey);
                    errorMessages.put(partitionId, cri.toJSONString());
                }
            }
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("Got smart delete responses from all partitions.");
            }
        }
        StringBuilder errorMsg = new StringBuilder();
        errorMessages.forEach((k,v) -> {
            errorMsg.append("Partition " + k + ": receives failure response from nibble delete system procedure: " + v);
            errorMsg.append("\n");
        });
        return new NibbleStatus(rowsLeft, rowsJustDeleted, errorMsg.toString());
    }

    public VoltTable run(String tableName, String columnName, String valueStr, String comparisonOp, long chunksize, long timeoutms) {

        // picked nanotime because it's momotonic and that's just easier
        long startTimeStampNS = System.nanoTime();

        VoltTable returnTable = new VoltTable(new ColumnInfo("ROWS_DELETED", VoltType.BIGINT),
                                new ColumnInfo("ROWS_LEFT", VoltType.BIGINT),
                                new ColumnInfo("ROUNDS", VoltType.INTEGER),
                                new ColumnInfo("DELETED_LAST_ROUND", VoltType.BIGINT),
                                new ColumnInfo("LAST_DELETE_TIMESTAMP", VoltType.BIGINT),
                                new ColumnInfo("STATUS", VoltType.BIGINT),
                                new ColumnInfo("MESSAGE", VoltType.STRING));

        // collect all the validated info and metadata needed
        // these throw helpful errors if they run into problems
        CatalogContext ctx = VoltDB.instance().getCatalogContext();
        assert(ctx != null);
        Table catTable = getValidatedTable(ctx, tableName);
        Column catColumn = getValidatedColumn(catTable, columnName);
        VoltType colType = VoltType.get((byte) catColumn.getType());
        Object value = getValidatedValue(colType, valueStr);

        // SCHEMA FOR RETURN TABLE
        long rowsDeleted = 0;
        int rounds = 1; // track how many times we run

        // always run nibble delete at least once
        NibbleStatus status = runNibbleDeleteOperation(tableName, columnName, comparisonOp, value, chunksize, catTable.getIsreplicated());
        rowsDeleted = status.rowsJustDeleted;
        // If any partition receive failure, report the delete status plus the error message back.
        if (!status.errorMessages.isEmpty()) {
            returnTable.addRow(rowsDeleted, status.rowsLeft, rounds, status.rowsJustDeleted, System.currentTimeMillis(),
                    ClientResponse.GRACEFUL_FAILURE, status.errorMessages);
            return returnTable;
        }
        // handle the case where we're jammed from the start (no rows deleted)
        if (status.rowsJustDeleted == 0 && status.rowsLeft > 0) {
            throw new VoltAbortException(String.format(
                    "While removing tuples from table %s, first delete deleted zero tuples while %d"
                  + " still met the criteria for delete. This is unexpected, but doesn't imply corrupt state.",
                    catTable.getTypeName(), status.rowsLeft));
        }
        long now = System.nanoTime();

        // loop until all done or until timeout, worth noting that 1 ms = 1,000,000 ns
        while ((status.rowsLeft > 0) && ((now - startTimeStampNS) < (timeoutms * 1000000))) {
            status = runNibbleDeleteOperation(tableName, columnName, comparisonOp, value, chunksize, catTable.getIsreplicated());
            rowsDeleted += status.rowsJustDeleted;
            rounds++;
            // If any partition receive failure, report the delete status plus the error message back.
            if (!status.errorMessages.isEmpty()) {
                returnTable.addRow(rowsDeleted, status.rowsLeft, rounds, status.rowsJustDeleted, System.currentTimeMillis(),
                        ClientResponse.GRACEFUL_FAILURE, status.errorMessages);
                return returnTable;
            }

            now = System.nanoTime();
        }

        if (hostLog.isDebugEnabled()){
            hostLog.debug("TTL results for table " + tableName + " deleted " + rowsDeleted + " left " + status.rowsLeft + " in " + rounds + " rounds");
        }
        returnTable.addRow(rowsDeleted, status.rowsLeft, rounds, status.rowsJustDeleted, System.currentTimeMillis(),
                ClientResponse.SUCCESS, "");
        return returnTable;
    }
}
