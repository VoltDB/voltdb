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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
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

        NibbleStatus(long rowsLeft, long rowsJustDeleted) {
            this.rowsLeft = rowsLeft; this.rowsJustDeleted = rowsJustDeleted;
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
            CompletableFuture<ClientResponse> cf = callProcedure("@NibbleDeleteMP", tableName, columnName, comparisonOp, parameter, chunksize);
            ClientResponse cr;
            try {
                cr = cf.get(ONE, TimeUnit.MINUTES);
            } catch (Exception e) {
                throw new VoltAbortException("Received exception while waiting response back "
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
                throw new VoltAbortException("Received failure response from smart delete system procedure: " + cri.toJSONString());
            }
        } else {
            // for single partitioned table, run the smart delete everywhere
            CompletableFuture<ClientResponseWithPartitionKey[]> pf = null;
            pf = callAllPartitionProcedure("@NibbleDeleteSP", tableName, columnName, comparisonOp, parameter, chunksize);
            ClientResponseWithPartitionKey[] crs;
            try {
                crs = pf.get(ONE, TimeUnit.MINUTES);
            } catch (Exception e) {
                throw new VoltAbortException("Received exception while waiting response back "
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
                    throw new VoltAbortException("Received failure response from smart delete system procedure: " + cri.toJSONString());
                }
            }
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("Got smart delete responses from all partitions.");
            }
        }
        return new NibbleStatus(rowsLeft, rowsJustDeleted);
    }

    public VoltTable run(String tableName, String columnName, String valueStr, String comparisonOp, long chunksize, long timeoutms) {

        // picked nanotime because it's momotonic and that's just easier
        long startTimeStampNS = System.nanoTime();

        VoltTable returnTable = new VoltTable(new VoltTable.ColumnInfo("rowsdeleted", VoltType.BIGINT),
                                new VoltTable.ColumnInfo("rowsleft", VoltType.BIGINT),
                                new VoltTable.ColumnInfo("rounds", VoltType.INTEGER),
                                new VoltTable.ColumnInfo("deletedLastRound", VoltType.BIGINT),
                                new VoltTable.ColumnInfo("note", VoltType.STRING));

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
        // handle the case where we're jammed from the start (no rows deleted)
        if (status.rowsJustDeleted == 0 && status.rowsLeft > 0) {
            throw new VoltAbortException(String.format(
                    "While removing tuples from table %s, first delete deleted zero tuples while %d"
                  + " still met the criteria for delete. This is unexpected, but doesn't imply corrupt state.",
                    catTable.getTypeName(), status.rowsLeft));
        }
        rowsDeleted += status.rowsJustDeleted;
        long now = System.nanoTime();

        // loop until all done or until timeout, worth noting that 1 ms = 1,000,000 ns
        while ((status.rowsLeft > 0) && ((now - startTimeStampNS) < (timeoutms * 1000000))) {
            status = runNibbleDeleteOperation(tableName, columnName, comparisonOp, value, chunksize, catTable.getIsreplicated());
            rowsDeleted += status.rowsJustDeleted;
            rounds++;

            // handle the case where we're jammed mid run (no rows deleted)
            if (status.rowsJustDeleted == 0 && status.rowsLeft > 0) {
                //Review: this might cause thousands of useless entries in the return table
                returnTable.addRow(rowsDeleted, status.rowsLeft, rounds, status.rowsJustDeleted, "");
            }

            now = System.nanoTime();
        }

        returnTable.addRow(rowsDeleted, status.rowsLeft, rounds, status.rowsJustDeleted, "");
        return returnTable;
    }

}
