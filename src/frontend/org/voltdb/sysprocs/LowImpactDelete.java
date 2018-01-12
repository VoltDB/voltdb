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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.voltdb.CatalogContext;
import org.voltdb.ParameterConverter;
import org.voltdb.VoltDB;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;

public class LowImpactDelete extends VoltNTSystemProcedure {

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
    }

    /**
     * Used to sort candidate indexes by uniqueness, column count,
     * where uniqueness is good and column count is bad
     */
    static Comparator<Index> indexComparator = new Comparator<Index>() {

        @Override
        public int compare(Index o1, Index o2) {
            // note: assuming not null;
            assert(o1 != null); assert(o2 != null);
            // favor uniqueness over everything else
            // assumeunique and unique are the same thing for this purpose
            int o1unique = (o1.getUnique() || o1.getAssumeunique()) ? 1000000 : 0;
            int o2unique = (o1.getUnique() || o1.getAssumeunique()) ? 1000000 : 0;

            // the -1 is to make fewer columns score higher
            int o1colCount = -1 * o1.getColumns().size();
            int o2colCount = -1 * o2.getColumns().size();

            // JH: I feel like I always end up ordering things wrong, so deferring
            // to Integer.compare with summed values for uniqueness feels like it should
            // work
            return Integer.compare(o1unique + o1colCount, o2unique + o2colCount);
        }
    };

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

    Index getBestIndex(Table table, Column column) {
        // look for all indexes where the first column matches our index
        List<Index> candidates = new ArrayList<>(2);
        for (Index catIndexIterator : table.getIndexes()) {
            for (ColumnRef colRef : catIndexIterator.getColumns()) {
                // we only care about the first index
                if (colRef.getIndex() != 0) continue;
                if (colRef.getColumn() == column) {
                    candidates.add(catIndexIterator);
                }
            }
        }
        // error no index found
        if (candidates.size() == 0) {
            String msg = String.format("Count not find index to support LowImpactDelete on column %s.%s. ",
                    table.getTypeName(), column.getTypeName());
            msg += String.format("Please create an index where column %s.%s is the first or only indexed column.",
                    table.getTypeName(), column.getTypeName());
            throw new VoltAbortException(msg);
        }
        // now make sure index is countable (which also ensures ordered because countable non-ordered isn't a thing)
        // note countable ordered indexes are the default... so something weird happened if this is the case
        // Then go and pick the best index sorted by uniqueness, columncount

        Index catIndex = candidates.stream()
                                   .filter(i -> i.getCountable())
                                   .max(indexComparator)
                                   .orElse(null);

        if (catIndex == null) {
            String msg = String.format("Count not find index to support LowImpactDelete on column %s.%s. ",
                    table.getTypeName(), column.getTypeName());
            msg += String.format("Indexes must support ordering and ranking (as default indexes do).",
                    table.getTypeName(), column.getTypeName());
            throw new VoltAbortException(msg);
        }

        return catIndex;
    }

    static class NibbleStatus {
        final long rowsLeft;
        final long rowsJustDeleted;

        NibbleStatus(long rowsLeft, long rowsJustDeleted) {
            this.rowsLeft = rowsLeft; this.rowsJustDeleted = rowsJustDeleted;
        }
    }

    NibbleStatus runNibbleDeleteOperation() {



        return null;
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
        Index catIndex = getBestIndex(catTable, catColumn);
        // will throw on bad input, but should frame error nicely
        ComparisonOperation op = ComparisonOperation.fromString(comparisonOp);

        // SCHEMA FOR RETURN TABLE
        long rowsDeleted = 0;
        int rounds = 1; // track how many times we run

        // always run nibble delete at least once
        NibbleStatus status = runNibbleDeleteOperation();
        // handle the case where we're jammed from the start (no rows deleted)
        if (status.rowsJustDeleted == 0 && status.rowsLeft > 0) {
            throw new VoltAbortException(String.format(
                    "While removing tuples from table %s, first delete deleted zero tuples while %d"
                  + " still met the criteria for delete. This is unexpected, but doesn't imply corrupt state.",
                    catTable.getTypeName(), status.rowsLeft));
        }
        rowsDeleted += status.rowsJustDeleted;
        long now = System.nanoTime();

        // loop until all done or until timeout
        while ((status.rowsLeft > 0) && ((now - startTimeStampNS) < (timeoutms * 1000000))) {
            status = runNibbleDeleteOperation();
            rowsDeleted += status.rowsJustDeleted;
            rounds++;

            // handle the case where we're jammed mid run (no rows deleted)
            if (status.rowsJustDeleted == 0 && status.rowsLeft > 0) {
                returnTable.addRow(rowsDeleted, status.rowsLeft, rounds, status.rowsJustDeleted, "");
            }

            now = System.nanoTime();
        }

        returnTable.addRow(rowsDeleted, status.rowsLeft, rounds, status.rowsJustDeleted, "");
        return returnTable;
    }

}
