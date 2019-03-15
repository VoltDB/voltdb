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

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

public class MigrateRowsBase extends VoltSystemProcedure {

    private static ColumnInfo[] schema = new ColumnInfo[] {
            new ColumnInfo("MIGRATED_ROWS", VoltType.BIGINT),  /* number of rows be migrated in this invocation */
            new ColumnInfo("LEFT_ROWS", VoltType.BIGINT)      /* number of rows to be deleted after this invocation */
    };

    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        return null;
    }

    /**
     * Migrate procedure for partitioned or replicated tables
     *
     * @param ctx         Internal API provided to all system procedures
     * @param partitionParam Partition parameter used to match invocation to partition
     * @param tableName   Name of persistent table
     * @param columnName  A column in the given table that its value can be used to provide
     *                    order for delete action. (Unique or non-unique) index is expected
     *                    on the column, if not a warning message will be printed.
     * @param compStr     ">", "<", ">=", "<=", "=="
     * @param parameter   value to compare
     * @param chunksize   maximum number of rows allow to be migrated
     * @param replicated  partitioned or replicated table
     * @return how many rows are migrated and how many rows left to be migrated (if any)
     */
    VoltTable migrateRowsCommon(SystemProcedureExecutionContext ctx,
            String tableName,
            String columnName,
            String compStr,
            VoltTable paramTable,
            long chunksize,
            boolean replicated) {
        VoltTable table = new VoltTable(schema);
        return table;
    }
}