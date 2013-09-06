/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

/**
 * Given as input a VoltTable with a schema corresponding to a persistent table,
 * insert into the appropriate persistent table. Should be faster than using
 * the auto-generated CRUD procs for batch inserts. Also a bit more generic.
 */
@ProcInfo(
    partitionInfo = "DUMMY: 0", // partitioning is done special for this class
    singlePartition = true
)
public class LoadPartitionData extends VoltSystemProcedure
{
    @Override
    public void init() {}

    /**
     * This single-partition sysproc has no special fragments
     */
    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        return null;
    }

    /**
     * These parameters, with the exception of ctx, map to user provided values.
     *
     * @param ctx
     *            Internal. Not a user-supplied parameter.
     * @param partitionParam Partitioning parameter
     * @param tableName
     *            Name of persistent table receiving data.
     * @param table
     *            A VoltTable with schema matching tableName containing data to
     *            load.
     * @return The number of rows modified.
     * @throws VoltAbortException
     */
    public long run(SystemProcedureExecutionContext ctx,
            int partitionParam,
            String tableName, VoltTable table)
            throws VoltAbortException {

        // if tableName is replicated, fail.
        // otherwise, create a VoltTable for each partition and
        // split up the incoming table .. then send those partial
        // tables to the appropriate sites.

        Table catTable = ctx.getDatabase().getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException("Table not present in catalog.");
        }
        // fix any case problems
        tableName = catTable.getTypeName();

        m_runner.voltLoadTable("cluster", "database", tableName, table, false);
        return 0L;
    }
}
