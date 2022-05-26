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

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.ConstraintType;

/**
 * Given as input a VoltTable with a schema corresponding to a persistent table,
 * insert into the appropriate persistent table. Should be faster than using
 * the auto-generated CRUD procs for batch inserts because it can do many inserts
 * with only one network round trip and one transactional context.
 * Also a bit more generic.
 */
public class LoadSinglepartitionTable extends VoltSystemProcedure
{
    /**
     * This is a `VoltSystemProcedure` subclass. This comes with some extra work to
     * register system procedure plan fragment, but since this is a very simple
     * single-partition procedure, there are no custom plan fragments and this
     * is all just VoltDB system procedure boilerplate code.
     */
    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

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
     * @param ctx Internal API provided to all system procedures.
     * @param partitionParam Partitioning parameter used to match invocation to partition.
     * @param tableName Name of persistent, parititoned table receiving data.
     * @param table A VoltTable with schema matching the target table containing data to load.
     *              It's assumed that each row in this table partitions to the same partition
     *              as the other rows, and to the same partition as the partition parameter.
     * @param upsertMode True if using upsert instead of insert. If using insert, this proc
     *              will fail if there are any uniqueness constraints violated.
     * @return The number of rows modified. This will be inserts in insert mode, but in upsert
     *              mode, this will be the sum of inserts and updates.
     * @throws VoltAbortException on any failure, but the most common failures are non-matching
     *              partitioning or unique constraint violations.
     */
    public long run(SystemProcedureExecutionContext ctx,
                    byte[] partitionParam,
                    String tableName,
                    byte upsertMode,
                    VoltTable table)
            throws VoltAbortException
    {
        // if tableName is replicated, fail.
        // otherwise, create a VoltTable for each partition and
        // split up the incoming table .. then send those partial
        // tables to the appropriate sites.

        // Get the metadata object for the table in question from the global metadata/config
        // store, the Catalog.
        Table catTable = ctx.getDatabase().getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException("Table not present in catalog.");
        }
        // if tableName is replicated, fail.
        if (catTable.getIsreplicated()) {
            throw new VoltAbortException(
                    String.format("LoadSinglepartitionTable incompatible with replicated table %s.",
                            tableName));
        }

        // convert from 8bit signed integer (byte) to boolean
        boolean isUpsert = (upsertMode != 0);

        // upsert requires a primary key on the table to work
        if (isUpsert) {
            boolean hasPkey = false;
            for (Constraint c : catTable.getConstraints()) {
                if (c.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                    hasPkey = true;
                    break;
                }
            }
            if (!hasPkey) {
                throw new VoltAbortException(
                        String.format("The --update argument cannot be used for LoadingSinglePartionTable because the table %s does not have a primary key. "
                                + "Either remove the --update argument or add a primary key to the table.",
                                tableName));
            }
        }

        // action should be either "insert" or "upsert"
        final String action = (isUpsert ? "upsert" :"insert");

        // fix any case problems
        tableName = catTable.getTypeName();

        // check that the schema of the input matches
        int columnCount = table.getColumnCount();

        //////////////////////////////////////////////////////////////////////
        // Find the insert/upsert statement for this table
        // This is actually the big trick this procedure does.
        // It borrows the insert plan from the auto-generated insert procedure
        // named "TABLENAME.insert" or "TABLENAME.upsert".
        // We don't like it when users do this stuff, but it is safe in this
        // case.
        //
        // Related code to read is org.voltdb.DefaultProcedureManager, which
        // manages all of the default (CRUD) procedures created lazily for
        // each table in the database, including the plans used here.
        //
        String crudProcName = String.format("%s.%s", tableName,action);
        Procedure p = ctx.ensureDefaultProcLoaded(crudProcName);
        if (p == null) {
            throw new VoltAbortException(
                    String.format("Unable to locate auto-generated CRUD %s statement for table %s",
                            action,tableName));
        }

        // statements of all single-statement procs are named "sql0"
        Statement catStmt = p.getStatements().get(VoltDB.ANON_STMT_NAME + "0");
        if (catStmt == null) {
            throw new VoltAbortException(
                    String.format("Unable to find SQL statement for found table %s: BAD",
                            tableName));
        }

        // Create a SQLStmt instance on the fly
        // This unusual to do, as they are typically required to be final instance variables.
        // This only works because the SQL text and plan is identical from the borrowed procedure.
        SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
        m_runner.initSQLStmt(stmt, catStmt);

        long queued = 0;
        long executed = 0;

        // make sure at the start of the table
        table.resetRowPosition();

        // iterate over the rows queueing a sql statement for each row to insert
        for (int i = 0; table.advanceRow(); ++i) {
            Object[] params = new Object[columnCount];

            // get the parameters from the volt table
            for (int col = 0; col < columnCount; ++col) {
                params[col] = table.get(col, table.getColumnType(col));
            }

            // queue an insert and count it
            voltQueueSQL(stmt, params);
            ++queued;

            // every 100 statements, exec the batch
            // 100 is an arbitrary number
            if ((i % 100) == 0) {
                executed += executeSQL();
            }
        }
        // execute any leftover batched statements
        if (queued > executed) {
            executed += executeSQL();
        }

        return executed;
    }

    /**
     * Execute a set of queued inserts. Ensure each insert successfully
     * inserts one row. Throw exception if not.
     *
     * @return Count of rows inserted or upserted.
     * @throws VoltAbortException if any failure at all.
     */
    long executeSQL() throws VoltAbortException {
        long count = 0;
        VoltTable[] results = voltExecuteSQL();
        for (VoltTable result : results) {
            long dmlUpdated = result.asScalarLong();
            if (dmlUpdated == 0) {
                throw new VoltAbortException("Insert failed for tuple.");
            }
            // validate our expectation that 1 procedure = 1 modified tuple
            if (dmlUpdated > 1) {
                throw new VoltAbortException("Insert modified more than one tuple.");
            }
            ++count;
        }
        return count;
    }

    /**
     * Note: I (JHH) can't figure out what uses this code. It may be dead.
     *
     * Called by the client interface to partition this invocation based on parameters.
     *
     * @param tables The set of active tables in the catalog.
     * @param spi The stored procedure invocation object
     * @return An object suitable for hashing to a partition with The Hashinator
     * @throws Exception thown on error with a descriptive message
     */
    public static Object partitionValueFromInvocation(Table catTable, VoltTable table) throws Exception {
        if (catTable.getIsreplicated()) {
            throw new Exception("Target table for LoadSinglepartitionTable is replicated.");
        }

        // check the number of columns
        int colCount = catTable.getColumns().size();
        if (table.getColumnCount() != colCount) {
            throw new Exception("Input table has the wrong number of columns for bulk insert.");
        }

        // note there's no type checking

        // get the partitioning info from the catalog
        Column pCol = catTable.getPartitioncolumn();
        int pIndex = pCol.getIndex();

        // make sure the table has one row and move to it
        table.resetRowPosition();
        boolean hasRow = table.advanceRow();
        if (!hasRow) {
            // table has no rows, so it can partition any which way
            return 0;
        }

        // get the value from the first row of the table at the partition key
        Object pvalue = table.get(pIndex, table.getColumnType(pIndex));
        return pvalue;
    }
}
