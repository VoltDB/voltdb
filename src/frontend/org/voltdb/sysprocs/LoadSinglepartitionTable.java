/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.ProcInfo;
import org.voltdb.ProcedureRunner;
import org.voltdb.SQLStmt;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.messaging.FastDeserializer;

/**
 * Given as input a VoltTable with a schema corresponding to a persistent table,
 * insert into the appropriate persistent table. Should be faster than using
 * the auto-generated CRUD procs for batch inserts. Also a bit more generic.
 */
@ProcInfo(
    partitionInfo = "DUMMY: 0", // partitioning is done special for this class
    singlePartition = true
)
public class LoadSinglepartitionTable extends VoltSystemProcedure
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
     * @param tableName
     *            Name of persistent table receiving data.
     * @param table
     *            A VoltTable with schema matching tableName containing data to
     *            load.
     * @return The number of rows modified.
     * @throws VoltAbortException
     */
    public long run(SystemProcedureExecutionContext ctx,
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
        if (catTable.getIsreplicated()) {
            throw new VoltAbortException(
                    String.format("LoadSinglepartitionTable incompatible with replicated table %s.",
                            tableName));
        }
        // fix any case problems
        tableName = catTable.getTypeName();

        // check that the schema of the input matches
        int columnCount = table.getColumnCount();

        // find the insert statement for this table
        String insertProcName = String.format("%s.insert", tableName);
        Procedure p = ctx.getDatabase().getProcedures().get(insertProcName);
        if (p == null) {
            throw new VoltAbortException(
                    String.format("Unable to locate auto-generated CRUD insert statement for table %s",
                            tableName));
        }

        // statements of all single-statement procs are named "sql"
        Statement catStmt = p.getStatements().get("sql");
        if (catStmt == null) {
            throw new VoltAbortException(
                    String.format("Unable to find SQL statement for found table %s: BAD",
                            tableName));
        }

        // create a SQLStmt instance on the fly (unusual to do)
        SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
        ProcedureRunner.initSQLStmt(stmt, catStmt);

        long queued = 0;
        long executed = 0;

        // make sure at the start of the table
        table.resetRowPosition();
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
     * @return Count of rows inserted.
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
            if (dmlUpdated > 1) {
                throw new VoltAbortException("Insert modified more than one tuple.");
            }
            ++count;
        }
        return count;
    }

    /**
     * Called by the client interface to partition this invocation based on parameters.
     *
     * @param tables The set of active tables in the catalog.
     * @param spi The stored procedure invocation object
     * @return An object suitable for hashing to a partition with The Hashinator
     * @throws Exception thown on error with a descriptive message
     */
    public static Object partitionValueFromInvocation(CatalogMap<Table> tables, StoredProcedureInvocation spi) throws Exception {
        String tableName = null;
        VoltTable table = null;
        ByteBuffer buf = spi.getSerializedParams();

        spi.getParams();

        // if the params buffer has been decoded
        if (buf == null) {
            Object[] params = spi.getParams().toArray();
            tableName = (String) params[0];
            table = (VoltTable) params[1];
        }
        // if the params buffer is not decoded
        else {
            // THE GOAL OF THIS CODE IS TO READ AS LITTLE AS POSSIBLE OF
            // WHAT MIGHT BE A BIG PARAMETER SET
            // i.e. read the name of the table and first row of the data provided

            FastDeserializer fds = new FastDeserializer(buf);

            // read the number of parameters
            int paramLen = fds.readShort();
            if (paramLen != 2) {
                throw new Exception("@LoadSinglepartitionTable requres exactly two parameters.");
            }

            // read the type of the first param, expecting string
            byte strType = fds.readByte();
            if (strType != VoltType.STRING.getValue()) {
                throw new Exception("@LoadSinglepartitionTable expects a String and a VoltTable for parameters (in order).");
            }

            // read the name of the table targeted
            tableName = fds.readString();

            // read the type of the second param, expecting table
            byte tableType = fds.readByte();
            if (tableType != VoltType.VOLTTABLE.getValue()) {
                throw new Exception("@LoadSinglepartitionTable expects a String and a VoltTable for parameters (in order).");
            }

            // read the size of the table and create a buffer for it
            int tableSize = fds.readInt();
            ByteBuffer tbuf = buf.slice();
            tbuf.limit(tableSize);

            // assume the buffer is pointing to the table
            table = PrivateVoltTableFactory.createVoltTableFromBuffer(tbuf, true);
        }

        // get the table from the catalog
        Table catTable = tables.getIgnoreCase(tableName);
        if (catTable == null) {
            throw new Exception(
                    String.format("Unable to find target table \"%s\" for LoadSinglepartitionTable.",
                            tableName));
        }

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
