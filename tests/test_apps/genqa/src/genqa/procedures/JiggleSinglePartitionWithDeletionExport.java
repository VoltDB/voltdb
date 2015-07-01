/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package genqa.procedures;

import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

@ProcInfo(
    partitionInfo = "partitioned_table.rowid:0",
    singlePartition = true
)

public class JiggleSinglePartitionWithDeletionExport extends VoltProcedure {
    public final SQLStmt check = new SQLStmt("SELECT TOP 1 * FROM partitioned_table WHERE rowid = ?");
    public final SQLStmt insert = new SQLStmt("INSERT INTO partitioned_table (rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp, type_not_null_timestamp, type_null_float, type_not_null_float, type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    public final SQLStmt update = new SQLStmt("UPDATE partitioned_table SET type_null_tinyint = ?, type_not_null_tinyint = ?, type_null_smallint = ?, type_not_null_smallint = ?, type_null_integer = ?, type_not_null_integer = ?, type_null_bigint = ?, type_not_null_bigint = ?, type_null_timestamp = ?, type_not_null_timestamp = ?, type_null_float = ?, type_not_null_float = ?, type_null_decimal = ?, type_not_null_decimal = ?, type_null_varchar25 = ?, type_not_null_varchar25 = ?, type_null_varchar128 = ?, type_not_null_varchar128 = ?, type_null_varchar1024 = ?, type_not_null_varchar1024 = ? WHERE rowid = ?;");
    public final SQLStmt delete = new SQLStmt("DELETE FROM partitioned_table WHERE rowid = ?");
    public final SQLStmt export = new SQLStmt("INSERT INTO export_partitioned_table (txnid, rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp, type_not_null_timestamp, type_null_float, type_not_null_float, type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public VoltTable[] run(long rowid, long ignore)
    {
        @SuppressWarnings("deprecation")
        long txid = getVoltPrivateRealTransactionIdDontUseMe();

        // Critical for proper determinism: get a cluster-wide consistent Random instance
        Random rand = new Random(txid);

        // Check if the record exists first
        voltQueueSQL(check, rowid);

        // Grab resultset for possibly existing record
        VoltTable item = voltExecuteSQL()[0];

        // If the record exist perform an update or delete
        if(item.getRowCount() > 0)
        {
            // Randomly decide whether to delete (or update) the record
            if (rand.nextBoolean())
            {
                voltQueueSQL(delete, rowid);
                // Export deletion
                VoltTableRow row = item.fetchRow(0);
                voltQueueSQL(
                              export
                            , txid
                            , rowid
                            , row.get( 1, VoltType.TINYINT)
                            , row.get( 2, VoltType.TINYINT)
                            , row.get( 3, VoltType.TINYINT)
                            , row.get( 4, VoltType.SMALLINT)
                            , row.get( 5, VoltType.SMALLINT)
                            , row.get( 6, VoltType.INTEGER)
                            , row.get( 7, VoltType.INTEGER)
                            , row.get( 8, VoltType.BIGINT)
                            , row.get( 9, VoltType.BIGINT)
                            , row.get(10, VoltType.TIMESTAMP)
                            , row.get(11, VoltType.TIMESTAMP)
                            , row.get(12, VoltType.FLOAT)
                            , row.get(13, VoltType.FLOAT)
                            , row.get(14, VoltType.DECIMAL)
                            , row.get(15, VoltType.DECIMAL)
                            , row.get(16, VoltType.STRING)
                            , row.get(17, VoltType.STRING)
                            , row.get(18, VoltType.STRING)
                            , row.get(19, VoltType.STRING)
                            , row.get(20, VoltType.STRING)
                            , row.get(21, VoltType.STRING)
                            );
            }
            else
            {
                SampleRecord record = new SampleRecord(rowid, rand);
                voltQueueSQL(
                              update
                            , record.type_null_tinyint
                            , record.type_not_null_tinyint
                            , record.type_null_smallint
                            , record.type_not_null_smallint
                            , record.type_null_integer
                            , record.type_not_null_integer
                            , record.type_null_bigint
                            , record.type_not_null_bigint
                            , record.type_null_timestamp
                            , record.type_not_null_timestamp
                            , record.type_null_float
                            , record.type_not_null_float
                            , record.type_null_decimal
                            , record.type_not_null_decimal
                            , record.type_null_varchar25
                            , record.type_not_null_varchar25
                            , record.type_null_varchar128
                            , record.type_not_null_varchar128
                            , record.type_null_varchar1024
                            , record.type_not_null_varchar1024
                            , rowid
                            );
            }
        }
        else
        {
                // Insert a new record
                SampleRecord record = new SampleRecord(rowid, rand);
                voltQueueSQL(
                              insert
                            , rowid
                            , record.rowid_group
                            , record.type_null_tinyint
                            , record.type_not_null_tinyint
                            , record.type_null_smallint
                            , record.type_not_null_smallint
                            , record.type_null_integer
                            , record.type_not_null_integer
                            , record.type_null_bigint
                            , record.type_not_null_bigint
                            , record.type_null_timestamp
                            , record.type_not_null_timestamp
                            , record.type_null_float
                            , record.type_not_null_float
                            , record.type_null_decimal
                            , record.type_not_null_decimal
                            , record.type_null_varchar25
                            , record.type_not_null_varchar25
                            , record.type_null_varchar128
                            , record.type_not_null_varchar128
                            , record.type_null_varchar1024
                            , record.type_not_null_varchar1024
                            );
        }

        // Execute last statement batch
        voltExecuteSQL(true);

        // Retun to caller
        return null;
    }
}
