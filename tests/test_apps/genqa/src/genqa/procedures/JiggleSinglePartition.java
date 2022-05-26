/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class JiggleSinglePartition extends VoltProcedure {
    public final SQLStmt check = new SQLStmt("SELECT TOP 1 rowid FROM partitioned_table WHERE rowid = ?");
    public final SQLStmt insert = new SQLStmt("INSERT INTO partitioned_table (txnid, rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp,  type_null_float, type_not_null_float, type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    public final SQLStmt update = new SQLStmt("UPDATE partitioned_table SET txnid = ?, type_null_tinyint = ?, type_not_null_tinyint = ?, type_null_smallint = ?, type_not_null_smallint = ?, type_null_integer = ?, type_not_null_integer = ?, type_null_bigint = ?, type_not_null_bigint = ?, type_null_timestamp = ?, type_not_null_timestamp = ?, type_null_float = ?, type_not_null_float = ?, type_null_decimal = ?, type_not_null_decimal = ?, type_null_varchar25 = ?, type_not_null_varchar25 = ?, type_null_varchar128 = ?, type_not_null_varchar128 = ?, type_null_varchar1024 = ?, type_not_null_varchar1024 = ? WHERE rowid = ?;");
    public final SQLStmt delete = new SQLStmt("DELETE FROM partitioned_table WHERE rowid = ?");

    public VoltTable[] run(long rowid, long ignore)
    {
        @SuppressWarnings("deprecation")
        long txid = DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this);

        // Critical for proper determinism: get a cluster-wide consistent Random instance
        Random rand = new Random(txid);

        // Check if the record exists first
        voltQueueSQL(check, rowid);

        // If the record exist perform an update or delete
        if(voltExecuteSQL()[0].getRowCount() > 0)
        {
            // Randomly decide whether to delete (or update) the record
            if (rand.nextBoolean())
                voltQueueSQL(delete, rowid);
            else
            {
                // TODO I am guessing that the partitioned_table data is not validated with
                //   the txnid since the updated did not include it until I added it here
                SampleRecord record = new SampleRecord(rowid, rand);
                voltQueueSQL(
                              update
                            , txid
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
                            , txid
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
                            // , record.type_not_null_timestamp
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

        // Return to caller
        return null;
    }
}
