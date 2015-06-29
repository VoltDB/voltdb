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
package exportbenchmark.procedures;

import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo(
        partitionInfo = "ALL_VALUES.rowid:0",
        singlePartition = true
    )

public class InsertExport0 extends VoltProcedure {
    public final String sqlBase = "(txnid, rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, "
            + "type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp, type_not_null_timestamp, type_null_float, type_not_null_float, "
            + "type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, "
            + "type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public final SQLStmt exportInsert = new SQLStmt("INSERT INTO ALL_VALUES_EXPORT " + sqlBase);
    public final SQLStmt dbInsert = new SQLStmt("INSERT INTO ALL_VALUES " + sqlBase);

    public long run(long rowid, int reversed)
    {
        @SuppressWarnings("deprecation")
        long txid = getVoltPrivateRealTransactionIdDontUseMe();
        //int[] iterations = {1, 5};
        int dbInserts = 1;
        int exportInserts = 0;

        // Critical for proper determinism: get a cluster-wide consistent Random instance
        Random rand = new Random(txid);

        // Insert into DB
        for (int row = 0; row < dbInserts; row++) {
            SampleRecord record = new SampleRecord(rowid, rand);
            voltQueueSQL(
                    dbInsert
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

        // Insert in export table
        for (int row = 0; row < exportInserts; row++) {
            SampleRecord record = new SampleRecord(rowid, rand);
            voltQueueSQL(
                    exportInsert
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

        // Execute queued statements
        voltExecuteSQL(true);

        // Return to caller
        return txid;
    }
}
