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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

/*
 * tranactions are all on export_partitioned_topic_cdc, and the callback
 * returns the tranaction type for stats gathering
 */
public class TablePartitionedExportCDCTopic extends VoltProcedure {
    public final SQLStmt check = new SQLStmt("SELECT TOP 1 * FROM export_partitioned_topic_cdc WHERE rowid = ? ORDER BY rowid"); // don't try these on a STREAM

    public final SQLStmt insert = new SQLStmt("INSERT INTO export_partitioned_topic_cdc (txnid, rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp, type_null_float, type_not_null_float, type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt update = new SQLStmt("UPDATE export_partitioned_topic_cdc SET type_null_tinyint = ?, type_not_null_tinyint = ?, type_null_smallint = ?, type_not_null_smallint = ?, type_null_integer = ?, type_not_null_integer = ?, type_null_bigint = ?, type_not_null_bigint = ?, type_null_timestamp = ?, type_null_float = ?, type_not_null_float = ?, type_null_decimal = ?, type_not_null_decimal = ?, type_null_varchar25 = ?, type_not_null_varchar25 = ?, type_null_varchar128 = ?, type_not_null_varchar128 = ?, type_null_varchar1024 = ?, type_not_null_varchar1024 = ? WHERE rowid = ?;");
    public final SQLStmt delete = new SQLStmt("DELETE FROM export_partitioned_topic_cdc WHERE rowid = ?");

    private final static int INSERT = 0;
    private final static int DELETE = 1;
    private final static int UPDATE  = 2;

    public VoltTable[] run(long rowid, int op)
    {
        long txid = getUniqueId();

        ColumnInfo[] column = new ColumnInfo[1];
        column[0] = new ColumnInfo("COUNT", VoltType.BIGINT);
        VoltTable t = new VoltTable(column);
        // Critical for proper determinism: get a cluster-wide consistent Random instance
        Random rand = getSeededRandomNumberGenerator();

        if (op == UPDATE || op == DELETE) {
            // Check if the record exists first
            voltQueueSQL(check, rowid);

            // Grab resultset for possibly existing record
            VoltTable item = voltExecuteSQL()[0];

            // If the record exist perform an update or delete
            if(item.getRowCount() > 0)
            {
                if (op == DELETE)
                {
                    voltQueueSQL(delete, rowid);
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
                            , rowid
                            );
                }
            }
            else
            {
                // row not found -- have to skip DELETE or UPDATE
                return (new VoltTable[] {t});
            }
        }
        else
        {
                // Insert a new record
                SampleRecord record = new SampleRecord(rowid, rand);
                // SampleRecord record = new SampleRecord(rowid, rand, getTransactionTime());
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
        t.addRow(1);
        return (new VoltTable[] {t});
    }
}
