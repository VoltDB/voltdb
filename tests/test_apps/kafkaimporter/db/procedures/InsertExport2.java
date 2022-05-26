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
package kafkaimporter.db.procedures;

import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertExport2 extends VoltProcedure {

    public final String sqlSuffix =
            "(key, value, rowid_group, type_null_tinyint, type_not_null_tinyint, " +
            "type_null_smallint, type_not_null_smallint, type_null_integer, " +
            "type_not_null_integer, type_null_bigint, type_not_null_bigint, " +
            "type_null_timestamp, type_not_null_timestamp, type_null_float, " +
            "type_not_null_float, type_null_decimal, type_not_null_decimal, " +
            "type_null_varchar25, type_not_null_varchar25, type_null_varchar128, " +
            "type_not_null_varchar128, type_null_varchar1024, type_not_null_varchar1024)" +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public final SQLStmt exportInsert = new SQLStmt("INSERT INTO kafkaExportTable2 " + sqlSuffix);
    public final SQLStmt mirrorInsert = new SQLStmt("INSERT INTO kafkaMirrorTable2 " + sqlSuffix);
    public final SQLStmt selectCounts = new SQLStmt("SELECT key FROM exportcounts ORDER BY key LIMIT 1");
    public final SQLStmt insertCounts = new SQLStmt("INSERT INTO exportcounts VALUES (?, ?)");
    public final SQLStmt updateCounts = new SQLStmt("UPDATE exportcounts SET total_rows_exported=total_rows_exported+? where key = ?");

    public long run(long key, long value)
    {
        //@SuppressWarnings("deprecation")
        //long key = getVoltPrivateRealTransactionIdDontUseMe();
        // Critical for proper determinism: get a cluster-wide consistent Random instance
        Random rand = new Random(value);

        // Insert a new record
        SampleRecord record = new SampleRecord(key, rand);

        voltQueueSQL(
            exportInsert, key, value, record.rowid_group,
            record.type_null_tinyint, record.type_not_null_tinyint,
            record.type_null_smallint, record.type_not_null_smallint,
            record.type_null_integer, record.type_not_null_integer,
            record.type_null_bigint, record.type_not_null_bigint,
            record.type_null_timestamp, record.type_not_null_timestamp,
            record.type_null_float, record.type_not_null_float,
            record.type_null_decimal, record.type_not_null_decimal,
            record.type_null_varchar25, record.type_not_null_varchar25,
            record.type_null_varchar128, record.type_not_null_varchar128,
            record.type_null_varchar1024, record.type_not_null_varchar1024
        );

        voltQueueSQL(
            mirrorInsert, key, value, record.rowid_group,
            record.type_null_tinyint, record.type_not_null_tinyint,
            record.type_null_smallint, record.type_not_null_smallint,
            record.type_null_integer, record.type_not_null_integer,
            record.type_null_bigint, record.type_not_null_bigint,
            record.type_null_timestamp, record.type_not_null_timestamp,
            record.type_null_float, record.type_not_null_float,
            record.type_null_decimal, record.type_not_null_decimal,
            record.type_null_varchar25, record.type_not_null_varchar25,
            record.type_null_varchar128, record.type_not_null_varchar128,
            record.type_null_varchar1024, record.type_not_null_varchar1024
        );

        voltQueueSQL(selectCounts);
        VoltTable[] result = voltExecuteSQL();
        VoltTable data = result[2];
        long nrows = data.getRowCount();
        if (nrows > 0) {
            long ck = data.fetchRow(0).getLong(0);
            voltQueueSQL(updateCounts, 1l, ck);
            voltExecuteSQL(true);
        } else {
            voltQueueSQL(insertCounts, key, 1l);
            voltExecuteSQL(true);
        }
        return 0;
    }
}
