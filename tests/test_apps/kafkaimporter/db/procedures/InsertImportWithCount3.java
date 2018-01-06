/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

/*
 * Stored procedure for Kafka import
 *
 * If incoming data is in the mirror table, increment received count.
 *
 * Else add to error table as a record of rows that didn't get
 * into the mirror table, a major error!
 */

package kafkaimporter.db.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertImportWithCount3 extends VoltProcedure {
    public final String sqlSuffix = "(key, value) VALUES (?, ?)";
    public final SQLStmt importInsert = new SQLStmt("INSERT INTO kafkaImportTable3 " + sqlSuffix);
    public final SQLStmt incrementMirrorRow = new SQLStmt("UPDATE kafkamirrortable1 SET import_count=import_count+1 WHERE key = ? and value = ?");
    public final SQLStmt insertError = new SQLStmt("INSERT into importnomatch (key, value, streamnumber) values(?, ?, 3)");

    public long run(long key, long value)
    {
        voltQueueSQL(incrementMirrorRow, EXPECT_SCALAR_LONG, key, value);
        VoltTable[] queryresult = voltExecuteSQL();
        VoltTable result = queryresult[0];
        long rowsUpdated = result.fetchRow(0).getLong(0);
        if (rowsUpdated == 0) { // row
            voltQueueSQL(insertError, key, value);
        }
        voltQueueSQL(importInsert, key, value);
        voltExecuteSQL(true);
        return 0;
    }
}
