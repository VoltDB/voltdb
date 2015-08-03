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

/*
 * Stored procedure for Kafka import
 *
 * If incoming data is in the mirror table, delete that row.
 *
 * Else add to import table as a record of rows that didn't get
 * into the mirror table, a major error!
 */

package kafkaimporter.db.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

//@ProcInfo(
//        partitionInfo = "ALL_VALUES1.rowid:0",
//        singlePartition = true
//    )

public class InsertImport extends VoltProcedure {
    public final String sqlSuffix = "(key, value) VALUES (?, ?)";
    public final SQLStmt importInsert = new SQLStmt("INSERT INTO kafkaImportTable1 " + sqlSuffix);
    public final SQLStmt deleteMirrorRow = new SQLStmt("DELETE FROM kafkamirrortable1 WHERE key = ?");

    public long run(long key, long value)
    {

        voltQueueSQL(deleteMirrorRow, EXPECT_SCALAR_LONG, key);
        long deletedCount = voltExecuteSQL()[0].asScalarLong();

        if (deletedCount == 0) {
            voltQueueSQL(importInsert, key, value);
            voltExecuteSQL(true);
        }
        return 0;
    }
}
