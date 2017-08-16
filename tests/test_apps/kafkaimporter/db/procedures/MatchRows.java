/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
 * Stored procedure for ExportBenchmark
 *
 * 2 tables -- all datatypes with nullable and not nullable variants
 *
 * Call the SP with DB tables insert count and export tables insert count.
 * This allows mixing DB insert to export insert ratio, following the recent
 * Flipkart customer case where export rows could exceed DB inserts as much as 10:1.
 *
 * Since DB inserts and export inserts are parameterized, it's possible to try many
 * variations in the test driver.
 */

package kafkaimporter.db.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltcore.logging.VoltLogger;

//@ProcInfo(
//        partitionInfo = "ALL_VALUES1.rowid:0",
//        singlePartition = true
//    )

public class MatchRows extends VoltProcedure {
    public final SQLStmt matchSelect = new SQLStmt("select ki.key from kafkaImportTable1 as ki, kafkaMirrorTable1 as km where ki.key = km.key and ki.value = km.value order by ki.key");

    public VoltTable[] run()
    {
        voltQueueSQL(matchSelect);
        return voltExecuteSQL(true);
    }
}
