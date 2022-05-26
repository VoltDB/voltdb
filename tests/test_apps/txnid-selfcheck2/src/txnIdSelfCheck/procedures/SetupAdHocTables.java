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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Make sure ids 0..9 have values initialied for the
 * AdHocMayhem thread.
 */
public class SetupAdHocTables extends VoltProcedure {

    public final SQLStmt p_find = new SQLStmt(
            "select count(*) from adhocp where id = ?;");

    public final SQLStmt p_insert = new SQLStmt(
            "INSERT INTO adhocp VALUES (?, ?, ?, ?);");

    public final SQLStmt r_find = new SQLStmt(
            "select count(*) from adhocr where id = ?;");

    public final SQLStmt r_insert = new SQLStmt(
            "INSERT INTO adhocr VALUES (?, ?, ?, ?);");

    public long run() {
        // partitioned
        for (long id = 0; id < 10; id++) {
            voltQueueSQL(p_find, EXPECT_SCALAR_LONG, id);
        }
        VoltTable[] results = voltExecuteSQL();
        assert(results.length == 10);
        boolean needExecute = false;
        for (int id = 0; id < results.length; id++) {
            long count = results[id].asScalarLong();
            if (count == 0) {
                voltQueueSQL(p_insert, id, 0, 0, 0);
                needExecute = true;
            }
        }
        if (needExecute) {
            voltExecuteSQL();
        }

        // replicated
        for (long id = 0; id < 10; id++) {
            voltQueueSQL(r_find, EXPECT_SCALAR_LONG, id);
        }
        results = voltExecuteSQL();
        assert(results.length == 10);
        needExecute = false;
        for (int id = 0; id < results.length; id++) {
            long count = results[id].asScalarLong();
            if (count == 0) {
                voltQueueSQL(r_insert, id, 0, 0, 0);
                needExecute = true;
            }
        }
        if (needExecute) {
            voltExecuteSQL();
        }

        return 0;
    }
}
