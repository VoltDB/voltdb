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
import org.voltdb.VoltTableRow;
//import org.voltdb.VoltProcedure.VoltAbortException;

public class PopulateDimension extends VoltProcedure {

    public final SQLStmt d_getCount = new SQLStmt(
            "select count(*) FROM dimension WHERE cid = ?");
    public final SQLStmt d_Insert = new SQLStmt(
            "insert into dimension (cid, desc) VALUES (?, ?);");

    // INSERT one row per cid to the dimension table unless already exists

    public VoltTable[] run(byte cid) {
        voltQueueSQL(d_getCount, cid);
        VoltTable[] results = voltExecuteSQL(false);
        VoltTable dim = results[0];
        long rowCount = dim.getRowCount();
        if (rowCount != 1) {
            throw new VoltAbortException(getClass().getName() +
                    " invalid row count " + rowCount + " for count query" +
                    " on dimension table for cid " + cid);
        }
        VoltTableRow row = dim.fetchRow(0);
        long c = row.getLong(0);
        switch ((int)c) {
        case 0:
            voltQueueSQL(d_Insert, cid, cid);
            results = voltExecuteSQL(true);
        case 1:
            return new VoltTable[] {};
        default:
            throw new VoltAbortException(getClass().getName() +
                    " invalid count " + c + " of dimension rows" +
                    " for cid " + cid);
        }
    }
}
