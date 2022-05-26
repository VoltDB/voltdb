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

import org.voltdb.*;

public class CopyLoadPartitionedBase extends ValidateLoadBase {

    public VoltTable[] doWork(SQLStmt select, SQLStmt insert, long cid, VoltTable vt) {

        VoltTable[] results = doValidate(select, cid, vt);
        VoltTable data = results[0];
        data.advanceRow();
        if (!insert.getText().contains("SELECT")) {
            long txnid = data.getLong(1);
            long rowid = data.getLong(2);
            voltQueueSQL(insert, cid, txnid, rowid);
            return voltExecuteSQL();

        } else { // use insert into select
            voltQueueSQL(insert, cid);
            results = voltExecuteSQL();
            data = results[0];
            int cnt = (int) data.fetchRow(0).getLong(0);
            if (cnt != 1) {
                throw new VoltAbortException("incorrect number of inserted rows=" + cnt + " for cid=" + cid);
            }
            return results;
        }
    }

    public long run() {
        return 0; // never called in base procedure
    }

}
