/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.dtxn;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

@ProcInfo (
    partitionInfo = "kv.key: 0",
    singlePartition = true
)
public class NonDeterministicSPProc extends VoltProcedure {

    static final int MISMATCH_VALUES = 0;
    static final int MISMATCH_LENGTH = 1;
    static final int MISMATCH_INSERTION = 2;

    public static final SQLStmt sql = new SQLStmt("insert into kv values ?, ?");

    public VoltTable run(long key, int failType) {
        long id = VoltDB.instance().getHostMessenger().getHostId();
        voltQueueSQL(sql, key, id);
        voltExecuteSQL();

        VoltTable retval = new VoltTable(new ColumnInfo("", VoltType.BIGINT));

        // non deterministic length if desired
        if (failType == MISMATCH_LENGTH) {
            for (int i = 0; i < id; i++)
                retval.addRow(0);
        }
        // non deterministic by value
        else if (failType == MISMATCH_VALUES) {
            retval.addRow(id);
        }
        else if (failType == MISMATCH_INSERTION) {
            // do nada
        }
        else {
            assert(false);
            throw new VoltAbortException("failType param is unknown value.");
        }


        return retval;
    }

}
