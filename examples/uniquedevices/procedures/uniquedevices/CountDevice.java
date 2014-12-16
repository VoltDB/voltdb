/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package uniquedevices;

import java.io.IOException;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.hll.HyperLogLog;

public class CountDevice extends VoltProcedure {

    final static SQLStmt selectHLL = new SQLStmt("select hll from estimates where appid = ?;");
    final static SQLStmt upsertHLL = new SQLStmt("upsert into estimates (appid, hll) values (?, ?);");

    public VoltTable[] run(long appId, long hashedDeviceId) {

        // get the bytes for the HLL
        voltQueueSQL(selectHLL, EXPECT_ZERO_OR_ONE_ROW, appId);
        VoltTable hllBytesTable = voltExecuteSQL()[0];

        HyperLogLog hll = null;
        byte[] hllBytes = null;

        if (hllBytesTable.getRowCount() == 0) {
            hll = new HyperLogLog(12);
        }
        else {
            hllBytes = hllBytesTable.fetchRow(0).getVarbinary("hll");
            try {
                hll = HyperLogLog.fromBytes(hllBytes);
            } catch (IOException e) {
                e.printStackTrace();
                throw new VoltAbortException(e);
            }
        }
        assert(hll != null);

        hll.offerHashed(hashedDeviceId);

        try {
            hllBytes = hll.toBytes();
        } catch (IOException e) {
            e.printStackTrace();
            throw new VoltAbortException(e);
        }

        voltQueueSQL(upsertHLL, EXPECT_SCALAR_MATCH(1), appId, hllBytes);
        return voltExecuteSQL(true);
    }
}
