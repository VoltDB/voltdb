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

package uniquedevices;

import java.io.IOException;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.hll.HyperLogLog;

public class CountDeviceEstimate extends VoltProcedure {

    final static SQLStmt estimatesSelect = new SQLStmt("select devicecount, hll from estimates where appid = ?;");
    final static SQLStmt estimatesUpsert = new SQLStmt("upsert into estimates (appid, devicecount, hll) values (?, ?, ?);");

    public VoltTable[] run(long appId, long hashedDeviceId) throws IOException {

        // get the HLL from the db
        voltQueueSQL(estimatesSelect, EXPECT_ZERO_OR_ONE_ROW, appId);
        VoltTable estimatesTable = voltExecuteSQL()[0];

        HyperLogLog hll = null;
        // if the row with the hyperloglog blob exists...
        if (estimatesTable.advanceRow()) {
            byte[] hllBytes = estimatesTable.getVarbinary("hll");
            hll = HyperLogLog.fromBytes(hllBytes);
        }
        // otherwise create a hyperloglog blob
        else {
            hll = new HyperLogLog(12);
        }

        // offer the hashed device id to the HLL
        hll.offerHashed(hashedDeviceId);

        // if the estimates row needs updating, upsert it
        if (hll.getDirty()) {
            // update the state with exact estimate and update blob for the hll
            voltQueueSQL(estimatesUpsert, EXPECT_SCALAR_MATCH(1), appId, hll.cardinality(), hll.toBytes());
            return voltExecuteSQL(true);
        }
        return null;
    }
}
