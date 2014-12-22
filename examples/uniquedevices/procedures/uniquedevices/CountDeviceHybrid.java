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

public class CountDeviceHybrid extends VoltProcedure {

    public final static long MAX_EXACT_COUNT = 1000;

    final static SQLStmt estimatesSelect = new SQLStmt("select devicecount, hll from estimates where appid = ?;");
    final static SQLStmt estimatesUpsert = new SQLStmt("upsert into estimates (appid, devicecount, hll) values (?, ?, ?);");
    final static SQLStmt exactUpsert = new SQLStmt("upsert into exact (appid, deviceid) values (?, ?);");
    final static SQLStmt exactCardinality = new SQLStmt("select count(*) from exact where appid = ?;");
    final static SQLStmt exactClear = new SQLStmt("delete from exact where appid = ?;");

    public long run(long appId, long hashedDeviceId) throws IOException {

        long current = 0;
        HyperLogLog hll = null;
        byte[] hllBytes = null;

        // get the HLL from the db or create one if needed
        voltQueueSQL(estimatesSelect, EXPECT_ZERO_OR_ONE_ROW, appId);
        VoltTable estimatesTable = voltExecuteSQL()[0];

        if (estimatesTable.advanceRow()) {
            estimatesTable.advanceRow();
            current = estimatesTable.getLong("devicecount");
            hllBytes = estimatesTable.getVarbinary("hll");
            hll = HyperLogLog.fromBytes(hllBytes);
        }
        else {
            hll = new HyperLogLog(12);
        }

        // offer the hashed device id to the HLL
        hll.offerHashed(hashedDeviceId);

        long newEstimate = current;
        if (current < MAX_EXACT_COUNT) {
            voltQueueSQL(exactUpsert, EXPECT_ZERO_OR_ONE_ROW, appId, hashedDeviceId);
            voltQueueSQL(exactCardinality, EXPECT_ONE_ROW, appId);
            newEstimate = voltExecuteSQL()[1].asScalarLong();

            // clear the exact table for this appid once it's not needed
            if (newEstimate == MAX_EXACT_COUNT) {
                voltQueueSQL(exactClear, EXPECT_SCALAR_MATCH(MAX_EXACT_COUNT), appId);
                voltExecuteSQL();
            }
        }
        else if (hll.getDirty()) {
            newEstimate = Math.max(MAX_EXACT_COUNT, hll.cardinality());
        }

        // if the estimates row needs updating, upsert it
        if ((newEstimate != current) || hll.getDirty()) {
            voltQueueSQL(estimatesUpsert, EXPECT_SCALAR_MATCH(1), appId, newEstimate, hll.toBytes());
            voltExecuteSQL(true);
        }

        return newEstimate;
    }
}
