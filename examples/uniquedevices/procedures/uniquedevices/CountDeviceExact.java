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

package uniquedevices;

import java.io.IOException;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class CountDeviceExact extends VoltProcedure {

    final static SQLStmt estimatesSelect = new SQLStmt("select devicecount from estimates where appid = ?;");
    final static SQLStmt estimatesUpsert = new SQLStmt("upsert into estimates (appid, devicecount) values (?, ?);");
    final static SQLStmt exactUpsert = new SQLStmt("upsert into exact (appid, deviceid) values (?, ?);");
    final static SQLStmt exactCardinality = new SQLStmt("select count(*) from exact where appid = ?;");

    public VoltTable[] run(long appId, long hashedDeviceId) throws IOException {

        // get the current cardinality
        voltQueueSQL(estimatesSelect, EXPECT_ZERO_OR_ONE_ROW, appId);
        // upsert this appid,deviceid pair into the "exact" table
        voltQueueSQL(exactUpsert, EXPECT_ZERO_OR_ONE_ROW, appId, hashedDeviceId);
        // count rows in the "exact" table for this appId
        voltQueueSQL(exactCardinality, EXPECT_SCALAR_LONG, appId);
        VoltTable[] results = voltExecuteSQL();

        // get the previous cardinality (if any)
        long current = 0;
        VoltTable estimatesTable = results[0];
        if (estimatesTable.advanceRow()) {
            current = estimatesTable.getLong("devicecount");
        }

        // get the new cardinality
        long newCardinality = results[2].asScalarLong();

        // if the estimates row needs updating, upsert it
        if (newCardinality != current) {
            voltQueueSQL(estimatesUpsert, EXPECT_SCALAR_MATCH(1), appId, newCardinality);
            voltExecuteSQL(true);
        }
        return null;
    }
}
