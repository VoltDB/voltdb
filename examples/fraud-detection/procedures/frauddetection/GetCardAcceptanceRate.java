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

package frauddetection;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetCardAcceptanceRate extends VoltProcedure {
    public static final SQLStmt getLastSecondCounts = new SQLStmt(
    "SELECT accept, cnt " +
    "FROM secondly_card_acceptance_rate " +
    "ORDER BY second DESC " +
    "LIMIT 2;"
    );

    public long run() {
        voltQueueSQL(getLastSecondCounts);
        final VoltTable countTable = voltExecuteSQL(true)[0];
        long acceptCount = 0;
        long rejectCount = 0;
        while (countTable.advanceRow()) {
            if (countTable.getLong("accept") == CardSwipe.ACTIVITY_ACCEPTED) {
                acceptCount += countTable.getLong("cnt");
            } else {
                rejectCount += countTable.getLong("cnt");
            }
        }

        return acceptCount * 100 / Math.max(acceptCount + rejectCount, 1);
    }
}
