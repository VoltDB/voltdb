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

package np;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Select extends VoltProcedure {

    public final SQLStmt selAcct = new SQLStmt("SELECT * FROM card_account WHERE pan >= ? and pan < ?;");

    public final SQLStmt selActv = new SQLStmt("SELECT * FROM card_activity WHERE pan >= ? and pan < ?;");

    public long run (String start, String end) {
        int startPan = Integer.parseInt(start);
        int endPan = Integer.parseInt(end);

        double totalBalance = 0;
        double totalTransfer = 0;

        voltQueueSQL(selAcct, EXPECT_NON_EMPTY, start, end);
        voltQueueSQL(selActv, start, end);
        VoltTable results[] = voltExecuteSQL(true);

        // Do a random lookup query and see if the total
        // transfer amount is larger than the total balance
        VoltTable tb1 = results[0];
        while (tb1.advanceRow()) {
            totalBalance += tb1.getDouble(3);
        }

        VoltTable tb2 = results[1];
        while (tb2.advanceRow()) {
            totalTransfer += tb2.getDouble(4);
        }

        return totalTransfer > totalBalance ? 1 : 0;
    }
}
