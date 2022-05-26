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
import org.voltdb.DeprecatedProcedureAPIAccess;

public class Initialize extends VoltProcedure
{
    // Check if the database has already been initialized
    public final SQLStmt checkStmt = new SQLStmt("SELECT * FROM replicated ORDER BY rid;");

    // Insert into the replicated table
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO replicated VALUES (?, ?, ?, ?);");

    public long run() {
        voltQueueSQL(checkStmt, EXPECT_ZERO_OR_ONE_ROW);
        VoltTable result = voltExecuteSQL()[0];

        // if the data is initialized, return the current count
        if (result.getRowCount() != 0) {
            result.advanceRow();
            return result.getLong("rid");
        }

        // initialize the data using the txnId
        @SuppressWarnings("deprecation")
        long txnId = DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this);
        long uniqueId = getUniqueId();
        voltQueueSQL(insertStmt, EXPECT_SCALAR_MATCH(1), txnId, uniqueId, -1, 0);
        voltExecuteSQL(true);

        // return the rid
        return 0;
    }
}
