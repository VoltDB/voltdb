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

package org.voltdb;

import org.voltdb.VoltTable.ColumnInfo;

/**
 * Used in replicated procedure invocation test. It is used to check if the
 * procedure-visible txn ID is the one that the client sent.
 */
public class ReplicatedProcedure extends VoltProcedure {
    @SuppressWarnings("unused")
    private final SQLStmt insert = new SQLStmt("INSERT INTO A VALUES (1)");

    public VoltTable run(long id, String desc) {
        if (id != 1 || !desc.equals("haha")) {
            throw new VoltAbortException();
        }

        VoltTable result = new VoltTable(new ColumnInfo("txnId", VoltType.BIGINT),
                                         new ColumnInfo("timestamp", VoltType.BIGINT));
        result.addRow(DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this), getUniqueId());

        // replicated txns get their results replaced by a hash... so stash this here
        setAppStatusString(result.toJSONString());
        return result;
    }
}
