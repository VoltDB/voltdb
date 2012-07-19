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

package voter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    partitionInfo = "transactions.pid:0",
    singlePartition = true
)
public class doTxn extends VoltProcedure {

    public final SQLStmt getLastTxnId = new SQLStmt(
            "SELECT txnid, rid FROM transactions ORDER BY txnid DESC LIMIT 1;");

    public final SQLStmt getReplicated = new SQLStmt(
            "SELECT * FROM replicated ORDER BY id LIMIT 10;");

    public final SQLStmt insertTxnid = new SQLStmt(
            "INSERT INTO transactions VALUES (?, ?, ?, ?);");

    public final SQLStmt deleteOldTxns = new SQLStmt(
            "DELETE FROM transactions WHERE rid < ? AND rid >= 0;");

    public VoltTable[] run(byte partition, long rid, long oldestRid, byte[] value) {
        final long txnId = getTransactionId();
        voltQueueSQL(getLastTxnId);
        voltQueueSQL(getReplicated);
        VoltTable[] results = voltExecuteSQL();

        if (results[0].advanceRow()) {
            long previousTxnId = results[0].getLong("txnid");
            long previousRid = results[0].getLong("rid");
            if (previousTxnId >= txnId || previousRid >= rid) {
                throw new VoltAbortException("doTxn executed out of order");
            }
            results[0].resetRowPosition();
        }
        if (results[1].advanceRow()) {
            long previousTxnId = results[1].getLong("id");
            if (previousTxnId >= txnId) {
                throw new VoltAbortException("doTxn and updateReplicated executed out of order");
            }
            results[1].resetRowPosition();
        }

        voltQueueSQL(insertTxnid, txnId, partition, rid, value);
        voltQueueSQL(deleteOldTxns, oldestRid);
        voltExecuteSQL(true);

        return results;
    }
}
