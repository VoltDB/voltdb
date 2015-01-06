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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class doTxn extends VoltProcedure {

    public final SQLStmt getLastTxnId = new SQLStmt(
            "SELECT txnid, ts, rid FROM partitioned WHERE cid = ? ORDER BY cid, rid LIMIT 2;");

    public final SQLStmt getReplicated = new SQLStmt(
            "SELECT * FROM replicated ORDER BY txnid LIMIT 10;");

    public final SQLStmt insertTxnid = new SQLStmt(
            "INSERT INTO partitioned VALUES (?, ?, ?, ?, ?);");

    public final SQLStmt deleteOldTxns = new SQLStmt(
            "DELETE FROM partitioned WHERE cid = ? AND rid > ? AND rid <= 0;");

    private long getRid(long rid) {
        return -rid;
    }

    public VoltTable[] run(byte cid, long rid, long oldestRid, byte[] value) {
        final long txnId = getVoltPrivateRealTransactionIdDontUseMe();
        final long uniqueId = getUniqueId();

        voltQueueSQL(insertTxnid, txnId, uniqueId, cid, getRid(rid), value);
        voltQueueSQL(deleteOldTxns, cid, getRid(oldestRid));
        voltExecuteSQL();

        voltQueueSQL(getLastTxnId, cid);
        voltQueueSQL(getReplicated);
        VoltTable[] results = voltExecuteSQL();

        if (results[0].getRowCount() == 2) {
            /*
             * if there are 2 row, the current row must be the first txn
             */
            results[0].advanceRow();
            long insertedTxnId = results[0].getLong("txnid");
            long insertedrid = getRid(results[0].getLong("rid"));
            if (insertedTxnId != txnId || insertedrid != rid) {
                throw new VoltAbortException("Failed to insert into partitioned");
            }

            // this one must be the previous txn
            results[0].advanceRow();
            long previousRid = getRid(results[0].getLong("rid"));
            if (previousRid >= rid) {
                throw new VoltAbortException("doTxn previous rid " + previousRid +
                                             " larger than current rid " + rid +
                                             " for cid " + cid);
            }
        } else if (results[0].getRowCount() == 1) {
            results[0].advanceRow();
            long insertedTxnId = results[0].getLong("txnid");
            long insertedrid = getRid(results[0].getLong("rid"));
            if (insertedTxnId != txnId || insertedrid != rid) {
                throw new VoltAbortException("Failed to insert into partitioned");
            }
        } else {
            throw new VoltAbortException("Unpossible");
        }
        results[0].resetRowPosition();

        return results;
    }
}
