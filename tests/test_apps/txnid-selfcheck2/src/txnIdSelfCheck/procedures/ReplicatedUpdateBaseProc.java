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
import org.voltdb.VoltTable;

public class ReplicatedUpdateBaseProc extends UpdateBaseProc {

    public final SQLStmt r_getCIDData = new SQLStmt(
            "SELECT * FROM replicated r INNER JOIN dimension d ON r.cid=d.cid WHERE r.cid = ? ORDER BY cid, rid desc;");

    public final SQLStmt r_cleanUp = new SQLStmt(
            "DELETE FROM replicated WHERE cid = ? and cnt < ?;");

    public final SQLStmt r_getAdhocData = new SQLStmt(
            "SELECT * FROM adhocr ORDER BY ts DESC, id LIMIT 1");

    public final SQLStmt r_insert = new SQLStmt(
            "INSERT INTO replicated VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public final SQLStmt r_export = new SQLStmt(
            "INSERT INTO replicated_export VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    // This is for DR. Make sure that the MP transaction gets inserted into the same place in the txn stream
    // at the master and replica.  If they differ, we should get a hash mismatch at the Doctor Agent since we've
    // jammed this into the returned results.
    public final SQLStmt r_getPartitionedSummary = new SQLStmt(
            "SELECT cid, max(txnid), max(ts), max(rid), max(cnt) FROM PARTITIONED GROUP BY cid ORDER BY cid DESC;");

    @Override
    public long run() {
        return 0; // never called in base procedure
    }

    public VoltTable[] doSummaryAndCombineResults(VoltTable[] workResults)
    {
        // Now get the partitioned table summary.
        voltQueueSQL(r_getPartitionedSummary);
        VoltTable[] results = voltExecuteSQL();

        VoltTable[] combined = new VoltTable[workResults.length + results.length];
        int combi = 0;
        for (int i = 0; i < workResults.length; i++) {
            combined[combi] = workResults[i];
            combi++;
        }
        for (int i = 0; i < results.length; i++) {
            combined[combi] = results[i];
            combi++;
        }

        return combined;
    }
}
