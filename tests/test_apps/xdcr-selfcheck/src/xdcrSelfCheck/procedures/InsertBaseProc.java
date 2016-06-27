/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package xdcrSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.utils.MiscUtils;

public class InsertBaseProc extends VoltProcedure {

    public final SQLStmt p_getCIDData = new SQLStmt(
            "SELECT * FROM xdcr_partitioned p WHERE p.cid=? AND p.rid=? ORDER BY p.cid, p.rid desc;");

    public final SQLStmt p_cleanUp = new SQLStmt(
            "DELETE FROM xdcr_partitioned WHERE cid = ? and cnt < ?;");

    public final SQLStmt p_insert = new SQLStmt(
            "INSERT INTO xdcr_partitioned (clusterid, txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, key, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public long run() {
        return 0; // never called in base procedure
    }

    protected VoltTable[] doWork(SQLStmt getCIDData, SQLStmt cleanUp, SQLStmt insert,
            byte cid, long rid, byte[] key, byte[] value, byte shouldRollback)
    {
        final long clusterid = getClusterId();
        final long txnid = getUniqueId();
        final long ts = getTransactionTime().getTime();

        voltQueueSQL(getCIDData, cid, rid);
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];

        long prevtxnid = 0;
        long prevrid = 0;
        long cnt = 0;

        // compute the cheesy checksum of all of the table's contents based on
        // this cid to subsequently store in the new row
        final long cidallhash = MiscUtils.cheesyBufferCheckSum(data.getBuffer());

        // get the most recent row's data
        int rowCount = data.getRowCount();
        if (rowCount != 0) {
            VoltTableRow row = data.fetchRow(0);
            cnt = row.getLong("cnt") + 1;
            prevtxnid = row.getLong("txnid");
            prevrid = row.getLong("rid");
        }

        validateCIDData(data, getClass().getName());

        // check the rids monotonically increase
        if (prevrid > rid) {
            throw new VoltAbortException(getClass().getName() +
                    " previous rid " + prevrid +
                    " >= than current rid " + rid +
                    " for cid " + cid);
        }

        voltQueueSQL(insert, clusterid, txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, key, value);
        voltQueueSQL(cleanUp, cid, cnt - 10);
        voltQueueSQL(getCIDData, cid, rid);
        VoltTable[] retval = voltExecuteSQL();
        // Is this comment below now obsolete and can be removed?
        // Verify that our update happened.  The client is reporting data errors on this validation
        // not seen by the server, hopefully this will bisect where they're occurring.
        data = retval[2];

        VoltTableRow row = data.fetchRow(0);
        if (row.getVarbinary("value").length == 0) {
            throw new VoltAbortException("Value column contained no data in InsertBaseProc");
        }

        validateCIDData(data, getClass().getName());

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }

        return retval;
    }

    public static void validateCIDData(VoltTable data, String callerId) {
        // empty tables are lamely valid
        if (data.getRowCount() == 0) return;

        byte cid = (byte) data.fetchRow(0).getLong("cid");

        data.resetRowPosition();
        long prevCnt = 0;
        while (data.advanceRow()) {
            // make sure all cnt values are consecutive
            long cntValue = data.getLong("cnt");
            if ((prevCnt > 0) && ((prevCnt - 1) != cntValue)) {
                throw new VoltAbortException(callerId +
                        " cnt values are not consecutive for cid " + cid + ". Got " + cntValue + ", prev was: " + prevCnt);
            }
            prevCnt = cntValue;
        }
    }
}
