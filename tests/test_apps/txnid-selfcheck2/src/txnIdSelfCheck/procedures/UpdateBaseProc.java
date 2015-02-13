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
import org.voltdb.VoltTableRow;
import org.voltdb.utils.MiscUtils;

public class UpdateBaseProc extends VoltProcedure {

    public final SQLStmt d_getCount = new SQLStmt(
            "SELECT count(*) FROM dimension where cid = ?;");

    // join partitioned tbl to replicated tbl. This enables detection of some replica faults.
    public final SQLStmt p_getCIDData = new SQLStmt(
            "SELECT * FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid WHERE p.cid = ? ORDER BY cid, rid desc;");

    public final SQLStmt p_cleanUp = new SQLStmt(
            "DELETE FROM partitioned WHERE cid = ? and cnt < ?;");

    public final SQLStmt p_getAdhocData = new SQLStmt(
            "SELECT * FROM adhocp ORDER BY ts DESC, id LIMIT 1");

    public final SQLStmt p_insert = new SQLStmt(
            "INSERT INTO partitioned VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public final SQLStmt p_export = new SQLStmt(
            "INSERT INTO partitioned_export VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    // PLEASE SEE ReplicatedUpdateBaseProc for the replicated procs
    // that can't be listed here (or SP procs wouldn't compile)

    public long run() {
        return 0; // never called in base procedure
    }

    protected VoltTable[] doWork(SQLStmt getCIDData, SQLStmt cleanUp, SQLStmt insert, SQLStmt export, SQLStmt getAdHocData,
                                 byte cid, long rid, byte[] value, byte shouldRollback)
    {
        voltQueueSQL(getCIDData, cid);
        voltQueueSQL(getAdHocData);
        voltQueueSQL(d_getCount, cid);
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];
        VoltTable adhoc = results[1];
        VoltTable dim = results[2];

        final long txnid = getUniqueId();
        final long ts = getTransactionTime().getTime();
        long prevtxnid = 0;
        long prevrid = 0;
        long cnt = 0;

        // read data modified by AdHocMayhemThread for later insertion
        final long adhocInc = adhoc.getRowCount() > 0 ? adhoc.fetchRow(0).getLong("inc") : 0;
        final long adhocJmp = adhoc.getRowCount() > 0 ? adhoc.fetchRow(0).getLong("jmp") : 0;

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
        if (prevrid >= rid) {
            throw new VoltAbortException(getClass().getName() +
                    " previous rid " + prevrid +
                    " >= than current rid " + rid +
                    " for cid " + cid);
        }

        voltQueueSQL(insert, txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocInc, adhocJmp, new byte[0]);
        voltQueueSQL(export, txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocInc, adhocJmp, new byte[0]);
        voltQueueSQL(cleanUp, cid, cnt - 10);
        voltQueueSQL(getCIDData, cid);
        assert dim.getRowCount() == 1;
        VoltTable[] retval = voltExecuteSQL();
        // Verify that our update happened.  The client is reporting data errors on this validation
        // not seen by the server, hopefully this will bisect where they're occuring.
        data = retval[3];
        validateCIDData(data, getClass().getName());

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }

        return retval;
    }

    @SuppressWarnings("deprecation")
    protected VoltTable[] doWorkInProcAdHoc(byte cid, long rid, byte[] value, byte shouldRollback) {
        voltQueueSQLExperimental("SELECT * FROM replicated r INNER JOIN dimension d ON r.cid=d.cid WHERE r.cid = ? ORDER BY cid, rid desc;", cid);
        voltQueueSQLExperimental("SELECT * FROM adhocr ORDER BY ts DESC, id LIMIT 1");
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];
        VoltTable adhoc = results[1];

        final long txnid = getUniqueId();
        final long ts = getTransactionTime().getTime();
        long prevtxnid = 0;
        long prevrid = 0;
        long cnt = 0;

        // read data modified by AdHocMayhemThread for later insertion
        final long adhocInc = adhoc.getRowCount() > 0 ? adhoc.fetchRow(0).getLong("inc") : 0;
        final long adhocJmp = adhoc.getRowCount() > 0 ? adhoc.fetchRow(0).getLong("jmp") : 0;

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
        if (prevrid >= rid) {
            throw new VoltAbortException(getClass().getName() +
                    " previous rid " + prevrid +
                    " >= than current rid " + rid +
                    " for cid " + cid);
        }

        voltQueueSQLExperimental("INSERT INTO replicated VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocInc, adhocJmp, new byte[0]);
        voltQueueSQLExperimental("INSERT INTO replicated_export VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocInc, adhocJmp, new byte[0]);
        voltQueueSQLExperimental("DELETE FROM replicated WHERE cid = ? and cnt < ?;", cid, cnt - 10);
        voltQueueSQLExperimental("SELECT * FROM replicated r INNER JOIN dimension d ON r.cid=d.cid WHERE r.cid = ? ORDER BY cid, rid desc;", cid);
        VoltTable[] retval = voltExecuteSQL();
        // Verify that our update happened.  The client is reporting data errors on this validation
        // not seen by the server, hopefully this will bisect where they're occurring.
        data = retval[3];
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
            // check that the inner join of partitioned and replicated tables
            // produce the expected result
            byte desc = (byte) data.getLong("desc");
            if (desc != cid) {
                throw new VoltAbortException(callerId +
                        " desc value " + desc +
                        " not equal to cid value " + cid);
            }
            // make sure all cnt values are consecutive
            long cntValue = data.getLong("cnt");
            if ((prevCnt > 0) && ((prevCnt - 1) != cntValue)) {
                throw new VoltAbortException(callerId +
                        " cnt values are not consecutive" +
                        " for cid " + cid + ". Got " + cntValue +
                        ", prev was: " + prevCnt);
            }
            prevCnt = cntValue;
        }
    }
}
