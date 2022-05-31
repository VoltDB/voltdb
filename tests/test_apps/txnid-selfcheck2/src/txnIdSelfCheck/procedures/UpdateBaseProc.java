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
import org.voltdb.SQLStmtAdHocHelper;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.utils.MiscUtils;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.types.TimestampType;

public class UpdateBaseProc extends VoltProcedure {

    // these SQLStmt statements maybe dynamically altered for testing inside run.sh for alternate jars.
    public final SQLStmt d_getCount = new SQLStmt(
            "SELECT count(*) FROM dimension where cid = ?;");

    // join partitioned tbl to replicated tbl. This enables detection of some replica faults.
    public final SQLStmt p_getCIDData = new SQLStmt(
            "SELECT * FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid WHERE p.cid = ? ORDER BY p.cid, p.rid desc;");

    public final SQLStmt p_cleanUp = new SQLStmt(
            "DELETE FROM partitioned WHERE cid = ? and cnt < ?;");

    public final SQLStmt p_getAdhocData = new SQLStmt(
            "SELECT * FROM adhocp ORDER BY ts DESC, id LIMIT 1");

    public final SQLStmt p_insert = new SQLStmt(
            "INSERT INTO partitioned (txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocinc, adhocjmp, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public final SQLStmt p_update = new SQLStmt(
            "UPDATE partitioned set txnid=?, prevtxnid=?, ts=?, cidallhash=?, rid=?, cnt=add2Bigint(cnt,1), adhocinc=?, adhocjmp=?, value=identityVarbin(value) where cid=? and rid=?");

    public final SQLStmt p_export = new SQLStmt(
            "INSERT INTO partitioned_export (txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocinc, adhocjmp, value) VALUES (?, ?, ?, ?, ?, ?, add2Bigint(?,1), ?, ?, ?);");

    public final SQLStmt p_getViewData = new SQLStmt(
            "SELECT * FROM partview WHERE cid=? ORDER BY cid DESC;");

    public final SQLStmt p_getExViewData = new SQLStmt(
            "SELECT * FROM ex_partview WHERE cid=? ORDER BY cid DESC;");

    public final SQLStmt p_getExViewShadowData = new SQLStmt(
            "SELECT * FROM ex_partview_shadow WHERE cid=? ORDER BY cid DESC;");

    public final SQLStmt p_upsertExViewShadowData = new SQLStmt(
            "UPSERT INTO ex_partview_shadow VALUES(?, ?, ?, ?, ?);");

    public final SQLStmt p_deleteExViewData = new SQLStmt(
            "DELETE FROM ex_partview WHERE cid=?;");

    public final SQLStmt p_deleteExViewShadowData = new SQLStmt(
            "DELETE FROM ex_partview_shadow WHERE cid=?;");

    public final SQLStmt p_updateExViewData = new SQLStmt(
            "UPDATE ex_partview SET entries = ?, minimum = ?, maximum = ?, summation = ? WHERE cid=?;");

    public final SQLStmt p_updateExViewShadowData = new SQLStmt(
            "UPDATE ex_partview_shadow SET entries = ?, minimum = ?, maximum = ?, summation = ? WHERE cid=?;");

    // PLEASE SEE ReplicatedUpdateBaseProc for the replicated procs
    // that can't be listed here (or SP procs wouldn't compile)

    public long run() {
        return 0; // never called in base procedure
    }

    protected VoltTable[] doWork(SQLStmt getCIDData, SQLStmt cleanUp, SQLStmt insert, SQLStmt update, SQLStmt export, SQLStmt getAdHocData, SQLStmt getViewData,
            byte cid, long rid, byte[] value, byte shouldRollback, boolean usestreamviews)
    {
        voltQueueSQL(getCIDData, cid);
        voltQueueSQL(getAdHocData);
        voltQueueSQL(d_getCount, cid);
        voltQueueSQL(getViewData, cid);
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];
        VoltTable adhoc = results[1];
        VoltTable dim = results[2];
        VoltTable view = results[3];

        final long txnid = getUniqueId();
        // final long ts = getTransactionTime().getTime();
        // final TimestampType ts = getTransactionTime();
        final TimestampType ts = new TimestampType(getTransactionTime());
        long prevtxnid = 0;
        long prevrid = 0;
        long cnt = 0;
        long prevcnt = 0;

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
            prevcnt = row.getLong("cnt");
            cnt = row.getLong("cnt") + 1;
            prevtxnid = row.getLong("txnid");
            prevrid = row.getLong("rid");
        }

        validateCIDData(data, view, getClass().getName());

        // check the rids monotonically increase
        if (prevrid >= rid) {
            throw new VoltAbortException(getClass().getName() +
                    " previous rid " + prevrid +
                    " >= than current rid " + rid +
                    " for cid " + cid);
        }

        voltQueueSQL(insert, txnid, prevtxnid, ts, cid, cidallhash, rid, prevcnt, adhocInc, adhocJmp, value);
        voltQueueSQL(update, txnid, prevtxnid, ts, cidallhash, rid, adhocInc, adhocJmp, cid, rid);
        voltQueueSQL(export, txnid, prevtxnid, ts, cid, cidallhash, rid, prevcnt, adhocInc, adhocJmp, value);
        voltQueueSQL(cleanUp, cid, cnt - 10);
        voltQueueSQL(getCIDData, cid);
        voltQueueSQL(getViewData, cid);
        assert dim.getRowCount() == 1;
        VoltTable[] retval = voltExecuteSQL();
        // Is this comment below now obsolete and can be removed?
        // Verify that our update happened.  The client is reporting data errors on this validation
        // not seen by the server, hopefully this will bisect where they're occurring.
        data = retval[retval.length-2];
        view = retval[retval.length-1];

        VoltTableRow row = data.fetchRow(0);
        if (row.getVarbinary("value").length == 0)
            throw new VoltAbortException("Value column contained no data in UpdateBaseProc");

        validateCIDData(data, view, getClass().getName());

        if (usestreamviews) {
            // insert to export table done, check corresponding materialized view
            validateView(cid, cnt, "insert");

            // update export materialized view & validate
            int someData = 5; // arbitrary. could use random but really doesn't matter
            voltQueueSQL(p_updateExViewData, someData, someData+1, someData+2, someData+3, cid);
            voltQueueSQL(p_updateExViewShadowData, someData, someData+1, someData+2, someData+3, cid);
            voltExecuteSQL();
            validateView(cid, cnt, "update");

            // delete from export materialized view & validate
            voltQueueSQL(p_deleteExViewData, cid);
            voltQueueSQL(p_deleteExViewShadowData, cid);
            voltExecuteSQL();
            validateView(cid, cnt, "delete");
        }

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }

        return retval;
    }

    protected void validateView(byte cid, long cnt, String type) {
        // we've inserted a row in the export (streaming) table.
        // now pull derived data from the materialized view for
        // checking that it's been updated
        voltQueueSQL(p_getExViewData, cid);
        voltQueueSQL(p_getExViewShadowData, cid);
        VoltTable[] streamresults = voltExecuteSQL();
        validateStreamData(type, streamresults[0], streamresults[1], cid, cnt);
    }

    @SuppressWarnings("deprecation")
    protected VoltTable[] doWorkInProcAdHoc(byte cid, long rid, byte[] value, byte shouldRollback)  {
        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "SELECT * FROM replicated r INNER JOIN dimension d ON r.cid=d.cid WHERE r.cid = ? ORDER BY r.cid, r.rid desc;", cid);
        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "SELECT * FROM adhocr ORDER BY ts DESC, id LIMIT 1");
        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "SELECT * FROM replview WHERE cid = ? ORDER BY cid desc;", cid);
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];
        VoltTable adhoc = results[1];
        VoltTable view = results[2];

        final long txnid = getUniqueId();
        // final long ts = getTransactionTime().getTime();
        // final TimestampType ts = getTransactionTime();
        final TimestampType ts = new TimestampType(getTransactionTime());
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

        validateCIDData(data, view, getClass().getName());

        // check the rids monotonically increase
        if (prevrid >= rid) {
            throw new VoltAbortException(getClass().getName() +
                    " previous rid " + prevrid +
                    " >= than current rid " + rid +
                    " for cid " + cid);
        }

        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "INSERT INTO replicated (txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocinc, adhocjmp, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocInc, adhocJmp, value);
        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "INSERT INTO replicated_export VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", txnid, prevtxnid, ts, cid, cidallhash, rid, cnt, adhocInc, adhocJmp, value);
        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "DELETE FROM replicated WHERE cid = ? and cnt < ?;", cid, cnt - 10);
        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "SELECT * FROM replicated r INNER JOIN dimension d ON r.cid=d.cid WHERE r.cid = ? ORDER BY r.cid, r.rid desc;", cid);
        SQLStmtAdHocHelperHelper.voltQueueSQLExperimental(this, "SELECT * FROM replview WHERE cid = ? ORDER BY cid desc;", cid);
        VoltTable[] retval = voltExecuteSQL();
        // Verify that our update happened.  The client is reporting data errors on this validation
        // not seen by the server, hopefully this will bisect where they're occurring.
        data = retval[3];
        view = retval[4];
        validateCIDData(data, view, getClass().getName());

        VoltTableRow row = data.fetchRow(0);
        if (row.getVarbinary("value").length == 0)
            throw new VoltAbortException("Value column contained no data in UpdateBaseProc");

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }

        return retval;
    }

    public static void validateCIDData(VoltTable data, VoltTable view, String callerId) {
        // empty tables are lamely valid
        if (data.getRowCount() == 0) return;

        byte cid = (byte) data.fetchRow(0).getLong("cid");

        data.resetRowPosition();
        long prevCnt = 0;
        long entries = 0;
        long max = 0;
        long min = Long.MAX_VALUE;
        long sum = 0;
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
            if (view != null) {
                entries += 1;
                max = Math.max(max, cntValue);
                min = Math.min(min, cntValue);
                sum += cntValue;
            }
            prevCnt = cntValue;
        }
        if (view != null) {
            if (view.getRowCount() != 1)
                throw new VoltAbortException("View has multiple entries of the same cid, that should not happen.");
            VoltTableRow row0 = view.fetchRow(0);
            long v_entries = row0.getLong("entries");
            long v_max = row0.getLong("maximum");
            long v_min = row0.getLong("minimum");
            long v_sum = row0.getLong("summation");

            if (v_entries != entries)
                throw new VoltAbortException(
                        "The count(*):"+v_entries+" materialized view aggregation does not match the number of cnt entries:"+entries+" for cid:"+cid);
            if (v_max != max)
                throw new VoltAbortException(
                        "The max(cnt):"+v_max+" materialized view aggregation does not match the max:"+max+" for cid:"+cid);
            if (v_min != min)
                throw new VoltAbortException(
                        "The min(cnt):"+v_min+" materialized view aggregation does not match the min:"+min+" for cid:"+cid);
            if (v_sum != sum)
                throw new VoltAbortException(
                        "The sum(cnt):"+v_sum+" materialized view aggregation does not match the sum:"+sum+" for cid:"+cid);
        }
    }

    private void validateStreamData(String type, VoltTable exview, VoltTable shadowview, byte cid, long cnt) {
        if (type == "delete") {
            if (exview.getRowCount() == 0) {
                return;      // success
            } else {
                throw new VoltAbortException("Export view has "+exview.getRowCount()+" rows for this id. Zero expected after delete");
            }
        }

        if (exview.getRowCount() != 1)
            throw new VoltAbortException("Export view has "+exview.getRowCount()+" entries of the same cid, that should not happen.");
        VoltTableRow row0 = exview.fetchRow(0);
        long v_entries = row0.getLong("entries");
        long v_max = row0.getLong("maximum");
        long v_min = row0.getLong("minimum");
        long v_sum = row0.getLong("summation");

        if (shadowview.getRowCount() == 1) {
            row0 = shadowview.fetchRow(0);
            long shadow_entries = row0.getLong("entries");
            long shadow_max = row0.getLong("maximum");
            long shadow_min = row0.getLong("minimum");
            long shadow_sum = row0.getLong("summation");

            // adjust the shadow values for updated cnt, not done for "update"
            if (type == "insert") {
                shadow_entries++;
                shadow_max = Math.max(shadow_max, v_max);
                shadow_min = Math.min(shadow_min, v_min);
                shadow_sum += cnt;
            }

            if (v_entries != shadow_entries)
                throw new VoltAbortException("View entries:" + v_entries +
                        " materialized view aggregation does not match the number of shadow entries:" + shadow_entries + " for cid:" + cid);

            if (v_max != shadow_max)
                throw new VoltAbortException("View v_max:" + v_max +
                        " materialized view aggregation does not match the shadow max:" + shadow_max + " for cid:" + cid);

            if (v_min != shadow_min)
                throw new VoltAbortException("View v_min:" + v_min +
                        " materialized view aggregation does not match the shadow min:" + shadow_min + " for cid:" + cid);

            if (v_sum != shadow_sum)
                throw new VoltAbortException("View v_sum:" + v_sum +
                        " materialized view aggregation does not match the shadow sum:" + shadow_sum + " for cid:" + cid);

            voltQueueSQL(p_upsertExViewShadowData, cid, shadow_entries, shadow_max, shadow_min, shadow_sum);
        } else {
            // first time through, get initial values into the shadow table
            voltQueueSQL(p_upsertExViewShadowData, cid, v_entries, v_max, v_min, v_sum);
        }
        // update the shadow table with the new matching values and return
        voltExecuteSQL();
        return;
    }
}
