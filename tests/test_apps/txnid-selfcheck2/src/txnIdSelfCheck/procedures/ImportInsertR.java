/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

/*
 * Stored procedure for Kafka import
 *
 * If incoming data is in the mirror table, delete that row.
 *
 * Else add to import table as a record of rows that didn't get
 * into the mirror table, a major error!
 */

/**
 * Created by prosegay on 1/3/17.
 */

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;


public class ImportInsertR extends VoltProcedure {
    public final SQLStmt select = new SQLStmt("SELECT ts,cid,cnt FROM importr WHERE cid=? ORDER BY 1,2,3");
    public final SQLStmt insert = new SQLStmt("INSERT INTO importr (ts,cid,cnt) values (?,?,?)");
    public final SQLStmt update = new SQLStmt("UPDATE importr set ts=?,cnt=? where cid=?");

    public long run(     long txnid
                        ,long prevtxnid
                        ,long ts
                        ,byte cid
                        ,long cidallhash
                        ,long rid
                        ,long cnt
                        ,long adhocinc
                        ,long adhocjmp
                        ,byte[] value) {

        voltQueueSQL(select, cid);
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];
        int rowCount = data.getRowCount();
        if (rowCount != 0) {
            if (rowCount != 1)
                throw new VoltAbortException(getClass().getName() + "should get only one row per cid");
            VoltTableRow row = data.fetchRow(0);
            long fcid = row.getLong("cid");
            if (fcid != cid)
                throw new VoltAbortException(getClass().getName() +
                        " serious error expected cid " + cid + " != fetched cid: "+ fcid);
            long mts = Math.max(row.getLong("ts"), ts);
            long mcnt = Math.max(row.getLong("cnt"), cnt);
            voltQueueSQL(update, mts, mcnt, cid);
        } else {
            voltQueueSQL(insert, ts, cid, cnt);
        }
        voltExecuteSQL(true);
        return 0;
    }
}

