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
import org.voltdb.VoltTableRow;
import org.voltdb.utils.Encoder;

public class ImportBaseProc extends VoltProcedure {

    public long run() {
        return 0; // never called in base procedure
    }

    protected VoltTable[] doWork(SQLStmt select
                                 ,SQLStmt update
                                 ,SQLStmt insert
                                 ,SQLStmt select_bitmap
                                 ,SQLStmt update_bitmap
                                 ,SQLStmt insert_bitmap
                                ,byte cid
                                ,long ts
                                ,long cnt) {

        byte[] bitmap;

        // compute sequence and offsets
        long seq = cnt / 1024;
        int bb = (int) ((cnt - seq*1024) % 1024);
        int B = bb / 8;
        int b = bb % 8;
        byte mask = (byte) Math.pow(2, 7-b);

        voltQueueSQL(select, cid);
        voltQueueSQL(select_bitmap, cid, seq);
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
            long mts = Math.max(row.getTimestampAsLong("ts"), ts);
            long mcnt = Math.max(row.getLong("cnt"), cnt);
            //System.err.println("Invoke update query: mts " + mts + " mcnt " + mcnt + " cid " + cid + " ts " + ts + " cnt " + cnt);
            voltQueueSQL(update, mts, mcnt, cid);
        } else {
            //System.err.println("Invoke insert query: cid " + cid + " ts " + ts + " cnt " + cnt);
            voltQueueSQL(insert, ts, cid, cnt, 1);
        }

        VoltTable bmdata = results[1];
        rowCount = bmdata.getRowCount();
        if (rowCount != 0) {
            VoltTableRow row = bmdata.fetchRow(0);
            bitmap = row.getVarbinary("bitmap");
            if ((bitmap[B] & mask) != 0) {
                // detect/report duplicates ?
            }
            bitmap[B] |= mask;
            //System.err.println("Invoke bitmap update query: bitmap " + Encoder.hexEncode(bitmap) + " cid " + cid + " seq " + seq + " cnt " + cnt);
            voltQueueSQL(update_bitmap, bitmap, cid, seq);
        } else {
            bitmap = new byte[1024];
            bitmap[B] |= mask;
            //System.err.println("Invoke bitmap insert query: cid " + cid + " seq " + seq + " bitmap " + Encoder.hexEncode(bitmap) + " cnt " + cnt);
            voltQueueSQL(insert_bitmap, cid, seq, bitmap);
        }
        return voltExecuteSQL(true);
    }
}
