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
import org.voltdb.VoltType;


public class Summarize_Import extends VoltProcedure {

    final SQLStmt select_i = new SQLStmt("select * from (select cid, cnt from importp where cid between ? and ? " +
                                            "UNION select cid, cnt from importr where cid between ? and ?) z " +
                                            "order by z.cid;");

    final SQLStmt select_e = new SQLStmt("select * from (select cid, max(cnt) as cnt from partitioned where cid between ? and ? group by cid " +
                                            "UNION select cid, max(cnt) as cnt from replicated where cid between ? and ? group by cid) z " +
                                            "order by z.cid;");

    final SQLStmt select_bm = new SQLStmt("select * from (select cid, seq, bitmap from importbp where cid between ? and ? " +
                                            "UNION select cid, seq, bitmap from importbr where cid between ? and ?) z " +
                                            "order by z.cid, z.seq;");

    public VoltTable[] run(int coffset, int ncid) {

        int cidl = coffset;
        int cidh = coffset + ncid - 1;

        voltQueueSQL(select_i, cidl, cidh, cidl, cidh);
        voltQueueSQL(select_bm, cidl, cidh, cidl, cidh);
        voltQueueSQL(select_e, cidl, cidh, cidl, cidh);

        VoltTable[] results = voltExecuteSQL();

        VoltTable imported = results[0];
        VoltTable data = results[1];
        VoltTable exported = results[2];

        VoltTable haves = new VoltTable(
                new VoltTable.ColumnInfo("tab", VoltType.TINYINT),
                new VoltTable.ColumnInfo("cid", VoltType.TINYINT),
                new VoltTable.ColumnInfo("start", VoltType.BIGINT),
                new VoltTable.ColumnInfo("end", VoltType.BIGINT),
                new VoltTable.ColumnInfo("source", VoltType.TINYINT)
        );

        VoltTable missing = new VoltTable(
                new VoltTable.ColumnInfo("tab", VoltType.TINYINT),
                new VoltTable.ColumnInfo("cid", VoltType.TINYINT),
                new VoltTable.ColumnInfo("start", VoltType.BIGINT),
                new VoltTable.ColumnInfo("end", VoltType.BIGINT),
                new VoltTable.ColumnInfo("last", VoltType.BIGINT),
                new VoltTable.ColumnInfo("source", VoltType.TINYINT)
        );

        VoltTable[] output = new VoltTable[] {exported, haves, missing};

        byte cid;
        long seq = -1;
        byte[] bitmap;
        byte state = 0;
        byte lcid = (byte) 0x80; // cid's have range of 0-127
        long run[] = new long[]  {-1, -1};  /* end-run, start-run */
        long ecnt = -1;

        assert data.getRowCount() == imported.getRowCount();
        assert data.getRowCount() == exported.getRowCount();

        while (data.advanceRow()) {

            cid = (byte) data.getLong("cid");
            seq = data.getLong("seq");
            bitmap = data.getVarbinary("bitmap");

            if (cid != lcid) {
                if (lcid >= 0) {
                    if (state != (byte) 0) {
                        // in a run? end it now
                        haves.addRow(0, lcid, run[1], ((seq + 1) * 1024) - 1, (byte)0);
                        run[1] = -1;
                    } else if (run[0] <= ecnt) {
                            missing.addRow(1, lcid, run[0], ecnt, ecnt, (byte) 0);
                            run[0] = -1;
                        }
                    }
                lcid = cid;
                run[0] = -1;
                run[1] = -1;
                imported.advanceRow();
                exported.advanceRow();
                ecnt = exported.getLong("cnt");
                //System.out.println("imported cid " + imported.getLong("cid")+ " count " + imported.getLong("cnt"));
                //System.out.println("exported cid " + exported.getLong("cid")+ " count " + exported.getLong("cnt"));
                state = (byte) 0;
            }

            for (int i = 0; i < 1024 / 8; i++) {
                byte B = bitmap[i];
                for (byte j = 0; j < 8; j++) {
                    byte mask = (byte) Math.pow(2, 7-j);
                    if (((B & mask) != (state & mask))) {
                        // flip state state = 1 data present, state = 0 data missing
                        state ^= (byte) 0xFF;
                        // remember point
                        run[state&1] = (seq * 1024 + i * 8 + j);
                        //System.out.println("cid " + cid + (state == (byte)0 ? " missing: ": " present: ") + run[state&1]);
                        if (state == (byte)0 && run[0] >= 0) {
                            haves.addRow(0, cid, run[1], run[0]-1, (byte)1);
                            run[1] = -1;
                        } else if (run[0] >= 0) {
                            missing.addRow(1, cid, run[0], run[1]-1, ecnt, (byte)1);
                            run[0] = -1;
                        }
                    }
                }
            }
        }

        if (state != (byte)0) {
            // in a run? end it now
            haves.addRow(0, lcid, run[1], ((seq+1)*1024)-1, (byte)2);
        } else if (run[0] <= ecnt) {
            missing.addRow(1, lcid, run[0], ecnt, ecnt, (byte)2);
        }
        return output;
    }
}

