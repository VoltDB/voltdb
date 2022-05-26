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
import org.voltdb.VoltTable;


public class ImportInsertP extends ImportBaseProc {

    public final SQLStmt select_p = new SQLStmt("SELECT ts,cid,cnt FROM importp WHERE cid=? ORDER BY 1,2,3");
    public final SQLStmt insert_p = new SQLStmt("INSERT INTO importp (ts,cid,cnt,rc) values (?,?,?,?)");
    public final SQLStmt update_p = new SQLStmt("UPDATE importp set ts=?,cnt=?,rc=rc+1 where cid=?");

    public final SQLStmt select_bitmap_p = new SQLStmt("SELECT bitmap FROM importbp WHERE cid=? and seq=?");
    //public final SQLStmt upsert_bitmap_p = new SQLStmt("UPSERT INTO importbp (cid,seq,bitmap) values (?,?,?)");
    public final SQLStmt insert_bitmap_p = new SQLStmt("INSERT INTO importbp (cid,seq,bitmap) values (?,?,?)");
    public final SQLStmt update_bitmap_p = new SQLStmt("UPDATE importbp set bitmap=? where cid=? and seq=?");

    public VoltTable[] run(long txnid
                        ,long prevtxnid
                        ,long ts
                        ,byte cid
                        ,long cidallhash
                        ,long rid
                        ,long cnt
                        ,long adhocinc
                        ,long adhocjmp
                        ,byte[] value) {

        return doWork(select_p, update_p, insert_p, select_bitmap_p, update_bitmap_p, insert_bitmap_p, cid, ts, cnt);
    }
}

