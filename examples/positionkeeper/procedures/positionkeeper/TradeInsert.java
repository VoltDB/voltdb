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

package positionkeeper;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.client.ClientResponse;

public class TradeInsert extends VoltProcedure {

    public final SQLStmt insertTrade = new SQLStmt(
        "INSERT INTO trd (codtrd, trd_cnt, trd_sec, trd_qty, trd_prc) VALUES (?, ?, ?, ?, ?);");

    public final SQLStmt updatePos = new SQLStmt(
        "UPDATE pos SET " +
        "  pos_cum_qty_exe = pos_cum_qty_exe + ?," +
        "  pos_cum_val_exe = pos_cum_qty_exe * pos_prc" +
        " WHERE codsec = ? AND codcnt = ?;");

    public final SQLStmt insertPos = new SQLStmt(
        "INSERT INTO pos VALUES (" +
        "?,?,?,?,?,?,?" +
        ");");

    public long run( int     codtrd,
                     int     trd_cnt,
                     int     trd_sec,
                     int     trd_qty,
                     double  trd_prc
             ) throws VoltAbortException
    {
        voltQueueSQL(insertTrade,
                     codtrd,
                     trd_cnt,
                     trd_sec,
                     trd_qty,
                     trd_prc);

        voltQueueSQL(updatePos,
                     trd_qty,
                     trd_sec,
                     trd_cnt);

        long rowsAffected = voltExecuteSQL()[1].asScalarLong();

        if (rowsAffected == 0) {
            // then insert
            voltQueueSQL(insertPos,
                         trd_cnt,
                         trd_sec,
                         0,
                         trd_qty,
                         trd_prc,
                         0,
                         trd_qty * trd_prc
                         );
            voltExecuteSQL(true);
        }

        return ClientResponse.SUCCESS;
    }
}
