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
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

public class OrderInsert extends VoltProcedure {

    public final SQLStmt insertOrder = new SQLStmt(
        "INSERT INTO ord (codord, ord_cnt, ord_sec, ord_qty, ord_prc) VALUES (?, ?, ?, ?, ?);");

    public final SQLStmt updatePos = new SQLStmt(
        "UPDATE pos SET " +
        "  pos_cum_qty_ord = pos_cum_qty_ord + ?," +
        "  pos_cum_val_ord = pos_cum_qty_ord * pos_prc" +
        " WHERE codsec = ? AND codcnt = ?;");

    public final SQLStmt insertPos = new SQLStmt(
        "INSERT INTO pos VALUES (" +
        "?,?,?,?,?,?,?" +
        ");");

    public long run( int     codord,
                     int     ord_cnt,
                     int     ord_sec,
                     int     ord_qty,
                     double  ord_prc) throws VoltAbortException
   {
        voltQueueSQL(insertOrder,
                     codord,
                     ord_cnt,
                     ord_sec,
                     ord_qty,
                     ord_prc);

        voltQueueSQL(updatePos,
                     ord_qty,
                     ord_sec,
                     ord_cnt);

        VoltTable results1[] = voltExecuteSQL();

        long rowsAffected = results1[1].asScalarLong();

        if (rowsAffected == 0) {
            // then insert
            voltQueueSQL(insertPos,
                         ord_cnt,
                         ord_sec,
                         ord_qty,
                         0,
                         ord_prc,
                         ord_qty * ord_prc,
                         0);
            voltExecuteSQL();
        }

        return ClientResponse.SUCCESS;
    }
}
