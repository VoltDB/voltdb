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

import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class PriceInsert extends VoltProcedure {

    public final SQLStmt insertPrice = new SQLStmt(
        "INSERT INTO prc (codprc, prc_sec, prc_price, prc_ts) VALUES (?, ?, ?, ?);");

    public final SQLStmt updatePos = new SQLStmt(
        "UPDATE pos SET " +
        "  pos_prc = ?," +
        "  pos_cum_val_ord = pos_cum_qty_ord * ?," +
        "  pos_cum_val_exe = pos_cum_qty_exe * ?" +
        " WHERE codsec = ?;");

    public VoltTable[] run( int     codprc,
                            int     prc_sec,
                            double  prc_price,
                            Date    prc_ts) throws VoltAbortException
    {
        voltQueueSQL(insertPrice,
                     codprc,
                     prc_sec,
                     prc_price,
                     prc_ts);

        voltQueueSQL(updatePos,
                     prc_price,
                     prc_price,
                     prc_price,
                     prc_sec);

        return voltExecuteSQL(true);
    }
}
