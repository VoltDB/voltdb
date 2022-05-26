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

package nbbo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class ProcessTick extends VoltProcedure {

    // base table
    public final SQLStmt insertTick = new SQLStmt(
            "INSERT INTO ticks VALUES (" +
            "?,?,?,?,?,?,?,?" + // 8
            ");");

    public final SQLStmt upsertLastTick = new SQLStmt(
            "UPSERT INTO last_ticks VALUES (" +
            "?,?,?,?,?,?,?,?" + // 8
            ");");

    public final SQLStmt selectMaxBid = new SQLStmt(
            "SELECT exch,bid,bid_size " +
            "FROM last_ticks " +
            "WHERE symbol = ? " +
            "ORDER BY bid DESC, seq, exch ASC, symbol ASC LIMIT 1;");

    public final SQLStmt selectMinAsk = new SQLStmt(
            "SELECT exch,ask,ask_size " +
            "FROM last_ticks " +
            "WHERE symbol = ? AND ask > 0 "+
            "ORDER BY ask ASC, seq ASC, exch ASC, symbol ASC LIMIT 1;");

    // NBBO output table
    public final SQLStmt insertNBBO = new SQLStmt(
            "INSERT INTO nbbos VALUES (" +
            "?,?,?,?,?,?,?,?,?" + // 9
            ");");

    // "main method" the procedure starts here.
    public long run(
            String symbol,
            TimestampType time,
            long seq_number,
            String exchange,
            int bidPrice,
            int bidSize,
            int askPrice,
            int askSize) throws VoltAbortException
    {
        // convert bid and ask 0 values to null
        Integer bidPriceSafe = askPrice > 0 ? askPrice : null;
        Integer askPriceSafe = askPrice > 0 ? askPrice : null;

        voltQueueSQL(insertTick,
                 symbol,
                 time,
                 seq_number,
                 exchange,
                 bidPriceSafe,
                 bidSize,
                 askPriceSafe,
                 askSize);

        voltQueueSQL(upsertLastTick,
                     symbol,
                     time,
                     seq_number,
                     exchange,
                     bidPrice,
                     bidSize,
                     askPrice,
                     askSize);

        // Queue best bid and ask selects
        voltQueueSQL(selectMaxBid, symbol);
        voltQueueSQL(selectMinAsk, symbol);

        // Execute queued statements
        VoltTable results0[] = voltExecuteSQL();

        // Read the best bid results
        VoltTable tb = results0[2];
        tb.advanceRow();
        String bex = tb.getString(0);
        long bid = tb.getLong(1);
        long bsize = tb.getLong(2);

        // Read the best ask results
        VoltTable ta = results0[3];
        ta.advanceRow();
        String aex = ta.getString(0);
        long ask = ta.getLong(1);
        long asize = ta.getLong(2);

        // check if the tick is part of the nbbo
        if (bex.equals(exchange) || aex.equals(exchange)) {
            // this new quote was the best bid or ask
            //  insert a new NBBO record
            //  use this quote's symbol, time and sequence number
            voltQueueSQL(insertNBBO,
                 symbol,
                 time,
                 seq_number,
                 bid,
                 bsize,
                 bex,
                 ask,
                 asize,
                 aex);

            voltExecuteSQL(true);
        }

        return ClientResponse.SUCCESS;
    }
}
