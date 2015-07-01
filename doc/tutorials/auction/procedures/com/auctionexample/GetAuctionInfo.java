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
package com.auctionexample;

import org.voltdb.*;

/**
 * Get the high bidder id and price for a given auction.
 *
 */
@ProcInfo(
    partitionInfo = "ITEM.ITEMID: 0",
    singlePartition = true
)
public class GetAuctionInfo extends VoltProcedure {

    public final SQLStmt getHighBid = new SQLStmt(
        "SELECT BIDDERID, BIDPRICE " +
        "FROM BID, ITEM WHERE ITEM.ITEMID = ? " +
        "AND ITEM.HIGHBIDID = BID.BIDID;"
    );

    /**
     * Get the high bidder id and price for a given auction.
     *
     * @param itemId The id of the item we're querying about.
     * @return An VoltTable with one row having fields BIDDERID and BIDPRICE,
     * @throws VoltAbortException
     */
    public VoltTable[] run(int itemId) throws VoltAbortException {
        voltQueueSQL(getHighBid, itemId);
        return voltExecuteSQL();
    }

}


