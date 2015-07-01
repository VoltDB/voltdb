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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * Attempt to bid on an auction. Return a status code that describes how the attempt went.
 *
 */
@ProcInfo(
    partitionInfo = "ITEM.ITEMID: 0",
    singlePartition = true
)
public class BidOnAuction extends VoltProcedure {

    public final SQLStmt getHighBid = new SQLStmt(
        "SELECT BIDDERID, BIDPRICE, BIDTIME " +
        "FROM BID, ITEM WHERE ITEM.ITEMID = ? " +
        "AND ITEM.HIGHBIDID = BID.BIDID;"
    );

    public final SQLStmt getSellerID = new SQLStmt(
        "SELECT SELLERID " +
        "FROM ITEM " +
        "WHERE ITEM.ITEMID = ?"
    );

    public final SQLStmt getEndTime = new SQLStmt(
        "SELECT ENDTIME " +
        "FROM ITEM WHERE ITEMID = ?;"
    );

    public final SQLStmt insertBid = new SQLStmt(
        "INSERT INTO BID VALUES (?, ?, ?, ?, ?);"
    );

    public final SQLStmt insertBidForExport = new SQLStmt(
            "INSERT INTO BID_EXPORT VALUES (?, ?, ?, ?, ?);"
    );

    public final SQLStmt updateAuctionBid = new SQLStmt(
        "UPDATE ITEM SET HIGHBIDID = ? " +
        "WHERE ITEMID = ?;"
    );

    // Possible results from this stored procedure
    // Client maintains an identical list
    final long SUCCESSFULL_BID = 0;
    final long POST_CLOSE_BID = 1;
    final long USER_IS_SELLER = 2;
    final long USER_IS_REBIDDING = 3;
    final long OLD_BID = 4;
    final long LOW_BID = 5;

    /**
     * Attempt to bid on an auction. Return a status code that describes how the attempt went.
     *
     * @param itemId The id of the item.
     * @param userId The id of the bidder in the USER table.
     * @param newBidAmount The amount of the new bid in USD.
     * @return A single VoltTable with value equal to a status code. The code is based on the set
     * of final longs declared just above this method, or the final longs declared in client.java.
     * @throws VoltAbortException
     */
    public long run(int itemId, int userId, double newBidAmount, int newBidId) throws VoltAbortException {
        voltQueueSQL(getHighBid, itemId);
        VoltTable highBidResult = voltExecuteSQL()[0];

        long prevBidderID = -1;
        double prevBidPrice = -1;
        long prevBidTime = -1;

        // make sure that we get a row before reading this info
        if (highBidResult.getRowCount() == 1) {
            prevBidderID = highBidResult.fetchRow(0).getLong("BIDDERID");
            prevBidPrice = highBidResult.fetchRow(0).getDouble("BIDPRICE");
            prevBidTime = highBidResult.fetchRow(0).getTimestampAsLong("BIDTIME");
        }

        // get the current seller id
        voltQueueSQL(getSellerID, itemId);
        VoltTable getSellerResult = voltExecuteSQL()[0];
        long currentSellerID = getSellerResult.fetchRow(0).getLong("SELLERID");

        // get the end time for this auction
        voltQueueSQL(getEndTime, itemId);
        VoltTable getEndTimeResult = voltExecuteSQL()[0];
        long endTime = getEndTimeResult.fetchRow(0).getTimestampAsLong("ENDTIME");
        TimestampType currentTime = new TimestampType();

        // zero represents a successful bid
        long bidTest = SUCCESSFULL_BID;
        // we can't bid if we're the seller
        if (userId == currentSellerID)
            bidTest = USER_IS_SELLER;
        // we can't re-bid if we're the highest bidder
        else if (userId == prevBidderID)
            bidTest = USER_IS_REBIDDING;
        // we can't bid if the auction is closed
        else if (currentTime.getTime() > endTime)
            bidTest = POST_CLOSE_BID;
        // we can't bid if we under-bid
        else if (newBidAmount <= prevBidPrice)
            bidTest = LOW_BID;
        // we can't bid if our bid is older than the high bid
        else if (prevBidTime >= currentTime.getTime())
            bidTest = OLD_BID;

        // if we're all set to insert
        if (bidTest == 0) {
            voltQueueSQL(insertBid, newBidId, itemId, userId, currentTime, newBidAmount);
            voltQueueSQL(insertBidForExport, newBidId, itemId, userId, currentTime, newBidAmount);
            voltQueueSQL(updateAuctionBid, newBidId, itemId);
            final VoltTable[] results = voltExecuteSQL();
            // ensure we successfully inserted the row
            if (results[0].asScalarLong() != 1) throw new VoltAbortException("Failed to insert Bid.");
            if (results[1].asScalarLong() != 1) throw new VoltAbortException("Failed to update Auction.");
        }

        return bidTest;
    }

}
