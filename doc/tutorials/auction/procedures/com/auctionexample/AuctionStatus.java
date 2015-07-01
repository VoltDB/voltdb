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
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

/**
 * Get a tuple of useful status info about an auction.
 *
 */
@ProcInfo(
    partitionInfo = "ITEM.ITEMID: 0",
    singlePartition = true
)
public class AuctionStatus extends VoltProcedure {

    public final SQLStmt getBidInfo = new SQLStmt(
        "SELECT BIDDERID, BIDPRICE " +
        "FROM BID " +
        "WHERE BID.BIDID = ? "
    );

    public final SQLStmt getBidCount = new SQLStmt(
        "SELECT COUNT(DISTINCT(BIDID)) " +
        "FROM BID " +
        "WHERE ITEMID = ?;"
    );

    public final SQLStmt getUserName = new SQLStmt(
        "SELECT FIRSTNAME, LASTNAME " +
        "FROM USER " +
        "WHERE USERID = ?;"
    );

    public final SQLStmt getItemInfo = new SQLStmt(
        "SELECT ITEMNAME, SELLERID, HIGHBIDID, ENDTIME " +
        "FROM ITEM " +
        "WHERE ITEMID = ?;"
    );

    /**
     * Get a tuple of useful status info about an auction.
     *
     * @param itemId The id of the item we'd like to know about.
     * @return A table with one row and fields:
     *  (item, bidder, seller, bidcount, price, endtime, status)
     * @throws VoltAbortException Currently doesn't abort.
     */
    public VoltTable[] run(int itemId) throws VoltAbortException {
        // create a new VoltTable to store our results
        VoltTable retval = new VoltTable(
                new VoltTable.ColumnInfo("item", VoltType.STRING),
                new VoltTable.ColumnInfo("bidder", VoltType.STRING),
                new VoltTable.ColumnInfo("seller", VoltType.STRING),
                new VoltTable.ColumnInfo("bidcount", VoltType.BIGINT),
                new VoltTable.ColumnInfo("price", VoltType.FLOAT),
                new VoltTable.ColumnInfo("endtime", VoltType.TIMESTAMP),
                new VoltTable.ColumnInfo("status", VoltType.STRING)
        );

        // get the seller id and item name from ITEM table
        voltQueueSQL(getItemInfo, itemId);
        VoltTable itemTable = voltExecuteSQL()[0];
        VoltTableRow itemRow = itemTable.fetchRow(0);
        // resulting info:
        long sellerId = itemRow.getLong("SELLERID");
        String itemName = itemRow.getString("ITEMNAME");
        long endTime = itemRow.getTimestampAsLong("ENDTIME");
        
        long highBidId = itemRow.getLong("HIGHBIDID");

        // get high bid info
        voltQueueSQL(getBidInfo, highBidId);
        VoltTable statusTable = voltExecuteSQL()[0];
        VoltTableRow row = statusTable.fetchRow(0);
        // resulting info:
        long bidderId = row.getLong("BIDDERID");
        double bidPrice = row.getDouble("BIDPRICE");

        // count the number of bids on the auction
        voltQueueSQL(getBidCount, itemId);
        VoltTable bidCountTable = voltExecuteSQL()[0];
        VoltTableRow bidCountRow = bidCountTable.fetchRow(0);
        // resulting info:
        // the minus one is for the fake initial bid
        long bidCount = bidCountRow.getLong(0) - 1;

        // get the names of the bidder and seller
        voltQueueSQL(getUserName, sellerId);
        if (bidderId >= 0) {
            voltQueueSQL(getUserName, bidderId);
        }
        VoltTable[] nameTables = voltExecuteSQL();
        VoltTableRow sellerNameRow = nameTables[0].fetchRow(0);
        // we should always have a seller name
        String sellerName = sellerNameRow.getString("FIRSTNAME") + " " + sellerNameRow.getString("LASTNAME");
        // we might not always have a bidder name, so need this if statement
        String bidderName = "NO BIDDER";
        if (bidderId >= 0) {
            VoltTableRow bidderNameRow = nameTables[1].fetchRow(0);
            bidderName = bidderNameRow.getString("FIRSTNAME") + " " + bidderNameRow.getString("LASTNAME");
        }

        // check the timing and set the auction status accordingly
        String status = "OPEN";
        long now = new TimestampType().getTime();
        
        if (endTime < now)
            status = "CLOSED";

        // add the single tuple to our return table
        retval.addRow(itemName, bidderName, sellerName, bidCount, bidPrice, endTime, status);

        return new VoltTable[] { retval };
    }
}
