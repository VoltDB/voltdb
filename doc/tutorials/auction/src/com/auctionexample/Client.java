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

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

/**
 * The Client class is the main driver for the Auction example.
 * See readme.txt for a description of it's behavior.
 *
 */
public class Client {

    // Possible results from the BidOnAuction stored procedure
    // BidOnAuction procedure maintains an identical list
    static final long SUCCESSFULL_BID = 0;
    static final long POST_CLOSE_BID = 1;
    static final long USER_IS_SELLER = 2;
    static final long USER_IS_REBIDDING = 3;
    static final long OLD_BID = 4;
    static final long LOW_BID = 5;

    // never sleep for more than 1/20th of a second
    static final int maxSleepMillis = 50;
    // bid increase std-dev is one dollar
    static final double bidIncreaseFactorStdDev = 1.0;
    // print status update every 10 seconds
    static final int statusPeriodInSeconds = 10;

    // list of all currently running auction ids
    static ArrayList<Integer> activeAuctionIds = new ArrayList<Integer>();
    // list of all auction ids
    static ArrayList<Integer> allAuctionIds;
    // list of all user ids
    static ArrayList<Integer> userIds;

    // random number generator
    static Random random = new Random();

    // This is hackish, but we insert bids with ids generated on the client side, due
    // to a missing "auto-increment" feature. It's actually a tough feature to implement
    // given our design, but we think we can add an "auto-unique" without much trouble.
    static int nextBidId = -1;

    /**
     * Get a random new bid amound based on an old bid amount.
     *
     * @param currentPrice The current bid price in dollars.
     * @return A randomly generated value greater than the old given bid price.
     */
    static double getRandomNewBidAmount(double currentPrice) {
        // determine how much to add to the bid
        double increaseFactor = Math.abs(random.nextGaussian()) * bidIncreaseFactorStdDev;
        // add to the bid
        Double newBid = (currentPrice + increaseFactor) * 100;

        // the last three lines just round to the nearest penny (lame)
        long money = newBid.longValue();
        newBid = (money) / 100.0;
        return newBid.doubleValue();
    }

    /**
     * Attempt to make a single bid on an auction.
     *
     * @param auctionId The id of the auction to bid on.
     * @param userId The id of the user making the bid.
     * @return A status code which can be found at the top of this class.
     */
    static int doBid(org.voltdb.client.Client client, int auctionId, int userId) {
        ///////////////////////////////////////
        // get the current bid price of an item
        ///////////////////////////////////////
        VoltTable infoResult = null;
        try {
            VoltTable[] infoResultSet = client.callProcedure("GetAuctionInfo", auctionId).getResults();
            if (infoResultSet.length != 1) throw new Exception("GetAuctionInfo returned no results");
            infoResult = infoResultSet[0];
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        double oldBidAmount = infoResult.fetchRow(0).getDouble("BIDPRICE");

        ////////////////////////////////////////
        // make a new bid on the exact same item
        ////////////////////////////////////////
        double newBidAmount = getRandomNewBidAmount(oldBidAmount);

        VoltTable bidResult = null;
        try {
            VoltTable[] bidResultSet = client.callProcedure("BidOnAuction", auctionId, userId, newBidAmount, nextBidId++).getResults();
            if (bidResultSet.length != 1) throw new Exception("BidOnAuction returned no results");
            bidResult = bidResultSet[0];
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // return the result of the bid procedure
        // we're primarily interested in POST_CLOSE_BID
        return (int)bidResult.asScalarLong();
    }

    /**
     * While there are still active auctions, repeatedly choose a random
     * user and an random auction and make a bid.
     */
    static void runBidLoop(org.voltdb.client.Client client) {
        while (activeAuctionIds.size() > 0) {

            // sleep for a random amount of time between each bid
            try { Thread.sleep(random.nextInt(maxSleepMillis)); } catch (Exception e) {}

            // get a random value from the active auctions and users lists
            int itemId = activeAuctionIds.get(random.nextInt(activeAuctionIds.size()));
            int userId = userIds.get(random.nextInt(userIds.size()));
            // make a bid
            long status = doBid(client, itemId, userId);

            // end auction if required (remove it from active list)
            if (status == POST_CLOSE_BID)
                for (int i = 0; i < activeAuctionIds.size(); i++)
                    if (activeAuctionIds.get(i) == itemId)
                        activeAuctionIds.remove(i);
        }
    }

    /**
     * Loop until all auctions have closed, printing status for each auction at
     * specific intervals.
     */
    static void runStatusLoop() {
        // create a second client handle for this second thread
        org.voltdb.client.Client client = null;
        try {
            ClientConfig config = new ClientConfig("program", "pass");
            client = ClientFactory.createClient(config);
            client.createConnection("localhost");
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        boolean auctionsOver = false;
        while (auctionsOver == false) {
            try {
                // assume auctions have ended. if any open auctions are found, then set this false
                auctionsOver = true;

                // print out the status header
                System.out.printf("\nAUCTION STATUS AS OF: %tT\n", new Date());
                System.out.printf("+-----------------------------------------------------" +
                        "-----------------------------------------------+\n");
                System.out.printf("| %-20s | %-16s | %-16s | %5s | %8s | %9s | %-6s |\n",
                        "ITEM", "BIDDER", "SELLER", "BIDS", "PRICE", "END", "STATUS");
                System.out.printf("+-----------------------------------------------------" +
                "-----------------------------------------------+\n");

                // loop over all auction ids, printing a row of status for each one
                for (int auctionId : allAuctionIds) {
                    VoltTable[] statusResultSet = client.callProcedure("AuctionStatus", auctionId).getResults();
                    if (statusResultSet.length != 1) throw new Exception("AuctionStatus returned no results");
                    VoltTable statusTable = statusResultSet[0];
                    VoltTableRow row = statusTable.fetchRow(0);
                    System.out.printf("| %-20s | %-16s | %-16s | %5d | %8.2f | %9tT | %-6s |\n",
                            row.getString("item"), row.getString("bidder"), row.getString("seller"),
                            row.getLong("bidcount"), row.getDouble("price"),
                            row.getTimestampAsTimestamp("endtime").asApproximateJavaDate(),
                            row.getString("status"));
                    if (row.getString("status").equals("OPEN"))
                        auctionsOver = false;
                }

                // print the status footer
                System.out.printf("+-----------------------------------------------------" +
                "-----------------------------------------------+\n");

                // wait for next status update
                Thread.sleep(1000 * statusPeriodInSeconds);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Load the initial data, then run the simulation.
     *
     * @param args This program uses no command line arguments.
     */
    public static void main(String[] args) {
        System.out.println("***************************************");
        System.out.println("* Welcome to Bobbi's Awesome Auctions *");
        System.out.println("*                                     *");
        System.out.println("* Connecting to Server...             *");

        // connect to VoltDB server
        org.voltdb.client.Client client = null;
        ClientConfig config = new ClientConfig("program", "pass");
        client = ClientFactory.createClient(config);
        int sleep = 1000;
        while(true)
        {
            try
            {
                client.createConnection("localhost");
                break;
            } catch (Exception e) {
                System.out.println("Connection failed - retrying in " + (sleep/1000) + " second(s).");
                try {Thread.sleep(sleep);} catch(Exception tie){}
                if (sleep < 8000)
                    sleep += sleep;
            }
        }

        System.out.println("* Connected                           *");
        System.out.println("* Running loader                      *");
        System.out.println("***************************************\n");

        // get itemId and userId from database
        try {
            VoltTable modCount = client.callProcedure("@AdHoc", "SELECT ITEMID FROM ITEM").getResults()[0];
            allAuctionIds = new ArrayList<Integer>();
            userIds = new ArrayList<Integer>();
            while( modCount.advanceRow() ){
                Integer i = (Integer) modCount.get(0, VoltType.INTEGER);
                allAuctionIds.add(i);
            }
            modCount = client.callProcedure("@AdHoc", "SELECT USERID FROM USER").getResults()[0];
            while( modCount.advanceRow() ){
                Integer i = (Integer) modCount.get(0, VoltType.INTEGER);
                userIds.add(i);
            }
            activeAuctionIds.addAll(allAuctionIds);    
        } catch (Exception e) {
            e.printStackTrace();
        }

        // since we create 1 bid per auction
        nextBidId = allAuctionIds.size() + 1;

        System.out.println("***************************************");
        System.out.println("* Finished running loader             *");
        System.out.println("* Running auctions                    *");
        System.out.println("***************************************");

        // create and start a thread that prints auction status every
        // 10 seconds, ending when all auctions have closed
        Thread statusThread = new Thread(new Runnable() {
            public void run() { runStatusLoop(); }
        });
        statusThread.start();

        // runloop executes bids until all auctions have ended
        runBidLoop(client);

        // wait for the status-printing thread to finish
        try { statusThread.join(); } catch (Exception e) {}

        // print out a joke with dramatic pauses
        System.out.println("\n***************************************");
        System.out.println("* Complete...                         *");
        System.out.println("*                                     *");
        try { Thread.sleep(1000); } catch (InterruptedException e) {}
        System.out.println("* Where do ghosts shop?               *");
        System.out.println("*                                     *");
        try { Thread.sleep(3000); } catch (InterruptedException e) {}
        System.out.println("* In Boo-tiques!                      *");
        System.out.println("***************************************");
        try {
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
