/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

/**
 * Loader class contains a set of static methods that
 * load data from CSV files into the server database. It
 * is specific to the Auction example for VoltDB.
 *
 */
class Loader {

    // random number generator
    static Random random = new Random();

    /**
     * Given a path to a CSV, get a CSVReader instance for that file.
     *
     * @param path Path to the CSV file requested, relative to current package.
     * @return A CSVReader instance loaded with the CSV file specified.
     */
    static CSVReader getReaderForPath(String path) {
        URL url = Loader.class.getResource(path);
        FileReader fr = null;
        try {
            fr = new FileReader(url.getPath());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        CSVReader reader = new CSVReader(fr);
        return reader;
    }

    /**
     * Insert records into the ITEM table from a csv file.
     *
     * @param client Connection to the database.
     * @return An ArrayList of ids of newly inserted objects.
     * @throws Exception Rethrows any exceptions thrown from within.
     */
    static ArrayList<Integer> loadItems(org.voltdb.client.Client client) throws Exception {
        System.out.printf("Loading ITEM Table\n");
        ArrayList<Integer> itemIds = new ArrayList<Integer>();
        CSVReader reader = getReaderForPath("datafiles/items.txt");

        String[] nextLine = null;
        while ((nextLine = reader.readNext()) != null) {
            // check this tuple is the right size
            if (nextLine.length <= 1)
                continue;
            if (nextLine.length != 6)
                throw new Exception("Malformed CSV Line for ITEM table.");

            // get the values of the tuple to insert
            int itemId = Integer.valueOf(nextLine[0]);
            String itemName = nextLine[1];
            String itemDescription = nextLine[2];
            int sellerId = Integer.valueOf(nextLine[3]);
            int categoryId = Integer.valueOf(nextLine[4]);
            double startPrice = Double.valueOf(nextLine[5]);

            // figure out auction start and end times
            final int oneMinuteOfMillis = 1000 * 1000 * 60;
            TimestampType startTime = new TimestampType();
            int duration = oneMinuteOfMillis + random.nextInt(oneMinuteOfMillis);
            TimestampType endTime = new TimestampType(startTime.getTime() + duration);

            VoltTable[] table = client.callProcedure("InsertIntoItem", itemId, itemName, itemDescription,
                    sellerId, categoryId, itemId, startPrice, startTime, endTime).getResults();
            if (table.length != 2)
                throw new Exception("InsertIntoItem returned the wrong number of tables.");
            if (table[0].asScalarLong() != 1)
                throw new Exception("InsertIntoItem modified the wrong number of tuples.");

            System.out.printf("  inserted (itemId %d, itemName \"%s\", itemDescription \"%s\", sellerId %d, categoryId %d, highBidId %d, startPrice %.2f, startTime %tT, endTime %tT,  )\n",
                    itemId, itemName, itemDescription, sellerId, categoryId, itemId, startPrice, startTime.asApproximateJavaDate(), endTime.asApproximateJavaDate());

            // insert a user-less bid into the bid table with price = auction.startprice
            table = client.callProcedure("InsertIntoBid", itemId, itemId, -1, 0L, startPrice).getResults();
            if (table.length != 2)
                throw new Exception("InsertIntoBid returned the wrong number of tables.");
            if (table[0].asScalarLong() != 1)
                throw new Exception("InsertIntoBid modified the wrong number of tuples.");

            itemIds.add(itemId);
        }

        System.out.printf("ITEM Table Loaded\n\n");
        return itemIds;
    }

    /**
     * Insert records into the CATEGORY table from a csv file.
     *
     * @param client Connection to the database.
     * @return An ArrayList of ids of newly inserted objects.
     * @throws Exception Rethrows any exceptions thrown from within.
     */
    static ArrayList<Byte> loadCategories(org.voltdb.client.Client client) throws Exception {
        System.out.printf("Loading CATEGORY Table\n");
        ArrayList<Byte> categoryIds = new ArrayList<Byte>();
        CSVReader reader = getReaderForPath("datafiles/categories.txt");

        String[] nextLine = null;
        while ((nextLine = reader.readNext()) != null) {
            // check this tuple is the right size
            if (nextLine.length <= 1)
                continue;
            if (nextLine.length != 3)
                throw new Exception("Malformed CSV Line for CATEGORY table.");

            // get the values of the tuple to insert
            Byte catId = Byte.valueOf(nextLine[0]);
            String catName = nextLine[1];
            String catDescription = nextLine[2];

            VoltTable[] table = client.callProcedure("InsertIntoCategory", catId, catName, catDescription).getResults();
            if (table.length != 1)
                throw new Exception("InsertIntoCategory returned the wrong number of tables.");
            if (table[0].asScalarLong() != 1)
                throw new Exception("InsertIntoCategory modified the wrong number of tuples.");

            System.out.printf("  inserted (%d, \"%s\", \"%s\")\n",
                    catId, catName, catDescription);

            categoryIds.add(catId);
        }

        System.out.printf("CATEGORY Table Loaded\n\n");
        return categoryIds;
    }

    /**
     * Insert records into the USER table from a csv file.
     *
     * @param client Connection to the database.
     * @return An ArrayList of ids of newly inserted objects.
     * @throws Exception Rethrows any exceptions thrown from within.
     */
    static ArrayList<Integer> loadUsers(org.voltdb.client.Client client) throws Exception {
        System.out.printf("Loading USER Table\n");
        ArrayList<Integer> userIds = new ArrayList<Integer>();
        CSVReader reader = getReaderForPath("datafiles/users.txt");

        String[] nextLine = null;
        while ((nextLine = reader.readNext()) != null) {
            // check this tuple is the right size
            if (nextLine.length <= 1)
                continue;
            if (nextLine.length != 8)
                throw new Exception("Malformed CSV Line for USER table.");

            // get the values of the tuple to insert
            int userId = Integer.valueOf(nextLine[0]);
            String lastName = nextLine[1];
            String firstName = nextLine[2];
            String streetAddress = nextLine[3];
            String city = nextLine[4];
            String state = nextLine[5];
            String zip = nextLine[6];
            String email = nextLine[7];

            VoltTable[] table = client.callProcedure("InsertIntoUser", userId, lastName, firstName,
                    streetAddress, city, state, zip, email).getResults();
            if (table.length != 1)
                throw new Exception("InsertIntoUser returned the wrong number of tables.");
            if (table[0].asScalarLong() != 1)
                throw new Exception("InsertIntoUser modified the wrong number of tuples.");

            System.out.printf("  inserted (%d, \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\")\n",
                    userId, lastName, firstName, streetAddress, city, state, zip, email);

            userIds.add(userId);
        }

        System.out.printf("USER Table Loaded\n\n");
        return userIds;
    }
}
