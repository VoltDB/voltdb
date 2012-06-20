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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import org.voltdb.utils.CSVLoader;

import au.com.bytecode.opencsv_voltpatches.CSVReader;


/**
 * Loader class contains a set of static methods that
 * load data from CSV files into the server database. It
 * is specific to the Auction example for VoltDB.
 *
 */
class Loader {
    /**
     * Insert records into the ITEM table from a csv file.
     *
     * @param client Connection to the database.
     * @return An ArrayList of ids of newly inserted objects.
     * @throws FileNotFoundException 
     * @throws Exception Rethrows any exceptions thrown from within.
       */
    static ArrayList<Integer> loadItems() throws FileNotFoundException{
    	ArrayList<Integer> itemIds = new ArrayList<Integer>();
    	URL url = Loader.class.getResource("datafiles/items.txt");
    	String []myOptions = {
         		"--file="+ url.getPath(), 
         		"--procedure=InsertIntoItemAndBid",
         		"--maxerrors=50",
         		"--user=program",
         		"--password=pass",
         		"--port="
         		};
    	try {
			CSVLoader.main( myOptions );
		} catch (Exception e) {
			e.printStackTrace();
		}
    	CSVReader csvReader = new CSVReader(new FileReader( url.getPath() ));
    	String[] line = null;
    	try {
            while( (line = csvReader.readNext()) != null )
                  itemIds.add( Integer.parseInt( line[0] ));
        } catch (Exception e) {
            e.printStackTrace();
        }
    	return itemIds;
    }
    
    /**
     * Insert records into the CATEGORY table from a csv file.
     *
     * @param client Connection to the database.
     * @return An ArrayList of ids of newly inserted objects.
     * @throws FileNotFoundException 
     * @throws Exception Rethrows any exceptions thrown from within.
     */
    
    static ArrayList<Integer> loadCategories() throws FileNotFoundException {
    	ArrayList<Integer> categoryIds = new ArrayList<Integer>();
    	URL url = Loader.class.getResource("datafiles/categories.txt");
    	String []myOptions = {
         		"--file="+ url.getPath(), 
         		"--procedure=InsertIntoCategory",
         		"--maxerrors=50",
         		"--user=program",
         		"--password=pass",
         		"--port="
         		};
    	try {
			CSVLoader.main( myOptions );
		} catch (Exception e) {
			e.printStackTrace();
		}
    	CSVReader csvReader = new CSVReader(new FileReader( url.getPath() ));
        String[] line = null;
        try {
            while( (line = csvReader.readNext()) != null )
                categoryIds.add( Integer.parseInt( line[0] ));
        } catch (Exception e) {
            e.printStackTrace();
        }
    	return categoryIds;
    }
  
    /**
     * Insert records into the USER table from a csv file.
     *
     * @param client Connection to the database.
     * @return An ArrayList of ids of newly inserted objects.
     * @throws FileNotFoundException 
     * @throws Exception Rethrows any exceptions thrown from within.
     * 
     * 
     */
    
    static ArrayList<Integer> loadUsers() throws FileNotFoundException {
    	ArrayList<Integer> userIds = new ArrayList<Integer>();
    	URL url = Loader.class.getResource("datafiles/users.txt");
    	String []myOptions = {
         		"--file="+ url.getPath(), 
         		"--procedure=InsertIntoUser",
         		"--maxerrors=50",
         		"--user=program",
         		"--password=pass",
         		"--port="
         		};
    	try {
			CSVLoader.main( myOptions );
		}catch (Exception e) {
            e.printStackTrace();
        }
    	CSVReader csvReader = new CSVReader(new FileReader( url.getPath() ));
        String[] line = null;
        try {
            while( (line = csvReader.readNext()) != null )
                userIds.add( Integer.parseInt( line[0] ));
        } catch (Exception e) {
            e.printStackTrace();
        }
    	return userIds;
    }
}