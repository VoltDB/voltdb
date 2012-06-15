/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCSVLoader extends TestCase {

	private String pathToCatalog;
    private String pathToDeployment;
    private ServerThread localServer;
    private VoltDB.Configuration config;
    private VoltProjectBuilder builder;
    private Client client;

    private final String userHome = System.getProperty("user.home");
    String path_csv = userHome + "/" + "test.csv";

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();


    }

    public void testCommon() throws Exception
	{
     String mySchema =
                "create table BLAH (" +
                "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_tinyint tinyint default 0, " +
                "clm_smallint smallint default 0, " +
                "clm_bigint bigint default 0, " +

                "clm_string varchar(20) default null, " +
                "clm_decimal decimal default null, " +
                "clm_float float default null "+ // for later
                //"clm_timestamp timestamp default null, " + // for later
                //"clm_varinary varbinary default null" + // for later
                "); ";
     String []myOptions = {
     		"-f" + userHome + "/test.csv",
     		//"--procedure=BLAH.insert",
     		//"--reportdir=" + reportdir,
     		//"--table=BLAH",
     		"--maxerrors=50",
     		//"-user",
     		"--user=",
     		"--password=",
     		"--port=",
     		"--separator=,",
     		"--quotechar=\"",
     		"--escape=\\",
     		"--skip=0",
     		"--nowhitespace",
     		//"--strictquotes",
     		"BLAH"
     		};

	    String []myData = { "1,1,1,11111111,first,1.10,1.11",
	    					"2,2,2,222222,second,3.30,NULL",
	    					"3,3,3,333333, third ,NULL, 3.33",
	    					"4,4,4,444444, NULL ,4.40 ,4.44",
	    					"5,5,5,5555555, fifth, 5.50, 5.55",
	    	 			    "6,6,NULL,666666, sixth, 6.60, 6.66",
	    					"7,NULL,7,7777777, seventh, 7.70, 7.77 ",
	    					"11, 1,1,\"1,000\",first,1.10,1.11",
	    					//invalid lines below
	    					"8, 8",
	    					"",
	    					"10,10,10,10 101 010,second,2.20,2.22",
	    					"12,n ull,12,12121212,twelveth,12.12,12.12"
	    					};
	    int invalidLineCnt = 4;
		test_Interface( mySchema, myOptions, myData, invalidLineCnt );
	}

    public void testNULL() throws Exception
    {
     String mySchema =
                "create table BLAH (" +
                "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_tinyint tinyint default 0, " +
                "clm_smallint smallint default 0, " +
                "clm_bigint bigint default 0, " +

                "clm_string varchar(20) default null, " +
                "clm_decimal decimal default null, " +
                "clm_float float default null "+ // for later
                //"clm_timestamp timestamp default null, " + // for later
                //"clm_varinary varbinary default null" + // for later
                "); ";
     String []myOptions = {
            "-f" + userHome + "/test.csv",
            //"--procedure=BLAH.insert",
            //"--reportdir=" + reportdir,
            //"--table=BLAH",
            "--maxerrors=50",
            //"-user",
            "--user=",
            "--password=",
            "--port=",
            "--separator=,",
            "--quotechar=\"",
            "--escape=\\",
            "--skip=0",
            "--nowhitespace",
            //"--strictquotes",
            "BLAH"
            };

     String []myData = {
             "1,\\N,1,11111111,\"\"NULL\"\",1.10,1.11",
             "2,\"\\N\",1,11111111,\"NULL\",1.10,1.11",
             "3,\\N,1,11111111,  \\N  ,1.10,1.11",
             "4,\\N,1,11111111,  \"\\N\"  ,1.10,1.11",
             "5,\\N,1,11111111, \"  \\N  \",1.10,1.11",
             "6,\\N,1,11111111, \"  \\N L \",1.10,1.11",
             "7,\\N,1,11111111,  \"abc\\N\"  ,1.10,1.11"
             };
        int invalidLineCnt = 0;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt );
    }

    /*
    public void testNew() throws Exception
   	{
        String mySchema =
                   "create table BLAH (" +
                   "clm_integer integer default 0 not null, " + // column that is partitioned on

//                   "clm_tinyint tinyint default 0, " +
//                   "clm_smallint smallint default 0, " +
//                   "clm_bigint bigint default 0, " +
//
//                   "clm_string varchar(10) default null, " +
//                   "clm_decimal decimal default null, " +
//                   "clm_float float default null "+ // for later
                   "clm_timestamp timestamp default null, " + // for later
                   //"clm_varinary varbinary default null" + // for later
                   "); ";
        String []myOptions = {
         		"--file=" + userHome + "/test.csv",
         		//"--procedure=BLAH.insert",
         		"--reportdir=" + reportdir,
         		"--table=BLAH",
         		"--maxerrors=50",
         		"--user=",
         		"--password=",
         		"--port="
         		};

   	    String []myData = { "1",
   	    					"2"
   	    					};
   	    //CSVLoader.setDefaultTimezone();
   	    //String [] addStr = { String.valueOf( (new TimestampType()).getTime() ) };
   	    CSVLoader.setTimezone("GMT+0");
   	    String [] addStr = { (new TimestampType()).toString() };
   	    int invalidLineCnt = 0;
   		//test_Interface( mySchema, myOptions, myData, invalidLineCnt );
   		test_Interface_lineByLine( mySchema, 2, myOptions, myData, invalidLineCnt, addStr );
   	}


    //csvloader --inputfile=/tmp/ --tablename=VOTES --abortfailurecount=50
//    public void testOptions() throws Exception
//	{
//     String mySchema =
//                "create table BLAH (" +
//                "clm_integer integer default 0 not null, " + // column that is partitioned on
//
//                "clm_tinyint tinyint default 0, " +
//                "); ";
//     String []myOptions = {
//     		"--inputfile=" + userHome + "/test.csv",
//     		//"--procedurename=BLAH.insert",
//     		"--reportdir=" + reportdir,
//     		"--tablename=BLAH",
//     		"--abortfailurecount=50",
//     		//"--separator=','"
//     		};
//	    String []myData = { "1,1,1,11111111,first,1.10,1.11",
//	    		  			"10,10,10,10 101 010,second,2.20,2.22",
//	    					"2,2,2,222222,second,3.30,null",
//	    					"3,3,3,333333, third ,NULL, 3.33",
//	    					"4,4,4,444444, null ,4.40 ,4.44",
//	    					"5,5,5,5555555, fifth, 5.50, 5.55",
//	    	 			    "6,6,null,666666, sixth, 6.60, 6.66",
//	    					"7,null,7,7777777, seventh, 7.70, 7.77 ",
//	    					"11, 1,1,\"1,000\",first,1.10,1.11",
//
//	    					"8, 8",
//	    					"",
//	    					"12,n ull,12,12121212,twelveth,12.12,12.12"
//	    					};
//	    int invalidLineCnt = 3;
//		test_Interface( mySchema, myOptions, myData, invalidLineCnt );
//	}

//	public void testDelimeters () throws Exception
//	{
//		simpleSchema =
//                "create table BLAH (" +
//                "clm_integer integer default 0 not null, " + // column that is partitioned on
//
//                "clm_tinyint tinyint default 0, " +
//                //"clm_smallint smallint default 0, " +
//                //"clm_bigint bigint default 0, " +
//
//               // "clm_string varchar(10) default null, " +
//                //"clm_decimal decimal default null, " +
//                //"clm_float float default 1.0, "+ // for later
//                //"clm_timestamp timestamp default null, " + // for later
//                //"clm_varinary varbinary default null" + // for later
//                "); ";
//		char str = '.';
//		 String []params = {
//		    		//userHome + "/testdb.csv",
//		    		"--inputfile=" + userHome + "/test.csv",
//		    		//"--procedurename=BLAH.insert",
//		    		"--reportdir=" + reportdir,
//		    		"--tablename=BLAH",
//		    		"--abortfailurecount=50",
//		    		"--separator=",""
//		    		};
//		 String []myData = {"1, 1","","2, 2"};
//		 test_Interface( params, myData );
//	}
/*
	public void testSimple() throws Exception {
	 simpleSchema =
                "create table BLAH (" +
                "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_tinyint tinyint default 0, " +
                //"clm_smallint smallint default 0, " +
                //"clm_bigint bigint default 0, " +

               	//"clm_string varchar(10) default null, " +
                //"clm_decimal decimal default null, " +
                //"clm_float float default 1.0, "+ // for later
                //"clm_timestamp timestamp default null, " + // for later
                //"clm_varinary varbinary default null" + // for later
                "); ";

		 String []params_simple = {
	    		//"--inputfile=" + userHome + "/testdb.csv",
	    		//"--procedurename=BLAH.insert",
	    		//"--reportdir=" + reportdir,
	    		//"--tablename=BLAH",
	    		//"--abortfailurecount=50",
				 "-help"
	    		};
        try {
            CSVLoader.main(params_simple);
            // do the test
            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            int rowct = modCount.getRowCount();

            BufferedReader csvreport = new BufferedReader(new FileReader(new String(reportdir + CSVLoader.reportfile)));
            int lineCount = 0;
            String line = "";
            String promptMsg = "Number of acknowledged tuples:";

            while ((line = csvreport.readLine()) != null) {
            	if (line.startsWith(promptMsg)) {
            		String num = line.substring(promptMsg.length());
            		lineCount = Integer.parseInt(num.replaceAll("\\s",""));
            		break;
            	}
            }
            System.out.println(String.format("The rows infected: (%d,%s)", lineCount, rowct));
            assertEquals(lineCount, rowct);
            CSVLoader.flush();
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
	}
	*/

	public void test_Interface( String my_schema, String[] my_options, String[] my_data, int invalidLineCnt ) throws Exception {
		try{
			BufferedWriter out_csv = new BufferedWriter( new FileWriter( path_csv ) );
			for( int i = 0; i < my_data.length; i++ )
				out_csv.write( my_data[ i ]+"\n" );
			out_csv.flush();
			out_csv.close();
		}
		catch( Exception e) {
			System.err.print( e.getMessage() );
		}

		try{
			pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
			pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
			builder = new VoltProjectBuilder();
			//builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
			//builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, 5, 'nullchar');");

			builder.addLiteralSchema(my_schema);
			builder.addPartitionInfo("BLAH", "clm_integer");
			boolean success = builder.compile(pathToCatalog, 2, 1, 0);
			assertTrue(success);
			MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
			config = new VoltDB.Configuration();
			config.m_pathToCatalog = pathToCatalog;
			config.m_pathToDeployment = pathToDeployment;
			localServer = new ServerThread(config);
			client = null;

          	localServer.start();
        	localServer.waitForInitialization();

        	client = ClientFactory.createClient();
        	client.createConnection("localhost");

        	CSVLoader.main( my_options );
            // do the test

            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            int rowct = modCount.getRowCount();

            BufferedReader csvreport = new BufferedReader(new FileReader(CSVLoader.pathReportfile));
            int lineCount = 0;
            String line = "";
            String promptMsg = "Number of acknowledged tuples:";

            String promptFailMsg = "Number of failed tuples:";
            int invalidlinecnt = 0;

            while ((line = csvreport.readLine()) != null) {
            	if (line.startsWith(promptMsg)) {
            		String num = line.substring(promptMsg.length());
            		lineCount = Integer.parseInt(num.replaceAll("\\s",""));
                    }
            	if( line.startsWith(promptFailMsg)){
            		String num = line.substring(promptFailMsg.length());
            		invalidlinecnt = Integer.parseInt(num.replaceAll("\\s",""));
            	}
            }
            System.out.println(String.format("The rows infected: (%d,%s)", lineCount, rowct));
            assertEquals(lineCount, rowct);
            assertEquals(invalidLineCnt, invalidlinecnt);

        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
	}

}
