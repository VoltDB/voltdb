/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import org.voltdb.types.TimestampType;

public class TestCSVLoader extends TestCase {

    private String pathToCatalog;
    private String pathToDeployment;
    private ServerThread localServer;
    private VoltDB.Configuration config;
    private VoltProjectBuilder builder;
    private Client client;

    private final String reportDir = "/tmp/";
    String path_csv = reportDir + "/" + "test.csv";

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
    }


    public void testSnapshotAndLoad () throws Exception {
        String my_schema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_tinyint tinyint default 0, " +
                "clm_smallint smallint default 0, " +
                "clm_bigint bigint default 0, " +

                "clm_string varchar(10) default null, " +
                "clm_decimal decimal default null, " +
                "clm_float float default null"+
                //"clm_varinary varbinary default null," +
                //"clm_timestamp timestamp default null " +
                ");";

        try{
            pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
            pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
            builder = new VoltProjectBuilder();

            builder.addLiteralSchema(my_schema);
            builder.addPartitionInfo("BLAH", "clm_integer");
            //builder.addStmtProcedure("Insert", "INSERT into BLAH values (?, ?, ?, ?, ?, ?, ?);");
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

            int expectedLineCnt = 5;
            client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1,1,1,11111111,'first',1.10,1.11);" );
            client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (2,2,2,222222,'second',2.20,2.22);" );
            client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (3,3,3,333333, 'third' ,3.33, 3.33);" );
            client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (4,4,4,444444, 'fourth' ,4.40 ,4.44);" );
            client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (5,5,5,5555555, 'fifth', 5.50, 5.55);" );

            client.callProcedure("@SnapshotSave", "{uripath:\"file:///tmp\",nonce:\"mydb\",block:true,format:\"csv\"}" );

            //clear the table then try to load the csv file
            client.callProcedure("@AdHoc", "DELETE FROM BLAH;");
            String []my_options = {
                    "-f" + "/tmp/mydb-BLAH-host_0.csv",
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
            CSVLoader.main( my_options );
            File file = new File( "/tmp/mydb-BLAH-host_0.csv" );
            file.delete();

            // do the test
            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            int rowct = modCount.getRowCount();
            assertEquals(expectedLineCnt, rowct);
            //assertEquals(0, invalidlinecnt);

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
                "clm_float float default null, "+
                //"clm_varinary varbinary default null," +
                "clm_timestamp timestamp default null " +
                "); ";
        String []myOptions = {
                "-f" + reportDir + "/test.csv",
                //"--procedure=blah.insert",
                "--reportdir=" + reportDir,
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
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = { "1,1,1,11111111,first,1.10,1.11,"+currentTime,
                "2,2,2,222222,second,3.30,NULL,"+currentTime,
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime,
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime,
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime,
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime,
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime,
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime,
                //invalid lines below
                "8, 8",
                "",
                "10,10,10,10 101 010,second,2.20,2.22"+currentTime,
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
                "clm_float float default null "+
                //"clm_timestamp timestamp default null, " +
                //"clm_varinary varbinary default null" +
                "); ";
        String []myOptions = {
                "-f" + reportDir + "/test.csv",
                //"--procedure=BLAH.insert",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
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

    public void testBlankNull() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "clm_bigint bigint default 0, " +
                        "clm_string varchar(20) default null, " +
                        "clm_decimal decimal default null, " +
                        "clm_float float default null, "+
                        "clm_timestamp timestamp default null, " +
                        "clm_varinary varbinary default null" +
                        "); ";
        String []myOptions = {
                "-f" + reportDir + "/test.csv",
                "--reportdir=" + reportDir,
                "--blank=" + "null",
                "BLAH"
        };

        String []myData = {
                "1,,,,,,,,",
        };
        int invalidLineCnt = 0;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt );
    }

    public void testBlankEmpty() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "clm_bigint bigint default 0, " +
                        "clm_string varchar(20) default null, " +
                        "clm_decimal decimal default null, " +
                        "clm_float float default null, "+
                        "clm_timestamp timestamp default null, " +
                        "clm_varinary varbinary default null" +
                        "); ";
        String []myOptions = {
                "-f" + reportDir + "/test.csv",
                "--reportdir=" + reportDir,
                "--blank=" + "empty",
                "BLAH"
        };

        String []myData = {
                "0,,,,,,,,",
        };
        int invalidLineCnt = 0;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt );
    }

    public void testBlankError() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "clm_bigint bigint default 0, " +
                        "clm_string varchar(20) default null, " +
                        "clm_decimal decimal default null, " +
                        "clm_float float default null, "+
                        "clm_timestamp timestamp default null, " +
                        "clm_varinary varbinary default null" +
                        "); ";
        String []myOptions = {
                "-f" + reportDir + "/test.csv",
                "--reportdir=" + reportDir,
                "--blank=" + "error",
                "BLAH"
        };

        String []myData = {
                "0,,,,,,,,",
        };
        int invalidLineCnt = 1;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt );
    }

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
            String promptMsg = "Number of rows successfully inserted:";
            String promptFailMsg = "Number of rows that could not be inserted:";
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
