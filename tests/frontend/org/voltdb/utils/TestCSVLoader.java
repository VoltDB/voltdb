/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

public class TestCSVLoader extends TestCase {

    protected String pathToCatalog;
    protected String pathToDeployment;
    protected ServerThread localServer;
    protected VoltDB.Configuration config;
    protected VoltProjectBuilder builder;
    protected Client client;
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    protected String userName = System.getProperty("user.name");
    protected String reportDir = String.format("/tmp/%s_csv", userName);
    protected String path_csv = String.format("%s/%s", reportDir, "test.csv");
    protected String dbName = String.format("mydb_%s", userName);

    public void prepare() {
        if (!reportDir.endsWith("/"))
            reportDir += "/";
        File dir = new File(reportDir);
        try {
            if (!dir.exists()) {
                dir.mkdirs();
            }

        } catch (Exception x) {
            m_log.error(x.getMessage(), x);
            System.exit(-1);
        }
    }

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
                "clm_float float default null, "+
                //"clm_varinary varbinary(20) default null," +
                "clm_timestamp timestamp default null " +
                "); ";
        String []myOptions = {
                "-f" + path_csv,
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
                "--skip=1",
                "--limitrows=100",
                //"--strictquotes",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime,
                "2,2,2,222222,second,3.30,NULL,"+currentTime,
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime,
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime,
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime,
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime,
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime,
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime,
                //empty line
                "",
                //invalid lines below
                "8, 8",
                "9, NLL,9,\"1,000\",nine,1.10,1.11,"+currentTime,
                "10,10,10,10 101 010,second,2.20,2.22"+currentTime,
                "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 4;
        int validLineCnt = 7;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    //Test -p option where we use just one processor and one line at a time processing of callProcedure.
    public void testProcedureOption() throws Exception {
        String mySchema =
                "create table BLAH ("
                + "clm_integer integer default 0 not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + //"clm_varinary varbinary(20) default null," +
                "clm_timestamp timestamp default null "
                + "); ";
        String[] myOptions = {
            "-f" + path_csv,
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
            "--skip=1",
            "--limitrows=100",
            "--procedure=BLAH.insert"
        };
        String currentTime = new TimestampType().toString();
        String[] myData = {
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2,2,2,222222,second,3.30,NULL," + currentTime,
            "3,3,3,333333, third ,NULL, 3.33," + currentTime,
            "4,4,4,444444, NULL ,4.40 ,4.44," + currentTime,
            "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime,
            "6,6,NULL,666666, sixth, 6.60, 6.66," + currentTime,
            "7,NULL,7,7777777, seventh, 7.70, 7.77," + currentTime,
            "11, 1,1,\"1,000\",first,1.10,1.11," + currentTime,
            //empty line
            "",
            //invalid lines below
            "8, 8",
            "9, NLL,9,\"1,000\",nine,1.10,1.11," + currentTime,
            "10,10,10,10 101 010,second,2.20,2.22" + currentTime,
            "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 4;
        int validLineCnt = 7;
        test_Interface(mySchema, myOptions, myData, invalidLineCnt, validLineCnt);
    }

    //Test batch option that splits.
    public void testBatchOptionThatSplits() throws Exception {
        String mySchema =
                "create table BLAH ("
                + "clm_integer integer default 0 not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + //"clm_varinary varbinary(20) default null," +
                "clm_timestamp timestamp default null "
                + "); ";
        String[] myOptions = {
            "-f" + path_csv,
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
            "--skip=1",
            "--limitrows=100",
            "--batch=2",
            "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String[] myData = {
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2,2,2,222222,second,3.30,NULL," + currentTime,
            "3,3,3,333333, third ,NULL, 3.33," + currentTime,
            "4,4,4,444444, NULL ,4.40 ,4.44," + currentTime,
            "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime,
            "6,6,NULL,666666, sixth, 6.60, 6.66," + currentTime,
            "7,NULL,7,7777777, seventh, 7.70, 7.77," + currentTime,
            "11, 1,1,\"1,000\",first,1.10,1.11," + currentTime,
            //empty line
            "",
            //invalid lines below
            "8, 8",
            "9, NLL,9,\"1,000\",nine,1.10,1.11," + currentTime,
            "10,10,10,10 101 010,second,2.20,2.22" + currentTime,
            "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 4;
        int validLineCnt = 7;
        test_Interface(mySchema, myOptions, myData, invalidLineCnt, validLineCnt);
    }

    //Test batch option that and gets constraint violations.
    //has a batch that fully fails and 2 batches that has 50% failure.
    public void testBatchOptionThatSplitsAndGetsViolations() throws Exception {
        String mySchema =
                "create table BLAH ("
                + "clm_integer integer not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        String[] myOptions = {
            "-f" + path_csv,
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
            "--limitrows=100",
            "--batch=2", //Batch size is small so we dont have to generate large dataset.
            "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String[] myData = {
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "3 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "4 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime, //Whole batch fails
            "5 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "6 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime, //Whole batch fails
            "7 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "8 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "11 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime, //Whole batch fails
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "12 ,1,1,11111111,first,1.10,1.11," + currentTime
        };
        int invalidLineCnt = 7;
        int validLineCnt = 10;
        test_Interface(mySchema, myOptions, myData, invalidLineCnt, validLineCnt);
    }

    //Test batch option that splits and gets constraint violations.
    public void testBatchOptionThatSplitsAndGetsViolationsAndDataIsSmall() throws Exception {
        String mySchema =
                "create table BLAH ("
                + "clm_integer integer not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        String[] myOptions = {
            "-f" + path_csv,
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
            "--limitrows=100",
            "--batch=2", //Batch size is small so we dont have to generate large dataset.
            "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String[] myData = {
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime,
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime
        };
        int invalidLineCnt = 2;
        int validLineCnt = 2;
        test_Interface(mySchema, myOptions, myData, invalidLineCnt, validLineCnt);
    }

    public void testOpenQuote() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_integer1 integer default 0, " +
                "clm_bigint bigint default 0, " +

                "clm_string varchar(200) default null, " +
                "clm_timestamp timestamp default null " +
                "); ";
        String []myOptions = {
                "-f" + path_csv,
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
                "BlAh"
        };
        String []myData = {
                        "1,1,1,\"Jesus\\\"\"loves"+ "\n" +"you\",\"7777-12-25 14:35:26\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testOpenQuoteAndStrictQuotes() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_integer1 integer default 0, " +
                "clm_bigint bigint default 0, " +

                "clm_string varchar(200) default null, " +
                "clm_timestamp timestamp default null " +
                "); ";
        String []myOptions = {
                "-f" + path_csv,
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
                "--strictquotes",
                "BlAh"
        };
        String []myData = {
                        "\"1\",\"1\",\"1\",\"Jesus\\\"\"loves"+ "\n" +"you\",\"7777-12-25 14:35:26\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testUnmatchQuote() throws Exception
    {
        //test the following csv data
        //1,1,1,"Jesus\""loves","7777-12-25 14:35:26"
        //1,1,1,"Jesus\""loves
        //you,"7777-12-25 14:35:26"
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_integer1 integer default 0, " +
                "clm_bigint bigint default 0, " +

                "clm_string varchar(200) default null, " +
                "clm_timestamp timestamp default null " +
                "); ";
        String []myOptions = {
                "-f" + path_csv,
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
                //"--strictquotes",
                "BlAh"
        };
        String []myData = {
                        //valid line from shopzilla: unmatched quote is between two commas(which is treated as strings).
                        "1,1,1,\"Jesus\\\"\"loves"+ "\n" +"you\",\"7777-12-25 14:35:26\"",
                        //invalid line: unmatched quote
                        "1,1,1,\"Jesus\\\"\"loves"+ "\n" +"you,\"7777-12-25 14:35:26\"",
        };
        int invalidLineCnt = 1;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
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
                //"clm_varinary varbinary(20) default null" +
                "); ";
        String []myOptions = {
                "-f" + path_csv,
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
                //"--strictquotes",
                "BLAH"
        };
        //Both \N and \\N as csv input are treated as NULL
        String []myData = {
                "1,\\" + Constants.CSV_NULL        + ",1,11111111,\"\"NULL\"\",1.10,1.11",
                "2," + Constants.QUOTED_CSV_NULL + ",1,11111111,\"NULL\",1.10,1.11",
                "3," + Constants.CSV_NULL        + ",1,11111111,  \\" + Constants.CSV_NULL        + "  ,1.10,1.11",
                "4," + Constants.CSV_NULL        + ",1,11111111,  " + Constants.QUOTED_CSV_NULL + "  ,1.10,1.11",
                "5,\\" + Constants.CSV_NULL        + ",1,11111111, \"  \\" + Constants.CSV_NULL   + "  \",1.10,1.11",
                "6,\\" + Constants.CSV_NULL        + ",1,11111111, \"  \\" + Constants.CSV_NULL  + " L \",1.10,1.11",
                "7,\\" + Constants.CSV_NULL        + ",1,11111111,  \"abc\\" + Constants.CSV_NULL + "\"  ,1.10,1.11"
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
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
                        "clm_varinary varbinary(20) default null" +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "null",
                "BLAH"
        };

        String []myData = {
                "1,,,,,,,,",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
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
                        "clm_varinary varbinary(20) default null" +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "empty",
                "BLAH"
        };

        String []myData = {
                "0,,,,,,,,",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    //SuperCSV treats empty string "" as null
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
                        "clm_varinary varbinary(20) default null" +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "error",
                "BLAH"
        };

        String []myData = {
                "0,,,,,,,,",
        };
        int invalidLineCnt = 1;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testStrictQuote() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--strictquotes",
                "BLAH"
        };

        String []myData = {
                "\"1\",\"1\",\"1\"",
                "2,2,2",
                "3,3,3",
                "\"4\",\"4\",\"4\"",
        };
        int invalidLineCnt = 2;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testSkip() throws Exception
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
                //"clm_varinary varbinary(20) default null," +
                "clm_timestamp timestamp default null " +
                "); ";
        String []myOptions = {
                "-f" + path_csv,
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
                "--skip=10",
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
                //empty line
                "",
                //invalid lines below
                "8, 8",
                "10,10,10,10 101 010,second,2.20,2.22"+currentTime,
                "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 2;
        int validLineCnt = 0;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testSkipOverFlow() throws Exception
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
                //"clm_varinary varbinary(20) default null," +
                "clm_timestamp timestamp default null " +
                "); ";
        String []myOptions = {
                "-f" + path_csv,
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
                //Skip the whole file
                "--skip=10000",
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
                //empty line
                "",
                //invalid lines below
                "8, 8",
                "10,10,10,10 101 010,second,2.20,2.22"+currentTime,
                "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 0;
        int validLineCnt = 0;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testEmptyFile() throws Exception
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
                        "clm_varinary varbinary(20) default null" +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "empty",
                "BLAH"
        };

        String []myData = null;
        int invalidLineCnt = 0;
        int validLineCnt = 0;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testEscapeChar() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                                "clm_string varchar(20), " +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--escape=~",
                "BLAH"
        };

        String []myData = {
                "~\"escapequotes,1,1,1",
                "~\\nescapenewline,2,2,2",
                "~'escapeprimesymbol,3,3,3"
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testNoWhiteSpace() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                                "clm_string varchar(20), " +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--nowhitespace",
                "BLAH"
        };

        String []myData = {
                "nospace,1,1,1",
                "   frontspace,2,2,2",
                "rearspace   ,3,3,3",
                "\" inquotespace \"   ,4,4,4"
        };
        int invalidLineCnt = 3;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testColumnLimitSize() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                                "clm_string varchar(20), " +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--columnsizelimit=10",
                "BLAH"
        };

        String []myData = {
                "\"openquote,1,1,1",
                "second,2,2,2",
                "third,3,3,3",
                "\"fourthfourthfourth\",4,4,4"
        };
        int invalidLineCnt = 1;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void testColumnLimitSize2() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_string varchar(20), "+
                        "); ";
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--columnsizelimit=4",
                "--skip=1",
                "BLAH"
        };

        String []myData = {
                "1,\"Edwr" + "\n" + "Burnam\"",
                "2,\"Tabatha" + "\n" +
                "Gehling",
        };
        int invalidLineCnt = 3;
        int validLineCnt = 0;
        test_Interface( mySchema, myOptions, myData, invalidLineCnt, validLineCnt );
    }

    public void test_Interface( String my_schema, String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt) throws Exception {
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

            prepare();
            CSVLoader.testMode = true;
            CSVLoader.main( my_options );
            // do the test

            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            int rowct = modCount.getRowCount();

            // Call validate partitioning to check if we are good.
            VoltTable valTable;
            valTable = client.callProcedure("@ValidatePartitioning", null, null).getResults()[0];
            System.out.println("Validate for BLAH:\n" + valTable);
            while (valTable.advanceRow()) {
                long miscnt = valTable.getLong("MISPARTITIONED_ROWS");
                assertEquals(miscnt, 0);
            }

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
            //assert validLineCnt specified equals the successfully inserted lineCount
            assertEquals(validLineCnt, lineCount);
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
