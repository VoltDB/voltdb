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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

import com.google_voltpatches.common.base.Strings;

public class TestCSVLoader {

    protected static ServerThread localServer;
    protected static Client client;
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    protected static String userName = System.getProperty("user.name");
    protected static String reportDir = String.format("/tmp/%s_csv", userName);
    protected static String path_csv = String.format("%s/%s", reportDir, "test.csv");
    protected static String dbName = String.format("mydb_%s", userName);

    @Rule
    public TestName testName = new TestName();

    public static void prepare() {
        if (!reportDir.endsWith("/")) {
            reportDir += "/";
        }
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

    @BeforeClass
    public static void startDatabase() throws Exception
    {
        prepare();

        String pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.addLiteralSchema(
                "create table BLAH MIGRATE TO TARGET target1 ("
                + "clm_integer integer not null, "
                + "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(32) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "clm_point geography_point default null, "
                + "clm_geography geography default null, "
                + "PRIMARY KEY(clm_integer) "
                + ");"
                + "DR TABLE BLAH;");
        builder.addPartitionInfo("BLAH", "clm_integer");
        if (MiscUtils.isPro()) {
            builder.setXDCR();
        }
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
        Configuration config = new Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        localServer = new ServerThread(config);
        client = null;

        localServer.start();
        localServer.waitForInitialization();

        client = ClientFactory.createClient(new ClientConfig());
        client.createConnection("localhost");
    }

    @AfterClass
    public static void stopDatabase() throws InterruptedException
    {
        if (client != null) {
            client.close();
        }
        client = null;

        if (localServer != null) {
            localServer.shutdown();
            localServer.join();
        }
        localServer = null;
    }

    @Before
    public void setup() throws IOException, ProcCallException
    {
        System.out.printf("=-=-=-= Start %s =-=-=-=\n", testName.getMethodName());
        final ClientResponse response = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM BLAH;");
        assertEquals(0, response.getResults()[0].asScalarLong());
    }

    @After
    public void tearDown() throws IOException, ProcCallException
    {
        final ClientResponse response = client.callProcedure("@AdHoc", "TRUNCATE TABLE BLAH;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.printf("=-=-=-= End %s =-=-=-=\n", testName.getMethodName());
    }

    @Test
    public void testNoQuotes() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--noquotechar",
                "--escape=\\",
                "--skip=1",
                "--limitrows=100",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        // Polygon (GEOGRAPHY) left null here, since it does not work with
        // comma separator, without quote char
        String []myData = {
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),",
                //unclosed quote below should work
                "1 ,1,1,11111111,fir\"st,1.10,1.11,"+currentTime+",POINT(1 1),",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 2;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testNoQuotesNonCommaSeparator() throws Exception
    {
        String[] myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=|",
                "--noquotechar",
                "--escape=\\",
                "--skip=1",
                "--limitrows=100",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "1 |1|1|11111111|first|1.10|1.11|"+currentTime+"|POINT(1 1)|POLYGON((0 0, 1 0, 0 1, 0 0))",
                "2|2|2|222222|second|3.30|NULL|"+currentTime+"|POINT(2 2)|POLYGON((0 0, 2 0, 0 2, 0 0))",
                //unclosed quote below should work
                "1 |1|1|11111111|fir\"st|1.10|1.11|"+currentTime+"|POINT(1 1)|POLYGON((0 0, 1 0, 0 1, 0 0))",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 2;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testNoQuotesSpecialSeparator() throws Exception
    {
        // Identical to previous test, except separator is Ctrl-A, not '|'.
        // Don't know how to make Ctrl-A visible in eclipse.
        String[] myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=",
                "--noquotechar",
                "--escape=\\",
                "--skip=1",
                "--limitrows=100",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "1 1111111111first1.101.11"+currentTime+"POINT(1 1)POLYGON((0 0, 1 0, 0 1, 0 0))",
                "222222222second3.30NULL"+currentTime+"POINT(2 2)POLYGON((0 0, 2 0, 0 2, 0 0))",
                //unclosed quote below should work
                "1 1111111111fir\"st1.101.11"+currentTime+"POINT(1 1)POLYGON((0 0, 1 0, 0 1, 0 0))",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 2;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testCommon() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--skip=1",
                "--limitrows=100",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
                //empty line
                "",
                //invalid lines below
                "8, 8",
                "9, NLL,9,\"1,000\",nine,1.10,1.11,"+currentTime+",POINT(9 9),\"POLYGON((0 0, 9 0, 0 9, 0 0))\"",
                "10,10,10,10 101 010,second,2.20,2.22"+currentTime+",POINT(10 10),\"POLYGON((0 0, 10 0, 0 10, 0 0))\"",
                "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 4;
        int validLineCnt = 7;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    //Test -p option where we use just one processor and one line at a time processing of callProcedure.
    @Test
    public void testProcedureOption() throws Exception {
        String[] myOptions = {
            "-f" + path_csv,
            "--reportdir=" + reportDir,
            "--maxerrors=50",
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
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2,2,2,222222,second,3.30,NULL," + currentTime + ",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
            "3,3,3,333333, third ,NULL, 3.33," + currentTime + ",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
            "4,4,4,444444, NULL ,4.40 ,4.44," + currentTime + ",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
            "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime + ",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
            "6,6,NULL,666666, sixth, 6.60, 6.66," + currentTime + ",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
            "7,NULL,7,7777777, seventh, 7.70, 7.77," + currentTime + ",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
            "11, 1,1,\"1,000\",first,1.10,1.11," + currentTime + ",POINT(11 11),\"POLYGON((0 0, 11 0, 0 11, 0 0))\"",
            //empty line
            "",
            //invalid lines below
            "8, 8",
            "9, NLL,9,\"1,000\",nine,1.10,1.11," + currentTime + ",POINT(9 9),\"POLYGON((0 0, 9 0, 0 9, 0 0))\"",
            "10,10,10,10 101 010,second,2.20,2.22" + currentTime + ",POINT(10 10),\"POLYGON((0 0, 10 0, 0 10, 0 0))\"",
            "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 4;
        int validLineCnt = 7;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    //Test batch option that splits.
    @Test
    public void testBatchOptionThatSplits() throws Exception {
        String[] myOptions = {
            "-f" + path_csv,
            "--reportdir=" + reportDir,
            "--maxerrors=50",
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
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2,2,2,222222,second,3.30,NULL," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "3,3,3,333333, third ,NULL, 3.33," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "4,4,4,444444, NULL ,4.40 ,4.44," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "6,6,NULL,666666, sixth, 6.60, 6.66," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "7,NULL,7,7777777, seventh, 7.70, 7.77," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "11, 1,1,\"1,000\",first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            //empty line
            "",
            //invalid lines below
            "8, 8",
            "9, NLL,9,\"1,000\",nine,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "10,10,10,10 101 010,second,2.20,2.22" + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 4;
        int validLineCnt = 7;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    //Test batch option that and gets constraint violations.
    //has a batch that fully fails and 2 batches that has 50% failure.
    @Test
    public void testBatchOptionThatSplitsAndGetsViolations() throws Exception {
        String[] myOptions = {
            "-f" + path_csv,
            "--reportdir=" + reportDir,
            "--maxerrors=50",
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
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "3 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "4 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"", //Whole batch fails
            "5 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "6 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"", //Whole batch fails
            "7 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "8 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "11,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"", //Whole batch fails
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "12,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\""
        };
        int invalidLineCnt = 7;
        int validLineCnt = 10;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    //Test using Upsert instead of insert.
    // In the same batch, upsert allows lines with same primary key
    @Test
    public void testUpsertSameBatch() throws Exception {
        String[] myOptions = {
            "-f" + path_csv,
            "--reportdir=" + reportDir,
            "--maxerrors=50",
            "--user=",
            "--password=",
            "--port=",
            "--separator=,",
            "--quotechar=\"",
            "--escape=\\",
            "--skip=0",
            "--limitrows=100",
            "--update",
            "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String[] myData = {
                "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL," + currentTime + ",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33," + currentTime + ",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                // this should not fail:
                "2,4,4,444444, NULL ,4.40 ,4.44," + currentTime + ",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                // this should not fail, and should exist in the table:
                "2,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime + ",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                // this should not fail, and should exist in the table:
                "1,6,NULL,666666, sixth, 6.60, 6.66," + currentTime + ",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\""
        };

        String[] myValidTableData = {
                "3,3,3,333333, third ,NULL, 3.33," + currentTime + ",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "2,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime + ",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "1,6,NULL,666666, sixth, 6.60, 6.66," + currentTime + ",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\""
        };

        int invalidLineCnt = 0;
        int validLineCnt = myData.length;
        int validUpsertLineCnt = 3;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt, validUpsertLineCnt, myValidTableData, "");
    }

    //Test using Upsert instead of insert.
    // load same csv file is allowed in upsertMode
    @Test
    public void testUpsertIdempotent() throws Exception {
        String[] myOptions = {
            "-f" + path_csv,
            "--reportdir=" + reportDir,
            "--maxerrors=50",
            "--user=",
            "--password=",
            "--port=",
            "--separator=,",
            "--quotechar=\"",
            "--escape=\\",
            "--skip=0",
            "--limitrows=100",
            "--update",
            "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String[] myData = {
                "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL," + currentTime + ",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33," + currentTime + ",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
        };

        int invalidLineCnt = 0;
        int validLineCnt = myData.length;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);

        // call again is Ok
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    //Test batch option that splits and gets constraint violations.
    @Test
    public void testBatchOptionThatSplitsAndGetsViolationsAndDataIsSmall() throws Exception {
        String[] myOptions = {
            "-f" + path_csv,
            "--reportdir=" + reportDir,
            "--maxerrors=50",
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
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "2 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\""
        };
        int invalidLineCnt = 2;
        int validLineCnt = 2;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test
    public void testOpenQuote() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
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
                        "1,1,1,1,\"Jesus\\\"\"loves"+ "\n" +"you\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testOpenQuoteAndStrictQuotes() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
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
                        "\"1\",\"1\",\"1\",\"1\",\"Jesus\\\"\"loves"+ "\n" +"you\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testUnmatchQuote() throws Exception
    {
        //test the following csv data
        //1,1,1,"Jesus\""loves","7777-12-25 14:35:26"
        //1,1,1,"Jesus\""loves
        //you,"7777-12-25 14:35:26"
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
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
                        //valid line from shopzilla: unmatched quote is between two commas(which is treated as strings).
                        "1,1,1,1,\"Jesus\\\"\"loves"+ "\n" +"you\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                        //invalid line: unmatched quote
                        "1,1,1,1,\"Jesus\\\"\"loves"+ "\n" +"you,1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 1;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testNULL() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--skip=0",
                "BLAH"
        };
        //Both \N and \\N as csv input are treated as NULL
        String []myData = {
                "1,\\" + Constants.CSV_NULL        + ",1,11111111,\"\"NULL\"\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2," + Constants.QUOTED_CSV_NULL + ",1,11111111,\"NULL\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "3," + Constants.CSV_NULL        + ",1,11111111,  \\" + Constants.CSV_NULL        + "  ,1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL,
                "4," + Constants.CSV_NULL        + ",1,11111111,  " + Constants.QUOTED_CSV_NULL + "  ,1.10,1.11,\"7777-12-25 14:35:26\"," + Constants.QUOTED_CSV_NULL + "," + Constants.QUOTED_CSV_NULL,
                "5,\\" + Constants.CSV_NULL        + ",1,11111111, \"  \\" + Constants.CSV_NULL   + "  \",1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL,
                "6,\\" + Constants.CSV_NULL        + ",1,11111111, \"  \\" + Constants.CSV_NULL  + " L \",1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL,
                "7,\\" + Constants.CSV_NULL        + ",1,11111111,  \"abc\\" + Constants.CSV_NULL + "\"  ,1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testCustomNULL() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--skip=0",
                "--customNullString=test",
                "BLAH"
        };
        //Both \N and \\N as csv input are treated as NULL
        String []myData = {
                "1,1,1,11111111,test,1.10,1.11,,,",
                "2,2,1,11111111,\"test\",1.10,1.11,,,",
                "3,3,1,11111111,testme,1.10,1.11,,,",
                "4,4,1,11111111,iamtest,1.10,1.11,,,",
                "5,5,5,5,\\N,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
        VoltTable ts_table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY clm_integer;").getResults()[0];
        int i = 0;
        int nulls = 0;
        while (ts_table.advanceRow()) {
            String value = ts_table.getString(4);
            if(i < 2) {
                assertEquals(value, null);
                nulls++;
            } else if(i == 4){
                // this test case should fail once we stop replacing the \N as NULL
                assertEquals(value, null);
                nulls++;
            } else {
                assertNotNull(value);
            }
            i++;
        }
        assertEquals(nulls, 3);
    }

    @Test
    public void testBlankDefault() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "BLAH"
        };

        String []myData = {
                "1,,,,,,,,,",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testBlankNull() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "null",
                "BLAH"
        };

        String []myData = {
                "1,,,,,,,,,",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testBlankEmpty() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "empty",
                "BLAH"
        };

        String []myData = {
                "0,,,,,,,,,",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    //SuperCSV treats empty string "" as null
    @Test
    public void testBlankError() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "error",
                "BLAH"
        };

        String []myData = {
                "0,,,,,,,,,",
        };
        int invalidLineCnt = 1;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testStrictQuote() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--strictquotes",
                "BLAH"
        };

        String []myData = {
                "\"1\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,2,a word,1.10,1.11,7777-12-25 14:35:26,POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,3,a word,1.10,1.11,7777-12-25 14:35:26,POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "\"4\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "5,\"5\",\"5\",\"5\",,,,,,",
                "\"5\",5,\"5\",\"5\",,,,,,",
                "\"5\",\"5\",,,,,,,,",
        };
        int invalidLineCnt = 4;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testSkip() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--skip=10",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = { "1,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL," + currentTime + ",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33," + currentTime + ",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44," + currentTime + ",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime + ",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66," + currentTime + ",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77," + currentTime + ",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11," + currentTime + ",POINT(11 11),\"POLYGON((0 0, 11 0, 0 11, 0 0))\"",
                //empty line
                "",
                //invalid lines below
                "8, 8",
                "10,10,10,10 101 010,second,2.20,2.22" + currentTime + ",POINT(10 10),\"POLYGON((0 0, 10 0, 0 10, 0 0))\"",
                "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 2;
        int validLineCnt = 0;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testSkipOverFlow() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                //Skip the whole file
                "--skip=10000",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = { "1,1,1,11111111,first,1.10,1.11,"+currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL," + currentTime + ",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33," + currentTime + ",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44," + currentTime + ",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime + ",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66," + currentTime + ",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77," + currentTime + ",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11," + currentTime + ",POINT(11 11),\"POLYGON((0 0, 11 0, 0 11, 0 0))\"",
                //empty line
                "",
                //invalid lines below
                "8, 8",
                "10,10,10,10 101 010,second,2.20,2.22" + currentTime + ",POINT(10 10),\"POLYGON((0 0, 10 0, 0 10, 0 0))\"",
                "12,n ull,12,12121212,twelveth,12.12,12.12"
        };
        int invalidLineCnt = 0;
        int validLineCnt = 0;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testEmptyFile() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--blank=" + "empty",
                "BLAH"
        };

        int invalidLineCnt = 0;
        int validLineCnt = 0;
        test_Interface(myOptions, null, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testEscapeChar() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--escape=~",
                "BLAH"
        };

        String []myData = {
                "1,1,1,1,~\"escapequotes,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,1,1,1,~\\nescapenewline,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "3,1,1,1,~'escapeprimesymbol,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\""
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testNoWhiteSpace() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--nowhitespace",
                "BLAH"
        };

        String []myData = {
                "1,1,1,1,nospace,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,1,1,1,   frontspace,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "3,1,1,1,rearspace   ,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "4,1,1,1,\" inquotespace \"   ,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\""
        };
        int invalidLineCnt = 3;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testColumnLimitSize() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--columnsizelimit=10",
                "BLAH"
        };

        String []myData = {
                "1,1,1,1,\"openquote,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),",
                "2,1,1,1,second,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),",
                "3,1,1,1,third,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),",
                "4,1,1,1,\"fourthfourthfourth\",1.10,1.11,7777-12-25 14:35:26,POINT(1 1),"
        };
        int invalidLineCnt = 2;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testColumnLimitSize2() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--columnsizelimit=4",
                "--skip=1",
                "BLAH"
        };

        String []myData = {
                "1,1,1,1,\"Edwr" + "\n" + "Burnam\",1.10,1.11,7777-12-25 14:35:26,,",
                "2,1,1,1,\"Tabatha" + "\n,1.10,1.11,7777-12-25 14:35:26,," +
                "3,1,1,1,Gehling,1.10,1.11,7777-12-25 14:35:26,,",
        };
        int invalidLineCnt = 3;
        int validLineCnt = 0;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testIncorrectColumnSize() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "BLAH"
        };

        String []myData = {
                "1,1,1,1,line one,1.10,1.11,7777-12-25 14:35:26,POINT(1 1)",                                     // too short
                "2,1,1,1,line two,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\",1", // too long
                "3,1,1,1,line three,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"", // just right
        };
        int invalidLineCnt = 2;
        int validLineCnt = 1;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test
    public void testTimestampStringRoundTrip() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "BLAH"
        };

        String []myData = {
                "1,,,,,,,7777-12-25,,",
                "2,,,,,,,7777-12-25 00:00:00,,",
                "3,,,,,,,2000-02-03,,",
                "4,,,,,,,2000-02-03 00:00:00.0,,",
                "5,,,,,,,2100-04-05,,",
                "6,,,,,,,2100-04-05 00:00:00.00,,",
                "7,,,,,,,2012-12-31,,",
                "8,,,,,,,2012-12-31 00:00:00.000,,",
                "9,,,,,,,2001-10-25,,",
                "10,,,,,,,2001-10-25 00:00:00.0000,,",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
        VoltTable ts_table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY clm_integer;").getResults()[0];
        while (ts_table.advanceRow()) {
            TimestampType ts1 = ts_table.getTimestampAsTimestamp(7);
            if (ts_table.advanceRow()) {
                TimestampType ts2 = ts_table.getTimestampAsTimestamp(7);
                assertEquals(ts1, ts2);
                continue;
            }
        }

    }

    @Test
    public void testTimeZone() throws Exception {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--timezone=PST",
                "BlAh"
        };
        String currentTime= "2007-09-23 10:10:10.0";
        String[] myData = {
            "1 ,1,1,11111111,first,1.10,1.11," + currentTime + ",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt =  1;
        TimeZone timezone = TimeZone.getDefault();
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
        //Resetting the JVM TimeZone
        TimeZone.setDefault(timezone);

        VoltTable ts_table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
        ts_table.advanceRow();
        long tableTimeCol = ts_table.getTimestampAsLong(7);
        // 2007-09-23 10:10:10.0 converted to long is 1190542210000000
        long time = 1190542210000000L;
        long diff = tableTimeCol - time;
        assertEquals(TimeUnit.MICROSECONDS.toHours(diff), 7);
    }

    @Test
    public void testGeographyPointStringRoundTrip() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "BLAH"
        };

        String []myData = {
                "1,,,,,,,,POINT(1.1 1.1),",
                "2,,,,,,,,POINT(1.1 1.1),",
                "3,,,,,,,,POINT(2.2 -2.2),",
                "4,,,,,,,,POINT(2.2 -2.2),",
                "5,,,,,,,,POINT(-3.3 3.3),",
                "6,,,,,,,,POINT(-3.3 3.3),",
                "7,,,,,,,,POINT(-4.4 -4.4),",
                "8,,,,,,,,POINT(-4.4 -4.4),",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
        VoltTable ts_table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY clm_integer;").getResults()[0];
        while (ts_table.advanceRow()) {
            GeographyValue poly1 = ts_table.getGeographyValue(9);
            if (ts_table.advanceRow()) {
                GeographyValue poly2 = ts_table.getGeographyValue(9);
                assertEquals(poly1, poly2);
                continue;
            }
        }

    }

    @Test
    public void testGeographyStringRoundTrip() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "BLAH"
        };

        String []myData = {
                "1,,,,,,,,,\"POLYGON((1.1 1.1, -1.1 1.1, -1.1 -1.1, 1.1 -1.1, 1.1 1.1))\"",
                "2,,,,,,,,,\"POLYGON((1.1 1.1, -1.1 1.1, -1.1 -1.1, 1.1 -1.1, 1.1 1.1))\"",
                "3,,,,,,,,,\"POLYGON((2.2 2.2, -2.2 2.2, -2.2 -2.2, 2.2 -2.2, 2.2 2.2),(1.1 1.1, 1.1 -1.1, -1.1 -1.1, -1.1 1.1, 1.1 1.1))\"",
                "4,,,,,,,,,\"POLYGON((2.2 2.2, -2.2 2.2, -2.2 -2.2, 2.2 -2.2, 2.2 2.2),(1.1 1.1, 1.1 -1.1, -1.1 -1.1, -1.1 1.1, 1.1 1.1))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
        VoltTable ts_table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY clm_integer;").getResults()[0];
        while (ts_table.advanceRow()) {
            GeographyPointValue pt1 = ts_table.getGeographyPointValue(8);
            if (ts_table.advanceRow()) {
                GeographyPointValue pt2 = ts_table.getGeographyPointValue(8);
                assertEquals(pt1, pt2);
                continue;
            }
        }

    }

    @Test
    public void testHeaderCommon() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "clm_integer,clm_tinyint,clm_smallint,clm_bigint,clm_string,clm_decimal,clm_float,clm_timestamp,clm_point,clm_geography",
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testHeaderDiffOrder() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "clm_string,clm_integer,clm_tinyint,clm_smallint,clm_bigint,clm_decimal,clm_float,clm_timestamp,clm_geography,clm_point",
                "first,1 ,1,1,11111111,1.10,1.11,"+currentTime+",\"POLYGON((0 0, 1 0, 0 1, 0 0))\",POINT(1 1)",
                "second,2,2,2,222222,3.30,NULL,"+currentTime+",\"POLYGON((0 0, 2 0, 0 2, 0 0))\",POINT(2 2)",
                " third ,3,3,3,333333,NULL, 3.33,"+currentTime+",\"POLYGON((0 0, 3 0, 0 3, 0 0))\",POINT(3 3)",
                " NULL ,4,4,4,444444,4.40 ,4.44,"+currentTime+",\"POLYGON((0 0, 4 0, 0 4, 0 0))\",POINT(4 4)",
                "  \"abcde\"g,5,5,5,5555555, 5.50, 5.55,"+currentTime+",\"POLYGON((0 0, 5 0, 0 5, 0 0))\",POINT(5 5)",
                " sixth,6,6,NULL,666666, 6.60, 6.66,"+currentTime+",\"POLYGON((0 0, 6 0, 0 6, 0 0))\",POINT(6 6)",
                " seventh,7,NULL,7,7777777, 7.70, 7.77,"+currentTime+",\"POLYGON((0 0, 7 0, 0 7, 0 0))\",POINT(7 7)",
                "first,11, 1,1,\"1,000\",1.10,1.11,"+currentTime+",\"POLYGON((0 0, 8 0, 0 8, 0 0))\",POINT(7 7)",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testHeaderSkip() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--skip=2",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "clm_integer,clm_tinyint,clm_smallint,clm_bigint,clm_string,clm_decimal,clm_float,clm_timestamp,clm_point,clm_geography",
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 6;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testHeaderNULL() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--skip=0",
                "--header",
                "BLAH"
        };
        //Both \N and \\N as csv input are treated as NULL
        String []myData = {
                "clm_integer,clm_tinyint,clm_smallint,clm_bigint,clm_string,clm_decimal,clm_float,clm_timestamp,clm_point,clm_geography",
                "1,\\" + Constants.CSV_NULL        + ",1,11111111,\"\"NULL\"\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2," + Constants.QUOTED_CSV_NULL + ",1,11111111,\"NULL\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "3," + Constants.CSV_NULL        + ",1,11111111,  \\" + Constants.CSV_NULL        + "  ,1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL,
                "4," + Constants.CSV_NULL        + ",1,11111111,  " + Constants.QUOTED_CSV_NULL + "  ,1.10,1.11,\"7777-12-25 14:35:26\"," + Constants.QUOTED_CSV_NULL + "," + Constants.QUOTED_CSV_NULL,
                "5,\\" + Constants.CSV_NULL        + ",1,11111111, \"  \\" + Constants.CSV_NULL   + "  \",1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL,
                "6,\\" + Constants.CSV_NULL        + ",1,11111111, \"  \\" + Constants.CSV_NULL  + " L \",1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL,
                "7,\\" + Constants.CSV_NULL        + ",1,11111111,  \"abc\\" + Constants.CSV_NULL + "\"  ,1.10,1.11,\"7777-12-25 14:35:26\",\\" + Constants.CSV_NULL + ",\\" + Constants.CSV_NULL
        };
        int invalidLineCnt = 0;
        int validLineCnt = myData.length - invalidLineCnt - 1;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testHeaderSuperSetHeader() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "clm_integer,clm_tinyint,clm_smallint,clm_bigint,clm_string,clm_decimal,clm_float,clm_timestamp,clm_point,clm_geography,clm_other",
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\",other1",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\",other2",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\",other3",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\",other4",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\",other5",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\",other6",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\",other7",
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\",other8",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testHeaderColumnNumNotSame() throws Exception
    {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "clm_integer,clm_tinyint,clm_smallint,clm_bigint,clm_string,clm_decimal,clm_float,clm_timestamp,clm_point,clm_geography,clm_other",
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
        };
        int invalidLineCnt = 8;
        int validLineCnt = 0;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testHeaderColumnWhitespace1() throws Exception
    {
        // ENG-12324: csvloader --header doesn't work if header has spaces. This is essentially the same test as testHeaderColumnNumNotSame, but with some
        // strategically placed whitespace in the header.

        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "  clm_integer, clm_tinyint,clm_smallint,  clm_bigint  ,clm_string,clm_decimal,clm_float  , clm_timestamp,clm_point ,clm_geography",
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
        };
        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testHeaderColumnWhitespace2() throws Exception
    {
        // ENG-12324: csvloader --header doesn't work if header has spaces. This is essentially the same test as testHeaderColumnNumNotSame, but with some
        // strategically placed whitespace in the header.

        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "  clm_integer, clm_tinyint,clm_smallint,  clm_bigint  ,clm_string,clm_decimal,clm_float  , clm_timestamp,clm_point ,clm_geography",
                "1 ,1,1,11111111,first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\",garbage",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
        };
        int invalidLineCnt = 1;
        int validLineCnt = 7;
        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt );
    }

    @Test
    public void testMultiByteCharacterUTF8() throws Exception
    {
        String encoding = "utf8";
        createCSVFile("utf8");

        String []myOptions = {
                "-f" + path_csv,
                "--charset=utf8",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "utf8_encoded_text.csv";
        List<String> list = new ArrayList<>();

        //read from the file
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(FILENAME);
        br = new BufferedReader(fr);
            String sCurrentLine;
            br = new BufferedReader(new FileReader(FILENAME));
            while ((sCurrentLine = br.readLine()) != null) {
                list.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        String[] myData2 = new String[list.size()];
        for(int i = 0; i < list.size(); i++) {
            myData2[i] = list.get(i);
        }

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt, encoding);

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
            int i = table.getActiveRowIndex();
            VoltTableRow row = table.fetchRow(i);
            assertEquals(row.getString(4),strings[pos]);
            pos++;
        }

        try {
            File temp = new File(FILENAME);
            if(temp.exists()) {
                temp.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUTF8WithGBKEncoding() throws Exception
    {
        String encoding = "utf8";
        createCSVFile("utf8");

        String []myOptions = {
                "-f" + path_csv,
                "--charset=gbk",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "utf8_encoded_text.csv";
        List<String> list = new ArrayList<>();

        //read from the file
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);
            String sCurrentLine;
            br = new BufferedReader(new FileReader(FILENAME));
            while ((sCurrentLine = br.readLine()) != null) {
                list.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        String[] myData2 = new String[list.size()];
        for(int i = 0; i < list.size(); i++) {
            myData2[i] = list.get(i);
        }

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt, encoding);

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
            int i = table.getActiveRowIndex();
            if(i==0) {
                assertFalse(table.fetchRow(i).getString(4).equals(strings[pos]));
                pos++;continue;
            }
            VoltTableRow row = table.fetchRow(i);
            assertEquals(row.getString(4),strings[pos]);
            pos++;
        }
        try {
            File temp = new File(FILENAME);
            if(temp.exists()) {
                temp.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGB2312WithGBKEncoding() throws Exception
    {
        String encoding = "gb2312";
        createCSVFile("gb2312");

        String []myOptions = {
                "-f" + path_csv,
                "--charset=gbk",    // use gbk to decode it
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "gb2312_encoded_text.csv";
        List<String> list = new ArrayList<>();

        //read from the file
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);
            String sCurrentLine;
            br = new BufferedReader(new FileReader(FILENAME));
            while ((sCurrentLine = br.readLine()) != null) {
                list.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        String[] myData2 = new String[list.size()];
        for(int i = 0; i < list.size(); i++) {
            myData2[i] = list.get(i);
        }

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt, encoding);

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
            int i = table.getActiveRowIndex();
            if(i == 0) {
                assertFalse(table.fetchRow(i).getString(4).equals(strings[pos]));
                pos++;
                continue;
            }
            VoltTableRow row = table.fetchRow(i);
            assertEquals(row.getString(4),strings[pos]);
            pos++;
        }
        try {
            File temp = new File(FILENAME);
            if(temp.exists()) {
                temp.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiByteCharacterGBK() throws Exception
    {
        String encoding = "gbk";
        createCSVFile("gbk");

        String []myOptions = {
                "-f" + path_csv,
                "--charset=gbk",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "gbk_encoded_text.csv";
        List<String> list = new ArrayList<>();

        //read from the file
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);
            String sCurrentLine;
            br = new BufferedReader(new InputStreamReader(new FileInputStream(FILENAME), Charset.forName("GBK")));
            while ((sCurrentLine = br.readLine()) != null) {
                list.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        String[] myData2 = new String[list.size()];
        for(int i = 0; i < list.size(); i++) {
            myData2[i] = list.get(i);
        }

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt, encoding);

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
            int i = table.getActiveRowIndex();
            VoltTableRow row = table.fetchRow(i);
            assertEquals(row.getString(4),strings[pos]);
            pos++;
        }
        try {
            File temp = new File(FILENAME);
            if(temp.exists()) {
                temp.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiByteCharacterGB2312() throws Exception
    {
        String encoding = "gb2312";
        createCSVFile("gb2312");

        String []myOptions = {
                "-f" + path_csv,
                "--charset=gb2312",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "gb2312_encoded_text.csv";
        List<String> list = new ArrayList<>();

        //read from the file
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);
            String sCurrentLine;
            br = new BufferedReader(new InputStreamReader(new FileInputStream(FILENAME), Charset.forName("GBK")));
            while ((sCurrentLine = br.readLine()) != null) {
                list.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        String[] myData2 = new String[list.size()];
        for(int i = 0; i < list.size(); i++) {
            myData2[i] = list.get(i);
        }

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt, encoding);

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的汉字查询内容",
                            "second",
                            "third",
                            null,
                            "abcdeg",
                            "sixth",
                            "seventh",
                            "first"};
        int pos = 0;
        while(table.advanceRow()) {
            int i = table.getActiveRowIndex();
            if(i==0) {
                assertFalse(table.fetchRow(i).getString(4).equals(strings[pos]));
                pos++;
                continue;
            }
            VoltTableRow row = table.fetchRow(i);
            assertEquals(row.getString(4),strings[pos]);
            pos++;
        }
        try {
            File temp = new File(FILENAME);
            if(temp.exists()) {
                temp.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // A bad decimal input should not make csvloader hang on forever
    @Test(timeout=2000)
    public void testBadDecimal() throws Exception {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--skip=0",
                "--limitrows=100",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String []myData = {
                "1 ,1,1,11111111,first,2000000000000000000000000000000.000000000000,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 1;
        int validLineCnt = 0;

        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test(timeout=2000)
    public void testBadString() throws Exception {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--skip=0",
                "--limitrows=100",
                "BlAh"
        };
        String currentTime = new TimestampType().toString();
        String longString = Strings.repeat("a", (1024*1024)+1);
        assertTrue(longString.length() > (1024*1024));
        String []myData = {
                "1 ,1,1,11111111,\""+longString+"\",2.0,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 1;
        int validLineCnt = 0;

        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test
    public void testSnapshotAndLoad() throws Exception {
        int expectedLineCnt = 5;
        client.callProcedure("@AdHoc",
                "INSERT INTO BLAH VALUES (1,1,1,11111111,'first',1.10,1.11,'7777-12-25 14:35:26',"
                        + "PointFromText('POINT(1 1)'), PolygonFromText('POLYGON((0 0, 1 0, 0 1, 0 0))'));");
        client.callProcedure("@AdHoc",
                "INSERT INTO BLAH VALUES (2,2,2,222222,'second',2.20,2.22,'7777-12-25 14:35:26', "
                        + "PointFromText('POINT(2 2)'), PolygonFromText('POLYGON((0 0, 2 0, 0 2, 0 0))'));");
        client.callProcedure("@AdHoc",
                "INSERT INTO BLAH VALUES (3,3,3,333333, 'third' ,3.33, 3.33,'7777-12-25 14:35:26', "
                        + "PointFromText('POINT(3 3)'), PolygonFromText('POLYGON((0 0, 3 0, 0 3, 0 0))'));");
        client.callProcedure("@AdHoc",
                "INSERT INTO BLAH VALUES (4,4,4,444444, 'fourth' ,4.40 ,4.44,'7777-12-25 14:35:26', "
                        + "PointFromText('POINT(4 4)'), PolygonFromText('POLYGON((0 0, 4 0, 0 4, 0 0))'));");
        client.callProcedure("@AdHoc",
                "INSERT INTO BLAH VALUES (5,5,5,5555555, 'fifth', 5.50, 5.55,'7777-12-25 14:35:26', "
                        + "PointFromText('POINT(5 5)'), PolygonFromText('POLYGON((0 0, 5 0, 0 5, 0 0))'));");

        client.callProcedure("@SnapshotSave",
                String.format("{uripath:\"file:///tmp\",nonce:\"%s\",block:true,format:\"csv\"}", dbName));

        // clear the table then try to load the csv file
        client.callProcedure("@AdHoc", "DELETE FROM BLAH;");

        String[] my_options = {
            "-f" + "/tmp/" + dbName + "-BLAH-host_0.csv",
            "--reportdir=" + reportDir,
            "--maxerrors=50",
            "--user=",
            "--password=",
            "--port=",
            "--separator=,",
            "--quotechar=\"",
            "--escape=\\",
            "--skip=0",
            "BLAH" };
        prepare();

        // put CSVLoader in test mode so it wont exit after done.
        CSVLoader.testMode = true;
        CSVLoader.main(my_options);
        File file = new File(String.format("/tmp/%s-BLAH-host_0.csv", dbName));
        file.delete();

        // do the test
        VoltTable modCount;
        modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
        System.out.println("data inserted to table BLAH:\n" + modCount);
        int rowct = modCount.getRowCount();
        assertEquals(expectedLineCnt, rowct);
    }

    private void createCSVFile(String encoding) {
        String FILENAME = encoding+"_encoded_text.csv";
        BufferedWriter bw = null;

        String currentTime = String.valueOf(System.currentTimeMillis());

        String []myData = {
                "  clm_integer, clm_tinyint,clm_smallint,  clm_bigint  ,clm_string,clm_decimal,clm_float  , clm_timestamp,clm_point ,clm_geography",
                "1 ,1,1,11111111,"+"first复杂的漢字查询内容"+",1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1 ,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
        };

        try {
            if(encoding.equalsIgnoreCase("utf8") || encoding.equalsIgnoreCase("utf-8")) {
                bw = new BufferedWriter
                        (new OutputStreamWriter(new FileOutputStream(FILENAME), Charset.forName("UTF-8")));
                for(String line: myData) {
                    bw.write(line+"\n");
                }
            } else if(encoding.equalsIgnoreCase("gbk")) {
                FileOutputStream fos = new FileOutputStream(FILENAME);
                OutputStreamWriter osw = new OutputStreamWriter(fos, "gbk");
                bw = new BufferedWriter(osw);
                for(String str: myData) {
                    bw.write(str + "\n");
                }
                bw.flush();
            } else if(encoding.equals("gb2312") || encoding.equals("GB2312")) {
                FileOutputStream fos = new FileOutputStream(FILENAME);
                OutputStreamWriter osw = new OutputStreamWriter(fos, "gb2312");
                bw = new BufferedWriter(osw);

                for(String str: myData) {
                    bw.write(str + "\n");
                }
                bw.flush();
                osw.flush();
                fos.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    // read from hard-coded data, no encoding
    public void test_Interface(String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt) throws Exception {
        test_Interface(my_options, my_data, invalidLineCnt, validLineCnt, 0, new String[0], new String());
    }

    //read from csv file, with encoding
    public void test_Interface(String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt, String encoding) throws Exception {
        test_Interface(my_options, my_data, invalidLineCnt, validLineCnt, 0, new String[0], encoding);
    }

    public void test_Interface(String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt, int validLineUpsertCnt, String[] validData, String encoding) throws Exception {
        try{
            BufferedWriter out_csv;
            if(encoding.equals("")) {
                out_csv = new BufferedWriter( new FileWriter( path_csv ) );
            } else {
                FileOutputStream fos = new FileOutputStream(path_csv);
                OutputStreamWriter osw = new OutputStreamWriter(fos, encoding);
                out_csv = new BufferedWriter(osw);
            }
            for (String aMy_data : my_data) {
                out_csv.write(aMy_data + "\n");
            }
            out_csv.flush();
            out_csv.close();
        }
        catch( Exception e) {
            System.err.print( e.getMessage() );
        }

        CSVLoader.testMode = true;
        CSVLoader.main( my_options );
        // do the test

        VoltTable modCount;
        modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
        System.out.println("data inserted to table BLAH:\n" + modCount);
        int rowct = modCount.getRowCount();

        // Call validate partitioning to check if we are good.
        VoltTable valTable;
        valTable = client.callProcedure("@ValidatePartitioning", (Object)null).getResults()[0];
        System.out.println("Validate for BLAH:\n" + valTable);
        while (valTable.advanceRow()) {
            long miscnt = valTable.getLong("MISPARTITIONED_ROWS");
            assertEquals(miscnt, 0);
        }

        BufferedReader csvreport = new BufferedReader(new FileReader(CSVLoader.pathReportfile));
        int lineCount = 0;
        String line;
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
        csvreport.close();
        System.out.println(String.format("The rows infected: (%d,%s)", lineCount, rowct));
        assertEquals(lineCount-validLineUpsertCnt,  rowct);
        //assert validLineCnt specified equals the successfully inserted lineCount
        assertEquals(validLineCnt, lineCount);
        assertEquals(invalidLineCnt, invalidlinecnt);

        // validate upsert the correct data
        if (validData != null && validData.length > 0) {
            tearDown();
            setup();
            try{
                BufferedWriter out_csv = new BufferedWriter( new FileWriter( path_csv ) );
                for (String aMy_data : validData) {
                    out_csv.write(aMy_data + "\n");
                }
                out_csv.flush();
                out_csv.close();
            }
            catch( Exception e) {
                e.printStackTrace();
            }

            CSVLoader.testMode = true;
            CSVLoader.main( my_options );

            VoltTable validMod;
            validMod = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            assertTrue(modCount.hasSameContents(validMod));
        }
    }
}
