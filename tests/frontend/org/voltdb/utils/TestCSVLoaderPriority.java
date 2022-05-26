/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

/**
 * Loader tests using explicit priority, using
 * the old Client interface.
 */
public class TestCSVLoaderPriority {

    protected static ServerThread localServer;
    protected static Client client;

    protected static String userName = System.getProperty("user.name");
    protected static String reportDir = String.format("/tmp/%s_csv2", userName);
    protected static String path_csv = String.format("%s/%s", reportDir, "test2.csv");
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
            System.err.printf("Exception: %s\n", x);
            System.exit(-1);
        }
    }

    @BeforeClass
    public static void startDatabase() throws Exception {
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
    public static void stopDatabase() throws InterruptedException {
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
    public void setup() throws IOException, ProcCallException {
        System.out.printf("=-=-=-= Start %s =-=-=-=\n", testName.getMethodName());
        final ClientResponse response = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM BLAH;");
        assertEquals(0, response.getResults()[0].asScalarLong());
    }

    @After
    public void tearDown() throws IOException, ProcCallException {
        final ClientResponse response = client.callProcedure("@AdHoc", "TRUNCATE TABLE BLAH;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.printf("=-=-=-= End %s =-=-=-=\n", testName.getMethodName());
    }

    @Test
    public void testWithPriority() throws Exception {
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
                "--priority=5",
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
        testInterface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test
    public void testTupleLoader() throws Exception {
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
            "--priority=5",
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
        testInterface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    // read from hard-coded data
    public void testInterface(String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt) throws Exception {
        testInterface(my_options, my_data, invalidLineCnt, validLineCnt, 0, new String[0]);
    }

    public void testInterface(String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt, int validLineUpsertCnt, String[] validData) throws Exception {
        try{
            BufferedWriter out_csv = new BufferedWriter(new FileWriter(path_csv));
            for (String d : my_data) {
                out_csv.write(d + "\n");
            }
            out_csv.flush();
            out_csv.close();
        }
        catch(Exception e) {
            System.err.print(e.getMessage());
            fail("Unexpected");
        }

        CSVLoader.testMode = true;
        CSVLoader.main(my_options);
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
            if (line.startsWith(promptFailMsg)){
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
            catch(Exception e) {
                e.printStackTrace();
                fail("Unexpected");
            }

            CSVLoader.testMode = true;
            CSVLoader.main( my_options );

            VoltTable validMod;
            validMod = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            assertTrue(modCount.hasSameContents(validMod));
        }
    }
}
