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
import static org.junit.Assert.assertTrue;

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

import org.voltcore.logging.VoltLogger;
import org.voltdb.ServerThread;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

public class TestJDBCLoader {

    protected static ServerThread localServer;
    protected static Client client;
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    protected static String userName = System.getProperty("user.name");
    protected static String reportDir = String.format("/tmp/%s_csv", userName);
    protected static String path_csv = String.format("%s/%s", reportDir, "test.csv");
    protected static String driver_class = "org.voltdb.jdbc.Driver";
    protected static String jdbc_url = "jdbc:voltdb://localhost";
    protected static String dbName = String.format("mydb_%s", userName);

    @Rule
    public TestName testName = new TestName();

    public static void prepare() {
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

    @BeforeClass
    public static void startDatabase() throws Exception
    {
        prepare();

        String pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.addLiteralSchema(
                "create table BLAH ("
                + "clm_integer integer not null, "
                + "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "PRIMARY KEY(clm_integer) "
                + ");\n"
                + "create table JBLAH ("
                + "clm_integer integer not null, "
                + "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(16) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "PRIMARY KEY(clm_integer) "
                + ");");
        builder.addPartitionInfo("BLAH", "clm_integer");
        builder.addPartitionInfo("JBLAH", "clm_integer");
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
        if (client != null) client.close();
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
        ClientResponse response = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM BLAH;");
        assertEquals(0, response.getResults()[0].asScalarLong());
        response = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM JBLAH;");
        assertEquals(0, response.getResults()[0].asScalarLong());
    }

    @After
    public void tearDown() throws IOException, ProcCallException
    {
        ClientResponse response = client.callProcedure("@AdHoc", "TRUNCATE TABLE BLAH;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("@AdHoc", "TRUNCATE TABLE JBLAH;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.printf("=-=-=-= End %s =-=-=-=\n", testName.getMethodName());
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
            "BlAh"
        };

        String[] jdbcOptions = {
            "--jdbcdriver=" + driver_class,
            "--jdbcurl=" + jdbc_url,
            "--jdbctable=" + "BlAh",
            "--reportdir=" + reportDir,
            "--maxerrors=50",
            "--user=",
            "--password=",
            "--port=",
            "JBlAh"
        };

        String currentTime = new TimestampType().toString();
        String []myData = {
            "1,2,2,222222,second,3.30,NULL," + currentTime,
            "2,3,3,333333, third ,NULL, 3.33," + currentTime,
            "3,4,4,444444, NULL ,4.40 ,4.44," + currentTime,
            "4,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime,
            "5,6,NULL,666666, sixth, 6.60, 6.66," + currentTime
        };
        int invalidLineCnt = 0;
        int validLineCnt = 4;
        test_Interface(myOptions, jdbcOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test
    public void testWithPriority() throws Exception
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
            "--priority=6",
            "BlAh"
        };

        String[] jdbcOptions = {
            "--jdbcdriver=" + driver_class,
            "--jdbcurl=" + jdbc_url,
            "--jdbctable=" + "BlAh",
            "--reportdir=" + reportDir,
            "--maxerrors=50",
            "--user=",
            "--password=",
            "--port=",
            "--priority=6",
            "JBlAh"
        };

        String currentTime = new TimestampType().toString();
        String []myData = {
            "1,2,2,222222,second,3.30,NULL," + currentTime,
            "2,3,3,333333, third ,NULL, 3.33," + currentTime,
            "3,4,4,444444, NULL ,4.40 ,4.44," + currentTime,
            "4,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime,
            "5,6,NULL,666666, sixth, 6.60, 6.66," + currentTime
        };
        int invalidLineCnt = 0;
        int validLineCnt = 4;
        test_Interface(myOptions, jdbcOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test
    public void testColumnSizeFailure() throws Exception
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
            "BlAh"
        };

        String[] jdbcOptions = {
            "--jdbcdriver=" + driver_class,
            "--jdbcurl=" + jdbc_url,
            "--jdbctable=" + "BlAh",
            "--reportdir=" + reportDir,
            "--maxerrors=50",
            "--user=",
            "--password=",
            "--port=",
            "JBlAh"
        };

        String currentTime = new TimestampType().toString();
        String []myData = {
            "1,2,2,222222,second,3.30,NULL," + currentTime,
            "2,3,3,333333, third ,NULL, 3.33," + currentTime,
            "3,4,4,444444, NULL ,4.40 ,4.44," + currentTime,
            "4,5,5,5555555,  \"abcde\"g, 5.50, 5.55," + currentTime,
            "5,6,NULL,666666, sixth, 6.60, 6.66," + currentTime,
            // the next one the string is too big
            "6,6,NULL,666666, longerthansixteencha, 6.60, 6.66," + currentTime,

        };
        int invalidLineCnt = 1;
        int validLineCnt = 4;
        test_Interface(myOptions, jdbcOptions, myData, invalidLineCnt, validLineCnt);
    }

    public void test_Interface(String[] csv_options, String[] jdbc_options, String[] my_data, int invalidLineCnt,
            int validLineCnt) throws Exception {
        try{
            BufferedWriter out_csv = new BufferedWriter(new FileWriter(path_csv));
            for (String aMy_data : my_data) {
                out_csv.write(aMy_data + "\n");
            }
            out_csv.flush();
            out_csv.close();
        }
        catch( Exception e) {
            System.err.print( e.getMessage() );
        }

        //Load using CSV
        CSVLoader.testMode = true;
        CSVLoader.main(csv_options);

        //Reload using JDBC
        JDBCLoader.testMode = true;
        JDBCLoader.main(jdbc_options);
        // do the test

        VoltTable modCount;
        modCount = client.callProcedure("@AdHoc", "SELECT * FROM JBLAH;").getResults()[0];
        System.out.println("data inserted to table BLAH:\n" + modCount);
        int rowct = modCount.getRowCount();

        // Call validate partitioning to check if we are good.
        VoltTable valTable;
        valTable = client.callProcedure("@ValidatePartitioning", TheHashinator.getCurrentHashinator().getConfigBytes()).getResults()[0];
        System.out.println("Validate for JBLAH:\n" + valTable);
        while (valTable.advanceRow()) {
            long miscnt = valTable.getLong("MISPARTITIONED_ROWS");
            assertEquals(miscnt, 0);
        }

        BufferedReader csvreport = new BufferedReader(new FileReader(JDBCLoader.pathReportfile));
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
        assertEquals(lineCount, rowct);
        //assert validLineCnt specified equals the successfully inserted lineCount
        assertEquals(validLineCnt, lineCount);
        assertEquals(invalidLineCnt, invalidlinecnt);
    }

}
