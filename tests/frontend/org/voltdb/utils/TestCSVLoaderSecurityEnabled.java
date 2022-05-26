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
import org.voltdb.TheHashinator;
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
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

public class TestCSVLoaderSecurityEnabled {

    protected static ServerThread localServer;
    protected static Client client;
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    protected static String userName = System.getProperty("user.name");
    protected static String reportDir = String.format("/tmp/%s_csv", userName);
    protected static String path_csv = String.format("%s/%s", reportDir, "test.csv");
    protected static String path_credentials = String.format("%s%s", reportDir, "credentials.csv");
    protected static String dbName = String.format("mydb_%s", userName);

    public static final RoleInfo[] GROUPS = new RoleInfo[] {
        new RoleInfo("Operator", true, true, true, true, true, true)
    };

    public static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("operator", "mech", new String[] {"Operator"}),
        new UserInfo("operator2", "mech!!!", new String[] {"Operator"})
    };

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
                + "clm_string varchar(32) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "clm_point geography_point default null, "
                + "clm_geography geography default null, "
                + "PRIMARY KEY(clm_integer) "
                + ");");
        builder.addPartitionInfo("BLAH", "clm_integer");
        builder.addUsers(USERS);
        builder.addRoles(GROUPS);
        builder.setSecurityEnabled(true, true);
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

        ClientConfig cfg = new ClientConfig("operator", "mech");
        client = ClientFactory.createClient(cfg);
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
        //TODO
    }

    @After
    public void tearDown() throws IOException, ProcCallException
    {
        final ClientResponse response = client.callProcedure("@AdHoc", "TRUNCATE TABLE BLAH;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.printf("=-=-=-= End %s =-=-=-=\n", testName.getMethodName());
    }

    // test with username / password
    @Test
    public void testBadDecimal() throws Exception {
        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=operator",
                "--password=mech",
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
                "1 ,1,1,11111111,first,2000.00,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
        };
        int invalidLineCnt = 1;
        int validLineCnt = 0;

        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    // test csvloader connection with credentials file
    @Test
    public void testUsingCredentialFile() throws Exception {
        try{
            BufferedWriter out_csv = new BufferedWriter( new FileWriter( path_credentials ) );
            String[] credentials = {"username: operator", "password: mech"};
            for (String token : credentials) {
                out_csv.write(token + "\n");
            }
            out_csv.flush();
            out_csv.close();
        }
        catch( Exception e) {
            System.err.print( e.getMessage() );
        }

        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--credentials="+path_credentials,
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
                "1 ,1,1,11111111,first,2000.00,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\""
        };
        int invalidLineCnt = 1;
        int validLineCnt = 0;

        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
    }

    @Test
    public void testUsingCredentialFileIncludingSpecialCharactersInPassword() throws Exception {
        try{
            BufferedWriter out_csv = new BufferedWriter( new FileWriter( path_credentials ) );
            String[] credentials = {"username: operator2", "password: mech!!!"};
            for (String token : credentials) {
                out_csv.write(token + "\n");
            }
            out_csv.flush();
            out_csv.close();
        }
        catch( Exception e) {
            System.err.print( e.getMessage() );
        }

        String []myOptions = {
                "-f" + path_csv,
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--credentials="+path_credentials,
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
                "1 ,1,1,11111111,first,2000.00,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\""
        };
        int invalidLineCnt = 1;
        int validLineCnt = 0;

        test_Interface(myOptions, myData, invalidLineCnt, validLineCnt);
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

        assertEquals(rowct, 1);
    }
}
