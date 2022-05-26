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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ServerThread;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

import junit.framework.TestCase;

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

public class TestVoltBulkLoader extends TestCase {

    protected String pathToCatalog;
    protected String pathToDeployment;
    protected ServerThread localServer;
    protected VoltDB.Configuration config;
    protected VoltProjectBuilder builder;
    protected Client client1;
    protected Client client2;
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    protected String userName = System.getProperty("user.name");
    protected String reportDir = String.format("/tmp/%s_vbl", userName);
    protected String dbName = String.format("mydb_%s", userName);
    Random rnd = new Random(28);
    protected static String geo = "polygon((0 0, 1 0, 1 1, 0 1, 0 0))";
    protected static String geopt = "point(0 0)";

    public class TestFailureCallback implements BulkLoaderFailureCallBack {
        ArrayList<Integer> failureRows = new ArrayList<Integer>(20);
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            failureRows.add((Integer)rowHandle);
        }

        public boolean failureRowListMatches(ArrayList<Integer> expectedFailureList) {
            int i = 0;
            int j = 0;
            Collections.sort(failureRows);

            boolean success = true;
            while (i < failureRows.size() && j < expectedFailureList.size()) {
                if (failureRows.get(i) < expectedFailureList.get(j)) {
                    System.out.println(String.format("The unexpected failure for row %d", failureRows.get(i)));
                    i++;// Increase I move to next element
                    success = false;
                }
                else if (expectedFailureList.get(j) < failureRows.get(i)) {
                    System.out.println(String.format("The missing expected failure for row %d", failureRows.get(i)));
                    j++;// Increase J move to next element
                    success = false;
                }
                else {
                    j++;
                    i++;// If same increase I & J both
                }
            }
            return (success && i==j && i==failureRows.size() && j==expectedFailureList.size());
        }

    }

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
                "clm_timestamp timestamp default null, " +
                "clm_geo geography default null," +
                "clm_geopt geography_point default null" +
                "); ";
        int myBatchSize = 200;
        TimestampType currentTime = new TimestampType();
        Object [][]myData = {
            {1 ,1,1,11111111,"first",1.10,1.11,currentTime, geo, geopt},
            {2,2,2,222222,"second",3.30,"NULL",currentTime, geo, geopt},
            {3,3,3,333333," third ",null,3.33,currentTime, geo, geopt},
            {4,4,4,444444," NULL ",4.40 ,4.44,currentTime, geo, geopt},
            {5,5,5,5555555,"  \"abcde\"g",5.50,5.55,currentTime, geo, geopt},
            {6,6,"NULL",666666," sixth", 6.60, 6.66,currentTime, geo, geopt},
            {7,null,7,7777777," seventh", 7.70, 7.77,currentTime, geo, geopt},
            {11,1,1,"\"1,000\"","first",1.10,1.11,currentTime, geo, geopt},
            //empty line
            {},
            //invalid lines below
            {8,8},
            {9,"NLL",9,"\"1,000\"","nine",1.10,1.11,currentTime},
            {10,10,10,"10 101 010","second",2.20,2.22},
            {12,"n ull",12,12121212,"twelveth",12.12,12.12}
        };
        Integer[] failures = {2,6,8,9,10,11,12,13};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
                // + "clm_varinary varbinary(20) default null," +
                + "clm_timestamp timestamp default null, "
                + "clm_geo geography default null,"
                + "clm_geopt geography_point default null"
                + "); ";
        int myBatchSize = 2;
        TimestampType currentTime = new TimestampType();
        Object [][] myData = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime, geo, geopt},
            {2,2,2,222222,"second",3.30,null,currentTime, geo, geopt},
            {3,3,3,333333," third ",null,3.33,currentTime, geo, geopt},
            {4,4,4,444444," NULL ",4.40 ,4.44,currentTime, geo, geopt},
            {5,5,5,5555555,"abcdeg",5.50,5.55,currentTime, geo, geopt},
            {6,6,null,666666,"sixth",6.60,6.66,currentTime, geo, geopt},
            {7,7,7,7777777," seventh",7.70,7.77,currentTime, geo, geopt},
            {11, 1,1,1000,"first",1.10,1.11,currentTime, geo, geopt},
            //empty line
            {},
            //invalid lines below
            {8,8},
            {9,9,9,900,"nine",1.10,1.11,currentTime, geo, geopt},
            {10,10,10,10,"second",2.20,2.22,currentTime, geo, geopt},
            {12,null,12,12121212,"twelveth",12.12,12.12,currentTime, geo, geopt}
        };
        Integer[] failures = {9,10};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
    }

    //Test flush with good and bad rows in < maxBatch
    public void testBatchOptionCommitByFlush() throws Exception {
        String mySchema
                = "create table BLAH ("
                + "clm_integer integer default 0 not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                //+ "clm_varinary varbinary(20) default null," +
                + "clm_timestamp timestamp default null "
                + "); ";
        //Make batch size large
        int myBatchSize = 200;
        TimestampType currentTime = new TimestampType();
        Object[][] myData = {
            {1, 1, 1, 11111111, "first", 1.10, 1.11, currentTime},
            {2, 2, 2, 222222, "second", 3.30, null, currentTime},
            {3, 3, 3, 333333, " third ", null, 3.33, currentTime},
            {4, 4, 4, 444444, " NULL ", 4.40, 4.44, currentTime},
            {5, 5, 5, 5555555, "abcdeg", 5.50, 5.55, currentTime},
            {6, 6, null, 666666, "sixth", 6.60, 6.66, currentTime},
            {7, 7, 7, 7777777, " seventh", 7.70, 7.77, currentTime},
            {11, 1, 1, 1000, "first", 1.10, 1.11, currentTime},
            //empty line
            {},
            //invalid lines below
            {8, 8},
            {9, 9, 9, 900, "nine", 1.10, 1.11, currentTime},
            {10, 10, 10, 10, "second", 2.20, 2.22, currentTime},
            {12, null, 12, 12121212, "twelveth", 12.12, 12.12, currentTime}
        };
        Integer[] failures = {9, 10};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 2);
    }

    //Test that gets constraint violations.
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
        int myBatchSize = 2;
        TimestampType currentTime = new TimestampType();
        Object [][] myData = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime},
            {2,1,1,11111111,"first",1.10,1.11,currentTime},
            {3,1,1,11111111,"first",1.10,1.11,currentTime},
            {4,1,1,11111111,"first",1.10,1.11,currentTime},
            {1,1,1,11111111,"first",1.10,1.11,currentTime}, //Whole batch fails
            {2,1,1,11111111,"first",1.10,1.11,currentTime}, //Whole batch fails
            {5,1,1,11111111,"first",1.10,1.11,currentTime},
            {6,1,1,11111111,"first",1.10,1.11,currentTime},
            {1,1,1,11111111,"first",1.10,1.11,currentTime}, //Whole batch fails
            {2,1,1,11111111,"first",1.10,1.11,currentTime}, //Whole batch fails
            {7,1,1,11111111,"first",1.10,1.11,currentTime},
            {8,1,1,11111111,"first",1.10,1.11,currentTime},
            {11,1,1,11111111,"first",1.10,1.11,currentTime},
            {1,1,1,11111111,"first",1.10,1.11,currentTime}, //Whole batch fails
            {2,1,1,11111111,"first",1.10,1.11,currentTime}, //Whole batch fails
            {1,1,1,11111111,"first",1.10,1.11,currentTime}, //Whole batch fails
            {12,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        Integer[] failures = {5,6,9,10,14,15,16};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
    }

    //Test that gets constraint violations.
    //has a batch that fully fails and 2 batches that has 50% failure.
    public void testBatchOptionAndGetsViolationsCommitByFlush() throws Exception {
        String mySchema
                = "create table BLAH ("
                + "clm_integer integer not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "clm_geo geography default null,"
                + "clm_geopt geography_point default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        int myBatchSize = 200;
        TimestampType currentTime = new TimestampType();
        Object[][] myData = {
            {1, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {3, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {4, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {1, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}, //Whole batch fails
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}, //Whole batch fails
            {5, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {6, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {1, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}, //Whole batch fails
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}, //Whole batch fails
            {7, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {8, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {11, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {1, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}, //Whole batch fails
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}, //Whole batch fails
            {1, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}, //Whole batch fails
            {12, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt}
        };
        Integer[] failures = {5, 6, 9, 10, 14, 15, 16};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 3);
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
                + "clm_geo geography default null,"
                + "clm_geopt geography_point default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        int myBatchSize = 2;
        TimestampType currentTime = new TimestampType();
        Object [][] myData = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime, geo, geopt},
            {2,1,1,11111111,"first",1.10,1.11,currentTime, geo, geopt},
            {2,1,1,11111111,"first",1.10,1.11,currentTime, geo, geopt},
            {1,1,1,11111111,"first",1.10,1.11,currentTime, geo, geopt}
        };
        Integer[] failures = {3,4};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
    }

    //Test batch option that splits and gets constraint violations.
    public void testBatchOptionThatSplitsAndGetsViolationsAndDataIsSmallInLastBatchFlush() throws Exception {
        String mySchema
                = "create table BLAH ("
                + "clm_integer integer not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "clm_geo geography default null,"
                + "clm_geopt geography_point default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        int myBatchSize = 2;
        TimestampType currentTime = new TimestampType();
        Object[][] myData = {
            {1, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
        };
        Integer[] failures = {3};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 2);
    }

    //Test batch option that splits and gets constraint violations.
    public void testBatchOptionLastRowGetsViolationsByFlush() throws Exception {
        String mySchema
                = "create table BLAH ("
                + "clm_integer integer not null, "
                + // column that is partitioned on
                "clm_tinyint tinyint default 0, "
                + "clm_smallint smallint default 0, "
                + "clm_bigint bigint default 0, "
                + "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "clm_geo geography default null,"
                + "clm_geopt geography_point default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        int myBatchSize = 2;
        TimestampType currentTime = new TimestampType();
        Object[][] myData = {
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},
            {2, 1, 1, 11111111, "first", 1.10, 1.11, currentTime, geo, geopt},};
        Integer[] failures = {2};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 2);
    }

    public void testOpenQuote() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_integer1 integer default 0, " +
                "clm_bigint bigint default 0, " +

                "clm_string varchar(200) default null, " +
                "clm_timestamp timestamp default null, " +
                "clm_geo geography default null," +
                "clm_geopt geography_point default null " +
                "); ";
        int myBatchSize = 200;
        TimestampType timeParam = new TimestampType("7777-12-25 14:35:26");
        Object [][]myData = {
            {1,1,1,"\"Jesus\\\"\"loves"+ "\n" +"you\"",timeParam, geo, geopt},
        };
        Integer[] failures = {};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
                "clm_float float default null, " +
                "clm_geo geography default null," +
                "clm_geopt geography_point default null "+
                //"clm_timestamp timestamp default null, " +
                //"clm_varinary varbinary(20) default null" +
                "); ";
        int myBatchSize = 200;
        //Both \N and \\N as csv input are treated as NULL
        Object [][]myData = {
            {1,Constants.CSV_NULL,1,11111111,null,1.10,1.11, geo, geopt},
            {2,Constants.QUOTED_CSV_NULL,1,11111111,null,1.10,1.11, geo, geopt},
            {3,Constants.CSV_NULL,1,11111111,"  \\" + Constants.CSV_NULL + "  ",1.10,1.11, geo, geopt},
            {4,Constants.CSV_NULL,1,11111111,"  " + Constants.QUOTED_CSV_NULL + "  ",1.10,1.11, geo, geopt},
            {5,null,1,11111111," \"  \\" + Constants.CSV_NULL   + "  \"",1.10,1.11, geo, geopt},
            {6,Constants.CSV_NULL,1,11111111," \"  \\" + Constants.CSV_NULL  + " L \"",1.10,1.11, geo, geopt},
            {7,Constants.CSV_NULL,1,11111111,"  \"abc\\" + Constants.CSV_NULL + "\"  ",1.10,1.11, geo, geopt}
        };
        Integer[] failures = {2};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
                        "clm_varinary varbinary(20) default null, " +
                        "clm_geo geography default null, " +
                        "clm_geo_point geography_point default null " +
                        "); ";
        int myBatchSize = 200;

        Object [][]myData = {
            {1,null,null,null,null,null,null,null,null,null,null}
        };
        Integer[] failures = {};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
        int myBatchSize = 200;

        Object [][]myData = {
            {0,null,null,null,null,null,null,null,null}
        };
        Integer[] failures = {};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
    }

    public void testUpsertWithNoPrimaryKey() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "); ";
        int myBatchSize = 200;

        Object [][]myData = {
            {1,1,1},
            {2,2,2},
            {3,3,3},
            {4,4,4},
        };
        Integer[] failures = {};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        boolean upsert = true;
        try {
            test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0, upsert);
            fail();
        }catch (IllegalArgumentException e){};
    }

    public void testStrictQuote() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_tinyint tinyint default 0, " +
                        "clm_smallint smallint default 0, " +
                        "); ";
        int myBatchSize = 200;

        Object [][]myData = {
            {"\"1\"","\"1\"","\"1\""},
            {2,2,2},
            {3,3,3},
            {"\"4\"","\"4\"","\"4\""},
        };
        Integer[] failures = {1, 4};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
        int myBatchSize = 200;

        Object [][]myData = null;
        Integer[] failures = {};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
        int myBatchSize = 200;

        Object [][]myData = {
            {"~\"escapequotes",1,1,1},
            {"~\\nescapenewline",2,2,2},
            {"~'escapeprimesymbol",3,3,3}
        };
        Integer[] failures = {};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
        int myBatchSize = 200;

        Object [][]myData = {
            {"nospace",1,1,1},
            {"   frontspace",2,2,2},
            {"rearspace   ",3,3,3},
            {"\" inquotespace \"   ",4,4,4}
        };
        Integer[] failures = {};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
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
        int myBatchSize = 200;

        Object [][]myData = {
            {"\"openquote",1,1,1},
            {"second",2,2,2},
            {"third",3,3,3},
            {"123456789012345678901",4,4,4}
        };
        Integer[] failures = {4};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
    }

    public void testColumnLimitSize2() throws Exception
    {
        String mySchema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null, " + // column that is partitioned on
                        "clm_string varchar(20), "+
                        "); ";
        int myBatchSize = 200;

        Object [][]myData = {
            {1,"\"Edwr"},
            {"Burnam\"",2,"\"Tabatha"},
            {"Gehling"}
        };

        Integer[] failures = {2, 3};
        ArrayList<Integer> expectedFailures = new ArrayList<Integer>(Arrays.asList(failures));
        test_Interface(mySchema, myData, myBatchSize, expectedFailures, 0);
    }

    //Test multiple tables with Multiple Clients and no errors.
    public void testMultipleTablesWithMultipleClients() throws Exception {
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
                + "); "

                + "create table BLAH2 ("
                + "clm_integer integer not null, "
                + // column that is partitioned on
                  "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        TimestampType currentTime = new TimestampType();
        Object [][] myData1 = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime},
            {2,1,1,11111111,"first",1.10,1.11,currentTime},
            {3,1,1,11111111,"first",1.10,1.11,currentTime},
            {4,1,1,11111111,"first",1.10,1.11,currentTime},
            {5,1,1,11111111,"first",1.10,1.11,currentTime},
            {6,1,1,11111111,"first",1.10,1.11,currentTime},
            {7,1,1,11111111,"first",1.10,1.11,currentTime},
            {8,1,1,11111111,"first",1.10,1.11,currentTime},
            {9,1,1,11111111,"first",1.10,1.11,currentTime},
            {10,1,1,11111111,"first",1.10,1.11,currentTime},
            {11,1,1,11111111,"first",1.10,1.11,currentTime},
            {12,1,1,11111111,"first",1.10,1.11,currentTime},
            {13,1,1,11111111,"first",1.10,1.11,currentTime},
            {14,1,1,11111111,"first",1.10,1.11,currentTime},
            {15,1,1,11111111,"first",1.10,1.11,currentTime},
            {16,1,1,11111111,"first",1.10,1.11,currentTime},
            {17,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize1 = 200;
        Integer[] failures1 = {};
        ArrayList<Integer> expectedFailures1 = new ArrayList<Integer>(Arrays.asList(failures1));
        Object [][] myData2 = {
                {1,"first",1.10,1.11,currentTime},
                {2,"first",1.10,1.11,currentTime},
                {3,"first",1.10,1.11,currentTime},
                {4,"first",1.10,1.11,currentTime},
                {5,"first",1.10,1.11,currentTime},
                {6,"first",1.10,1.11,currentTime},
                {7,"first",1.10,1.11,currentTime},
                {8,"first",1.10,1.11,currentTime},
                {9,"first",1.10,1.11,currentTime},
                {10,"first",1.10,1.11,currentTime},
                {11,"first",1.10,1.11,currentTime},
                {12,"first",1.10,1.11,currentTime},
                {13,"first",1.10,1.11,currentTime},
                {14,"first",1.10,1.11,currentTime},
                {15,"first",1.10,1.11,currentTime},
            };
        int myBatchSize2 = 200;
        Integer[] failures2 = {};
        ArrayList<Integer> expectedFailures2 = new ArrayList<Integer>(Arrays.asList(failures2));
        test_multiplexing( mySchema, true, true, true,
                "BLAH", myData1, myBatchSize1, expectedFailures1, false,
                "BLAH2", myData2, myBatchSize2, expectedFailures2, false);
    }

    //Test multiple tables with no errors (MultiPartition).
    public void testMultipleTablesWithMultipleClientsMP() throws Exception {
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
                + "); "

                + "create table BLAH2 ("
                + "clm_integer integer not null, "
                + // column that is partitioned on
                  "clm_string varchar(20) default null, "
                + "clm_decimal decimal default null, "
                + "clm_float float default null, "
                + "clm_timestamp timestamp default null, "
                + "PRIMARY KEY(clm_integer) "
                + "); ";
        TimestampType currentTime = new TimestampType();
        Object [][] myData1 = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime},
            {2,1,1,11111111,"first",1.10,1.11,currentTime},
            {3,1,1,11111111,"first",1.10,1.11,currentTime},
            {4,1,1,11111111,"first",1.10,1.11,currentTime},
            {5,1,1,11111111,"first",1.10,1.11,currentTime},
            {6,1,1,11111111,"first",1.10,1.11,currentTime},
            {7,1,1,11111111,"first",1.10,1.11,currentTime},
            {8,1,1,11111111,"first",1.10,1.11,currentTime},
            {9,1,1,11111111,"first",1.10,1.11,currentTime},
            {10,1,1,11111111,"first",1.10,1.11,currentTime},
            {11,1,1,11111111,"first",1.10,1.11,currentTime},
            {12,1,1,11111111,"first",1.10,1.11,currentTime},
            {13,1,1,11111111,"first",1.10,1.11,currentTime},
            {14,1,1,11111111,"first",1.10,1.11,currentTime},
            {15,1,1,11111111,"first",1.10,1.11,currentTime},
            {16,1,1,11111111,"first",1.10,1.11,currentTime},
            {17,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize1 = 200;
        Integer[] failures1 = {};
        ArrayList<Integer> expectedFailures1 = new ArrayList<Integer>(Arrays.asList(failures1));
        Object [][] myData2 = {
                {1,"first",1.10,1.11,currentTime},
                {2,"first",1.10,1.11,currentTime},
                {3,"first",1.10,1.11,currentTime},
                {4,"first",1.10,1.11,currentTime},
                {5,"first",1.10,1.11,currentTime},
                {6,"first",1.10,1.11,currentTime},
                {7,"first",1.10,1.11,currentTime},
                {8,"first",1.10,1.11,currentTime},
                {9,"first",1.10,1.11,currentTime},
                {10,"first",1.10,1.11,currentTime},
                {11,"first",1.10,1.11,currentTime},
                {12,"first",1.10,1.11,currentTime},
                {13,"first",1.10,1.11,currentTime},
                {14,"first",1.10,1.11,currentTime},
                {15,"first",1.10,1.11,currentTime},
            };
        int myBatchSize2 = 200;
        Integer[] failures2 = {};
        ArrayList<Integer> expectedFailures2 = new ArrayList<Integer>(Arrays.asList(failures2));
        test_multiplexing( mySchema, true, true, true,
                "BLAH", myData1, myBatchSize1, expectedFailures1, false,
                "BLAH2", myData2, myBatchSize2, expectedFailures2, false);
    }

    //Test single table with multiple clients.
    public void testSingleTableOnMultipleClients() throws Exception {
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
        TimestampType currentTime = new TimestampType();
        Object [][] myData1 = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime},
            {2,1,1,11111111,"first",1.10,1.11,currentTime},
            {3,1,1,11111111,"first",1.10,1.11,currentTime},
            {4,1,1,11111111,"first",1.10,1.11,currentTime},
            {5,1,1,11111111,"first",1.10,1.11,currentTime},
            {6,1,1,11111111,"first",1.10,1.11,currentTime},
            {7,1,1,11111111,"first",1.10,1.11,currentTime},
            {8,1,1,11111111,"first",1.10,1.11,currentTime},
            {9,1,1,11111111,"first",1.10,1.11,currentTime},
            {10,1,1,11111111,"first",1.10,1.11,currentTime},
            {11,1,1,11111111,"first",1.10,1.11,currentTime},
            {12,1,1,11111111,"first",1.10,1.11,currentTime},
            {13,1,1,11111111,"first",1.10,1.11,currentTime},
            {14,1,1,11111111,"first",1.10,1.11,currentTime},
            {15,1,1,11111111,"first",1.10,1.11,currentTime},
            {16,1,1,11111111,"first",1.10,1.11,currentTime},
            {17,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize1 = 200;
        Integer[] failures1 = {};
        ArrayList<Integer> expectedFailures1 = new ArrayList<Integer>(Arrays.asList(failures1));
        Object [][] myData2 = {
            {18,1,1,11111111,"first",1.10,1.11,currentTime},
            {19,1,1,11111111,"first",1.10,1.11,currentTime},
            {20,1,1,11111111,"first",1.10,1.11,currentTime},
            {21,1,1,11111111,"first",1.10,1.11,currentTime},
            {22,1,1,11111111,"first",1.10,1.11,currentTime},
            {23,1,1,11111111,"first",1.10,1.11,currentTime},
            {24,1,1,11111111,"first",1.10,1.11,currentTime},
            {25,1,1,11111111,"first",1.10,1.11,currentTime},
            {26,1,1,11111111,"first",1.10,1.11,currentTime},
            {27,1,1,11111111,"first",1.10,1.11,currentTime},
            {28,1,1,11111111,"first",1.10,1.11,currentTime},
            {29,1,1,11111111,"first",1.10,1.11,currentTime},
            {30,1,1,11111111,"first",1.10,1.11,currentTime},
            {31,1,1,11111111,"first",1.10,1.11,currentTime},
            {32,1,1,11111111,"first",1.10,1.11,currentTime},
            {33,1,1,11111111,"first",1.10,1.11,currentTime},
            {34,1,1,11111111,"first",1.10,1.11,currentTime},
            {35,1,1,11111111,"first",1.10,1.11,currentTime},
            {36,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize2 = 3;
        Integer[] failures2 = {};
        ArrayList<Integer> expectedFailures2 = new ArrayList<Integer>(Arrays.asList(failures2));
        test_multiplexing( mySchema, true, true, false,
                "BLAH", myData1, myBatchSize1, expectedFailures1, false,
                "BLAH", myData2, myBatchSize2, expectedFailures2, false);
    }

    //Test single table with single loader.
    public void testSingleTableOnSingleLoader() throws Exception {
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
        TimestampType currentTime = new TimestampType();
        Object [][] myData1 = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime},
            {2,1,1,11111111,"first",1.10,1.11,currentTime},
            {3,1,1,11111111,"first",1.10,1.11,currentTime},
            {4,1,1,11111111,"first",1.10,1.11,currentTime},
            {5,1,1,11111111,"first",1.10,1.11,currentTime},
            {6,1,1,11111111,"first",1.10,1.11,currentTime},
            {7,1,1,11111111,"first",1.10,1.11,currentTime},
            {8,1,1,11111111,"first",1.10,1.11,currentTime},
            {9,1,1,11111111,"first",1.10,1.11,currentTime},
            {10,1,1,11111111,"first",1.10,1.11,currentTime},
            {11,1,1,11111111,"first",1.10,1.11,currentTime},
            {12,1,1,11111111,"first",1.10,1.11,currentTime},
            {13,1,1,11111111,"first",1.10,1.11,currentTime},
            {14,1,1,11111111,"first",1.10,1.11,currentTime},
            {15,1,1,11111111,"first",1.10,1.11,currentTime},
            {16,1,1,11111111,"first",1.10,1.11,currentTime},
            {17,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize1 = 200;
        Integer[] failures1 = {};
        ArrayList<Integer> expectedFailures1 = new ArrayList<Integer>(Arrays.asList(failures1));
        Object [][] myData2 = {
            {18,1,1,11111111,"first",1.10,1.11,currentTime},
            {19,1,1,11111111,"first",1.10,1.11,currentTime},
            {20,1,1,11111111,"first",1.10,1.11,currentTime},
            {21,1,1,11111111,"first",1.10,1.11,currentTime},
            {22,1,1,11111111,"first",1.10,1.11,currentTime},
            {23,1,1,11111111,"first",1.10,1.11,currentTime},
            {24,1,1,11111111,"first",1.10,1.11,currentTime},
            {25,1,1,11111111,"first",1.10,1.11,currentTime},
            {26,1,1,11111111,"first",1.10,1.11,currentTime},
            {27,1,1,11111111,"first",1.10,1.11,currentTime},
            {28,1,1,11111111,"first",1.10,1.11,currentTime},
            {29,1,1,11111111,"first",1.10,1.11,currentTime},
            {30,1,1,11111111,"first",1.10,1.11,currentTime},
            {31,1,1,11111111,"first",1.10,1.11,currentTime},
            {32,1,1,11111111,"first",1.10,1.11,currentTime},
            {33,1,1,11111111,"first",1.10,1.11,currentTime},
            {34,1,1,11111111,"first",1.10,1.11,currentTime},
            {35,1,1,11111111,"first",1.10,1.11,currentTime},
            {36,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize2 = 5;
        Integer[] failures2 = {};
        ArrayList<Integer> expectedFailures2 = new ArrayList<Integer>(Arrays.asList(failures2));
        test_multiplexing( mySchema, false, false, false,
                "BLAH", myData1, myBatchSize1, expectedFailures1, false,
                "BLAH", myData2, myBatchSize2, expectedFailures2, false);
    }

    //Test single table with single loader (MultiPartition).
    public void testSingleTableOnSingleLoaderMP() throws Exception {
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
        TimestampType currentTime = new TimestampType();
        Object [][] myData1 = {
            {1,1,1,11111111,"first",1.10,1.11,currentTime},
            {2,1,1,11111111,"first",1.10,1.11,currentTime},
            {3,1,1,11111111,"first",1.10,1.11,currentTime},
            {4,1,1,11111111,"first",1.10,1.11,currentTime},
            {5,1,1,11111111,"first",1.10,1.11,currentTime},
            {6,1,1,11111111,"first",1.10,1.11,currentTime},
            {7,1,1,11111111,"first",1.10,1.11,currentTime},
            {8,1,1,11111111,"first",1.10,1.11,currentTime},
            {9,1,1,11111111,"first",1.10,1.11,currentTime},
            {10,1,1,11111111,"first",1.10,1.11,currentTime},
            {11,1,1,11111111,"first",1.10,1.11,currentTime},
            {12,1,1,11111111,"first",1.10,1.11,currentTime},
            {13,1,1,11111111,"first",1.10,1.11,currentTime},
            {14,1,1,11111111,"first",1.10,1.11,currentTime},
            {15,1,1,11111111,"first",1.10,1.11,currentTime},
            {16,1,1,11111111,"first",1.10,1.11,currentTime},
            {17,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize1 = 200;
        Integer[] failures1 = {};
        ArrayList<Integer> expectedFailures1 = new ArrayList<Integer>(Arrays.asList(failures1));
        Object [][] myData2 = {
            {18,1,1,11111111,"first",1.10,1.11,currentTime},
            {19,1,1,11111111,"first",1.10,1.11,currentTime},
            {20,1,1,11111111,"first",1.10,1.11,currentTime},
            {21,1,1,11111111,"first",1.10,1.11,currentTime},
            {22,1,1,11111111,"first",1.10,1.11,currentTime},
            {23,1,1,11111111,"first",1.10,1.11,currentTime},
            {24,1,1,11111111,"first",1.10,1.11,currentTime},
            {25,1,1,11111111,"first",1.10,1.11,currentTime},
            {26,1,1,11111111,"first",1.10,1.11,currentTime},
            {27,1,1,11111111,"first",1.10,1.11,currentTime},
            {28,1,1,11111111,"first",1.10,1.11,currentTime},
            {29,1,1,11111111,"first",1.10,1.11,currentTime},
            {30,1,1,11111111,"first",1.10,1.11,currentTime},
            {31,1,1,11111111,"first",1.10,1.11,currentTime},
            {32,1,1,11111111,"first",1.10,1.11,currentTime},
            {33,1,1,11111111,"first",1.10,1.11,currentTime},
            {34,1,1,11111111,"first",1.10,1.11,currentTime},
            {35,1,1,11111111,"first",1.10,1.11,currentTime},
            {36,1,1,11111111,"first",1.10,1.11,currentTime}
        };
        int myBatchSize2 = 5;
        Integer[] failures2 = {};
        ArrayList<Integer> expectedFailures2 = new ArrayList<Integer>(Arrays.asList(failures2));
        test_multiplexing( mySchema, false, false, true,
                "BLAH", myData1, myBatchSize1, expectedFailures1, false,
                "BLAH", myData2, myBatchSize2, expectedFailures2, false);
    }

    public void test_Interface(String my_schema, Object[][] my_data,
            int my_batchSize, ArrayList<Integer> expectedFailList, int flushInterval) throws Exception {
        test_Interface(my_schema, my_data,
                my_batchSize, expectedFailList, flushInterval, false);
    }

    public void test_Interface(String my_schema, Object[][] my_data,
            int my_batchSize, ArrayList<Integer> expectedFailList, int flushInterval, boolean upsert) throws Exception {
        try{
            pathToCatalog = Configuration.getPathToCatalogForTest("vbl.jar");
            pathToDeployment = Configuration.getPathToCatalogForTest("vbl.xml");
            builder = new VoltProjectBuilder();

            builder.addLiteralSchema(my_schema);
            builder.addPartitionInfo("BLAH", "clm_integer");
            boolean success = builder.compile(pathToCatalog, 2, 1, 0);
            assertTrue(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
            config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            localServer = new ServerThread(config);
            client1 = null;

            localServer.start();
            localServer.waitForInitialization();

            client1 = ClientFactory.createClient();
            client1.createConnection("localhost");

            prepare();
            TestFailureCallback testCallback = new TestFailureCallback();
            VoltBulkLoader bulkLoader = client1.getNewBulkLoader("BLAH", my_batchSize, upsert, testCallback);
            if (flushInterval > 0) {
                bulkLoader.setFlushInterval(0, flushInterval);
            }
            // do the test

            VoltTable modCount;
            modCount = client1.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);

            // Call validate partitioning to check if we are good.
            VoltTable valTable;
            valTable = client1.callProcedure("@ValidatePartitioning", (Object)null).getResults()[0];
            System.out.println("Validate for BLAH:\n" + valTable);
            while (valTable.advanceRow()) {
                long miscnt = valTable.getLong("MISPARTITIONED_ROWS");
                assertEquals(miscnt, 0);
            }

            int rowCnt=1;
            try{
                for (Object[] nextRow : my_data) {
                    Integer rowId = new Integer(rowCnt);
                    bulkLoader.insertRow(rowId, nextRow);
                    rowCnt++;
                    if (flushInterval <= 0 && (rnd.nextInt() % 30 == 0)) {
                        //  Randomly inject a flush if no timer flush is involved.
                        bulkLoader.flush();
                    }
                }
            }
            catch( Exception e) {
                System.err.print( e.getMessage() );
            }
            System.out.println(String.format("Attempted inserting %d rows", --rowCnt));

            if (flushInterval <= 0 && rnd.nextBoolean()) {
                // One in 10 tests generate a sync and VoltBulkLoader internal state verification
                bulkLoader.drain();
                assertEquals(0, bulkLoader.getOutstandingRowCount());
                assertEquals(rowCnt, bulkLoader.getCompletedRowCount());
            }
            if (flushInterval > 0) {
                //Lets get timerFlush in
                Thread.sleep(flushInterval + 500);
                bulkLoader.drain();
                //We should have everything processed callbacked.
                assertEquals(0, bulkLoader.getOutstandingRowCount());
                assertEquals(rowCnt, bulkLoader.getCompletedRowCount());
            }

            bulkLoader.close();
            assertEquals(rowCnt, bulkLoader.getCompletedRowCount());
            assertTrue(testCallback.failureRowListMatches(expectedFailList));
        }
        finally {
            if (client1 != null) client1.close();
            client1 = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }

    public void test_multiplexing( String my_schema, boolean multipleClients, boolean multipleLoaders, boolean multiPartTable,
            String my_tableName1, Object[][] my_data1, int my_batchSize1, ArrayList<Integer> expectedFailList1, boolean abort1,
            String my_tableName2, Object[][] my_data2, int my_batchSize2, ArrayList<Integer> expectedFailList2, boolean abort2) throws Exception {
            test_multiplexing(my_schema, multipleClients, multipleLoaders, multiPartTable,
                    my_tableName1, my_data1, my_batchSize1, expectedFailList1, abort1, false,
                    my_tableName2, my_data2, my_batchSize2, expectedFailList2, abort2, false);
    }

    public void test_multiplexing( String my_schema, boolean multipleClients, boolean multipleLoaders, boolean multiPartTable,
            String my_tableName1, Object[][] my_data1, int my_batchSize1, ArrayList<Integer> expectedFailList1, boolean abort1, boolean upsert1,
            String my_tableName2, Object[][] my_data2, int my_batchSize2, ArrayList<Integer> expectedFailList2, boolean abort2, boolean upsert2) throws Exception {
        try{
            assert(!(abort1 && abort2));
            if (abort1 || abort2)
                // No point in testing abort with a single loader
                assert(multipleLoaders);

            boolean sameTable = startServer(my_schema, multiPartTable, my_tableName1, my_tableName2);

            client1 = null;
            client2 = null;
            client1 = ClientFactory.createClient();
            client1.createConnection("localhost");
            if (multipleClients) {
                client2 = ClientFactory.createClient();
                client2.createConnection("localhost");
                // This is implicit
                multipleLoaders = true;
            }
            else
                client2 = client1;

            prepare();
            TestFailureCallback testCallback1 = new TestFailureCallback();
            TestFailureCallback testCallback2 = new TestFailureCallback();

            VoltBulkLoader bulkLoader1 = client1.getNewBulkLoader(my_tableName1, my_batchSize1, upsert1, testCallback1);
            VoltBulkLoader bulkLoader2;
            if (multipleLoaders) {
                bulkLoader2 = client2.getNewBulkLoader(my_tableName2, my_batchSize2, upsert2, testCallback2);
                if (!multipleClients && sameTable) {
                    assert(bulkLoader1.getMaxBatchSize() == Math.min(my_batchSize1, my_batchSize2));
                    assert(bulkLoader1.getMaxBatchSize() == bulkLoader2.getMaxBatchSize());
                }
            }
            else
                bulkLoader2 = bulkLoader1;

            // do the test

            VoltTable modCount1;
            modCount1 = client1.callProcedure("@AdHoc", "SELECT * FROM " + my_tableName1 + ";").getResults()[0];
            System.out.println("data inserted to table " + my_tableName1 + ":\n" + modCount1);

            // Call validate partitioning to check if we are good.
            VoltTable valTable1;
            valTable1 = client1.callProcedure("@ValidatePartitioning", (Object)null).getResults()[0];
            System.out.println("Validate for " + my_tableName1 + ":\n" + valTable1);
            while (valTable1.advanceRow()) {
                long miscnt = valTable1.getLong("MISPARTITIONED_ROWS");
                assertEquals(miscnt, 0);
            }

            if (multipleClients) {
                VoltTable modCount2;
                modCount2 = client2.callProcedure("@AdHoc", "SELECT * FROM " + my_tableName2 + ";").getResults()[0];
                System.out.println("data inserted to table " + my_tableName2 + ":\n" + modCount2);

                // Call validate partitioning to check if we are good.
                VoltTable valTable2;
                valTable2 = client2.callProcedure("@ValidatePartitioning", (Object)null).getResults()[0];
                System.out.println("Validate for " + my_tableName1 + ":\n" + valTable2);
                while (valTable2.advanceRow()) {
                    long miscnt = valTable2.getLong("MISPARTITIONED_ROWS");
                    assertEquals(miscnt, 0);
                }
            }

            int rowCnt1=1;
            int rowCnt2=1;
            try{
                while (rowCnt1 <= my_data1.length || rowCnt2 <= my_data2.length) {
                    if (rowCnt1 <= my_data1.length) {
                        Integer rowId = new Integer(rowCnt1);
                        bulkLoader1.insertRow(rowId, my_data1[rowCnt1-1]);
                        rowCnt1++;
//                      if (rnd.nextInt() % 30 == 0)
//                          //  Randomly inject a flush
//                          bulkLoader1.flush();
                    }
                    if (rowCnt2 <= my_data2.length) {
                        Integer rowId = new Integer(rowCnt2);
                        bulkLoader2.insertRow(rowId, my_data2[rowCnt2-1]);
                        rowCnt2++;
//                      if (rnd.nextInt() % 30 == 0)
//                          //  Randomly inject a flush
//                          bulkLoader2.flush();
                    }
                }
            }
            catch( Exception e) {
                System.err.print( e.getMessage() );
            }
            System.out.println(String.format("Attempted inserting %d rows in Table %s and %d rows in Table %s",
                    --rowCnt1, my_tableName1, --rowCnt2, my_tableName2));

            if (!abort1 && rnd.nextInt() % 4 == 0) {
                // One in 4 tests generate a sync and VoltBulkLoader internal state verification
                bulkLoader1.drain();
                assert(bulkLoader1.getOutstandingRowCount() == 0);
                assert(bulkLoader1.getCompletedRowCount() == rowCnt1);
            }
            if (multipleLoaders && !abort2 && rnd.nextInt() % 4 == 0) {
                bulkLoader2.drain();
                assert(bulkLoader2.getOutstandingRowCount() == 0);
                assert(bulkLoader2.getCompletedRowCount() == rowCnt2);
            }

            bulkLoader1.close();
            if (multipleLoaders)
                bulkLoader2.close();
            assert(abort1 || testCallback1.failureRowListMatches(expectedFailList1));
            assert(abort2 || testCallback2.failureRowListMatches(expectedFailList2));
        }
        finally {
            if (client1 != null) {
                client1.close();
                if (multipleClients && client2 != null) {
                    client2.close();
                    client2 = null;
                }
                client1 = null;
            }

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }

    private boolean startServer(String my_schema, boolean multiPartTable, String my_tableName1, String my_tableName2) throws Exception
    {
        pathToCatalog = Configuration.getPathToCatalogForTest("vbl.jar");
        pathToDeployment = Configuration.getPathToCatalogForTest("vbl.xml");
        builder = new VoltProjectBuilder();

        builder.addLiteralSchema(my_schema);
        boolean sameTable = my_tableName1.equals(my_tableName2);

        if (!multiPartTable) {
            builder.addPartitionInfo(my_tableName1, "clm_integer");
            if (!sameTable)
                builder.addPartitionInfo(my_tableName2, "clm_integer");
        }
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
        config = new Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();
        return sameTable;
    }

    // ENG-11823
    public void testConcurrentLoaders() throws Exception {
        startServer("create table test1 (c1 int);", true, "test1", "test1");

        int threadNum = 2;
        ClientConfig config = new ClientConfig();
        Client client = ClientFactory.createClient(config);
        VoltBulkLoader[] bulkLoaders = new VoltBulkLoader[threadNum];
        try {
            client.createConnection("localhost");

            Thread[] threads = new Thread[threadNum];
            for (int i = 0; i < threadNum; i++) {
                bulkLoaders[i] = client.getNewBulkLoader("test1", 50, new BulkLoaderFailureCallBack() {

                    @Override
                    public void failureCallback(Object arg0, Object[] arg1, ClientResponse arg2)
                    {
                        System.out.println("Insert failed: " + arg0);
                    }

                });
                Runnable runnable = new MyRunnable(bulkLoaders[i]);
                threads[i] = new Thread(runnable, "loaderThread-" + i);
                threads[i].start();
            }

            for (int i = 0; i < threadNum; i++) {
                threads[i].join();
            }

        } finally {
            for (int i = 0; i < threadNum; i++) {
                bulkLoaders[i].close();
            }
            client.close();

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;
        }
    }

    static class MyRunnable implements Runnable {
        private VoltBulkLoader loader;
        private Random random;

        public MyRunnable(VoltBulkLoader loader) {
            this.loader = loader;
            this.random = new Random();
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 1000; i++) {
                    for (int j = 0; j < 50; j++) {
                        int obj = random.nextInt();
                        loader.insertRow(obj, obj);
                    }
                    loader.drain();
                    Thread.sleep(5 + Math.abs(random.nextInt()) % 5);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(Thread.currentThread().getName() + " finish.");
        }
    }
}
