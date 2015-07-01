/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.types;

import java.security.MessageDigest;
import java.util.Arrays;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestBlobType extends TestCase {
    public void testVarbinary() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "b varbinary(256) default null, " +
            "s varchar(256) default null," +
            "bs varbinary(2) default null," +
            "PRIMARY KEY(ival));\n" +
            "create index idx on blah (ival,s);";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("blah", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?, ?);", null);
        builder.addStmtProcedure("Select", "select * from blah;", null);
        builder.addStmtProcedure("Update", "update blah set b = ? where ival = ?", null);
        builder.addStmtProcedure("FindString", "select * from blah where ival = ? and s = ?", null);
        builder.addStmtProcedure("LiteralUpdate", "update blah set b = '0a1A' where ival = 5", null);
        builder.addStmtProcedure("LiteralInsert", "insert into blah values (13, 'aabbcc', 'hi', 'aabb');", null);
        builder.addProcedures(VarbinaryStringLookup.class);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("binarytest.jar"), 1, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("binarytest.xml"));

        ServerThread localServer = null;
        Client client = null;

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("binarytest.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("binarytest.xml");
            config.m_backend = BackendTarget.NATIVE_EE_JNI;
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            // insert data
            ClientResponse cr = client.callProcedure("Insert", 5, new byte[] { 'a', 'b', 'c', 'd' }, "hi", new byte[] { 'a' });
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

            // make sure strings as bytes works
            cr = client.callProcedure("FindString", 5, "hi".getBytes("UTF-8"));
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(1, cr.getResults()[0].getRowCount());
            cr = client.callProcedure("VarbinaryStringLookup", 5, "hi".getBytes("UTF-8"), "hi");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(1, cr.getResults()[0].getRowCount());
            assertEquals(1, cr.getResults()[1].getRowCount());

            // literal update
            cr = client.callProcedure("LiteralUpdate");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(1, cr.getResults()[0].getRowCount());
            assertEquals(1, cr.getResults()[0].asScalarLong());

            // see if we can get the binary value from the '0a1A' update above
            cr = client.callProcedure("Select");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            VoltTable t = cr.getResults()[0];
            assertEquals(1, t.getRowCount());
            t.resetRowPosition();
            t.advanceRow();
            byte[] vb = t.getVarbinary("b");
            assertEquals(2, vb.length);
            assertEquals((byte) 10, vb[0]);
            assertEquals((byte) 26, vb[1]);

            // try again with generic call
            vb = (byte[]) t.get("b", VoltType.VARBINARY);
            assertEquals(2, vb.length);
            assertEquals((byte) 10, vb[0]);
            assertEquals((byte) 26, vb[1]);

            // insert hex data
            cr = client.callProcedure("Insert", 9, "aabbccdd", "hi", "aabb");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

            // literal inserts
            cr = client.callProcedure("LiteralInsert");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(1, cr.getResults()[0].getRowCount());
            assertEquals(1, cr.getResults()[0].asScalarLong());

            // adhoc queries
            cr = client.callProcedure("@AdHoc", "update blah set b = 'Bb01' where ival = 5");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(1, cr.getResults()[0].getRowCount());
            assertEquals(1, cr.getResults()[0].asScalarLong());
            cr = client.callProcedure("@AdHoc", "insert into blah values (12, 'aabbcc', 'hi', 'aabb');");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(1, cr.getResults()[0].getRowCount());
            assertEquals(1, cr.getResults()[0].asScalarLong());

            // try bad value insert for normal query
            try {
                cr = client.callProcedure("Insert", 6, new byte[] { 'a' }, "hi", new byte[] { 'a', 'b', 'c' });
                fail();
            }
            catch (ProcCallException e) {}

            // try invalid hex literal strings in adhoc query
            try {
                cr = client.callProcedure("@AdHoc", "update blah set b = 'Bb01nt' where ival = 5");
                fail();
            }
            catch (ProcCallException e) {}
            try {
                cr = client.callProcedure("@AdHoc", "update blah set b = 'Bb0' where ival = 5");
                fail();
            }
            catch (ProcCallException e) {}

            // test invalid comparison
            try {
                cr = client.callProcedure("@AdHoc", "update blah set ival = 5 where b = 'Bb01'");
                fail();
            }
            catch (ProcCallException e) {}

            // test too long varbinary
            byte[] overlong = new byte[VoltType.MAX_VALUE_LENGTH + 1];
            try {
                cr = client.callProcedure("Insert", 6, new byte[] { 'a' }, "hi", overlong);
                fail();
            }
            catch (ProcCallException e) {}
        }
        finally {
            // stop execution
            if (client != null) {
                client.close();
            }
            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
        }
    }

    public void testIndexRejection() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "b varbinary(256) default null, " +
            "PRIMARY KEY(ival));\n" +
            "create index idx on blah (ival,b);";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("blah", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?);", null);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("binarytest.jar"), 1, 1, 0);
        assertFalse(success);
    }

    public void testTPCCCustomerLookup() throws Exception {

        // constants used int the benchmark
        final short W_ID = 3;
        final byte D_ID = 7;
        final int C_ID = 42;

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(TPCCProjectBuilder.ddlURL);
        for (String pair[] : TPCCProjectBuilder.partitioning) {
            builder.addPartitionInfo(pair[0], pair[1]);
        }
        builder.addStmtProcedure("InsertCustomer", "INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "CUSTOMER.C_W_ID: 2");
        builder.addStmtProcedure("Fake1", "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_LAST = ? AND C_D_ID = ? AND C_W_ID = ? ORDER BY C_FIRST;");

        builder.addProcedures(FakeCustomerLookup.class);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("binarytest2.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("binarytest2.xml"));

        ServerThread localServer = null;
        Client client = null;

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("binarytest2.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("binarytest2.xml");
            config.m_backend = BackendTarget.NATIVE_EE_JNI;
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            // insert data
            // long c_id, long c_d_id, long c_w_id, String c_first, String c_middle,
            // String c_last, String c_street_1, String c_street_2, String d_city,
            // String d_state, String d_zip, String c_phone, Date c_since, String
            // c_credit, double c_credit_lim, double c_discount, double c_balance,
            // double c_ytd_payment, double c_payment_cnt, double c_delivery_cnt,
            // String c_data
            final double initialBalance = 15.75;
            final double initialYTD = 15241.45;
            VoltTable customer1 = client.callProcedure("InsertCustomer", C_ID, D_ID,
                    W_ID, "I", "Be", "lastname", "Place", "Place2", "BiggerPlace",
                    "AL", "91083", "(193) 099 - 9082", new TimestampType(), "BC",
                    19298943.12, .13, initialBalance, initialYTD, 0L, 15L,
                    "Some History").getResults()[0];
            // check for successful insertion.
            assertEquals(1L, customer1.asScalarLong());

            VoltTable customer2 = client.callProcedure("InsertCustomer", C_ID + 1,
                    D_ID, W_ID, "We", "R", "Customer", "Random Department",
                    "Place2", "BiggerPlace", "AL", "13908", "(913) 909 - 0928",
                    new TimestampType(), "GC", 19298943.12, .13, initialBalance, initialYTD,
                    1L, 15L, "Some History").getResults()[0];
            // check for successful insertion.
            assertEquals(1L, customer2.asScalarLong());

            VoltTable customer3 = client.callProcedure("InsertCustomer", C_ID + 2,
                    D_ID, W_ID, "Who", "Is", "Customer", "Receiving",
                    "450 Mass F.X.", "BiggerPlace", "CI", "91083",
                    "(541) 931 - 0928", new TimestampType(), "GC", 19899324.21, .13,
                    initialBalance, initialYTD, 2L, 15L, "Some History").getResults()[0];
            // check for successful insertion.
            assertEquals(1L, customer3.asScalarLong());

            VoltTable customer4 = client.callProcedure("InsertCustomer", C_ID + 3,
                    D_ID, W_ID, "ICanBe", "", "Customer", "street", "place",
                    "BiggerPlace", "MA", "91083", "(913) 909 - 0928", new TimestampType(),
                    "GC", 19298943.12, .13, initialBalance, initialYTD, 3L, 15L,
                    "Some History").getResults()[0];
            // check for successful insertion.
            assertEquals(1L, customer4.asScalarLong());

            // make sure strings as bytes works
            ClientResponse cr = client.callProcedure("Fake1", "Customer".getBytes("UTF-8"), D_ID, W_ID);
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(3, cr.getResults()[0].getRowCount());

            cr = client.callProcedure("Fake1", "Customer", D_ID, W_ID);
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            assertEquals(3, cr.getResults()[0].getRowCount());

            cr = client.callProcedure("FakeCustomerLookup", W_ID, W_ID, D_ID, "Customer".getBytes("UTF-8"));
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        }
        finally {
            // stop execution
            if (client != null) {
                client.close();
            }
            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
        }
    }

    public void testBigFatBlobs() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "b varbinary(256) default null, " +
            "s varchar(256) default null, " +
            "PRIMARY KEY(ival));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addProcedures(BigFatBlobAndStringMD5.class);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("bigfatblobs.jar"), 1, 1, 0);
        assertTrue("Failed to compile catalog", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("bigfatblobs.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("bigfatblobs.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("bigfatblobs.xml");
        config.m_backend = BackendTarget.NATIVE_EE_JNI;

        ServerThread localServer = null;
        Client client = null;

        try {
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            byte[] b = new byte[5000000];
            char[] c = new char[5000000];
            for (int i = 0; i < b.length; i++) {
                b[i] = (byte) (i % 256);
                c[i] = (char) (i % 128);
            }
            String s = new String(c);
            ClientResponse cr = client.callProcedure("BigFatBlobAndStringMD5", b, s);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            VoltTable t = cr.getResults()[0];
            assertEquals(1, t.getRowCount());
            assertTrue(t.advanceRow());
            // Validate MD5 sums instead of the actual data. The returned VoltTable
            // can't hold it anyway due to a 1 MB limit.
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            assertTrue(Arrays.equals(md5.digest(b), t.getVarbinary("b_md5")));
            assertTrue(Arrays.equals(md5.digest(s.getBytes()), t.getVarbinary("s_md5")));
        }
        finally {
            // stop execution
            if (client != null) {
                client.close();
            }
            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
        }
    }
}
