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

package org.voltdb;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdHocQueries extends TestCase {

    public void testSimple() throws Exception {
        String simpleSchema =
            "create table BLAH (" +
            "IVAL bigint default 0 not null, " +
            "TVAL timestamp default null," +
            "DVAL decimal default null," +
            "PRIMARY KEY(IVAL));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("BLAH", "IVAL");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
        builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.002', 5);");
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("adhoc.jar"), 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("adhoc.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("adhoc.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        // do the test
        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        VoltTable modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);

        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        // test single-partition stuff
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 0);
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;", 1).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        try {
            client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (0, 0, 0);", 1);
            fail("Badly partitioned insert failed to throw expected exception");
        }
        catch (Exception e) {}

        try {
            client.callProcedure("@AdHoc", "SLEECT * FROOM NEEEW_OOORDERERER;");
            fail("Bad SQL failed to throw expected exception");
        }
        catch (Exception e) {}

        // try a huge bigint literal
        modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26', 5);").getResults()[0];
        modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL = '2011-06-24 10:30:26';").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL > '2011-06-24 10:30:25';").getResults()[0];
        assertEquals(2, result.getRowCount());
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL < '2011-06-24 10:30:27';").getResults()[0];
        System.out.println(result.toString());
        // We inserted a 1,1,1 row way earlier
        assertEquals(2, result.getRowCount());

        // try a decimal calculation (ENG-1093)
        modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (2, '2011-06-24 10:30:26', 1.12345*1);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 2;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        localServer.shutdown();
        localServer.join();
    }
}
