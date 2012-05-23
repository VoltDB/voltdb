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

package org.voltdb;

import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdHocQueries extends TestCase {

    public void testSP() throws Exception {
        String spSchema =
            "create table PARTED1 (" +
            "PARTVAL bigint not null, " +
            "NONPART bigint not null," +
            "PRIMARY KEY(PARTVAL));" +

            "create table PARTED2 (" +
            "PARTVAL bigint not null, " +
            "NONPART bigint not null," +
            "PRIMARY KEY(PARTVAL));" +

            "create table PARTED3 (" +
            "PARTVAL bigint not null, " +
            "NONPART bigint not null," +
            "PRIMARY KEY(NONPART));" +

            "create table REPPED1 (" +
            "REPPEDVAL bigint not null, " +
            "NONPART bigint not null," +
            "PRIMARY KEY(REPPEDVAL));" +

            "create table REPPED2 (" +
            "REPPEDVAL bigint not null, " +
            "NONPART bigint not null," +
            "PRIMARY KEY(REPPEDVAL));";

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocsp.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocsp.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(spSchema);
        builder.addPartitionInfo("PARTED1", "PARTVAL");
        builder.addPartitionInfo("PARTED2", "PARTVAL");
        builder.addPartitionInfo("PARTED3", "PARTVAL");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            // do the test
            client = ClientFactory.createClient();
            client.createConnection("localhost");

            VoltTable modCount;

            modCount = client.callProcedure("@AdHoc", "INSERT INTO PARTED1 VALUES (0, 0);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO PARTED1 VALUES (1, 1);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO PARTED2 VALUES (0, 0);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO PARTED2 VALUES (2, 2);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO PARTED3 VALUES (0, 0);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO PARTED3 VALUES (3, 3);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO REPPED1 VALUES (0, 0);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO REPPED1 VALUES (1, 1);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO REPPED2 VALUES (0, 0);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            modCount = client.callProcedure("@AdHoc", "INSERT INTO REPPED2 VALUES (2, 2);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);

            callAdHoc(client);
            callSPAdHoc(client, "@AdHocSP");
            callSPAdHoc(client, "@AdHoc");

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

    /**
     * @param client
     * @param adHocMaybeSP the system proc name
     * @throws ProcCallException
     * @throws IOException
     * @throws NoConnectionsException
     */
    private void callSPAdHoc(Client client, String adHocMaybeSP) throws NoConnectionsException, IOException, ProcCallException {
        // TODO Auto-generated method stub

        VoltTable result;
        int count0 = 0;
        int count1 = 0;

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1;", 0).getResults()[0];
        count0 = result.getRowCount();
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 WHERE PARTVAL != 0;", 1).getResults()[0];
        count1 = result.getRowCount();
        assertTrue(count0 + count1 == 2);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3;", 0).getResults()[0];
        count0 = result.getRowCount();
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 WHERE PARTVAL != 0;", 1).getResults()[0];
        count1 = result.getRowCount();
        assertTrue(count0 + count1 == 2);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 2);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 WHERE PARTVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 WHERE PARTVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 WHERE REPPEDVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL = 0;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and A.REPPEDVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.REPPEDVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL = A.REPPEDVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = 0 and A.REPPEDVAL = B.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and A.PARTVAL = B.REPPEDVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.REPPEDVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and B.REPPEDVAL = A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

/* NYET
        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());
*/

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL != A.REPPEDVAL;", 0).getResults()[0];
        count0 = result.getRowCount();
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL != A.REPPEDVAL;", 1).getResults()[0];
        count1 = result.getRowCount();
        assertTrue(count0 + count1 == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL != A.PARTVAL;", 0).getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());
    }

    /**
     * @param client
     * @param adHocMaybeSP the system proc name
     * @throws ProcCallException
     * @throws IOException
     * @throws NoConnectionsException
     */
    private void callAdHoc(Client client) throws NoConnectionsException, IOException, ProcCallException {
        // TODO Auto-generated method stub

        String adHocMaybeSP = "@AdHoc"; // only implicitly SP
        VoltTable result;

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());



        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1;").getResults()[0];
        assertTrue(result.getRowCount() == 2);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 WHERE PARTVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 WHERE PARTVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 WHERE REPPEDVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL = 0;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and A.REPPEDVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.REPPEDVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL = A.REPPEDVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = 0 and A.REPPEDVAL = B.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and A.PARTVAL = B.REPPEDVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());


        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.REPPEDVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and B.REPPEDVAL = A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

/* Not SP and not YET supported.
        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());
*/
/* NOT SP
        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL != A.REPPEDVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());
*/

        result = client.callProcedure(adHocMaybeSP, "SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL != A.PARTVAL;").getResults()[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());
    }

    public void testSimple() throws Exception {
        String simpleSchema =
            "create table BLAH (" +
            "IVAL bigint default 0 not null, " +
            "TVAL timestamp default null," +
            "DVAL decimal default null," +
            "PRIMARY KEY(IVAL));";

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhoc.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("BLAH", "IVAL");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
        builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.002', 5);");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            // do the test
            client = ClientFactory.createClient();
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
            modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.123012', 5);").getResults()[0];
            modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);
            result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
            assertTrue(result.getRowCount() == 1);
            System.out.println(result.toString());
            result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL = '2011-06-24 10:30:26.123012';").getResults()[0];
            assertTrue(result.getRowCount() == 1);
            System.out.println(result.toString());
            result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL > '2011-06-24 10:30:25';").getResults()[0];
            assertEquals(2, result.getRowCount());
            System.out.println(result.toString());
            result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL < '2011-06-24 10:30:27';").getResults()[0];
            System.out.println(result.toString());
            // We inserted a 1,1,1 row way earlier
            assertEquals(2, result.getRowCount());

            // try something like the queries in ENG-1242
            try {
                client.callProcedure("@AdHoc", "select * from blah; dfvsdfgvdf select * from blah WHERE IVAL = 1;");
                fail("Bad SQL failed to throw expected exception");
            }
            catch (Exception e) {}
            client.callProcedure("@AdHoc", "select\n* from blah;");

            // try a decimal calculation (ENG-1093)
            modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (2, '2011-06-24 10:30:26', 1.12345*1);").getResults()[0];
            assertTrue(modCount.getRowCount() == 1);
            assertTrue(modCount.asScalarLong() == 1);
            result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 2;").getResults()[0];
            assertTrue(result.getRowCount() == 1);
            System.out.println(result.toString());
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
