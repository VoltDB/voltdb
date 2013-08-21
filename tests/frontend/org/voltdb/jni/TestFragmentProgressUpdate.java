/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
package org.voltdb.jni;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import junit.framework.TestCase;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.sysprocs.LoadSinglepartitionTable;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;

public class TestFragmentProgressUpdate extends TestCase {

    private String pathToCatalog;
    private String pathToDeployment;
    private ServerThread localServer;
    private VoltDB.Configuration config;
    private VoltProjectBuilder builder;
    private Client client;
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    private String userName = System.getProperty("user.name");
    private String reportDir = String.format("/tmp/%s_csv", userName);
    private String dbName = String.format("mydb_%s", userName);


    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
    }


    public void testFragmentProgressUpdate () throws Exception {
        String my_schema =
                "create table BLAH (" +
                        "clm_integer integer default 0 not null," + // replicated table
                        "clm_string varchar(20) default null" +
                        ");";

        try{
            pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
            pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
            builder = new VoltProjectBuilder();

            builder.addLiteralSchema(my_schema);
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

            VoltTable warehousedata = new VoltTable(
                    new VoltTable.ColumnInfo("clm_integer", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("clm_tinyint", VoltType.STRING)
            );
            for (int i = 0; i < 10001; ++i) {
                warehousedata.addRow(i, "name"+i);
            }
//
//            (new Thread(new LoadSinglepartitionTable(null, null, "BLAH", warehousedata))).start();
            //client.callProcedure("@LoadSinglepartitionTable", "null BLAH warehousedata");

            // do the test
            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            int rowct = modCount.getRowCount();
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
}
