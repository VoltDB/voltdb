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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.BuildDirectoryUtils;

public class MilestoneOneExecutionTest extends TestCase {

    private ServerThread server;
    private Client client;

    public void testMilestoneOneSeparate() throws InterruptedException, IOException, ProcCallException {
        // call the insert procedure
        VoltTable[] results = client.callProcedure("MilestoneOneInsert", 99L, "TEST").getResults();
        // check one table was returned
        assertEquals(1, results.length);
        // check one tuple was modified
        assertEquals(1, results[0].asScalarLong());

        System.out.println("JUNIT: Ran first statement to completion.");

        // call the select procedure
        results = client.callProcedure("MilestoneOneSelect", 99L).getResults();
        // check one table was returned
        assertTrue(results.length == 1);
        // check one tuple was modified
        VoltTable result = results[0];
        VoltTableRow row = result.fetchRow(0);
        String resultStr = row.getString(0);
        assertTrue(resultStr.equals("TEST"));
    }

    @Override
    public void setUp() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "milestoneOneCatalog.jar";
        // start VoltDB server
        server = new ServerThread(catalogJar, BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();

        // run the test
        ClientConfig config = new ClientConfig("program", "none");
        client = ClientFactory.createClient(config);
        client.createConnection("localhost");
    }

    @Override
    public void tearDown() throws InterruptedException {
        server.shutdown();
        server = null;
        client = null;
    }

    /*public void testMilestoneOneCombined() {
        // start VoltDB server
        ServerThread server = new ServerThread("milestoneOneCatalog.jar", true);
        server.start();
        server.waitForInitialization();

        // run the test
        VoltClient client = VoltClient.createConnection("localhost", "program", "none");
        try {
            // call the insert procedure
            VoltTable[] results = client.callProcedure("MilestoneOneCombined", 99L, "TEST");
            // check one table was returned
            assertTrue(results.length == 1);
            // check one tuple was modified
            VoltTable result = results[0];
            VoltTableRow row = result.fetchRow(0);
            String resultStr = row.getString(1);
            assertTrue(resultStr.equals("TEST"));

        } catch (ProcCallException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        // stop execution
        server.shutdown();
    }

    public void testMilestoneOneOutOfProcess() {
        // start VoltDB server
        ServerThread server = new ServerThread("milestoneOneCatalog.jar", true);
        server.start();
        server.waitForInitialization();

        // run the test
        VoltClient client = VoltClient.createConnection("localhost", "program", "none");
        try {
            // call the insert procedure
            VoltTable[] results = client.callProcedure("MilestoneOneCombined", 99L, "TEST");
            // check one table was returned
            assertTrue(results.length == 1);
            // check one tuple was modified
            VoltTable result = results[0];
            VoltTableRow row = result.fetchRow(0);
            String resultStr = row.getString(1);
            assertTrue(resultStr.equals("TEST"));

        } catch (ProcCallException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        // stop execution
        server.shutdown();
    }*/
}
