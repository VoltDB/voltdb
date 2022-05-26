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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;

import org.voltdb.ClientResponseImpl;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;

public class TestTwoPartitionProcs extends TestCase {
    private String nothing_jar;
    private String testout_jar;

    @Override
    public void setUp() {
        nothing_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "nothing.jar";
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    public void tearDown() {
        File njar = new File(nothing_jar);
        njar.delete();
        File tjar = new File(testout_jar);
        tjar.delete();
    }

    // get the first stats table for any selector
    final VoltTable getStats(Client client, String selector) {
        ClientResponse response = null;
        try {
            response = client.callProcedure("@Statistics", selector);
        } catch (IOException | ProcCallException e) {
            fail();
        }
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        return response.getResults()[0];
    }

    private boolean compileDDL(String ddl, VoltCompiler compiler) {
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        String schemaPath = schemaFile.getPath();

        return compiler.compileFromDDL(testout_jar, schemaPath);
    }

    public static class TwoPartitionProcA extends VoltProcedure {
        public long run(long partitionValueA, long partitionValueB) {
            return 0;
        }
    }

    public void testCompile() throws IOException {
        String ddl = "CREATE TABLE BLAH ( PKEY BIGINT NOT NULL, NUM INTEGER, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE BLAH ON COLUMN PKEY;" +
                "create procedure partition on table BLAH column pkey and on table BLAH column pkey from class org.voltdb.compiler.TestTwoPartitionProcs$TwoPartitionProcA;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(ddl, compiler);
        assertTrue("compilation failed", success);

        for (Feedback fb : compiler.m_warnings) {
            System.out.println(fb.getStandardFeedbackLine());
        }
    }

    public void testRun() throws Exception {
        String ddl = "CREATE TABLE BLAH ( PKEY BIGINT NOT NULL, NUM INTEGER, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE BLAH ON COLUMN PKEY;" +
                "create procedure partition on table BLAH column pkey and on table BLAH column pkey from class org.voltdb.compiler.TestTwoPartitionProcs$TwoPartitionProcA;";

        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
        pb.setHTTPDPort(8080);
        pb.setJSONAPIEnabled(true);
        boolean success = pb.compile(Configuration.getPathToCatalogForTest("compileNT.jar"));
        assertTrue("compilation failed", success);
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("compileNT.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("compileNT.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("compileNT.xml");
        config.m_sitesperhost = 12;
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response;

        response = (ClientResponseImpl) client.callProcedure("TestTwoPartitionProcs$TwoPartitionProcA", 93, 57);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println(statsT.toFormattedString());

        //Thread.sleep(1000000);

        client.drain();
        client.close();

        localServer.shutdown();
        localServer.join();
    }

}
