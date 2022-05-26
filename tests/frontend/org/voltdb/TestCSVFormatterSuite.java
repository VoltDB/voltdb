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

package org.voltdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;

/**
 * End to end CSV formatter tests using the injected socket importer.
 **/
public class TestCSVFormatterSuite extends TestCSVFormatterSuiteBase {

    public TestCSVFormatterSuite(final String name) {
        super(name);
    }

    private VoltTable tryForResult(Client client, CountDownLatch latch) throws Exception {
        String query = "SELECT * FROM importCSVTable ORDER BY clm_integer;";
        VoltTable table;
        Thread.sleep(500);
        // For the quickest victory, race to read the result,
        // hoping to lose that race to the writer.
        table = client.callProcedure("@AdHoc", query).getResults()[0];
        if (table.getRowCount() > 0) {
            return table;
        }
        // The writer was too slow.
        // Make sure it at least sent the data before trying again.
        latch.await();
        table = client.callProcedure("@AdHoc", query).getResults()[0];
        if (table.getRowCount() > 0) {
            return table;
        }
        // The writer was still too slow in processing the sent data
        // -- or maybe it's having a problem?
        // Purposely wait a little before trying one last time --
        // and returning whatever data is found.
        Thread.sleep(500);
        table = client.callProcedure("@AdHoc", query).getResults()[0];
        return table;
    }

    public void testCustomNULL() throws Exception {

        System.out.println("testCustomNULL");
        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info to be initialized");
        }

        //Both \N and \\N as csv input are treated as NULL
        String[] myData = {
                "1,1,1,11111111,test,1.10,1.11,,,\n",
                "2,2,1,11111111,\"test\",1.10,1.11,,,\n",
                "3,3,1,11111111,testme,1.10,1.11,,,\n",
                "4,4,1,11111111,iamtest,1.10,1.11,,,\n",
                "5,5,5,5,\\N,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n" };

        CountDownLatch latch = pushDataAsync(7001, myData);
        VoltTable ts_table = tryForResult(client, latch);

        assertEquals(5, ts_table.getRowCount());

        int i = 0;
        int nulls = 0;
        while (ts_table.advanceRow()) {
            String value = ts_table.getString(4);
            if (i < 2) {
                assertNull(value);
                nulls++;
            }
            else if (i == 4) {
                // this test case should fail once we stop replacing the \N as NULL
                assertNull(value);
                nulls++;
            }
            else {
                assertNotNull(value);
            }
            i++;
        }
        assertEquals(3, nulls);
        client.close();
    }

    public void testNoWhiteSpace() throws Exception {

        System.out.println("testNoWhiteSpace");

        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info to be initialized");
        }

        String[] myData = {
                "1,1,1,1,nospace,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "2,1,1,1,   frontspace,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "3,1,1,1,rearspace   ,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "4,1,1,1,\" inquotespace \"   ,1.10,1.11,7777-12-25 14:35:26,POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n" };

        CountDownLatch latch = pushDataAsync(7001, myData);
        VoltTable ts_table = tryForResult(client, latch);
        assertEquals(1, ts_table.getRowCount());
    }

    public void testUnmatchQuote() throws Exception {
        System.out.println("testUnmatchQuote");

        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info to be initialized");
        }

        String[] myData = {
                "1,1,1,1,\"Jesus loves you\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                //invalid line: unmatched quote
                "1,1,1,1,\"Jesus\"loves you\",1.10,1.11,\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n", };
        CountDownLatch latch = pushDataAsync(7001, myData);
        VoltTable ts_table = tryForResult(client, latch);
        assertEquals(1, ts_table.getRowCount());
    }

    public void testStrictQuoteAndBlankError() throws Exception {
        System.out.println("testStrictQuote");

        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info to be initialized");
        }

        String[] myData = {
                "\"1\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "2,2,2,2,a word,1.10,1.11,7777-12-25 14:35:26,POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"\n",
                "3,3,3,3,a word,1.10,1.11,7777-12-25 14:35:26,POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"\n",
                "\"4\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "5,\"5\",\"5\",\"5\",,,,,,\n", "\"5\",5,\"5\",\"5\",,,,,,\n", "\"5\",\"5\",,,,,,,,\n", };

        CountDownLatch latch = pushDataAsync(7002, myData);
        VoltTable ts_table = tryForResult(client, latch);
        assertEquals(2, ts_table.getRowCount());
    }

    public void testTrimunquoted() throws Exception {
        System.out.println("testTrimunquoted");

        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info to be initialized");
        }

        String[] myData = {
                "12,10.05,  test" };

        CountDownLatch latch = pushDataAsync(7002, myData);
        VoltTable ts_table = tryForResult(client, latch);

        while (ts_table.advanceRow()) {
            String value = ts_table.getString(3);
            assertEquals("  test", value);
            break;
        }
    }

    static public junit.framework.Test suite() throws Exception {
        return buildEnv();
    }

    static public MultiConfigSuiteBuilder buildEnv() throws Exception {
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCSVFormatterSuite.class);
        Map<String, String> additionalEnv = new HashMap<>();
        //Specify bundle location
        String bundleLocation = System.getProperty("user.dir") + "/bundles";
        System.out.println("Bundle location is: " + bundleLocation);
        additionalEnv.put("voltdbbundlelocation", bundleLocation);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-import-ddl.sql"));
        project.addPartitionInfo("importCSVTable", "clm_integer");

        // configure socket importer 1
        Properties props = buildProperties(
                "port", "7001",
                "decode", "true",
                "procedure", "importCSVTable.insert");

        Properties formatConfig = buildProperties(
                "nullstring", "test",
                "separator", ",",
                "blank", "empty",
                "escape", "\\",
                "quotechar", "\"",
                "nowhitespace", "true");

        project.addImport(true, "custom", "csv", "socketstream.jar", props, formatConfig);

        // configure socket importer 2
        props = buildProperties(
                "port", "7002",
                "decode", "true",
                "procedure", "importCSVTable.insert");

        formatConfig = buildProperties(
                "nullstring", "test",
                "separator", ",",
                "blank", "error",
                "escape", "\\",
                "quotechar", "\"",
                "strictquotes", "true",
                "trimunquoted", "false");
        project.addImport(true, "custom", "csv", "socketstream.jar", props, formatConfig);

        project.addPartitionInfo("importCSVTable", "clm_integer");

        /*
         * compile the catalog all tests start with
         */

        LocalCluster config = new LocalCluster("import-ddl-cluster-rep.jar", 4, 1, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(true);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
