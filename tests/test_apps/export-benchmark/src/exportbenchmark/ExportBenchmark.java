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
/*
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package exportbenchmark;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

public class ExportBenchmark {
    
    private Client client;
    
    static class ExportBenchmarkCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            System.err.println("Log insertion into VoltDB failed:");
            System.err.println(response.getStatusString());
        }
    }
    
    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     */
    public ExportBenchmark() {
        ClientConfig clientConfig = new ClientConfig("", "");
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
    }

    /**
     * Runs the export benchmark test
     */
    private void runTest() {
        System.out.println("Test initialization");
        
        // Server connection
        VoltBulkLoader bulkLoader = null;
        try {
            client.createConnection("localhost");
            bulkLoader = client.getNewBulkLoader("valuesToExport", 50, new ExportBenchmarkCallback());
        }
        catch (Exception e) {
            System.err.printf("Connection to VoltDB failed\n" + e.getMessage());
            System.exit(1);
        }
        System.out.println("Initialization complete");
        
        
        // Insert objects
        long startTime = System.nanoTime();
        try {
            System.out.println("Inserting objects");
            Object rowHandle = null;
            for (int i = 0; i < 1000000; i++) {
                bulkLoader.insertRow(rowHandle, i, 42);
            }
            bulkLoader.drain();
            System.out.println("Object insertion complete");
        } catch (Exception e) {
            System.err.println("Couldn't insert into VoltDB\n" + e.getMessage());
            System.exit(1);
        }
        
        // Wait until export is done
        try {
            while (true) {
                VoltTable results = client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
                results.advanceRow();
                if (results.getLong("TUPLE_ALLOCATED_MEMORY") == 0) {
                    break;
                }
                Thread.sleep(100);
            }
        } catch (Exception e) {
            System.err.println("Unable to analyze export table");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        
        // See how much time elapsed
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("Export time elapsed (ms) for 1,000,000 objects: " + estimatedTime/1000000);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) throws Exception {
        ExportBenchmark bench = new ExportBenchmark();
        bench.runTest();
    }
}