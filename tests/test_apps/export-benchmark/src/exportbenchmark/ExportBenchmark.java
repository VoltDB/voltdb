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
    
    long count = 10000;
    String host = "localhost";
    int port = 21212;
    
    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     * @param args The arguments passed to the program
     */
    public ExportBenchmark(String[] args) {
        parseCommandLine(args);
        
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
    }
    
    /**
     * Loops through the command line arguments & applies them
     * @param args The arguments passed to the program
     */
    private void parseCommandLine(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String[] arg = args[i].split("=");
            if (arg[0].equals("--count")) {
                try {
                    count = Long.parseLong(arg[1]);
                } catch (NumberFormatException e) {
                    System.err.println("'" + arg[1] + "': not a valid number");
                    System.exit(1);
                }
            } else if (arg[0].equals("--servers")) {
                host = arg[1];
            } else if (arg[0].equals("--port")) {
                port = Integer.parseInt(arg[1]);
            } else {
                System.err.println("Unknown argument: '" + args[i] + "' - ignoring");
            }
        }
    }
    
    public void waitForStreamedAllocatedMemoryZero() throws Exception {
        boolean passed = false;

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = System.currentTimeMillis() + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "table", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                String ttype = stats.getString("TABLE_TYPE");
                Long tts = stats.getLong("TIMESTAMP");
                //Get highest timestamp and watch is change
                if (tts > ts) {
                    ts = tts;
                }
                if (ttype.equals("StreamedTable")) {
                    if (0 != stats.getLong("TUPLE_ALLOCATED_MEMORY")) {
                        passedThisTime = false;
                        System.out.println("Partition Not Zero.");
                        break;
                    }
                }
            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                //we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                System.out.println("Passed but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        System.out.println("Passed is: " + passed);
        System.out.println(stats);
    }


    /**
     * Runs the export benchmark test
     */
    private void runTest() {
        System.out.println("Test initialization");
        
        // Server connection
        try {
            client.createConnection(host, port);
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
            String sql = "";
            for (int i = 0; i < count; i++) {
                sql = "INSERT INTO valuesToExport VALUES (" + 4 + "," + 8 + ","  + 16 + "," + 32 + "," + 42.15 + "," + 12.52 + ",'string1'," + 4215 + ");";
                client.callProcedure("@AdHoc", sql);
            }
            System.out.println("Object insertion complete");
        } catch (Exception e) {
            System.err.println("Couldn't insert into VoltDB\n" + e.getMessage());
            System.exit(1);
        }
        
        // Wait until export is done
        try {
            waitForStreamedAllocatedMemoryZero();
        } catch (Exception e) {
            System.err.println("Error while waiting for export: ");
            e.printStackTrace();
            System.exit(1);
        }
        
        // See how much time elapsed
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("Export time elapsed (ms) for " + count + " objects: " + estimatedTime/1000000);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) throws Exception {
        ExportBenchmark bench = new ExportBenchmark(args);
        bench.runTest();
    }
}