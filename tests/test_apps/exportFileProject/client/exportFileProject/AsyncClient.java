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

package exportFileProject.client.exportFileProject;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

public class AsyncClient {
    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */

    static Client client;
    static KVConfig config;

    static class KVConfig extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "Benchmark tuples, in rows.")
        int rows = 1000000;
    }

    public static void connect(String servers) {
        ClientConfig config = new ClientConfig("","");
        config.setReconnectOnConnectionLoss(true);
        try {
               client = ClientFactory.createClient(config);
               for (String server : servers.split(",")) {
                   System.out.println("Connecting to " + server);
                   client.createConnection(server);
               }
        } catch (java.io.IOException e) {
               e.printStackTrace();
               System.exit(-1);
        }
    }

    static class MyCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(clientResponse.getStatusString());
            }
        }
    }

    private static long exportLoop(int duration) {
        // TODO Auto-generated method stub
        long currentTime = System.currentTimeMillis();
        long endTime = currentTime + duration*1000;
        long writeCount = 0;

        //while (currentTime < endTime) {
        for (writeCount=0; writeCount < config.rows; writeCount++) {
            VoltTable[] results;
            try {
                client.callProcedure(new MyCallback(), "InsertExport", writeCount, currentTime);
            } catch (Exception e) {
                 e.printStackTrace();
                 System.exit(-1);
            }
            if ((writeCount % 50000) == 0) {
                currentTime = System.currentTimeMillis();
                System.out.print(".");
            }
        }
        System.out.println("");
        return writeCount;
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        config = new KVConfig();
        config.parse(AsyncClient.class.getName(), args);
        System.out.println(" Command Line Configuration");
        System.out.println(config.getConfigDumpString());
        connect(config.servers);

        long count = exportLoop(config.duration);
        System.out.println("Export Client: " + count + " rows in " + config.duration + " seconds.");

        try {
            client.drain();
            client.close();
        } catch (NoConnectionsException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
