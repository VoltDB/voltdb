/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.VoltTable;

public class PollClient {

    // Client connection to the server
    final Client client;
    // Validated CLI config
    final PollClientConfig  config;

    static class PollClientConfig extends CLIConfig {

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Name of stream or table to poll from")
        String topic = "ALL_VALUES";

        @Override
        public void validate() {
        }
    }

    /**
     * Clean way of exiting from an exception
     * @param message   Message to accompany the exception
     * @param e         The exception thrown
     */
    private void exitWithException(String message, Exception e) {
        System.err.println(message);
        System.err.println(e.getLocalizedMessage());
        System.exit(-1);
    }

    public PollClient(PollClientConfig config) {
        this.config = config;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (IOException e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (InterruptedException interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    void run() {
        // Connect to servers
        try {
            System.out.println("Poll client initialization");
            connect(config.servers);
        }
        catch (InterruptedException e) {
            exitWithException("ERROR: Error connecting to VoltDB", e);
        }

        // Poll continuously until CTRL-C
        try {
            while(true) {
                ClientResponse resp = client.callProcedure("@PollTopic", config.topic);
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    System.out.println("Failed polling, status: " + resp.getStatus()
                        + " (" + resp.getStatusString() + ")\n");
                    Thread.sleep(2000);
                    continue;
                }

                VoltTable res[] = resp.getResults();
                if (res == null || res.length == 0) {
                    System.out.println("Empty poll\n");
                    continue;
                }

                for (int i =0; i < res.length; i++) {
                    VoltTable vt = res[i];
                    System.out.println(vt);
                    System.out.println();
                }
            }
        }
        catch (Exception e) {
            exitWithException("ERROR: Failed to poll", e);
        }
    }

    public static void main(String[] args) {
        PollClientConfig config = new PollClientConfig();
        config.parse(PollClient.class.getName(), args);

        try {
            PollClient pc = new PollClient(config);
            pc.run();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}