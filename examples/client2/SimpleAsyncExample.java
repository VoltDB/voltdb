/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.example;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

/*
 * This is a very simple example of how to use some of the
 * asynchronous (non-blocking) calls of the Client2 API.
 * It does not purport to do anything useful, but it shows
 * some API calls in context. It does not illustrate any
 * backpressure/flow control calls that a real application
 * will probably use.
 */
public class SimpleAsyncExample {

    // Comma-separate lists of VoltDB servers.
    // We connect to the first available.
    final String servers;

    // Client API object
    Client2 client;

    // Constructor
    SimpleAsyncExample(String servers) {
        this.servers = servers;
    }

    // Main entry point. The arguments, if provided,
    // are VoltDB servers: hostname or address,
    // optionally followed by :port. For IPv6 addresses,
    // the hostname must be enclosed in brackets.
    public static void main(String... args) {
        String list = "localhost"; // default
        if (args.length != 0) {
            list = String.join(",", args);
        }
        (new SimpleAsyncExample(list)).run();
    }

    // Execute our example code in the context of an
    // instance of SimpleAsyncExample. Error handling
    // is rudimentary.
    void run() {
        try {
            connectToCluster();
            businessLogic();
            disconnectFromCluster();
        }
        catch (Exception ex) {
            System.out.printf("*** Exception caught:%n    %s%n", ex);
        }
        finally {
            disconnectFromCluster();
        }
    }

    // Establishes the initial connection to the
    // VoltDB cluster, using the list of potential
    // servers set up in the constructor. Retry
    // with timeout is used.
    void connectToCluster() throws IOException, ExecutionException, InterruptedException {
        System.out.println("Connecting to VoltDB");

        // Define client configuration. Here we connect
        // handlers for various client API events; these
        // are optional, used here as examples.
        Client2Config config = new Client2Config()
            .connectFailureHandler(this::connectFailed)
            .connectionUpHandler(this::connectionUp)
            .connectionDownHandler(this::connectionDown)
            .errorLogHandler(this::logError);

        // Instantiate the API instance
        client = ClientFactory.createClient(config);

        // Iniitate the connection procedure. This will try
        // each of the supplied servers in turn, until one
        // is successfully connected. After that, the topology-
        // aware client will bring up other connections as
        // needed. On failure to connect to any server, it
        // retries every 10 secs, to a total of 120 secs.
        CompletableFuture<Void> future = client.connectAsync(servers, 120, 10, TimeUnit.SECONDS);

        // For this example, we'll wait here until the asynchronous
        // connection process completes. Note that an exception
        // will be thrown if it completes with failure, by the
        // standard CompletableFuture<> method. There is no
        // return value of interest.
        future.get();
        System.out.println("Connected to VoltDB");
    }

    // Disconnect from VoltDB cluster and deallocate
    // API resources. This is necessarily synchronous;
    // there is no async option.
    void disconnectFromCluster() {
        if (client != null) {
            System.out.println("Disconnecting from VoltDB");
            client.close();
            client = null;
        }
    }

    // This is a placeholder for the work that a real application
    // might want to do. Here we execute a successful call to
    // a system procedure (this example does not have any application-
    // specific procedures), followed by a call that will fail, this
    // latter just to demonstrate the failure path. Completions
    // may occur out-of-order.
    void businessLogic() throws IOException, InterruptedException {
        System.out.printf("Initiating procedure calls%n");
        CountDownLatch latch = new CountDownLatch(2);
        CompletableFuture<ClientResponse> fut1 = client.callProcedureAsync("@SystemInformation", "overview");
        CompletableFuture<ClientResponse> fut2 = client.callProcedureAsync("BadProcedure");
        System.out.println("Procedure calls initiated\n");
        fut1.whenComplete((resp, th) -> callComplete("@SystemInformation", resp, th, latch));
        fut2.whenComplete((resp, th) -> callComplete("BadProcedure", resp, th, latch));
        latch.await(); // synchronize with completions
    }

    // Asynchronous notification of call completion; may be called
    // in an arbitrary thread. We do little with the response other
    // than check it for success or failure; this example is only
    // intended to demonstrate API usage.
    void callComplete(String name, ClientResponse response, Throwable th, CountDownLatch countdown) {
        try {
            if (th != null) {
                System.out.printf("Procedure call to %s completed exceptionally:%n" +
                                  " %s%n%n", name, th);
            }
            else {
                int status = response.getStatus();
                if (status == ClientResponse.SUCCESS) {
                    System.out.printf("Procedure call to %s succeeded%n%n", name);
                }
                else {
                    System.out.printf("Procedure call to %s failed with status %s%n" +
                                      " and status string \"%s\"%n%n",
                                      name, status, response.getStatusString());
                }
            }
        }
        finally {
            countdown.countDown();
        }
    }

    // Notification handlers. In this simple-minded example, we do nothing
    // except print out an appropriate message.

    // Called on failure to set up a connection (as distinct from
    // the failure of a previously-established connection).
    void connectFailed(String host, int port) {
        System.out.printf("*** Connect failed: %s %s%n", host, port);
    }

    // Called when a new connection to the cluster is established.
    // This may be due to the initial connectSync call, or it may
    // be an additional connection to some other cluster member,
    // under the control of the topology-aware code.
    void connectionUp(String host, int port) {
        System.out.printf("*** Connection up: %s %s%n", host, port);
    }

    // Called when an established connection fails. It will be
    // automatically reconnected after a few seconds; in the
    // meantime, requests to this particular host will fail.
    void connectionDown(String host, int port) {
        System.out.printf("*** Connection down: %s %s%n", host, port);
    }

    // Called to log an unexpected error that is not the failure
    // of a particular API call but of a more general nature.
    void logError(String text) {
        System.out.printf("Error log: %s%n", text);
    }
}
