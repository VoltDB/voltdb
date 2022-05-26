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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

/*
 * This example demonstrates, in a simple way, methods for
 * flow control when using the asynchronous features of the
 * Client2 API.
 *
 * The main thread sits in a tight loop issuing procedure calls.
 * Since it almost certainly can outrun the VoltDB server, the
 * following thngs will happen:
 *
 * 1. The network layer will report backpressure. This is not
 *    passed on to the application; the Client2 API handles it.
 *
 * 2. The API will begin to queue requests internally.
 *
 * 3. At some point we may hit the "warning" level on the API's
 *    request queue; the actual level is set by a configuration
 *    call, clientRequestBackpressureLevel(). The API will call
 *    an application backpressure-notification routine, in
 *    effect saying "slow down". Procedure calls are still
 *    queued if issued during backpressure (but see the point
 *    about the hard limit, below).
 *
 * 4. The application must implement a mechanism to slow down
 *    its request rate. Here, we stop sending requests.
 *
 * 5. Eventually, the API's queue of requests will fall to the
 *    "resume" level set by clientRequestBackpressureLevel().
 *    The API calls the application backpressure-notification
 *    routine, saying "resume".
 *
 * 6. And the application then undoes whatever it did to slow
 *    down sending. Here, we start sending requests again.

 * 7. If when told to slow down, the application did not slow
 *    down, or at least did not slow down in time, the number
 *    of queued requests could exceed the hard limit, as set
 *    by a call to clientRequestLimit(), If that happens,
 *    subsequent procedure calls are not queued, but will be
 *    rejected by the API throwing a RequestLimitException.
 *
 * 8. This example does not handle a RequestLimitException in
 *    any useful way; it simply logs a message and carries on
 *    doing what it was doing. We don't expect to actually
 *    see such an exception, because we suppose we have adequate
 *    headroom between the warning level and the hard limit.
 */
public class AsyncFlowControl {

    // Comma-separate lists of VoltDB servers.
    // We connect to the first available.
    final String servers;

    // Client API object
    Client2 client;

    // A gate may be open or closed. We use a gate to
    // stop the main thread from queueing more requests
    // when there is backpressure on requests.
    static class Gate {
        boolean closed;

        synchronized void waitOpen() throws InterruptedException {
            while (closed)
                wait();
        }

        synchronized void operate(boolean closing) {
            if (closing ^ closed) {
                closed = closing;
                if (!closed)
                    notifyAll();
            }
        }
    }

    // The gate, initially open
    Gate gate = new Gate();

    // Success/fail count
    int good, bad;

    // But stop reporting if too many failures; this
    // just avoids excessive and useless console output.
    int logQuench;

    // Constructor
    AsyncFlowControl(String servers) {
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
        (new AsyncFlowControl(list)).run();
    }

    // Execute our example code in the context of an
    // instance of AsyncFlowControl. Error handling
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

        // Configuration related to flow control. Backpressure
        // begins at 800 outstanding requests, ends when the
        // level falls to 200, and there is hard limit at 1000.
        config.clientRequestBackpressureLevel(800, 200)
            .clientRequestLimit(1000)
            .requestBackpressureHandler(this::backpressure);

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
    // might want to do. Here we sit in a tight loop, issuing
    // procedure calls to VoltDB. We may be stalled by backpressure;
    // this is handled by the 'gate' which is closed during periods
    // of backpressure. For a procedure, we arbitrarily use the
    // VoltDB system procedure @PingPartitions, since we have
    // no real application procedures to use.
    void businessLogic() throws IOException, InterruptedException {
        System.out.printf("Initiating procedure calls%n");
        long duration = 60_000; // run for 1 minute
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < duration) {
            gate.waitOpen();
            client.callProcedureAsync("@PingPartitions", "0")
                .whenComplete(this::callComplete);
        }
        System.out.printf("%,d procedure calls were initiated%n" +
                          "  %,d succeeded, %,d failed%n",
                          good+bad, good, bad);
        System.out.printf("Draining procedure calls%n");
        client.drain();
    }

    // Asynchronous notification of call completion; may be called
    // in an arbitrary thread. We do nothing except check for
    // any error, and print a message accordingly.
    void callComplete(ClientResponse response, Throwable th) {
        if (th != null) {
            ++bad;
            if (++logQuench <= 10)
                System.out.printf("Procedure call completed exceptionally:%n" +
                                  " %s%n%n", th);
        }
        else if (response.getStatus() == ClientResponse.SUCCESS) {
            ++good;
        }
        else {
            ++bad;
            if (++logQuench <= 10)
                System.out.printf("Procedure call failed with status %s%n" +
                                  " and status string \"%s\"%n%n",
                                  response.getStatus(), response.getStatusString());
        }
    }

    // Backpressure handler. Backpressure is an indication that
    // there are too many requests pending (queued in the client or
    // sent to the network, and not yet responded to). The application
    // is warned to slow down, and later told it can resume.
    void backpressure(boolean slowdown) {
        gate.operate(slowdown);
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
