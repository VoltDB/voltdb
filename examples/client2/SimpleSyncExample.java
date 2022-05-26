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
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

/*
 * This is a very simple example of how to use some of
 * the synchronous calls of the Client2 API.  It does
 * not purport to do anything useful, but it shows
 * some API calls in context.
 */
public class SimpleSyncExample {

    // Comma-separate lists of VoltDB servers.
    // We connect to the first available.
    final String servers;

    // Client API object
    Client2 client;

    // Constructor
    SimpleSyncExample(String servers) {
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
        (new SimpleSyncExample(list)).run();
    }

    // Execute our example code in the context of an
    // instance of SimpleSyncExample. Error handling is
    // rudimentary.
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
    // servers set up in the constructor.
    void connectToCluster() throws IOException {
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

        // Connect to the first available server from the
        // supplied list. After we have one connection, the
        // topology-aware features will maintain connections
        // to other VoltDB servers.
        client.connectSync(servers);
    }

    // Disconnect from VoltDB cluster and deallocate
    // API resources.
    void disconnectFromCluster() {
        if (client != null) {
            System.out.println("Disconnecting from VoltDB");
            client.close();
            client = null;
        }
    }

    // Call a procedure in the cluster. This is a simple wrapper
    // around the Client2 method, to provide logging for the purposes
    // of this example.
    ClientResponse callProcedure(String name, Object... params) throws IOException {
        ClientResponse response = null;
        try {
            response = client.callProcedureSync(name, params);
            System.out.printf("Procedure call to %s succeeded%n%n", name);
        }
        catch (ProcCallException ex) {
            response = ex.getClientResponse();
            System.out.printf("Procedure call to %s failed with status %s%n" +
                              " and status string \"%s\"%n%n",
                              name, response.getStatus(), response.getStatusString());
        }
        return response;
    }

    // This is a placeholder for the work that a real application
    // might want to do. Here we execute a successful call to
    // a system procedure (this example does not have any application-
    // specific procedures), followed by a call that will fail, this
    // latter just to demonstrate the failure path. In neither case
    // do we bother to do anything with the response; this example
    // is only intended to demonstrate API usage.
    void businessLogic() throws IOException {
        System.out.printf("Executing procedure calls%n%n");
        ClientResponse resp1 = callProcedure("@SystemInformation", "overview");
        callProcedure("BadProcedure");
    }

    // Notification handlers. In this simple-minded example, we do nothing
    // except print out an appropriate message.

    // Called on failure to set up a connection (as distinct from
    // the failure of a previously-established connection)
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
