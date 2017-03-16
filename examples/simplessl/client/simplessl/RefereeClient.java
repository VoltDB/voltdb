/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package simplessl;

import java.io.IOException;

import org.voltdb.CLIConfig;
import org.voltdb.client.*;


public class RefereeClient
{
    static final String[] athleteNames = { "Adam", "Bob" }; // FIXME query VoltDB?

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class Parameters extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Option(desc = "SSL configuration file.")
        String sslFile = null;

        @Option(desc = "Enable topology awareness")
        boolean topologyaware = false;

        @Override
        public void validate() {
            if (sslFile == null) exitWithMessageAndUsage("must provide SSL configuration file");
        }
    }

    static class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            //if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            //}
        }
    }

    public static void main(String[] args)
    {
        Parameters config = new Parameters();
        config.parse(RefereeClient.class.getName(), args);

        System.out.println("Connecting to VoltDB");
        try {
            ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
            //clientConfig.setTrustStoreConfigFromPropertyFile(config.sslFile);
            //clientConfig.enableSSL();

            final Client client = ClientFactory.createClient(clientConfig);

            for (String s : args) {
                client.createConnection(s, Client.VOLTDB_SERVER_PORT);
            }

            final String refName = config.user;
            final int athleteScore = (int) Math.floor(Math.random() * 11);
            for (String athleteName : athleteNames){
                System.out.println("Referee " + refName + " awarding " + athleteScore + " points to " + athleteName);
                ClientResponse response =
                    client.callProcedure("@AdHoc", "INSERT (" + refName + ", " + athleteName + ", " + athleteScore + ") INTO scores;");
                if (response.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException(response.getStatusString());
                }
            }
            System.out.println("All scores submitted successfully.");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (ProcCallException e) {
            throw new RuntimeException(e);
        }
    }
}
