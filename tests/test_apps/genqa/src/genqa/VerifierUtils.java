/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package genqa;


import java.io.IOException;
import java.net.UnknownHostException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;

//import com.google_voltpatches.base.Splitter;

public class VerifierUtils {
    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    // static VoltLogger log = new VoltLogger("VerifierUtils");

    public static class Config extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 300;

        @Option(desc = "Maximum export TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Comma separated list of the form server[:port] to connect to for database queries")
        String servers = "localhost";

        @Option(desc = "Number of rows to expect to import from the Kafka topic")
        long expected_rows = 10_000_000;

        @Option(desc = "Report latency for kafka benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Test using all VoltDB datatypes (except varbin).")
        boolean alltypes = false;

        @Option(desc = "Set to true to use voltdb export instead of groovy loader to populate kafka topic(s).")
        boolean useexport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            // 0, means we're not expecting any rows -- part of new offset checking test
            // if (expected_rows <= 0) exitWithMessageAndUsage("row number must be > 0");
            if (!useexport && alltypes) exitWithMessageAndUsage("groovy loader and alltypes are mutually exclusive");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            System.out.println("finished validating args");
        }
    }

    /**
     * Connect to one or more VoltDB servers.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional). Assumes 21212 if not specified otherwise.
     * @throws IOException
     * @throws UnknownHostException
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static Client dbconnect(String servers, int ratelimit) throws UnknownHostException, IOException {
        //final Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

        System.out.println("Connecting to VoltDB Interface...");
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setMaxTransactionsPerSecond(ratelimit);
        clientConfig.setReconnectOnConnectionLoss(true);
        Client client = ClientFactory.createClient(clientConfig);

        // for (String server: COMMA_SPLITTER.split(servers)) {
           for (String server: servers.split(",")) {
            System.out.println("..." + server);
            client.createConnection(server);
        }
        return client;
    }
}
