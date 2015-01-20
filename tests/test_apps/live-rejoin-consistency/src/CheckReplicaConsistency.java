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
 * This is the "checking" part of the master/replica consistency test app
 * it relies on a "short circuit" behavior in to obtain the node local data
 */

package LiveRejoinConsistency;

import java.io.IOException;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class CheckReplicaConsistency {

    // validated command line configuration
    final AppConfig config;
    // Reference to the database connection we will use
    final Client client;

    int nPartitions = 0;

    /**
     * Uses included {@link CLIConfig} class to declaratively state command line
     * options with defaults and validation.
     */
    static class AppConfig extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Rejoining node name or ip.")
        String rejoinnode = "";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";
    }

    /**
     * Constructor for benchmark instance. Configures VoltDB client and prints
     * configuration.
     *
     * @param config
     *            Parsed & validated CLI options.
     */
    public CheckReplicaConsistency(AppConfig config) {
        this.config = config;
        client = null;
    }

    public long checkAndReturnCounter(String server, Client client, int pid, String tbl)
            throws NoConnectionsException, IOException, ProcCallException {

        ClientResponse r_sp = null;

        // this sp relies on a "short circuit" behavior in voltdb to obtain the node local
        // value of the counter (so we can compare the data stored on different nodes.
        // Therefore, when the counter table is configured REPLICATED, the query
        // must join to a partitioned table in order to obtain similar "short circuit".
        r_sp = client.callProcedure("getCountFrom"+tbl, pid);

        if (r_sp.getStatus() != ClientResponse.SUCCESS) {
            System.err.printf("SP failed %s\n", r_sp.getStatusString());
            throw new RuntimeException();
        }

        System.err.printf("checkAndReturnCounter %s %d %d\n", server, pid,
                r_sp.getResults()[0].fetchRow(0).getLong(0));

        return r_sp.getResults()[0].fetchRow(0).getLong(0);
    }

    public long returnCRC(String server, Client client, int pid, String tbl)
            throws NoConnectionsException, IOException, ProcCallException {

        // this sp relies on a "short circuit" behavior in voltdb to obtain the node local
        // value of the counter (so we can compare the data stored on different nodes.
        // Therefore, when the counter table is configured REPLICATED, the query
        // must join to a partitioned table in order to obtain similar "short circuit".
        ClientResponse r = client.callProcedure("getCRCFrom"+tbl, pid);

        if (r.getStatus() != ClientResponse.SUCCESS) {
            System.err.printf("SP failed %s\n", r.getStatusString());
            throw new RuntimeException();
        }

        System.err.printf("returnCRC %s %d %d\n", server, pid,
                r.getResults()[0].fetchRow(0).getLong(0));

        return r.getResults()[0].fetchRow(0).getLong(0);
    }

    /**
     * Core benchmark code. Connect. Initialize. Run the loop. Cleanup. Print
     * Results.
     *
     * @throws Exception
     *             if anything unexpected happens.
     */
    public void checkForCorrectness(int ptnOrRep) throws Exception {

        // compare state of all nodes
        // we're employing "short circuit" read queries to get the data on the node
        // exit with nonzero if there is an error

        String z = config.servers;
        String servers[] = z.split(",");
        String[] tblSuffix_ = new String[] {"Ptn", "Rep"};
        String sPtnOrRep = tblSuffix_[ptnOrRep];
        int nTables = 0; // used for catalog check

        System.out.printf("Checking data across nodes, case: %s, servers: %s ...\n", sPtnOrRep, z);

        ClientConfig clientConfig = new ClientConfig(config.user, config.password);
        long[] counters = null;
        long[] crcs = null;
        int crcCount = -1;

        for (int iServer = 0; iServer < servers.length; iServer++) {

            String server = servers[iServer];
            Client client = ClientFactory.createClient(clientConfig);

            try { client.createConnection(server); }
            catch (Exception e) {
                System.err.printf("Error connecting to node %d %s\n", iServer,
                        server);
                e.printStackTrace();
                throw new RuntimeException();
            }

            ClientResponse resp = null;

            if (iServer == 0) {
                // get the partition count
                resp = client.callProcedure("@Statistics", "PARTITIONCOUNT", 0);
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    System.err.printf("Get partition count failed %s\n", resp.getStatusString());
                    throw new RuntimeException();
                }
                VoltTable[] tpc = resp.getResults();
                nPartitions=0;
                while (tpc[0].advanceRow()) {
                    nPartitions = (int) tpc[0].getLong("PARTITION_COUNT");
                }
                System.out.printf("partition count: %d\n", nPartitions);
                if (nPartitions < 2) {
                    System.err.printf("Less than 2 partitions\n", nPartitions);
                    throw new RuntimeException();
                }
                counters = new long[nPartitions];
                crcs = new long[nPartitions];
            }

            if (nPartitions == 0) {
                System.err.println("Zero partitions should not happen");
                throw new RuntimeException();
            }

            // check the catalog by comparing the number of tables
            resp = client.callProcedure("@Statistics", "TABLE", 0);
            int nt = resp.getResults()[0].getRowCount();
            System.out.printf("table count: %d\n", nt);
            if (iServer == 0)
                nTables = nt;
            else {
                if (nTables != nt) {
                    System.err.printf("TEST FAILED Catalog Table count mismatch %d != %d %s %s\n",
                            nTables, nt, server, servers[0]);
                    System.exit(1);
                    //throw new RuntimeException();
                } else {
                    System.out.printf("Catalog test passed\n");
                }
            }

            for (int pid = 0; pid < nPartitions; pid++) {

                // check the data in the counters tables between the nodes. Use local read
                // techniques ie. "short circuit" read.
                try {
                    long counter = checkAndReturnCounter(server, client,
                            pid, sPtnOrRep);
                    if (iServer == 0)
                        counters[pid] = counter;
                    else if (counters[pid] != counter) {
                        System.err.printf("TEST FAILED Node counter datacompare mismatch %d != %d %s %s\n",
                                counter, counters[pid], server, servers[0]);
                        System.exit(1);
                        //throw new RuntimeException();
                    }
                } catch (Exception e) {
                    System.err.printf("Exception received calling checkAndReturnCounter: server: %s pid: %d\n%s\n",
                            server, pid, e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException();
                }

                // do the same thing to check the like_counters tbls. We already checked their
                // row counts, now, get a crc of the counter columnn values from all rows (using
                // short circuit read techniques) and compare the crc's.
                try {
                    long crc = returnCRC(server, client,
                            pid, sPtnOrRep);
                    if (iServer == 0)
                        crcs[pid] = crc;
                    else if (crcs[pid] != crc) {
                        System.err.printf("TEST FAILED Node crc datacompare mismatch %d != %d %s %s\n",
                                crc, crcs[pid], server, servers[0]);
                        System.exit(1);
                        //throw new RuntimeException();
                    }
                } catch (Exception e) {
                    System.err.printf("Exception received calling returnCRC: server: %s pid: %d\n%s\n",
                            server, pid, e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException();
                } }

            client.drain();
            client.close();
        }
        System.out.printf("case: %s All nodes of counter identical\n", sPtnOrRep);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args
     *            Command line arguments.
     * @throws Exception
     *             if anything goes wrong.
     * @see {@link AppConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        AppConfig config = new AppConfig();
        config.parse(CheckReplicaConsistency.class.getName(), args);
        CheckReplicaConsistency benchmark = new CheckReplicaConsistency(config);
        System.out.println("Checking replicated tables...");
        benchmark.checkForCorrectness(1); // replicated scenario
        System.out.println("Checking partitioned tables...");
        benchmark.checkForCorrectness(0); // partitioned scenario
        System.out.println("Normal End of check\n\n");


    }
}
