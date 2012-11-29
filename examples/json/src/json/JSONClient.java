/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
 * This samples demonstrates how to use VoltDB JSON functions to
 * implement a flexible schema-type of application.  The application
 * demonstrated here is a single sign-on session tracking application.
 * Logins from multiple sites (URLs) are tracked in a user session table
 * in VoltDBs.  Each login has common fields such as username and global
 * session id.  Further, each login has site-specific data stored in a
 * varchar column as JSON data.
 *
 * This sample application uses the VoltDB synchronous API.
 *
 * The sample first creates 10 threads and loads up as many random logins as possible
 * in 10 seconds.
 *
 * After the data is loaded, the sample application executes a series of
 * AdHoc SQL queries, demonstrating various queries on the JSON data.
 */

package json;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import json.procedures.Login;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;

import com.google.gson.*;

public class JSONClient {

    // validated command line configuration
    final LoginConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Random record generator
    LoginGenerator random_login_generator;
    static Random random = new Random();
    // Flags to tell the worker threads to stop or go
    AtomicBoolean loadComplete = new AtomicBoolean(false);
    // Statistics manager object
    final ClientStatsContext fullStatsContext;
    // Statistics counters
    AtomicLong acceptedLogins = new AtomicLong(0);
    AtomicLong badLogins = new AtomicLong(0);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class LoginConfig extends CLIConfig {
        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 10;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of concurrent threads synchronously calling procedures.")
        int threads = 10;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (duration < 0) exitWithMessageAndUsage("duration must be >= 0");
            if (threads <= 0) exitWithMessageAndUsage("threads must be > 0");
        }
    }

    /*
     * This class generates random login records.  For this sample, we have three different
     * sites that are tracked:  VoltDB, Amazon and Gmail.  The sites are created as Java objects
     * which are then automatically converted to JSON using Google's gson library.
     */
    static class LoginGenerator {
        public static class LoginRecord {
            public String username;
            public final String password;
            public final String json;

            protected LoginRecord(String username, String password, String json) {
                this.username = username;
                this.password = password;
                this.json = json;
            }
        }

        /**
         * Generates a random login
         * @return Login containing login credentials
         */
        public LoginRecord createLoginRecord()
        {
            // Generate a "random" set of data for this login.  The assumption is
            // that each site that logs in with this user name will want to store different data
            // associated with that login.  We don't know the schema of this data ahead of time so we'll
            // store it in a json field.
            Gson gson = new Gson();
            SiteBase site = null;

            switch (random.nextInt(2)) {
            case 0:  AmazonSite asite = new AmazonSite();
                     switch (random.nextInt(3))
                     {
                         case 0:
                             asite.addToCart("Kindle Fire", "B007T36PSM");
                             break;
                         case 1:
                             asite.addToCart("iPod Touch", "B0097BEF0O");
                             break;
                         case 2:
                             asite.addToCart("The Hobbit", "0395177111");
                             break;
                         case 3:
                             asite.addToCart("TaylorMade R11 S Driver", "B006SU1122");
                             break;
                         default:
                             asite.addToCart("Sony Bravia 40-inch TV", "B006U1VG74");
                             break;
                     }
                     site = asite;
                     break;
            case 1:  site = new GmailSite();
                     break;
            default: site = new VoltDBSite();
                     break;
            }
            site.props.put("last-login", new Long(System.currentTimeMillis()).toString());

            // Return the generated user login
            return new LoginRecord("user-" + random.nextInt(100000), "pwd", gson.toJson(site));
        }

        // A unique record is created so that the sample queries can target specific records.
        public LoginRecord createUniqueLogin(String username, String version, String prop, String val)
        {
            Gson gson = new Gson();
            VoltDBSite site = new VoltDBSite();
            site.download_version = version;
            site.props.put("last-login", new Long(System.currentTimeMillis()).toString());
            site.props.put(prop, val);
            // Return the generated login
            return new LoginRecord(username, "pwd", gson.toJson(site));
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the sample is still active
            if (loadComplete.get() == false) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Constructor for sample.
     *
     * @param config Parsed & validated CLI options.
     */
    public JSONClient(LoginConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        client = ClientFactory.createClient(clientConfig);

        fullStatsContext = client.createStatsContext();

        random_login_generator = new LoginGenerator();
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
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
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

    /**
     * Prints the results and statistics of the data load.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        String display = "\nA total of %d login requests were received...\n";
        System.out.printf(display, stats.getInvocationsCompleted());
        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
    }

    /**
     * Invoke the Login stored procedure to add a login record to the database.
     * If the login is called multiple times for the same username, the last accessed
     * time for the login is updated.  Thus this sample client can be run repeatedly without
     * having to cycle the database.
     */
    private void doLogin(LoginGenerator.LoginRecord login) {
        // Synchronously call the "Login" procedure passing in a json string containing
        // login-specific structure/data.
        try {
            ClientResponse response = client.callProcedure("Login",
                                                            login.username,
                                                            login.password,
                                                            login.json);


            long resultCode = response.getResults()[0].asScalarLong();
            if (resultCode == Login.LOGIN_SUCCESSFUL)
                acceptedLogins.incrementAndGet();
            else
                badLogins.incrementAndGet();
        }
        catch (Exception e) {
            badLogins.incrementAndGet();
            e.printStackTrace();
        }
    }

    /**
     * While <code>loadComplete</code> is set to false, run as many
     * synchronous procedure calls as possible.
     *
     */
    class LoginThread implements Runnable {

        @Override
        public void run() {
            while (loadComplete.get() == false) {
                // Generate the next login
                LoginGenerator.LoginRecord login = random_login_generator.createLoginRecord();
                doLogin(login);
            }

        }
    }

    /*
     * Create a unique well-known record on which we can query for specific attributes.
     */
    public void createUniqueData()
    {
        LoginGenerator.LoginRecord unique_login = random_login_generator.createUniqueLogin("voltdb", "v2.8", "download-count", "3");
        doLogin(unique_login);
    }

    /**
     * Load the database with as much data as possible within the specified time range.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void loadDatabase() throws Exception {
        // create/start the requested number of threads
        Thread[] loginThreads = new Thread[config.threads];
        for (int i = 0; i < config.threads; ++i) {
            loginThreads[i] = new Thread(new LoginThread());
            loginThreads[i].start();
        }

        // Initialize the statistics
        fullStatsContext.fetchAndResetBaseline();

        // Run the data loading the specified duration
        System.out.println("\nLoading database...");
        Thread.sleep(1000l * config.duration);

        // stop the threads
        loadComplete.set(true);

        // block until all outstanding txns return
        client.drain();

        // join on the threads
        for (Thread t : loginThreads) {
            t.join();
        }

        // print the summary statistics of the data load
        printResults();

        // Create entries that we can query on.
        createUniqueData();
    }

    /**
     * Demonstrates various JSON/flexible schema queries.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runQueries() throws Exception {

        System.out.println();
        System.out.println("Running Queries: ");
        System.out.println();

        // Select some sessions - see a portion of the full result set
        System.out.println("Select a sample portion of the data:");
        String SQL = "SELECT username, json_data FROM user_session_table ORDER BY username LIMIT 10";
        ClientResponse resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        printResults(resp.getResults()[0]);
        System.out.println();

        // Select some sessions that have logged into site Amazon.com
        System.out.println("Select logins that have been made to amazon.com, limit results to 10:");
        SQL = "SELECT username, json_data FROM user_session_table WHERE field(json_data, 'site')='amazon.com' ORDER BY username LIMIT 10";
        resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        printResults(resp.getResults()[0]);
        System.out.println();

        // Select Amazon users who have a Kindle Fire in their shopping cart
        System.out.println("Select Amazon sessions that have a Kindle Fire in their shopping cart:");
        SQL = "SELECT username, json_data FROM user_session_table WHERE field(json_data, 'site')='amazon.com' AND field(json_data, 'shopping_cart') like '%Fire%' order by username limit 10;";
        resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        printResults(resp.getResults()[0]);
        System.out.println();

        // Drill deeper into the JSON data looking for a specific record
        System.out.println("Look for all records of voltdb logins who have downloaded v2.8.  There should only be 1:");
        SQL = "SELECT username, json_data FROM user_session_table WHERE field(json_data, 'download_version')='v2.8' ORDER BY username";
        resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        printResults(resp.getResults()[0]);
        System.out.println();

        // Nest field() functions to drill into the JSON
        System.out.println("Look deep into the JSON data for all records of voltdb logins who have a download count of 3.  There should only be 1:");
        SQL = "SELECT username, json_data FROM user_session_table WHERE field(field(json_data, 'props'), 'download-count')='3' ORDER BY username";
        resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        printResults(resp.getResults()[0]);
        System.out.println();

        // Display the results as JSON.
        System.out.println("Retrieve the JSON data for a particular login.  Treat the whole result set as JSON:");
        SQL = "SELECT json_data FROM user_session_table WHERE username='voltdb'";
        resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        System.out.println(resp.getResults()[0].toJSONString());
        System.out.println();

        // Display the results as JSON.
        System.out.println("Retrieve the JSON data for a particular login. Just grab the JSON value from the result:");
        SQL = "SELECT json_data FROM user_session_table WHERE username='voltdb'";
        resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        VoltTable table = resp.getResults()[0];
        table.advanceRow();
        System.out.println(table.get("json_data", VoltType.STRING));
        System.out.println();
    }

    public void initialize() throws Exception
    {
        // connect to one or more servers, loop until success
        connect(config.servers);
    }

    public void shutdown() throws Exception
    {
        // close down the client connections
        client.close();
    }

    // Display formatting utility functions
    public static String paddingString(String s, int n, char c, boolean paddingLeft)
    {
        if (s == null)
            return s;

        int add = n - s.length();

        if(add <= 0)
            return s;

        StringBuffer str = new StringBuffer(s);
        char[] ch = new char[add];
        Arrays.fill(ch, c);
        if(paddingLeft)
            str.insert(0, ch);
        else
            str.append(ch);


       return str.toString();
    }

    private static String byteArrayToHexString(byte[] data)
    {
        StringBuffer hexString = new StringBuffer();
        for (int i=0;i<data.length;i++)
        {
            String hex = Integer.toHexString(0xFF & data[i]);
            if (hex.length() == 1)
                hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    // Print pretty results
    public void printResults(VoltTable t)
    {
        int columnCount = t.getColumnCount();
        int[] padding = new int[columnCount];
        String[] fmt = new String[columnCount];
        for (int i = 0; i < columnCount; i++)
            padding[i] = t.getColumnName(i).length();
        t.resetRowPosition();
        while(t.advanceRow())
        {
            for (int i = 0; i < columnCount; i++)
            {
                Object v = t.get(i, t.getColumnType(i));
                if (t.wasNull())
                    v = "NULL";
                int l = 0;  // length
                if (t.getColumnType(i) == VoltType.VARBINARY && !t.wasNull()) {
                    l = ((byte[])v).length*2;
                }
                else {
                    l= v.toString().length();
                }

                if (padding[i] < l)
                    padding[i] = l;
            }
        }
        for (int i = 0; i < columnCount; i++)
        {
            padding[i] += 1;
            fmt[i] = "%1$" +
                ((t.getColumnType(i) == VoltType.STRING ||
                  t.getColumnType(i) == VoltType.TIMESTAMP ||
                  t.getColumnType(i) == VoltType.VARBINARY) ? "-" : "")
                + padding[i] + "s";
        }
        for (int i = 0; i < columnCount; i++)
        {
            System.out.printf("%1$-" + padding[i] + "s", t.getColumnName(i));
            if (i < columnCount - 1)
                System.out.print(" ");
        }
        System.out.print("\n");
        for (int i = 0; i < columnCount; i++)
        {
            System.out.print(paddingString("", padding[i], '-', false));
            if (i < columnCount - 1)
                System.out.print(" ");
        }
        System.out.print("\n");
        t.resetRowPosition();
        while(t.advanceRow())
        {
            for (int i = 0; i < columnCount; i++)
            {
                Object v = t.get(i, t.getColumnType(i));
                if (t.wasNull())
                    v = "NULL";
                else if (t.getColumnType(i) == VoltType.VARBINARY)
                    v = byteArrayToHexString((byte[])v);
                else
                    v = v.toString();
                System.out.printf(fmt[i], v);
                if (i < columnCount - 1)
                    System.out.print(" ");
            }
            System.out.print("\n");
        }
        System.out.printf("\n(%d row(s) affected)\n", t.getRowCount());
    }

    /**
     * Main routine creates a client instance, loads the database, then executes example
     * queries against the data.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link LoginConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        LoginConfig config = new LoginConfig();
        config.parse(JSONClient.class.getName(), args);
        JSONClient app = new JSONClient(config);

        // Initialize connections
        app.initialize();

        // load data, measuring the throughput.
        app.loadDatabase();

        // run sample JSON queries
        app.runQueries();

        // Disconnect
        app.shutdown();
    }
}
