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

package jsonsessions;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

    // potential return codes (must be synced with procedure code)
    public static final long LOGIN_SUCCESSFUL = 0;
    public static final long LOGIN_FAIL = 1;
    public static final long LOGIN_TIMEOUT = 2;

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
    private static final String list_of_languages[] = {"C++", "PHP", "Java"};

    /*
     * This class generates random login records.  For this sample, we have three different
     * sites that are tracked:  VoltDB Blog, Forum and Management.  The sites are created as
     * Java objects which are then automatically converted to JSON using Google's gson library.
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
            SessionBase site = null;

            switch (random.nextInt(3)) {
                case 0:  site = new BlogSession();
                     break;
                case 1:  site = new ManagementSession();
                     break;
                default: site = new ForumSession();
                     break;
            }
            site.props.put("last-login", new Long(System.currentTimeMillis()));

            // This sample uses the Google GSON library to conveniently convert a Java object (the Session objects above) into a JSON
            // string encoding. For example, a sample JSON representation of a ForumSession object would be as follows:
            //
            // { "moderator":true,
            //   "download_count":1,
            //   "site":"VoltDB Forum",
            //   "props": { "last-login":"1354912929504",
            //              "download_version":"v3.0",
            //              "client_languages":"[Java"]
            //            }
            //  }
            //
            // Once we've created the JSON representation of the data, we'll insert it into a VoltDB row in the "json_data" column.
            // Later on in this sample, once the database is populated, we'll execute SQL queries on elements of this JSON data.
            // For example, we'll query on all sessions where the download_version is 'v3.0' and the client language is 'Java'.

            // Return the generated user login
            return new LoginRecord("user-" + random.nextInt(100000), "pwd", gson.toJson(site));
        }

        // A unique record is created so that the sample queries can target specific records.
        public LoginRecord createUniqueLogin(String username, boolean createForumSession)
        {
            Gson gson = new Gson();
            SessionBase site;
            if (createForumSession) {
                site = new ForumSession();
                site.props.put("client_languages", list_of_languages);
                ForumSession fsite = (ForumSession) site;
                fsite.setDownloadVersion("v2.6");
                fsite.setDownloadVersion("v2.7");
                fsite.setDownloadVersion("v3.0");
            } else {
                site = new BlogSession("writer", "reader");
            }
            site.props.put("last-login", new Long(System.currentTimeMillis()));
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
    public JSONClient() {
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
        System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
        System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
        System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
        System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
        System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
        System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
        System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
        System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));
        System.out.println("\n\n" + stats.latencyHistoReport());
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
            if (resultCode == LOGIN_SUCCESSFUL)
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
        LoginGenerator.LoginRecord unique_login;
        unique_login = random_login_generator.createUniqueLogin("voltdb", true);
        doLogin(unique_login);
        unique_login = random_login_generator.createUniqueLogin("voltdb2", false);
        doLogin(unique_login);
    }

    /**
     * Load the database with as much data as possible within the specified time range.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void loadDatabase() throws Exception {
        // create/start the requested number of threads
        int thread_count = 10;
        Thread[] loginThreads = new Thread[thread_count];
        for (int i = 0; i < thread_count; ++i) {
            loginThreads[i] = new Thread(new LoginThread());
            loginThreads[i].start();
        }

        // Initialize the statistics
        fullStatsContext.fetchAndResetBaseline();

        // Run the data loading for 10 seconds
        System.out.println("\nLoading database...");
        Thread.sleep(1000l * 10);

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
    private VoltTable runQuery(String description, String SQL) throws Exception {
        System.out.println(description);
        ClientResponse resp = client.callProcedure("@AdHoc", SQL);
        System.out.println("SQL query: " + SQL);
        System.out.println();
        VoltTable table = resp.getResults()[0];
        System.out.println(table.toFormattedString());
        System.out.println();
        return table;
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

        // Specify, run and print the queries (and their results), one at a time
        runQuery("Select some login sessions - just a small portion of the data (limit results to 10):",
                 "SELECT username, json_data FROM user_session_table ORDER BY username LIMIT 10");
        runQuery("Select some login sessions that have been made to the VoltDB Forum, using 'field' (limit results to 10):",
                 "SELECT username, field(json_data,'site') AS site, json_data FROM user_session_table "
                 + "WHERE field(json_data, 'site')='VoltDB Forum' ORDER BY username LIMIT 10");

        runQuery("Select VoltDB Forum sessions that are moderators (at most 10), using 'field' multiple times:",
                 "SELECT username, field(json_data,'moderator') AS moderator, json_data FROM user_session_table "
                 + "WHERE field(json_data, 'site')='VoltDB Forum' AND field(json_data, 'moderator')='true' ORDER BY username LIMIT 10");
        runQuery("Change user 'voltdb' to be a moderator, using 'set_field':",
                 "UPDATE user_session_table SET json_data = set_field(json_data, 'moderator', 'true') "
                 + "WHERE username='voltdb'");
        runQuery("Select VoltDB Forum sessions that are moderators (again, after update):",
                 "SELECT username, field(json_data,'moderator') AS moderator, json_data FROM user_session_table "
                 + "WHERE field(json_data, 'site')='VoltDB Forum' AND field(json_data, 'moderator')='true' ORDER BY username LIMIT 10");

        runQuery("Select records of (VoltDB Forum) logins with a download_count larger than one:",
                 "SELECT username, field(json_data,'download_count') AS download_count, json_data FROM user_session_table "
                 + "WHERE cast(field(json_data,'download_count') AS INTEGER) > 1 ORDER BY username LIMIT 10");

        runQuery("User pattern matching (SQL LIKE) to look for records of (VoltDB Forum) logins who have downloaded version v2.x "
                 + "- the old way, using nested calls of 'field':",
                 "SELECT username, field(field(json_data,'props'), 'download_version') AS download_version, json_data FROM user_session_table "
                 + "WHERE field(field(json_data,'props'), 'download_version') LIKE 'v2%' ORDER BY username LIMIT 10");
        runQuery("Identical query (logins who have downloaded version v2.x), using newer, simpler dot notation, rather than 'field' twice:",
                 "SELECT username, field(json_data,'props.download_version') AS download_version, json_data FROM user_session_table "
                 + "WHERE field(json_data, 'props.download_version') LIKE 'v2%' ORDER BY username LIMIT 10");
        runQuery("Change several users to set 'props.download_version' to 'v2.9', using 'set_field' with dot notation:",
                 "UPDATE user_session_table SET json_data = set_field(json_data, 'props.download_version', '\"v2.9\"') "
                 + "WHERE username < 'user-10000'");
        runQuery("User pattern matching (SQL LIKE) to look for records of (VoltDB Forum??) logins who have downloaded "
                 + "version v2.x (again, after update):",
                 "SELECT username, field(json_data,'props.download_version') AS download_version, json_data FROM user_session_table "
                 + "WHERE field(json_data, 'props.download_version') LIKE 'v2%' ORDER BY username LIMIT 10");

        runQuery("Select records of (VoltDB Blog) logins whose first role is 'writer' - the old way, using 'array_element':",
                 "SELECT username, array_element(field(json_data,'roles'), 0) AS first_role, json_data FROM user_session_table "
                 + "WHERE array_element(field(json_data,'roles'), 0)='writer' ORDER BY username LIMIT 10");
        runQuery("Identical query (logins whose first role is 'writer'), using newer, simpler bracket notation, "
                 + "rather than 'array_element':",
                 "SELECT username, field(json_data,'roles[0]') AS first_role, json_data FROM user_session_table "
                 + "WHERE field(json_data,'roles[0]')='writer' ORDER BY username LIMIT 10");

        runQuery("Select records of (VoltDB Blog) logins that have more than one role, using 'array_length':",
                 "SELECT username, field(json_data,'roles[0]') AS first_role, field(json_data,'roles[1]') AS second_role, "
                 + "field(json_data,'roles[2]') AS third_role, json_data FROM user_session_table "
                 + "WHERE array_length(field(json_data,'roles')) > 1 ORDER BY username LIMIT 10");
        runQuery("Change user voltdb2's first role to be 'author' (to match the others), using 'set_field' with bracket notation:",
                 "UPDATE user_session_table SET json_data = set_field(json_data, 'roles[0]', '\"author\"') "
                 + "WHERE username='voltdb2'");
        runQuery("Add an additional role to user 'voltdb2': 'administrator', using 'set_field' with [-1] to mean beyond the last:",
                 "UPDATE user_session_table SET json_data = set_field(json_data, 'roles[-1]', '\"administrator\"') "
                 + "WHERE username='voltdb2'");
        runQuery("Select records of (VoltDB Blog) logins that have more than one role (again, after updates):",
                 "SELECT username, field(json_data,'roles[0]') AS first_role, field(json_data,'roles[1]') AS second_role, "
                 + "field(json_data,'roles[2]') AS third_role, json_data FROM user_session_table "
                 + "WHERE array_length(field(json_data,'roles')) > 1 ORDER BY username LIMIT 10");

        runQuery("Select records of (VoltDB Blog) logins whose last role is 'administrator', using 'field' with [-1] to mean the last:",
                 "SELECT username, field(json_data,'roles[-1]') AS last_role, json_data FROM user_session_table "
                 + "WHERE field(json_data,'roles[-1]')='administrator' ORDER BY username LIMIT 10");

        runQuery("Look deep into the JSON data for records of (VoltDB Forum) logins who have downloaded version v3.0 and "
                 + "have specified a (first) client language of Java (combines dot and bracket notation):",
                 "SELECT username, json_data FROM user_session_table "
                 + "WHERE field(json_data, 'props.download_version')='v3.0' and field(json_data, 'props.client_languages[0]')='Java' "
                 + "ORDER BY username LIMIT 10");

        runQuery("Select records of (VoltDB Forum) logins that have more than one client language (combines "
                 + "'array_length' and 'field' with dot notation):",
                 "SELECT username, field(json_data,'props.client_languages') AS client_languages, json_data FROM user_session_table "
                 + "WHERE array_length(field(json_data,'props.client_languages')) > 1 ORDER BY username LIMIT 10");

        runQuery("Select records of (VoltDB Forum) logins whose second client language is Java:",
                 "SELECT username, field(json_data,'props.client_languages') AS client_languages, json_data FROM user_session_table "
                 + "WHERE field(json_data,'props.client_languages[1]')='Java' ORDER BY username LIMIT 10");
        runQuery("Change user voltdb's second client language to be 'Java' (to match):",
                 "UPDATE user_session_table SET json_data = set_field(json_data, 'props.client_languages[1]', '\"Java\"') "
                 + "WHERE username='voltdb'");
        runQuery("Change user voltdb's third client language to be 'Ruby':",
                 "UPDATE user_session_table SET json_data = set_field(json_data, 'props.client_languages[2]', '\"Ruby\"') "
                 + "WHERE username='voltdb'");
        runQuery("Select records of (VoltDB Forum) logins whose second client language is Java (again, after updates):",
                 "SELECT username, field(json_data,'props.client_languages') AS client_languages, json_data FROM user_session_table "
                 + "WHERE field(json_data,'props.client_languages[1]')='Java' ORDER BY username LIMIT 10");
        runQuery("Add an additional client language to user 'voltdb': 'Python' (using 'set_field', combines dot and "
                 + "bracket notation, with [-1]):",
                 "UPDATE user_session_table SET json_data = set_field(json_data, 'props.client_languages[-1]', '\"Python\"') "
                 + "WHERE username='voltdb'");
        runQuery("Select records of (VoltDB Forum) logins whose second client language is Java (again, after one more update):",
                 "SELECT username, field(json_data,'props.client_languages') AS client_languages, json_data FROM user_session_table "
                 + "WHERE field(json_data,'props.client_languages[1]')='Java' ORDER BY username LIMIT 10");

        runQuery("Select records of (VoltDB Blog) logins whose last client language is 'Python' (using 'field', combines "
                 + "dot and bracket notation, with [-1]):",
                 "SELECT username, field(json_data,'props.client_languages') AS client_languages, json_data FROM user_session_table "
                 + "WHERE field(json_data,'props.client_languages[-1]')='Python' ORDER BY username LIMIT 10");

        // Display the whole query results as a JSON String
        String SQL = "SELECT json_data FROM user_session_table WHERE username='voltdb' OR username='voltdb2' ORDER BY username";
        VoltTable table = runQuery("Retrieve the JSON data for two particular logins. Treat the whole result set as JSON:", SQL);
        System.out.println("The whole result set as JSON:");
        System.out.println(table.toJSONString());
        System.out.println();

        // Display individual results as JSON
        table = runQuery("Retrieve the JSON data for two particular logins. Just grab a JSON value from the result:", SQL);
        table.advanceRow();
        System.out.println("JSON value from result's first tuple:");
        System.out.println(table.get("json_data", VoltType.STRING));
        System.out.println();
        table.advanceRow();
        System.out.println("JSON value from result's second tuple:");
        System.out.println(table.get("json_data", VoltType.STRING));
        System.out.println();
    }

    public void initialize() throws Exception
    {
        // connect to the database on the local machine
        connect("localhost:21212");
    }

    public void shutdown() throws Exception
    {
        // close down the client connections
        client.close();
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
        JSONClient app = new JSONClient();

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
