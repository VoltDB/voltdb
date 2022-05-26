/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package voter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.CLIConfig;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.utils.Encoder;

import voter.procedures.Vote;

public class HTTPBenchmark {

    // Initialize some common constants and variables
    static final String CONTESTANT_NAMES_CSV =
            "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
            "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
            "Kurt Walser,Ericka Dieter,Loraine Nygren,Tania Mattioli";

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final VoterConfig config;
    // Phone number generator
    PhoneCallGenerator switchboard;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // voter benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);


    static class Response {
        public byte status = 0;
        public String statusString = null;
        public byte appStatus = Byte.MIN_VALUE;
        public String appStatusString = null;
        public VoltTable[] results = new VoltTable[0];
        public String exception = null;
    }

    static String getHTTPVarString(Map<String,String> params) throws UnsupportedEncodingException {
        String s = "";
        for (Entry<String, String> e : params.entrySet()) {
            String encodedValue = URLEncoder.encode(e.getValue(), "UTF-8");
            s += "&"+ e.getKey() + "=" + encodedValue;
        }
        s = s.substring(1);
        return s;
    }

    public static String callProcOverJSONRaw(String varString, int expectedCode) throws Exception {
        URL jsonAPIURL = new URL("http://localhost:8080/api/1.0/");

        HttpURLConnection conn = (HttpURLConnection) jsonAPIURL.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.connect();

        OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
        out.write(varString);
        out.flush();
        out.close();
        out = null;
        conn.getOutputStream().close();

        BufferedReader in = null;
        try {
            if(conn.getInputStream()!=null){
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getInputStream(), "UTF-8"));
            }
        } catch(IOException e){
            if(conn.getErrorStream()!=null){
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getErrorStream(), "UTF-8"));
            }
        }
        if(in==null) {
            throw new Exception("Unable to read response from server");
        }

        StringBuffer decodedString = new StringBuffer();
        String line;
        while ((line = in.readLine()) != null) {
            decodedString.append(line);
        }
        in.close();
        in = null;
        // get result code
        int responseCode = conn.getResponseCode();

        String response = decodedString.toString();

        try {
            conn.getInputStream().close();
            conn.disconnect();
        }
        // ignore closing problems here
        catch (Exception e) {}
        conn = null;

        //System.err.println(response);

        return response;
    }

    public static String getHashedPasswordForHTTPVar(String password) {
        assert(password != null);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte hashedPassword[] = null;
        try {
            hashedPassword = md.digest(password.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("JVM doesn't support UTF-8. Please use a supported JVM", e);
        }

        String retval = Encoder.hexEncode(hashedPassword);
        return retval;
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, false, 200 /* HTTP_OK */);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin) throws Exception {
        return callProcOverJSON(procName, pset, username, password, preHash, admin, 200 /* HTTP_OK */);
    }

    public static String callProcOverJSON(String procName, ParameterSet pset, String username, String password, boolean preHash, boolean admin, int expectedCode) throws Exception {
        // Call insert
        String paramsInJSON = pset.toJSONString();
        //System.out.println(paramsInJSON);
        HashMap<String,String> params = new HashMap<String,String>();
        params.put("Procedure", procName);
        params.put("Parameters", paramsInJSON);
        if (username != null) {
            params.put("User", username);
        }
        if (password != null) {
            if (preHash) {
                params.put("Hashedpassword", getHashedPasswordForHTTPVar(password));
            } else {
                params.put("Password", password);
            }
        }
        if (admin) {
            params.put("admin", "true");
        }

        String varString = getHTTPVarString(params);

        varString = getHTTPVarString(params);

        return callProcOverJSONRaw(varString, expectedCode);
    }

    public static Response responseFromJSON(String jsonStr) throws JSONException, IOException {
        Response response = new Response();
        JSONObject jsonObj = new JSONObject(jsonStr);
        JSONArray resultsJson = jsonObj.getJSONArray("results");
        response.results = new VoltTable[resultsJson.length()];
        for (int i = 0; i < response.results.length; i++) {
            JSONObject tableJson = resultsJson.getJSONObject(i);
            response.results[i] =  VoltTable.fromJSONObject(tableJson);
            System.out.println(response.results[i].toString());
        }
        if (jsonObj.isNull("status") == false) {
            response.status = (byte) jsonObj.getInt("status");
        }
        if (jsonObj.isNull("appstatus") == false) {
            response.appStatus = (byte) jsonObj.getInt("appstatus");
        }
        if (jsonObj.isNull("statusstring") == false) {
            response.statusString = jsonObj.getString("statusstring");
        }
        if (jsonObj.isNull("appstatusstring") == false) {
            response.appStatusString = jsonObj.getString("appstatusstring");
        }
        if (jsonObj.isNull("exception") == false) {
            response.exception = jsonObj.getString("exception");
        }

        return response;
    }

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class VoterConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 120;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 5;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of contestants in the voting contest (from 1 to 10).")
        int contestants = 6;

        @Option(desc = "Maximum number of votes cast per voter.")
        int maxvotes = 2;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Number of concurrent threads synchronously calling procedures.")
        int threads = 40;

        @Override
        public void validate() {
            if (duration <= 0) {
                exitWithMessageAndUsage("duration must be > 0");
            }
            if (warmup < 0) {
                exitWithMessageAndUsage("warmup must be >= 0");
            }
            if (displayinterval <= 0) {
                exitWithMessageAndUsage("displayinterval must be > 0");
            }
            if (contestants <= 0) {
                exitWithMessageAndUsage("contestants must be > 0");
            }
            if (maxvotes <= 0) {
                exitWithMessageAndUsage("maxvotes must be > 0");
            }
            if (threads <= 0) {
                exitWithMessageAndUsage("threads must be > 0");
            }
        }
    }


    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public HTTPBenchmark(VoterConfig config) {
        this.config = config;

        switchboard = new PhoneCallGenerator(config.contestants);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
    }


    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {

        // 1. Voting Board statistics, Voting results and performance statistics
        String display = "\n" +
                         HORIZONTAL_RULE +
                         " Voting Results\n" +
                         HORIZONTAL_RULE +
                         " - %,9d Accepted\n" +
                         " - %,9d Rejected (Invalid Contestant)\n" +
                         " - %,9d Rejected (Maximum Vote Count Reached)\n" +
                         " - %,9d Failed (Transaction Error)\n\n";
        System.out.printf(display,
                acceptedVotes.get(), badContestantVotes.get(),
                badVoteCountVotes.get(), failedVotes.get());

        // 2. Voting results
        String res = callProcOverJSON("Results", ParameterSet.emptyParameterSet(), "myadmin", "voltdbadmin", false);
        Response response = responseFromJSON(res);
        VoltTable result = response.results[0];

        System.out.println("Contestant Name\t\tVotes Received");
        while(result.advanceRow()) {
            System.out.printf("%s\t\t%,14d\n", result.getString(0), result.getLong(2));
        }
        System.out.printf("\nThe Winner is: %s\n\n", result.fetchRow(0).getString(0));
    }

    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     *
     */
    class VoterThread implements Runnable {

        @Override
        public void run() {

            // Set up some usernames/passwords
            String names[] = {"john", "scott", "bruce"};
            String passwords[] = {"piekos", "jarr", "reading"};

            // Choose random credentials
            int idx = (int)(Math.random() * names.length);
            String username = names[idx];
            String password = passwords[idx];

            while (warmupComplete.get() == false) {
                // Get the next phone call
                PhoneCallGenerator.PhoneCall call = switchboard.receive();

                // Use the JSON/HTTP API to synchronously call the "Vote" procedure
                try {
                    ParameterSet pset = ParameterSet.fromArrayNoCopy(call.phoneNumber,
                                                                    call.contestantNumber,
                                                                    config.maxvotes);
                    callProcOverJSON("Vote", pset, username, password, true);
                }
                catch (Exception e) {}
            }

            while (benchmarkComplete.get() == false) {
                // Get the next phone call
                PhoneCallGenerator.PhoneCall call = switchboard.receive();

                // Use the JSON/HTTP API to synchronously call the "Vote" procedure
                try {
                    ParameterSet pset = ParameterSet.fromArrayNoCopy(call.phoneNumber,
                                                                    call.contestantNumber,
                                                                    config.maxvotes);

                    String res = callProcOverJSON("Vote", pset, username, password, true);
                    Response response = responseFromJSON(res);
                    assert (response.results[0].advanceRow());
                    long resultCode = response.results[0].getLong(0);
                    if (resultCode == Vote.ERR_INVALID_CONTESTANT) {
                        badContestantVotes.incrementAndGet();
                    }
                    else if (resultCode == Vote.ERR_VOTER_OVER_VOTE_LIMIT) {
                        badVoteCountVotes.incrementAndGet();
                    }
                    else {
                        assert(resultCode == Vote.VOTE_SUCCESSFUL);
                        acceptedVotes.incrementAndGet();
                    }
                }
                catch (Exception e) {
                    failedVotes.incrementAndGet();
                    e.printStackTrace(System.out);
                }
            }

        }

    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        System.out.println("\nPopulating Static Tables\n");

        // initialize using JSON over HTTP call
        ParameterSet pset = ParameterSet.fromArrayNoCopy(config.contestants, CONTESTANT_NAMES_CSV);
        String res = callProcOverJSON("Initialize", pset, "myadmin", "voltdbadmin", false);
        System.out.println("JSON response INITIALIZE: " + res);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // create/start the requested number of threads
        Thread[] voterThreads = new Thread[config.threads];
        for (int i = 0; i < config.threads; ++i) {
            voterThreads[i] = new Thread(new VoterThread());
            voterThreads[i].start();
        }

        // Run the benchmark loop for the requested warmup time
        System.out.println("Warming up...");
        Thread.sleep(1000l * config.warmup);

        // signal to threads to end the warmup phase
        warmupComplete.set(true);

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();

        // Run the benchmark loop for the requested warmup time
        System.out.println("\nRunning benchmark...");
        Thread.sleep(1000l * config.duration);

        // stop the threads
        benchmarkComplete.set(true);

        // join on the threads
        for (Thread t : voterThreads) {
            t.join();
        }

        // print the summary results
        printResults();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link VoterConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        VoterConfig config = new VoterConfig();
        config.parse(HTTPBenchmark.class.getName(), args);

        HTTPBenchmark benchmark = new HTTPBenchmark(config);
        benchmark.runBenchmark();
    }
}
