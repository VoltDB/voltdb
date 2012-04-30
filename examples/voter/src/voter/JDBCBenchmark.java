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
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the standard JDBC interface
 * available for VoltDB.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package voter;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.CLIConfig;
import org.voltdb.jdbc.IVoltDBConnection;

public class JDBCBenchmark
{
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
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (duration < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (contestants <= 0) exitWithMessageAndUsage("contestants must be > 0");
            if (maxvotes <= 0) exitWithMessageAndUsage("maxvotes must be > 0");
            if (threads <= 0) exitWithMessageAndUsage("threads must be > 0");
        }
    }

    // Initialize some common constants and variables
    private static final String ContestantNamesCSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    private static final AtomicLongArray VotingBoardResults = new AtomicLongArray(4);

    // Reference to the database connection we will use in them main thread
    private static Connection Con;

    // Class for each thread that will be run in parallel, performing JDBC requests against the VoltDB server
    private static class ClientThread implements Runnable
    {
        private final String url;
        private final long duration;
        private final PhoneCallGenerator switchboard;
        private final int maxVoteCount;
        public ClientThread(String url, PhoneCallGenerator switchboard, long duration, int maxVoteCount) throws Exception
        {
            this.url = url;
            this.duration = duration;
            this.switchboard = switchboard;
            this.maxVoteCount = maxVoteCount;
        }

        @Override
        public void run()
        {
            // Each thread gets its dedicated JDBC connection, and posts votes against it.
            Connection con = null;
            try
            {
                con = DriverManager.getConnection(url, "", "");
                final CallableStatement voteCS = con.prepareCall("{call Vote(?,?,?)}");
                long endTime = System.currentTimeMillis() + (1000l * this.duration);
                while (endTime > System.currentTimeMillis())
                {
                    PhoneCallGenerator.PhoneCall call = this.switchboard.receive();
                    voteCS.setLong(1, call.phoneNumber);
                    voteCS.setInt(2, call.contestantNumber);
                    voteCS.setLong(3, this.maxVoteCount);
                    try
                    {
                        VotingBoardResults.incrementAndGet(voteCS.executeUpdate());
                    }
                    catch(Exception x)
                    {
                        VotingBoardResults.incrementAndGet(3);
                    }
                }
            }
            catch(Exception x)
            {
                System.err.println("Exception: " + x);
                x.printStackTrace();
            }
            finally
            {
                try { con.close(); } catch (Exception x) {}
            }
        }
    }

    // Application entry point
    public static void main(String[] args)
    {
        try
        {
            VoterConfig config = new VoterConfig();
            config.parse(JDBCBenchmark.class.getName(), args);

            System.out.println(config.getConfigDumpString());

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We need only do this once, to "hot cache" the JDBC driver reference so the JVM may realize it's there.
            Class.forName("org.voltdb.jdbc.Driver");

            // Prepare the JDBC URL for the VoltDB driver
            String url = "jdbc:voltdb://" + config.servers;

            // Get a client connection - we retry for a while in case the server hasn't started yet
            System.out.printf("Connecting to: %s\n", url);
            int sleep = 1000;
            while(true)
            {
                try
                {
                    Con = DriverManager.getConnection(url, "", "");
                    break;
                }
                catch (Exception e)
                {
                    System.err.printf("Connection failed - retrying in %d second(s).\n", sleep/1000);
                    try {Thread.sleep(sleep);} catch(Exception tie){}
                    if (sleep < 8000)
                        sleep += sleep;
                }
            }
            System.out.println("Connected.  Starting benchmark.");

            // Initialize the application
            final CallableStatement initializeCS = Con.prepareCall("{call Initialize(?,?)}");
            initializeCS.setInt(1, config.contestants);
            initializeCS.setString(2, ContestantNamesCSV);
            final int maxContestants = initializeCS.executeUpdate();

            // Get a Phone Call Generator that will simulate voter entries from the call center
            PhoneCallGenerator switchboard = new PhoneCallGenerator(maxContestants);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the Vote procedure
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask()
            {
                @Override
                public void run()
                {
                    try { System.out.print(Con.unwrap(IVoltDBConnection.class).getStatistics("Vote")); } catch(Exception x) {}
                }
            }
            , config.displayinterval*1000l
            , config.displayinterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create multiple processing threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < config.threads; i++)
                threads.add(new Thread(new ClientThread(url, switchboard, config.duration, config.maxvotes)));

            // Start threads
            for (Thread thread : threads)
                thread.start();

            // Wait for threads to complete
            for (Thread thread : threads)
                thread.join();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Now print application results:

            // 1. Voting Board statistics, Voting results and performance statistics
            System.out.printf(
              "-------------------------------------------------------------------------------------\n"
            + " Voting Results\n"
            + "-------------------------------------------------------------------------------------\n\n"
            + "A total of %d votes was received...\n"
            + " - %,9d Accepted\n"
            + " - %,9d Rejected (Invalid Contestant)\n"
            + " - %,9d Rejected (Maximum Vote Count Reached)\n"
            + " - %,9d Failed (Transaction Error)\n"
            + "\n\n"
            + "-------------------------------------------------------------------------------------\n"
            + "Contestant Name\t\tVotes Received\n"
            , Con.unwrap(IVoltDBConnection.class).getStatistics("Vote").getExecutionCount()
            , VotingBoardResults.get(0)
            , VotingBoardResults.get(1)
            , VotingBoardResults.get(2)
            , VotingBoardResults.get(3)
            );

            // 2. Voting results
            final CallableStatement resultsCS = Con.prepareCall("{call Results}");
            ResultSet result = resultsCS.executeQuery();
            String winner = "";
            long winnerVoteCount = 0;
            while (result.next())
            {
                if (result.getLong(3) > winnerVoteCount)
                {
                    winnerVoteCount = result.getLong(3);
                    winner = result.getString(1);
                }
                System.out.printf("%s\t\t%,14d\n", result.getString(1), result.getLong(3));
            }
            System.out.printf("\n\nThe Winner is: %s\n-------------------------------------------------------------------------------------\n", winner);

            // 3. Performance statistics (we only care about the Vote procedure that we're benchmarking)
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " System Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.unwrap(IVoltDBConnection.class).getStatistics("Vote").toString(false));

            // Dump statistics to a CSV file
            Con.unwrap(IVoltDBConnection.class).saveStatistics(config.statsfile);

            Con.close();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

        }
        catch(Exception x)
        {
            System.out.println("Exception: " + x);
            x.printStackTrace();
        }
    }
}
