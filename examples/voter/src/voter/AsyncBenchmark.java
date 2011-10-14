/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
 * This samples uses the native asynchronous request processing protocol
 * to post requests to the VoltDB server, thus leveraging to the maximum
 * VoltDB's ability to run requests in parallel on multiple database
 * partitions, and multiple servers.
 *
 * While asynchronous processing is (marginally) more convoluted to work
 * with and not adapted to all workloads, it is the preferred interaction
 * model to VoltDB as it guarantees blazing performance.
 *
 * Because there is a risk of 'firehosing' a database cluster (if the
 * cluster is too slow (slow or too few CPUs), this sample performs
 * self-tuning to target a specific latency (10ms by default).
 * This tuning process, as demonstrated here, is important and should be
 * part of your pre-launch evalution so you can adequately provision your
 * VoltDB cluster with the number of servers required for your needs.
 */

package voter;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;
import org.voltdb.client.exampleutils.IRateLimiter;
import org.voltdb.client.exampleutils.LatencyLimiter;
import org.voltdb.client.exampleutils.RateLimiter;

public class AsyncBenchmark
{
    // Initialize some common constants and variables
    private static final String ContestantNamesCSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    private static final AtomicLongArray VotingBoardResults = new AtomicLongArray(4);

    // Reference to the database connection we will use
    private static ClientConnection Con;

    // Application entry point
    public static void main(String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(AsyncBenchmark.class.getCanonicalName())
                .add("display-interval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("contestants", "contestant_count", "Number of contestants in the voting contest (from 1 to 10).", 6)
                .add("max-votes", "max_votes_per_phone_number", "Maximum number of votes accepted for a given voter (phone number).", 2)
                .add("rate-limit", "rate_limit", "Rate limit to start from (number of transactions per second).", 100000)
                .add("auto-tune", "auto_tune", "Flag indicating whether the benchmark should self-tune the transaction rate for a target execution latency (true|false).", "true")
                .add("latency-target", "latency_target", "Execution latency to target to tune transaction rate (in milliseconds).", 10.0d)
                .setArguments(args)
            ;

            // Retrieve parameters
            long displayInterval = apph.longValue("display-interval");
            long duration        = apph.longValue("duration");
            String servers       = apph.stringValue("servers");
            int port             = apph.intValue("port");
            int contestantCount  = apph.intValue("contestants");
            int maxVoteCount     = apph.intValue("max-votes");
            long rateLimit       = apph.longValue("rate-limit");
            boolean autoTune     = apph.booleanValue("auto-tune");
            double latencyTarget = apph.doubleValue("latency-target");
            final String csv     = apph.stringValue("stats");


            // Validate parameters
            apph.validate("contestants", (contestantCount > 0))
                .validate("max-votes", (maxVoteCount > 0))
                .validate("rate-limit", (rateLimit > 0))
                .validate("latency-target", (latencyTarget > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Get a client connection - we retry for a while in case the server hasn't started yet
            System.out.printf("Connecting to servers: %s at port: %d\n", servers, port);
            int sleep = 1000;
            while(true)
            {
                try
                {
                    Con = ClientConnectionPool.get(servers, port);
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
            final int maxContestants = (int)Con.execute("Initialize", contestantCount, ContestantNamesCSV).getResults()[0].fetchRow(0).getLong(0);

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
                    System.out.print(Con.getStatistics("Vote"));
                }
            }
            , displayInterval*1000l
            , displayInterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Pick the transaction rate limiter helping object to use based on user request (rate limiting or latency targeting)
            IRateLimiter limiter = null;
            if (autoTune)
                limiter = new LatencyLimiter(Con, "Vote", latencyTarget, rateLimit);
            else
                limiter = new RateLimiter(rateLimit);

            // Run the benchmark loop for the requested duration
            final long endTime = System.currentTimeMillis() + (1000l * duration);
            while (endTime > System.currentTimeMillis())
            {
                // Get the next phone call
                PhoneCallGenerator.PhoneCall call = switchboard.receive();

                // Post the vote, asynchronously
                Con.executeAsync(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) throws Exception
                    {
                        // Track the result of the vote (Accepted, Rejected, Failure...)
                        if (response.getStatus() == ClientResponse.SUCCESS)
                            VotingBoardResults.incrementAndGet((int)response.getResults()[0].fetchRow(0).getLong(0));
                        else
                            VotingBoardResults.incrementAndGet(3);
                    }
                }
                , "Vote"
                , call.phoneNumber
                , call.contestantNumber
                , maxVoteCount
                );

                // Use the limiter to throttle client activity
                limiter.throttle();
            }

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
            , Con.getStatistics("Vote").getExecutionCount()
            , VotingBoardResults.get(0)
            , VotingBoardResults.get(1)
            , VotingBoardResults.get(2)
            , VotingBoardResults.get(3)
            );

            // 2. Voting results
            VoltTable result = Con.execute("Results").getResults()[0];
            String winner = "";
            long winnerVoteCount = 0;
            while(result.advanceRow())
            {
                if (result.getLong(2) > winnerVoteCount)
                {
                    winnerVoteCount = result.getLong(2);
                    winner = result.getString(0);
                }
                System.out.printf("%s\t\t%,14d\n", result.getString(0), result.getLong(2));
            }
            System.out.printf("\n\nThe Winner is: %s\n-------------------------------------------------------------------------------------\n", winner);

            // 3. Performance statistics (we only care about the Vote procedure that we're benchmarking)
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " System Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.getStatistics("Vote").toString(false));

            // Dump statistics to a CSV file
            Con.saveStatistics(csv);

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
