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

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.VoltTable;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;

public class SyncBenchmark
{
    // Initialize some common constants and variables
    private static final String ContestantNamesCSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    private static final AtomicLongArray VotingBoardResults = new AtomicLongArray(4);

    // Reference to the database connection we will use in them main thread
    private static ClientConnection Con;

    // Class for each thread that will be run in parallel, performing requests against the VoltDB server
    private static class ClientThread implements Runnable
    {
        private final String servers;
        private final int port;
        private final long duration;
        private final PhoneCallGenerator switchboard;
        private final int maxVoteCount;
        public ClientThread(String servers, int port, PhoneCallGenerator switchboard, long duration, int maxVoteCount) throws Exception
        {
            this.servers = servers;
            this.port = port;
            this.duration = duration;
            this.switchboard = switchboard;
            this.maxVoteCount = maxVoteCount;
        }

        @Override
        public void run()
        {
            // Each thread gets its dedicated JDBC connection, and posts votes against it.
            ClientConnection con = null;
            try
            {
                con = ClientConnectionPool.get(servers, port);
                long endTime = System.currentTimeMillis() + (1000l * this.duration);
                while (endTime > System.currentTimeMillis())
                {
                    PhoneCallGenerator.PhoneCall call = this.switchboard.receive();
                    try
                    {
                        VotingBoardResults.incrementAndGet((int)con.execute("Vote", call.phoneNumber, call.contestantNumber, this.maxVoteCount).getResults()[0].fetchRow(0).getLong(0));
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

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(SyncBenchmark.class.getCanonicalName())
                .add("threads", "thread_count", "Number of concurrent threads attacking the database.", 1)
                .add("display-interval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("contestants", "contestant_count", "Number of contestants in the voting contest (from 1 to 10).", 6)
                .add("max-votes", "max_votes_per_phone_number", "Maximum number of votes accepted for a given voter (phone number).", 2)
                .setArguments(args)
            ;

            // Retrieve parameters
            int threadCount      = apph.intValue("threads");
            long displayInterval = apph.longValue("display-interval");
            long duration        = apph.longValue("duration");
            String servers       = apph.stringValue("servers");
            int port             = apph.intValue("port");
            int contestantCount  = apph.intValue("contestants");
            int maxVoteCount     = apph.intValue("max-votes");

            // Validate parameters
            apph.validate("threads", (threadCount > 0))
                .validate("contestants", (contestantCount > 0))
                .validate("max-votes", (maxVoteCount > 0))
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

            // Create multiple processing threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++)
                threads.add(new Thread(new ClientThread(servers, port, switchboard, duration, maxVoteCount)));

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
