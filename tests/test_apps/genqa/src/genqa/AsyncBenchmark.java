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

package genqa;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.Random;

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
    private static final AtomicLongArray TrackingResults = new AtomicLongArray(2);

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
                .add("displayinterval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("poolsize", "pool_size", "Size of the record pool to operate on - larger sizes will cause a higher insert/update-delete rate.", 100000)
                .add("procedure", "procedure_name", "Procedure to call.", "JiggleSinglePartition")
                .add("wait", "wait_duration", "Wait duration (only when calling one of the Wait procedures), in milliseconds.", 0)
                .add("ratelimit", "rate_limit", "Rate limit to start from (number of transactions per second).", 100000)
                .add("autotune", "auto_tune", "Flag indicating whether the benchmark should self-tune the transaction rate for a target execution latency (true|false).", "true")
                .add("latencytarget", "latency_target", "Execution latency to target to tune transaction rate (in milliseconds).", 10.0d)
                .setArguments(args)
            ;

            // Retrieve parameters
            final long displayInterval = apph.longValue("displayinterval");
            final long duration        = apph.longValue("duration");
            final String servers       = apph.stringValue("servers");
            final int port             = apph.intValue("port");
            final int poolSize         = apph.intValue("poolsize");
            final String procedure     = apph.stringValue("procedure");
            final long wait            = apph.intValue("wait");
            final long rateLimit       = apph.longValue("ratelimit");
            final boolean autoTune     = apph.booleanValue("autotune");
            final double latencyTarget = apph.doubleValue("latencytarget");
            final String csv           = apph.stringValue("statsfile");


            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("poolsize", (duration > 0))
                .validate("wait", (wait >= 0))
                .validate("ratelimit", (rateLimit > 0))
                .validate("latencytarget", (latencyTarget > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Get a client connection - we retry for a while in case the server hasn't started yet
            Con = ClientConnectionPool.getWithRetry(servers, port);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the procedure
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask()
            {
                @Override
                public void run()
                {
                    System.out.print(Con.getStatistics(procedure));
                }
            }
            , displayInterval*1000l
            , displayInterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Pick the transaction rate limiter helping object to use based on user request (rate limiting or latency targeting)
            IRateLimiter limiter = null;
            if (autoTune)
                limiter = new LatencyLimiter(Con, procedure, latencyTarget, rateLimit);
            else
                limiter = new RateLimiter(rateLimit);

            // Run the benchmark loop for the requested duration
            final long endTime = System.currentTimeMillis() + (1000l * duration);
            Random rand = new Random();
            while (endTime > System.currentTimeMillis())
            {
                // Post the request, asynchronously
                Con.executeAsync(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) throws Exception
                    {
                        // Track the result of the request (Success, Failure)
                        if (response.getStatus() == ClientResponse.SUCCESS)
                            TrackingResults.incrementAndGet(0);
                        else
                            TrackingResults.incrementAndGet(1);
                    }
                }
                , procedure
                , (long)rand.nextInt(poolSize)
                , wait
                );

                // Use the limiter to throttle client activity
                limiter.throttle();
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Now print application results:

            // 1. Tracking statistics
            System.out.printf(
              "-------------------------------------------------------------------------------------\n"
            + " Benchmark Results\n"
            + "-------------------------------------------------------------------------------------\n\n"
            + "A total of %d calls was received...\n"
            + " - %,9d Succeeded\n"
            + " - %,9d Failed (Transaction Error)\n"
            + "\n\n"
            + "-------------------------------------------------------------------------------------\n"
            , TrackingResults.get(0)+TrackingResults.get(1)
            , TrackingResults.get(0)
            , TrackingResults.get(1)
            );

            // 3. Performance statistics (we only care about the procedure that we're benchmarking)
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " System Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.getStatistics(procedure).toString(false));

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
