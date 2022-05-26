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

package eng1969;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;
import org.voltdb.client.exampleutils.IRateLimiter;
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
            // Use the AppHelper utility class to retrieve command line application parameters
            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(AsyncBenchmark.class.getCanonicalName())
                .add("displayinterval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("pool-size", "pool_size", "Size of the record pool to operate on - larger sizes will cause a higher insert/update-delete rate.", 100000)
                .add("procedure", "procedure_name", "Procedure to call.", "UpdateKey")
                .add("wait", "wait_duration", "Wait duration (only when calling one of the Wait procedures), in milliseconds.", 0)
                .add("ratelimit", "rate_limit", "Rate limit to start from (number of transactions per second).", 100000)
                .add("run-loader", "Run the leveldb loader", "true")
                .setArguments(args)
            ;

            // Retrieve parameters
            final long displayInterval = apph.longValue("displayinterval");
            final long duration        = apph.longValue("duration");
            final String servers       = apph.stringValue("servers");
            final int port             = apph.intValue("port");
            final int poolSize         = apph.intValue("pool-size");
            final String procedure     = apph.stringValue("procedure");
            final long wait            = apph.intValue("wait");
            final long rateLimit       = apph.longValue("ratelimit");
            final String csv           = apph.stringValue("stats");
            final boolean runLoader    = apph.booleanValue("run-loader");

            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("pool-size", (duration > 0))
                .validate("wait", (wait >= 0))
                .validate("ratelimit", (rateLimit > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

            // Get a client connection - we retry for a while in case the server hasn't started yet
            Con = ClientConnectionPool.getWithRetry(servers, port);

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

            // Pick the transaction rate limiter helping object to use for rate limiting
            IRateLimiter limiter = new RateLimiter(rateLimit);

            // Run the loader first.
            if (runLoader) {
                doLoader(poolSize);
            }

            // Run the benchmark loop for the requested duration
            final long endTime = System.currentTimeMillis() + (1000l * duration);
            Random rand = new Random();
            while (endTime > System.currentTimeMillis())
            {
                doBenchmark(procedure, poolSize, rand, wait);

                // Use the limiter to throttle client activity
                limiter.throttle();
            }

            // We're done - stop the performance statistics display task
            timer.cancel();

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
        }
        catch(Exception x)
        {
            System.out.println("Exception: " + x);
            x.printStackTrace();
        }
    }

    // There are poolSize rowid_groups in the database (default = 100,000)
    // Each rowid_group has 1000 unique rowids such that (rowid_group, rowid) is unique.
    // This gives 100M records in the DB by default.
    private static void doBenchmark(String procedure, final int poolSize, final Random rand, long wait) throws Exception {

        final int totalPartitions = 4;
        final int maxGroupsPerPartition = 1000;

        // ~1% chance of cold data
        boolean coldData = (Math.abs(rand.nextLong()) % 100) <= 1;

        long rowid_group = coldData ?
                Math.abs(rand.nextLong()) % poolSize :
                Math.abs(rand.nextLong()) % (totalPartitions * maxGroupsPerPartition);

        long rowid = Math.abs(rand.nextLong()) % 1000;

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
        , rowid
        , rowid_group
        , new String("ABCDEFGHIJKLMNOPQRSTUVWXYZ").getBytes()
        );
    }

    private static void doLoader(final int poolSize) throws Exception {
        for (int i=0; i <= poolSize; i++) {
            for (int rowid = 0; rowid < 1000; rowid++) {
                Con.executeAsync(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) throws Exception
                    {
                        if (response.getStatus() != ClientResponse.SUCCESS) {
                            System.out.println("Loader failed with response: " + response.getStatusString());
                        }
                    }
                }
                , "CreateKey"
                , Long.valueOf(rowid)
                , Long.valueOf(i)
                , new String("ABCDEF").getBytes()
                );
            }
            if (i % 10000 == 0) {
                System.out.println("Loaded " + i + " groups.");
            }
        }
    }


}
