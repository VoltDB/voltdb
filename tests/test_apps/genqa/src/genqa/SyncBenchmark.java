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

package genqa;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;

public class SyncBenchmark
{
    // Initialize some common constants and variables
    private static final AtomicLongArray TrackingResults = new AtomicLongArray(2);

    // Reference to the database connection we will use in them main thread
    private static ClientConnection Con;

    // Class for each thread that will be run in parallel, performing requests against the VoltDB server
    private static class ClientThread implements Runnable
    {
        private final String servers;
        private final int port;
        private final long duration;
        private final String procedure;
        private final Random rand = new Random();
        private final int poolSize;
        private final long wait;
        public ClientThread(String servers, int port, String procedure, int poolSize, long wait, long duration) throws Exception
        {
            this.servers = servers;
            this.port = port;
            this.duration = duration;
            this.procedure = procedure;
            this.poolSize = poolSize;
            this.wait = wait;
        }

        @Override
        public void run()
        {
            // Each thread gets its dedicated connection, and posts requests against it.
            ClientConnection con = null;
            try
            {
                con = ClientConnectionPool.get(servers, port);
                long endTime = System.currentTimeMillis() + (1000l * this.duration);
                while (endTime > System.currentTimeMillis())
                {
                    try
                    {
                        con.execute(procedure, (long)rand.nextInt(this.poolSize), this.wait);
                        TrackingResults.incrementAndGet(0);
                    }
                    catch(Exception x)
                    {
                        TrackingResults.incrementAndGet(1);
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
                .add("displayinterval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("poolsize", "pool_size", "Size of the record pool to operate on - larger sizes will cause a higher insert/update-delete rate.", 100000)
                .add("procedure", "procedure_name", "Procedure to call.", "JiggleSinglePartition")
                .add("wait", "wait_duration", "Wait duration (only when calling one of the Wait procedures), in milliseconds.", 0)
                .setArguments(args)
            ;

            // Retrieve parameters
            final int threadCount      = apph.intValue("threads");
            final long displayInterval = apph.longValue("displayinterval");
            final long duration        = apph.longValue("duration");
            final String servers       = apph.stringValue("servers");
            final int port             = apph.intValue("port");
            final int poolSize         = apph.intValue("pool-size");
            final String procedure     = apph.stringValue("procedure");
            final long wait            = apph.intValue("wait");
            final String csv           = apph.stringValue("statsfile");

            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("threads", (threadCount > 0))
                .validate("poolsize", (poolSize > 0))
                .validate("wait", (wait >= 0))
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

            // Create multiple processing threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++)
                threads.add(new Thread(new ClientThread(servers, port, procedure, poolSize, wait, duration)));

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
