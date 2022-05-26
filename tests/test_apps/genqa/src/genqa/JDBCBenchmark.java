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
 * synchronous requests to the database, using the standard JDBC interface
 * available for VoltDB.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package genqa;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.Random;

import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.jdbc.IVoltDBConnection;

public class JDBCBenchmark
{
    // Initialize some common constants and variables
    private static final AtomicLongArray TrackingResults = new AtomicLongArray(2);

    // Reference to the database connection we will use in them main thread
    private static Connection Con;
    private static ClientStatsContext periodicStatsContext;
    private static long benchmarkStartTS;

    // Class for each thread that will be run in parallel, performing JDBC requests against the VoltDB server
    private static class ClientThread implements Runnable
    {
        private final String url;
        private final long duration;
        private final String procedure;
        private final Random rand = new Random();
        private final int poolSize;
        private final long wait;
        public ClientThread(String url, String procedure, int poolSize, long wait, long duration) throws Exception
        {
            this.url = url;
            this.duration = duration;
            this.procedure = procedure;
            this.poolSize = poolSize;
            this.wait = wait;
        }

        @Override
        public void run()
        {
            // Each thread gets its dedicated connection, and posts requests against it.
            Connection con = null;
            try
            {
                con = DriverManager.getConnection(url, "", "");
                final CallableStatement procedureCS = con.prepareCall("{call " + procedure + "(?,?)}");
                long endTime = System.currentTimeMillis() + (1000l * this.duration);
                while (endTime > System.currentTimeMillis())
                {
                    procedureCS.setLong(1, (long)rand.nextInt(this.poolSize));
                    procedureCS.setLong(2, this.wait);
                    try
                    {
                        //procedureCS.executeUpdate();
                        procedureCS.execute();
                        TrackingResults.incrementAndGet(0);
                    }
                    catch(Exception x)
                    {
                        x.printStackTrace();
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

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public static synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms, ", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        System.out.printf("Success/Failures %d/%d\n", TrackingResults.get(0),
                TrackingResults.get(1));
    }

    // Application entry point
    public static void main(String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(JDBCBenchmark.class.getCanonicalName())
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
            final int poolSize         = apph.intValue("poolsize");
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

            // We need only do this once, to "hot cache" the JDBC driver reference so the JVM may realize it's there.
            Class.forName("org.voltdb.jdbc.Driver");

            // Prepare the JDBC URL for the VoltDB driver
            String url = "jdbc:voltdb://" + servers + ":" + port;

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

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            final ClientStatsContext fullStatsContext = ((IVoltDBConnection) Con).createStatsContext();
            periodicStatsContext = ((IVoltDBConnection) Con).createStatsContext();
            benchmarkStartTS = System.currentTimeMillis();

            // Create a Timer task to display performance data on the procedure
            Timer timer = new Timer();
            TimerTask statsPrinting = new TimerTask() {
                @Override
                public void run() { printStatistics(); }
            };
            timer.scheduleAtFixedRate(statsPrinting
            , displayInterval*1000l
            , displayInterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create multiple processing threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++)
                threads.add(new Thread(new ClientThread(url, procedure, poolSize, wait, duration)));

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
            try {
                //System.out.print(fullStatsContext.getStatsForProcedure(procedure).toString());
                System.out.print(fullStatsContext.getStats().toString());
            } catch  (Exception e) {
                e.printStackTrace();
            }
            if (TrackingResults.get(0) == 0 ) {
                System.err.println("ERROR No transactions succeeded");
                System.exit(1);
            }
            // Dump statistics to a CSV file
            Con.unwrap(IVoltDBConnection.class).saveStatistics(fullStatsContext.getStats(), csv);

            Con.close();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

        }
        catch(Exception x)
        {
            System.out.println("ERROR Exception: " + x);
            x.printStackTrace();
            System.exit(1);
        }
    }
}
