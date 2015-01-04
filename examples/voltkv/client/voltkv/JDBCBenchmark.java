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
 * synchronous requests to the database, using the standard JDBC interface
 * available for VoltDB.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */
package voltkv;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.CLIConfig;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.jdbc.IVoltDBConnection;

public class JDBCBenchmark
{
    // Initialize some common constants and variables
    private static final AtomicLongArray GetStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray GetCompressionResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutCompressionResults = new AtomicLongArray(2);

    private static ClientStatsContext periodicStatsContext;
    private static long benchmarkStartTS;

    // Reference to the database connection we will use in them main thread
    private static Connection Con;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class KVConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 10;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of keys to preload.")
        int poolsize = 100000;

        @Option(desc = "Whether to preload a specified number of keys and values.")
        boolean preload = true;

        @Option(desc = "Fraction of ops that are gets (vs puts).")
        double getputratio = 0.90;

        @Option(desc = "Size of keys in bytes.")
        int keysize = 32;

        @Option(desc = "Minimum value size in bytes.")
        int minvaluesize = 1024;

        @Option(desc = "Maximum value size in bytes.")
        int maxvaluesize = 1024;

        @Option(desc = "Number of values considered for each value byte.")
        int entropy = 127;

        @Option(desc = "Compress values on the client side.")
        boolean usecompression= false;

        @Option(desc = "Number of concurrent threads synchronously calling procedures.")
        int threads = 40;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (poolsize <= 0) exitWithMessageAndUsage("poolsize must be > 0");
            if (getputratio < 0) exitWithMessageAndUsage("getputratio must be >= 0");
            if (getputratio > 1) exitWithMessageAndUsage("getputratio must be <= 1");

            if (keysize <= 0) exitWithMessageAndUsage("keysize must be > 0");
            if (keysize > 250) exitWithMessageAndUsage("keysize must be <= 250");
            if (minvaluesize <= 0) exitWithMessageAndUsage("minvaluesize must be > 0");
            if (maxvaluesize <= 0) exitWithMessageAndUsage("maxvaluesize must be > 0");
            if (entropy <= 0) exitWithMessageAndUsage("entropy must be > 0");
            if (entropy > 127) exitWithMessageAndUsage("entropy must be <= 127");

            if (threads <= 0) exitWithMessageAndUsage("threads must be > 0");
        }
    }

    // Class for each thread that will be run in parallel, performing JDBC requests against the VoltDB server
    private static class ClientThread implements Runnable
    {
        private final String url;
        private final long duration;
        private final PayloadProcessor processor;
        private final double getPutRatio;
        public ClientThread(String url, PayloadProcessor processor, long duration, double getPutRatio) throws Exception
        {
            this.url = url;
            this.duration = duration;
            this.processor = processor;
            this.getPutRatio = getPutRatio;
        }

        @Override
        public void run()
        {
            // Each thread gets its dedicated JDBC connection, and posts operations against it.
            Connection con = null;
            try
            {
                con = DriverManager.getConnection(url, "", "");
                final CallableStatement getCS = con.prepareCall("{call STORE.select(?)}");
                final CallableStatement putCS = con.prepareCall("{call STORE.upsert(?,?)}");
                long endTime = System.currentTimeMillis() + (1000l * this.duration);
                Random rand = new Random();
                while (endTime > System.currentTimeMillis())
                {
                    // Decide whether to perform a GET or PUT operation
                    if (rand.nextDouble() < getPutRatio)
                    {
                        try
                        {
                            getCS.setString(1, processor.generateRandomKeyForRetrieval());
                            ResultSet result = getCS.executeQuery();
                            if (result.next())
                            {
                                final PayloadProcessor.Pair pair = processor.retrieveFromStore(result.getString(1), result.getBytes(2));
                                GetStoreResults.incrementAndGet(0);
                                GetCompressionResults.addAndGet(0, pair.getStoreValueLength());
                                GetCompressionResults.addAndGet(1, pair.getRawValueLength());
                            }
                            else
                                GetStoreResults.incrementAndGet(1);
                        }
                        catch(Exception x)
                        {
                            GetStoreResults.incrementAndGet(1);
                        }
                    }
                    else
                    {
                        final PayloadProcessor.Pair pair = processor.generateForStore();
                        try
                        {
                            // Put a key/value pair using inbuilt upsert procedure, asynchronously
                            putCS.setString(1, pair.Key);
                            putCS.setBytes(2, pair.getStoreValue());
                            putCS.executeUpdate();
                            PutStoreResults.incrementAndGet(0);
                        }
                        catch(Exception x)
                        {
                            PutStoreResults.incrementAndGet(1);
                        }
                        finally
                        {
                            PutCompressionResults.addAndGet(0, pair.getStoreValueLength());
                            PutCompressionResults.addAndGet(1, pair.getRawValueLength());
                        }
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
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
    }

    // Application entry point
    public static void main(String[] args)
    {
        try
        {
            KVConfig config = new KVConfig();
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

            // Statistics manager objects from the connection, used to generate latency histogram
            ClientStatsContext fullStatsContext = ((IVoltDBConnection) Con).createStatsContext();
            periodicStatsContext = ((IVoltDBConnection) Con).createStatsContext();

            System.out.println("Connected.  Starting benchmark.");

            // Get a payload generator to create random Key-Value pairs to store in the database and process (uncompress) pairs retrieved from the database.
            final PayloadProcessor processor = new PayloadProcessor(
                    config.keysize, config.minvaluesize, config.maxvaluesize,
                    config.entropy, config.poolsize, config.usecompression);

            // Initialize the store
            if (config.preload) {
                System.out.print("Initializing data store... ");

                final PreparedStatement removeCS = Con.prepareStatement("DELETE FROM store;");
                final CallableStatement putCS = Con.prepareCall("{call STORE.upsert(?,?)}");
                for(int i=0;i<config.poolsize ;i++) {
                    if (i == 0) {
                        removeCS.execute();
                    }
                    putCS.setString(1, String.format(processor.KeyFormat, i));
                    putCS.setBytes(2,processor.generateForStore().getStoreValue());
                    putCS.execute();
                }
                System.out.println(" Done.");
            }
            // start the stats
            fullStatsContext.fetchAndResetBaseline();
            periodicStatsContext.fetchAndResetBaseline();
            benchmarkStartTS = System.currentTimeMillis();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the operating procedures
            Timer timer = new Timer();
            TimerTask statsPrinting = new TimerTask() {
                @Override
                public void run() { printStatistics(); }
            };
            timer.scheduleAtFixedRate(statsPrinting
            , config.displayinterval*1000l
            , config.displayinterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create multiple processing threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < config.threads; i++)
                threads.add(new Thread(new ClientThread(url, processor, config.duration, config.getputratio)));

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

            // stop and fetch the stats
            ClientStats stats = fullStatsContext.fetch().getStats();

            // 1. Store statistics as tracked by the application (ops counts, payload traffic)
            System.out.printf(
              "\n-------------------------------------------------------------------------------------\n"
            + " Store Results\n"
            + "-------------------------------------------------------------------------------------\n\n"
            + "A total of %,d operations was posted...\n"
            + " - GETs: %,9d Operations (%,9d Misses/Failures)\n"
            + "         %,9d MB in compressed store data\n"
            + "         %,9d MB in uncompressed application data\n"
            + "         Network Throughput: %6.3f Gbps*\n\n"
            + " - PUTs: %,9d Operations (%,9d Failures)\n"
            + "         %,9d MB in compressed store data\n"
            + "         %,9d MB in uncompressed application data\n"
            + "         Network Throughput: %6.3f Gbps*\n\n"
            + " - Total Network Throughput: %6.3f Gbps*\n\n"
            + "* Figure includes key & value traffic but not database protocol overhead.\n"
            + "\n"
            + "-------------------------------------------------------------------------------------\n"
            , GetStoreResults.get(0)+GetStoreResults.get(1)+PutStoreResults.get(0)+PutStoreResults.get(1)
            , GetStoreResults.get(0)
            , GetStoreResults.get(1)
            , GetCompressionResults.get(0)/1048576l
            , GetCompressionResults.get(1)/1048576l
            , ((double)GetCompressionResults.get(0) + (GetStoreResults.get(0)+GetStoreResults.get(1))*config.keysize)/(134217728d*config.duration)
            , PutStoreResults.get(0)
            , PutStoreResults.get(1)
            , PutCompressionResults.get(0)/1048576l
            , PutCompressionResults.get(1)/1048576l
            , ((double)PutCompressionResults.get(0) + (PutStoreResults.get(0)+PutStoreResults.get(1))*config.keysize)/(134217728d*config.duration)
            , ((double)GetCompressionResults.get(0) + (GetStoreResults.get(0)+GetStoreResults.get(1))*config.keysize)/(134217728d*config.duration)
            + ((double)PutCompressionResults.get(0) + (PutStoreResults.get(0)+PutStoreResults.get(1))*config.keysize)/(134217728d*config.duration)
            );

            System.out.println(
                    "\n\n-------------------------------------------------------------------------------------\n"
                  + " Client Latency Statistics\n"
                  + "-------------------------------------------------------------------------------------\n\n");
            System.out.printf("Average latency:               %,9.2f ms\n",
                    stats.getAverageLatency());
            System.out.printf("10th percentile latency:       %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.1));
            System.out.printf("25th percentile latency:       %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.25));
            System.out.printf("50th percentile latency:       %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.5));
            System.out.printf("75th percentile latency:       %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.75));
            System.out.printf("90th percentile latency:       %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.9));
            System.out.printf("95th percentile latency:       %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.95));
            System.out.printf("99th percentile latency:       %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.99));
            System.out.printf("99.5th percentile latency:     %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.995));
            System.out.printf("99.9th percentile latency:     %,9.2f ms\n",
                    stats.kPercentileLatencyAsDouble(.999));
            System.out.println("\n\n" + stats.latencyHistoReport());

            // Dump statistics to a CSV file
            Con.unwrap(IVoltDBConnection.class).saveStatistics(stats, config.statsfile);

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
