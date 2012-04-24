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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
import org.voltdb.client.exampleutils.LatencyLimiter;
import org.voltdb.client.exampleutils.RateLimiter;

public class AsyncExportClient
{
    // Transactions between catalog swaps.
    public static long CATALOG_SWAP_INTERVAL = 500000;
    // Number of txn ids per client log file.
    public static long CLIENT_TXNID_FILE_SIZE = 250000;

    static class TxnIdWriter
    {
        String m_nonce;
        String m_txnLogPath;
        FileOutputStream m_curFile = null;
        OutputStreamWriter m_outs = null;
        long m_count = 0;

        public TxnIdWriter(String nonce, String txnLogPath)
        {
            m_nonce = nonce;
            m_txnLogPath = txnLogPath;

            File logPath = new File(m_txnLogPath);
            if (!logPath.exists()) {
                if (!logPath.mkdir()) {
                    System.err.println("Problem creating log directory");
                }
            }
        }

        public void createNewFile() throws IOException
        {
            if (m_curFile != null)
            {
                m_outs.close();
                m_curFile.flush();
                m_curFile.close();
            }
            File blah = new File(m_txnLogPath, m_count + "-" + m_nonce + "-txns");
            m_curFile = new FileOutputStream(blah);
            m_outs = new OutputStreamWriter(m_curFile);
        }

        public void write(String txnId) throws IOException
        {
            if ((m_count % CLIENT_TXNID_FILE_SIZE) == 0)
            {
                createNewFile();
            }
            m_outs.write(txnId);
            m_count++;
        }
    }

    static class AsyncCallback implements ProcedureCallback
    {
        private final TxnIdWriter m_writer;
        public AsyncCallback(TxnIdWriter writer)
        {
            super();
            m_writer = writer;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Track the result of the request (Success, Failure)
            if (clientResponse.getStatus() == ClientResponse.SUCCESS)
            {
                TrackingResults.incrementAndGet(0);
                try
                {
                    m_writer.write(clientResponse.getResults()[0].asScalarLong() + "\n");
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            else
            {
                TrackingResults.incrementAndGet(1);
            }
        }
    }

    // Initialize some common constants and variables
    private static final AtomicLongArray TrackingResults = new AtomicLongArray(2);

    // Reference to the database connection we will use
    private static ClientConnection Con;

    private static File[] catalogs = {new File("genqa.jar"), new File("genqa2.jar")};
    private static File deployment = new File("deployment.xml");

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
                .add("pool-size", "pool_size", "Size of the record pool to operate on - larger sizes will cause a higher insert/update-delete rate.", 100000)
                .add("procedure", "procedure_name", "Procedure to call.", "JiggleExportSinglePartition")
                .add("ratelimit", "rate_limit", "Rate limit to start from (number of transactions per second).", 100000)
                .add("autotune", "auto_tune", "Flag indicating whether the benchmark should self-tune the transaction rate for a target execution latency (true|false).", "true")
                .add("latency-target", "latency_target", "Execution latency to target to tune transaction rate (in milliseconds).", 10.0d)
                .add("catalog-swap", "Swap catalogs from the client", "true")
                .setArguments(args)
            ;

            // Retrieve parameters
            final long displayInterval = apph.longValue("displayinterval");
            final long duration        = apph.longValue("duration");
            final String servers       = apph.stringValue("servers");
            final int port             = apph.intValue("port");
            final int poolSize         = apph.intValue("pool-size");
            final String procedure     = apph.stringValue("procedure");
            final long rateLimit       = apph.longValue("ratelimit");
            final boolean autoTune     = apph.booleanValue("autotune");
            final double latencyTarget = apph.doubleValue("latency-target");
            final boolean catalogSwap  = apph.booleanValue("catalog-swap");
            final String csv           = apph.stringValue("stats");

            TxnIdWriter writer = new TxnIdWriter("dude", "clientlog");

            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("pool-size", (duration > 0))
                .validate("ratelimit", (rateLimit > 0))
                .validate("latency-target", (latencyTarget > 0))
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
            int swap_count = 0;
            boolean first_cat = false;
            while (endTime > System.currentTimeMillis())
            {
                // Post the request, asynchronously
                Con.executeAsync(new AsyncCallback(writer),
                                 procedure,
                                 (long)rand.nextInt(poolSize),
                                 0);
                swap_count++;
                if (((swap_count % CATALOG_SWAP_INTERVAL) == 0) && catalogSwap)
                {
                    System.out.println("Changing catalogs...");
                    Con.updateApplicationCatalog(catalogs[first_cat ? 0 : 1], deployment);
                    first_cat = !first_cat;
                }
                // Use the limiter to throttle client activity
                limiter.throttle();
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------
            Con.drain();
            Thread.sleep(10000);

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
