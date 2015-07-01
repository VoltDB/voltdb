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
package overhead;

import java.nio.ByteBuffer;
import java.util.Random;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
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
    private static HashMap<String, Long> hm = new HashMap<String, Long>();
    private static AppHelper apph = new AppHelper(AsyncBenchmark.class.getCanonicalName());
    // Reference to the database connection we will use
    private static ClientConnection Con;
    private static boolean stopTheMadness = false;
    private static Timer timer;

    private static long duration;
    private static int paramSize;
    private static int resultSize;
    private static IRateLimiter limiter;

    // Application entry point
    public static void main(String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            apph.add("displayinterval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("resultsize", "result_size", "Size of the result value returned by each operation", 0)
                .add("paramsize", "param_size", "Size of the op parameter if the op supports arbitrary size params", 0)
                .add("operation", "operation", "The procedure to invoke", "NoArgs")
                .add("ratelimit", "rate_limit", "Rate limit to start from (number of transactions per second).", 900000)
                .setArguments(args)
            ;

            // Retrieve parameters
            long displayInterval   = apph.longValue("displayinterval");
            duration          = apph.longValue("duration");
            String servers         = apph.stringValue("servers");
            int port               = apph.intValue("port");
            resultSize           = apph.intValue("resultsize");
            paramSize            = apph.intValue("paramsize");
            long rateLimit         = apph.longValue("ratelimit");
            final String csv       = apph.stringValue("statsfile");
            final String op = apph.stringValue("operation");

            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("displayinterval", (displayInterval > 0))
                .validate("resultsize", (resultSize >= 0))
                .validate("paramsize", (paramSize >= 0))
                .validate("ratelimit", (rateLimit > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Get a client connection - we retry for a while in case the server hasn't started yet
            Con = ClientConnectionPool.getWithRetry(servers, port);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the operating procedures
            timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask()
            {
                @Override
                public void run()
                {
                    System.out.print(Con.getStatistics(op));
                }
            }
            , displayInterval*1000l
            , displayInterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Pick the transaction rate limiter helping object to use based on user request (rate limiting or latency targeting)
            limiter = new RateLimiter(rateLimit);

            // Run the benchmark loop for the requested duration
            if (op.substring(0, 6).equalsIgnoreCase("noargs")) {
                runNoArgs( op.endsWith("RW") ? false : true);
            } else if (op.substring(0, 13).equalsIgnoreCase("BinaryPayload")) {
                runBinaryPayload( op.endsWith("RW") ? false : true);
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Now print application results:

            // 1. Overall performance statistics for GET/PUT operations
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " System Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.getStatistics(op).toString(false));

            // 2. Per-procedure detailed performance statistics
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " Detailed Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.getStatistics().toString(false));
            // Dump statistics to a CSV file
            Con.saveStatistics(csv);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

        }

        catch(org.voltdb.client.NoConnectionsException x)
        {
            System.out.println("Exception: " + x);
            System.out.println("\n\n-------------------------------------------------------------------------------------\n");
            System.out.print("Lost connection - will try to reconnect ... \n");
            Con.close();
            timer.cancel();
            try {
                Con = ClientConnectionPool.getWithRetry(apph.stringValue("servers"), apph.intValue("port"));
            }
            catch (Exception e){
                System.out.println("Another exception, I guess " + e);
            }
        }
        catch(Exception x)
        {
            System.out.println("Exception: " + x);
            x.printStackTrace();
            System.exit(1);
        }
    }

    public static void runNoArgs( boolean readOnly) throws Exception {
        Random r = new java.util.Random();
        // Run the benchmark loop for the requested duration
        long endTime = System.currentTimeMillis() + (1000l * duration);
        while (endTime > System.currentTimeMillis() && ! stopTheMadness)
        {
            Con.executeAsync( new org.voltdb.client.ProcedureCallback() {
                public void clientCallback(ClientResponse clientResponse) throws Exception {
                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                        System.out.println(clientResponse.getStatusString()); System.exit(-1);
                    }
                }
            }, readOnly ? "NoArgs" : "NoArgsRW", r.nextLong(), resultSize);
            // Use the limiter to throttle client activity
            limiter.throttle();
        }
    }

    public static void runBinaryPayload( boolean readOnly) throws Exception {
        Random r = new java.util.Random();
        byte param[] = new byte[paramSize];
        // Run the benchmark loop for the requested duration
        long endTime = System.currentTimeMillis() + (1000l * duration);
        while (endTime > System.currentTimeMillis() && ! stopTheMadness)
        {
            Con.executeAsync( new org.voltdb.client.ProcedureCallback() {
                public void clientCallback(ClientResponse clientResponse) throws Exception {
                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                        System.out.println(clientResponse.getStatusString()); System.exit(-1);
                    }
                }
            }, readOnly ? "BinaryPayload" : "BinaryPayloadRW", r.nextLong(), resultSize, param );
            // Use the limiter to throttle client activity
            limiter.throttle();
        }
    }
}
