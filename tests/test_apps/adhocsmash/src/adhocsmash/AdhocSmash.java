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

package adhocsmash;

import java.util.Random;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;
import org.voltdb.client.exampleutils.IRateLimiter;
import org.voltdb.client.exampleutils.RateLimiter;

import adhocsmash.AdhocSmash;

public class AdhocSmash
{
    // Reference to the database connection we will use
    private static ClientConnection Con;

    static class InsertCallback implements ProcedureCallback
    {
        public boolean m_adhoc = false;
        public int m_cycle;
        public InsertCallback(boolean adhoc, int cycle)
        {
            m_adhoc = adhoc;
            m_cycle = cycle;
        }
        @Override
        public void clientCallback(ClientResponse response)
        throws Exception
        {
            if (response.getStatus() != ClientResponse.SUCCESS)
            {
                System.out.println("FAIL INSERT!");
                System.out.println("Response: " + response.getStatus());
                System.out.println("Response string: " + response.getStatusString());
            }
            else
            {
                if (response.getResults()[0].asScalarLong() != 1)
                {
                    System.out.println("BOOO, insert fail!");
                }
            }
        }

    }

    static class UpdateCallback implements ProcedureCallback
    {
        public boolean m_adhoc = false;
        public int m_cycle;
        public UpdateCallback(boolean adhoc, int cycle)
        {
            m_adhoc = adhoc;
            m_cycle = cycle;
        }
        @Override
        public void clientCallback(ClientResponse response)
        throws Exception
        {
            if (response.getStatus() != ClientResponse.SUCCESS)
            {
                System.out.println("FAIL UPDATE!");
                System.out.println("Response: " + response.getStatus());
                System.out.println("Response string: " + response.getStatusString());
            }
            else
            {
                if (response.getResults()[0].asScalarLong() != 1)
                {
                    System.out.println("BOOO, update fail!");
                }
            }
        }

    }

    static class DeleteCallback implements ProcedureCallback
    {
        public boolean m_adhoc = false;
        public int m_cycle;
        public DeleteCallback(boolean adhoc, int cycle)
        {
            m_adhoc = adhoc;
            m_cycle = cycle;
        }
        @Override
        public void clientCallback(ClientResponse response)
        throws Exception
        {
            if (response.getStatus() != ClientResponse.SUCCESS)
            {
                System.out.println("FAIL DELETE!");
                System.out.println("Response: " + response.getStatus());
                System.out.println("Response string: " + response.getStatusString());
            }
            else
            {
                if (response.getResults()[0].asScalarLong() != 1)
                {
                    System.out.println("BOOO, delete fail!");
                }
            }
        }

    }

    public static void main(String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(AdhocSmash.class.getCanonicalName())
                .add("displayinterval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("ratelimit", "rate_limit", "Rate limit to start from (number of transactions per second).", 100000)
                .setArguments(args)
            ;

            // Retrieve parameters
            long displayInterval = apph.longValue("displayinterval");
            long duration        = apph.longValue("duration");
            String servers       = apph.stringValue("servers");
            int port             = apph.intValue("port");
            long rateLimit       = apph.longValue("ratelimit");

            Random rand = new Random();

            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("displayinterval", (displayInterval > 0))
                .validate("ratelimit", (rateLimit > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Get a client connection - we retry for a while in case the server hasn't started yet
            Con = ClientConnectionPool.getWithRetry(servers, port);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Pick the transaction rate limiter helping object to use based on user request (rate limiting or latency targeting)
            IRateLimiter limiter = null;
            limiter = new RateLimiter(rateLimit);

            int cycle = 0;
            // Run the benchmark loop for the requested duration
            final long endTime = System.currentTimeMillis() + (1000l * duration);
            while (endTime > System.currentTimeMillis())
            {
                // So, here's how we'll expose out-of-order replicated adhoc writes:
                // 1) do an insert/update/delete cycle on a given primary key
                // 2) some small fraction of the time, make one of these operations adhoc
                //    -- We do the adhoc synchronously on the master so that the ordering
                //       is deterministic
                // 3) Add in replication and watch the DRagent go boom when the adhoc
                //    queries are performed asynchronously and out-of-order on the replica
                // First, Insert
                if (rand.nextInt(1000) < 5)
                {
                    //System.out.println("Insert adhoc");
                    String query = "insert into votes (phone_number, state, contestant_number) values (" + cycle + ", 'MA', 999);";
                    ClientResponse response = Con.execute("@AdHoc", query);
                    InsertCallback blah = new InsertCallback(true, cycle);
                    blah.clientCallback(response);
                }
                else
                {
                    //System.out.println("Insert regular");
                    Con.executeAsync(new InsertCallback(false, cycle),
                                     "VOTES.insert", cycle, "MA", 999);
                }

                // Then, update
                if (rand.nextInt(1000) < 5)
                {
                    //System.out.println("Update adhoc");
                    ClientResponse response = Con.execute("@AdHoc", "update votes set state='RI', contestant_number=" + cycle + " where phone_number=" + cycle + ";");
                    UpdateCallback blah = new UpdateCallback(true, cycle);
                    blah.clientCallback(response);
                }
                else
                {
                    //System.out.println("Update regular");
                    Con.executeAsync(new UpdateCallback(false, cycle),
                                     "VOTES.update", cycle, "MA", cycle, cycle);
                }

                // Finally, delete
                if (rand.nextInt(1000) < 5)
                {
                    //System.out.println("Delete adhoc");
                    ClientResponse response = Con.execute("@AdHoc", "delete from votes where contestant_number=" + cycle + ";");
                    DeleteCallback blah = new DeleteCallback(true, cycle);
                    blah.clientCallback(response);
                }
                else
                {
                    //System.out.println("Delete regular");
                    Con.executeAsync(new DeleteCallback(false, cycle),
                                     "Delete", cycle);
                }
                cycle++;
                // Use the limiter to throttle client activity
                limiter.throttle();
            }

// --------------------------------------------------------------------------------------------------------

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
