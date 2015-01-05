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


package windowing;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;

/**
 * <p>Given a time duration, insert random tuples into the 'timedata'
 * table as fast as possible.</p>
 *
 * <p>If the user selects 'inline' deletes, this class will call procedures
 * that delete data as they insert it.</p>
 *
 * <p>The rate of insertion is probably rate-limited by a user specified
 * rate limiting parameter passed to the insert client.</p>
 *
 */
public class RandomDataInserter {

    // Global state
    final WindowingApp app;
    // Client used just for inserts so stats will be isolated
    final Client client;

    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;

    // Seed PRNG with 0 for what predictability we can have in the async world...
    static final Random rand = new Random(0);

    RandomDataInserter(WindowingApp app, Client client) {
        this.app = app;
        this.client = client;

        // Tracks client-side stats like invocation counts and end2end latency
        periodicStatsContext = client.createStatsContext();
    }

    void printReport() {
        // Get the client stats since the last time this method was called.
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();

        System.out.printf("  Insert Statistics:\n" +
                          "    Throughput %d/s, Aborts/Failures %d/%d, Avg/95%% Latency %.2f/%dms\n",
                          stats.getTxnThroughput(),
                          stats.getInvocationAborts(),
                          stats.getInvocationErrors(),
                          stats.getAverageLatency(),
                          stats.kPercentileLatency(0.95));
    }

    class InsertWithDeleteCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            assert(clientResponse.getStatus() == ClientResponse.SUCCESS);
            long deleted = clientResponse.getResults()[0].asScalarLong();
            app.addToDeletedTuples(deleted);
        }
    }

    void basicInsert(String uuid, long val, Date update_ts)
            throws NoConnectionsException, IOException
    {
        // Call the proc with the NullCallback because we're not
        // being super picky about failure handling. If it fails,
        // it will show up in the client-side statistics we print
        // out in printReport().
        client.callProcedure(new NullCallback(),
                "TIMEDATA.insert",
                uuid,
                val,
                update_ts);
    }

    void insertWithDateDelete(String uuid, long val, Date update_ts)
            throws NoConnectionsException, IOException
    {
        // Async call with a callback that tracks deleted tuples.
        client.callProcedure(new InsertWithDeleteCallback(),
                "InsertAndDeleteAfterDate",
                uuid,
                val,
                update_ts,
                app.getTargetDate(),
                app.config.deletechunksize);
    }

    void insertWithRowcountDelete(String uuid, long val, Date update_ts)
            throws NoConnectionsException, IOException
    {
        // Async call with a callback that tracks deleted tuples.
        client.callProcedure(new InsertWithDeleteCallback(),
                "InsertAndDeleteOldestToTarget",
                uuid,
                val,
                update_ts,
                app.getTargetRowsPerPartition(),
                app.config.deletechunksize);
    }

    /**
     * Run a loop inserting tuples into the database for the amount
     * of time specified in config.duration.
     */
    public void run() {
        // Reset the stats managed by the client.
        periodicStatsContext.fetchAndResetBaseline();

        // Run in a loop for config.duration seconds.
        final long benchmarkEndTime = app.startTS + (1000l * app.config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {

            // GENERATE A RANDOM ROW

            // unique identifier and partition key
            String uuid = UUID.randomUUID().toString();

            // millisecond timestamp
            Date now = new Date();

            // for some odd reason, this will give LONG_MAX if the
            // computed value is > LONG_MAX.
            long val = (long) (rand.nextGaussian() * 1000.0);

            // CALL THE USER'S SELECTED FLAVOR OF INSERT

            try {
                // Do an vanilla insert.
                if (!app.config.inline) {
                    basicInsert(uuid, val, now);
                }
                // Do an insert with date-based deleting
                else if (app.config.historyseconds > 0) {
                    insertWithDateDelete(uuid, val, now);
                }
                // Do an insert with timestamp-based deleting
                else if (app.config.maxrows > 0) {
                    insertWithRowcountDelete(uuid, val, now);
                }
            }
            catch (IOException e) {
                // Not being super picky about failure handling. If this
                // fails, it will show up in the client-side statistics we
                // print out in printReport().
            }
        }

        // When ready to end processing...
        try {
            // Block until all async calls have returned.
            client.drain();
            // Clean close of the client connection.
            client.close();
        }
        catch (IOException | InterruptedException e) {
            // Live dangerously and ignore this error that probably
            // won't happen at shutdown time.
        }

    }
}
