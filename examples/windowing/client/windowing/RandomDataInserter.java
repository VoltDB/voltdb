/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ProcedureCallback;

public class RandomDataInserter {

    final GlobalState state;
    final Client client;

    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    static final Random rand = new Random(0);

    private AtomicBoolean isDone = new AtomicBoolean(false);

    RandomDataInserter(GlobalState state, Client client) {
        this.state = state;
        this.client = client;

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();
    }

    boolean isDone() {
        return isDone.get();
    }

    void printReport() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();

        System.out.printf("  Insert Statistics:\n    Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                          stats.getInvocationAborts(),
                          stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%dms",
                          stats.getAverageLatency(),
                          stats.kPercentileLatency(0.95));
        System.out.printf("\n");
    }

    class InsertCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            assert(clientResponse.getStatus() == ClientResponse.SUCCESS);
        }
    }

    public void run() {
        // reset the stats
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        final long benchmarkEndTime = state.benchmarkStartTS + (1000l * state.config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            // unique identifier and partition key
            String uuid = UUID.randomUUID().toString();

            // millisecond timestamp
            Date now = new Date();

            // for some odd reason, this will give LONG_MAX if the
            // computed value is > LONG_MAX.
            long val = (long) (rand.nextGaussian() * 1000.0);

            try {
                client.callProcedure(new InsertCallback(),
                                     "TIMEDATA.insert",
                                     uuid,
                                     val,
                                     now);
            }
            catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        isDone.set(true);

        try {
            client.drain();
            client.close();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
