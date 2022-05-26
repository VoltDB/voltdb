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
package org.voltdb.example.util;

import java.util.Timer;
import java.util.TimerTask;
import org.voltdb.*;
import org.voltdb.client.*;

public class BenchmarkStats {

    private Timer timer;
    private Client client;
    private long startTimeMillis;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;
    final boolean showLatency;
    final String HORIZONTAL_RULE =
        "----------" + "----------" + "----------" + "----------" +
        "----------" + "----------" + "----------" + "----------" + "\n";

    public BenchmarkStats(Client client, boolean showLatency) {
        this.client = client;
        this.showLatency = showLatency;
        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();
    }

    public void startBenchmark() {

        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();
        startTimeMillis = System.currentTimeMillis();
        schedulePeriodicStats();
        System.out.println();
        System.out.println("Time     Txn/sec Aborts Failures");
        System.out.println("-------- ------- ------ --------");
    }

    public void endBenchmark() throws Exception {
        timer.cancel();
        client.drain();
        printResults();
    }

    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
                @Override
                public void run() { printStatistics(); }
            };
        timer.scheduleAtFixedRate(statsPrinting,
                                  5 * 1000,
                                  5 * 1000);
    }

    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - startTimeMillis) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("%7d ", stats.getTxnThroughput());
        System.out.printf("%6d %8d",
                          stats.getInvocationAborts(), stats.getInvocationErrors());
        if(showLatency) {
            System.out.printf(", Avg/95%% Latency %.2f/%.2fms", stats.getAverageLatency(),
                              stats.kPercentileLatencyAsDouble(0.95));
        }
        System.out.printf("\n");
    }

    public synchronized void printResults() throws Exception {

        ClientStats stats = fullStatsContext.fetch().getStats();

        System.out.println();
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Benchmark Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());

        if (showLatency) {
            System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
            System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
            System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
            System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
            System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
            System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
            System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
            System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
            System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
            System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" System Server Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" Latency Histogram");
            System.out.println(HORIZONTAL_RULE);
            System.out.println(stats.latencyHistoReport());
        }
        System.out.println();
    }
}
