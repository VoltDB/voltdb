/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.benchmark;

import org.voltdb.benchmark.BenchmarkResults.Result;

public class ResultsPrinter implements BenchmarkController.BenchmarkInterest {

    long[] firstIntervalClientRTT = null;
    long[] firstIntervalClusterRTT = null;

    @Override
    public void benchmarkHasUpdated(BenchmarkResults results,
            long[] clusterLatencyBuckets, long[] clientLatencyBuckets) {

        long txnCount = 0;
        for (String client : results.getClientNames()) {
            for (String txn : results.getTransactionNames()) {
                Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                for (Result r : rs)
                    txnCount += r.transactionCount;
            }
        }

        long txnDelta = 0;
        for (String client : results.getClientNames()) {
            for (String txn : results.getTransactionNames()) {
                Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                Result r = rs[rs.length - 1];
                txnDelta += r.transactionCount;
            }
        }

        int pollIndex = results.getCompletedIntervalCount();
        long duration = results.getTotalDuration();
        long pollCount = duration / results.getIntervalDuration();
        long currentTime = pollIndex * results.getIntervalDuration();

        // remember the first seen latencies; they are anomalous.
        if (firstIntervalClientRTT == null) {
            firstIntervalClientRTT = clientLatencyBuckets;
            firstIntervalClusterRTT = clusterLatencyBuckets;
        }

        System.out.printf("\nAt time %d out of %d (%d%%):\n", currentTime, duration, currentTime * 100 / duration);
        System.out.printf("  In the past %d ms:\n", duration / pollCount);
        System.out.printf("    Completed %d txns at a rate of %.2f txns/s\n",
                txnDelta,
                txnDelta / (double)(results.getIntervalDuration()) * 1000.0);
        System.out.printf("  Since the benchmark began:\n");
        System.out.printf("    Completed %d txns at a rate of %.2f txns/s\n",
                txnCount,
                txnCount / (double)(pollIndex * results.getIntervalDuration()) * 1000.0);


        if ((pollIndex * results.getIntervalDuration()) >= duration) {
            // print the final results
            System.out.println("\n============================== BENCHMARK RESULTS ==============================");
            System.out.printf("Time: %d ms\n", duration);
            System.out.printf("Total transactions: %d\n", txnCount);
            System.out.printf("Transactions per second: %.2f\n", txnCount / (double)duration * 1000.0);
            for (String transactionName : results.getTransactionNames()) {
                txnCount = getTotalCountForTransaction(transactionName, results);
                System.out.printf("%23s: %10d total %12.2f txn/s %12.2f txn/m\n",
                        transactionName,
                        txnCount,
                        txnCount / (double)duration * 1000.0,
                        txnCount / (double)duration * 1000.0 * 60.0);
            }
            System.out.println("Breakdown by client:");
            for (String clientName : results.getClientNames()) {
                txnCount = getTotalCountForClient(clientName, results);
                System.out.printf("%23s: %10d total %12.2f txn/s %12.2f txn/m\n",
                        clientName,
                        txnCount,
                        txnCount / (double)duration * 1000.0,
                        txnCount / (double)duration * 1000.0 * 60.0);
            }

            System.out.println("Latency summary");
            System.out.printf("%4s %10s %10s %5s\n", "MS", "CLIENT RTT", "VOLT RTT", "PERC");

            // remove warm-up period stats from the totals
            for (int i=0; i < firstIntervalClientRTT.length; i++) {
                clusterLatencyBuckets[i] -= firstIntervalClusterRTT[i];
                clientLatencyBuckets[i] -= firstIntervalClientRTT[i];
            }


            long total_bucketed_txns = 0;
            long running_total = 0;
            int dots_printed = 0;
            for (int i=0; i < clusterLatencyBuckets.length; i++) {
                total_bucketed_txns += clusterLatencyBuckets[i];
            }
            for (int i=0; i < clusterLatencyBuckets.length; i++) {
                running_total += clusterLatencyBuckets[i];
                int dotcount = (int)Math.floor(running_total/(total_bucketed_txns / 50)) - dots_printed;
                dots_printed += dotcount;
                String dots = "";
                for (int j=0; j < dotcount; j++) dots = dots.concat(".");
                System.out.printf("%4d %10d %10d %5.1f %s\n", i*10,
                        clientLatencyBuckets[i], clusterLatencyBuckets[i],
                        (new Double(running_total)/new Double(total_bucketed_txns)) * 100.0, dots);
            }
            System.out.println("===============================================================================\n");
        }

        System.out.flush();
    }

    private long getTotalCountForClient(String clientName, BenchmarkResults results) {
        long txnCount = 0;
        for (String txnName : results.getTransactionNames()) {
            Result[] rs = results.getResultsForClientAndTransaction(clientName, txnName);
            for (Result r : rs)
                txnCount += r.transactionCount;
        }
        return txnCount;
    }

    private long getTotalCountForTransaction(String txnName, BenchmarkResults results) {
        long txnCount = 0;
        for (String clientName : results.getClientNames()) {
            Result[] rs = results.getResultsForClientAndTransaction(clientName, txnName);
            for (Result r : rs)
                txnCount += r.transactionCount;
        }
        return txnCount;
    }

}
