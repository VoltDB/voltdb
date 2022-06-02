/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package compound;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientAffinityStats;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;

public class CompoundClient {

    final Config config;
    final Client2 client;
    final ClientStatsContext statsCtx;

    static class Config extends CLIConfig {
        @Option(desc = "Test duration, in seconds.")
        int duration = 20;

        @Option(desc = "Comma-separated list of Volt servers.")
        String servers = "localhost";

        @Option(desc = "Test case, 'simple' or 'null'")
        String test = "simple";

        @Option(desc = "Report affinity stats to stdout")
        boolean affinityreport = false;

        @Option(desc = "Report network I/O stats to stdout")
        boolean ioreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Transaction rate limit (tps).")
        int ratelimit = 0;

        @Override
        public void validate() {
            if (duration <= 0)
                exitWithMessageAndUsage("duration must be greater than 0");
            if (servers.isEmpty())
                exitWithMessageAndUsage("servers required");
            if (!test.equalsIgnoreCase("simple") && !test.equalsIgnoreCase("null"))
                exitWithMessageAndUsage("test must be 'simple' or 'null'");
        }
    }

    // Flow control
    //
    static class Gate {
        boolean closed;

        synchronized void waitOpen() throws InterruptedException {
            while (closed)
                wait();
        }

        synchronized void operate(boolean closing) {
            if (closing ^ closed) {
                closed = closing;
                if (!closed)
                    notifyAll();
            }
        }
    }

    final Gate gate = new Gate();

    // Counts
    int callsIssued;
    AtomicInteger completed = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    AtomicInteger excepts = new AtomicInteger();

    // Constructor. We use the default values for
    // most client operational parameters.
    //
    public CompoundClient(Config config) {
        this.config = config;

        Client2Config clientConfig = new Client2Config()
            .requestBackpressureHandler((s) -> gate.operate(s))
            .connectionUpHandler((h,p) -> print("[up: %s %d]", h, p))
            .connectionDownHandler((h,p) -> print("[down: %s %d]", h, p))
            .connectFailureHandler((h,p) -> print("[fail: %s %d]", h, p));

        if (config.ratelimit > 0) {
            clientConfig.transactionRateLimit(config.ratelimit);
        }

        client = ClientFactory.createClient(clientConfig);
        statsCtx = client.createStatsContext();
 }

    // Handle the response to a stored procedure call.
    //
    Void callComplete(ClientResponse resp) {
        completed.incrementAndGet();
        int status = resp.getStatus();
        if (status != ClientResponse.SUCCESS) {
            int f = failed.getAndIncrement();
            if ((f % 10000) == 0)
                System.err.printf("Procedure call failed: %s %s%n",
                                  status, resp.getStatusString());
        }
        return null;
    }

    // Any procedure call failure (exceptional completion case).
    // Handling is primitive just write a message and forget it.
    //
    Void callException(Throwable th) {
        int e = excepts.getAndIncrement();
        if ((e % 10000) == 0)
            System.err.printf("Procedure call exception: %s%n", th);
        return null;
    }

    // Core benchmark code.
    //
    void runBenchmark() throws Exception {

        print("Connecting to VoltDB ...");
        client.connectSync(config.servers, 300, 5, TimeUnit.SECONDS);

        String procName = "SimpleCompoundProc";
        if (config.test.equalsIgnoreCase("null"))
            procName = "NullCompoundProc";

        if (config.ratelimit > 0) {
            print("Using rate limit of %d tps", config.ratelimit);
        }

        print("Running %s test for %d secs ...", procName, config.duration);
        final long benchmarkStartTime = System.currentTimeMillis();
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            gate.waitOpen();
            callsIssued++;
            client.callProcedureAsync(procName)
                .thenAccept(this::callComplete)
                .exceptionally(this::callException);
        }

        client.drain();

        print("");
        printStats();
        if (config.affinityreport)
            printAffinityStats();
        if (config.ioreport)
            printIoStats();

        client.close();
    }

    // Result stats
    void printStats() {
        long serverTps = 0L;
        long elapsedMs = 0L;

        // code duplicates ExportBenchmark stats handling
        elapsedMs = config.duration;
        serverTps = completed.get() / config.duration;

        print("Calls issued: %,d; completed: %,d; failed: %,d",
                          callsIssued, completed.get(), failed.get());
        ClientStats stats = statsCtx.fetch().getStats();
        print("Average throughput: %,d txns/sec", stats.getTxnThroughput());
        print("Average end-to-end latency: %,.2f ms", stats.getAverageLatency());
        print("Average latency at server: %,.2f ms", stats.getAverageInternalLatency());
        print("10th percentile latency:       %,9.2f ms", stats.kPercentileLatencyAsDouble(.1));
        print("25th percentile latency:       %,9.2f ms", stats.kPercentileLatencyAsDouble(.25));
        print("50th percentile latency:       %,9.2f ms", stats.kPercentileLatencyAsDouble(.5));
        print("75th percentile latency:       %,9.2f ms", stats.kPercentileLatencyAsDouble(.75));
        print("90th percentile latency:       %,9.2f ms", stats.kPercentileLatencyAsDouble(.9));
        print("95th percentile latency:       %,9.2f ms", stats.kPercentileLatencyAsDouble(.95));
        print("99th percentile latency:       %,9.2f ms", stats.kPercentileLatencyAsDouble(.99));
        print("99.5th percentile latency:     %,9.2f ms", stats.kPercentileLatencyAsDouble(.995));
        print("99.9th percentile latency:     %,9.2f ms", stats.kPercentileLatencyAsDouble(.999));
        print("99.999th percentile latency:   %,9.2f ms", stats.kPercentileLatencyAsDouble(.99999));
        print("");

        // Write stats to file if requested
        try {
            if (config.statsfile != null && config.statsfile.length() != 0) {
                FileWriter fw = new FileWriter(config.statsfile);
                fw.append(String.format("%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,0,0,0\n",
                                    stats.getStartTimestamp(),
                                    stats.getDuration(),
                                    completed.get(),
                                    stats.kPercentileLatencyAsDouble(0.0),
                                    stats.kPercentileLatencyAsDouble(1.0),
                                    stats.kPercentileLatencyAsDouble(0.95),
                                    stats.kPercentileLatencyAsDouble(0.99),
                                    stats.kPercentileLatencyAsDouble(0.999),
                                    stats.kPercentileLatencyAsDouble(0.9999),
                                    stats.kPercentileLatencyAsDouble(0.99999)
                                    ));
                fw.close();
            }
        } catch (IOException e) {
            System.err.println("Error writing stats file");
            e.printStackTrace();
        }
    }

    // Affinity stats
    void printAffinityStats() {
        Map<Integer,ClientAffinityStats> casMap = statsCtx.getAffinityStats();
        if (casMap.isEmpty()) {
            print("No affinity stats available");
        }
        else {
            print("%10s %21s %21s", "",
                  "Affinity    ", "Round Robin   ");
            print("%10s %10s %10s %10s %10s", "Partition",
                   "Reads", "Writes", "Reads", "Writes");
            for (Map.Entry<Integer,ClientAffinityStats>  ent : casMap.entrySet()) {
                int partitionId = ent.getKey();
                ClientAffinityStats cas = ent.getValue();
                print("%10d %,10d %,10d %,10d %,10d", partitionId,
                                  cas.getAffinityReads(), cas.getAffinityWrites(),
                                  cas.getRrReads(), cas.getRrWrites());
            }
        }
        print("");
    }

    // I/O stats
    void printIoStats() {
        Map<Long, ClientStats> csMap = statsCtx.getStatsByConnection();
        if (csMap.isEmpty()) {
            print("No I/O stats available");
        }
        else {
            print("%8s  %12s %15s %15s", "Conn Id", "Proc Calls", "Bytes Written", "Bytes Read");
            for (Map.Entry<Long, ClientStats> ent : csMap.entrySet()) {
                long cxnId = ent.getKey();
                ClientStats cs = ent.getValue();
                print("%8d  %,12d %,15d %,15d", cxnId, cs.getInvocationsCompleted(),
                      cs.getBytesWritten(), cs.getBytesRead());
            }
        }
        print("");
    }

    // Utility timestamped print routine
    void print(String fmt, Object... args) {
        String s = String.format(fmt, args);
        System.out.printf("%s  %s%n", Instant.now(), s);
    }

    // Main routine creates a benchmark instance and kicks off the run method.
    //
    public static void main(String... args) throws Exception {
        Config config = new Config();
        config.parse(CompoundClient.class.getName(), args);
        CompoundClient benchmark = new CompoundClient(config);
        benchmark.runBenchmark();
    }
}
