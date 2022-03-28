/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
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

        client = ClientFactory.createClient(clientConfig);
        statsCtx = client.createStatsContext();
 }

    // Handle the response to a stored procedure call.
    //
    Void callComplete(ClientResponse resp) {
        completed.incrementAndGet();
        int status = resp.getStatus();
        if (status != ClientResponse.SUCCESS) {
            failed.incrementAndGet();
            System.err.printf("Procedure call failed: %s %s",
                              status, resp.getStatusString());
        }
        return null;
    }

    // Any procedure call failure (exceptional completion case).
    // Handling is primitive just write a message and forget it.
    //
    Void callException(Throwable th) {
        excepts.incrementAndGet();
        System.err.printf("Procedure call exception: %s", th);
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

        print("Running %s test for %d secs ...", procName, config.duration);
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            gate.waitOpen();
            callsIssued++;
            client.callProcedureAsync(procName)
                .thenAccept(this::callComplete)
                .exceptionally(this::callException);
        }

        client.drain();
        printStats();
        client.close();
    }

    // Result stats
    void printStats() {
        print("");
        print("Calls issued: %,d; completed: %,d; failed: %,d",
                          callsIssued, completed.get(), failed.get());
        ClientStats stats = statsCtx.fetch().getStats();
        print("Average throughput: %,d txns/sec", stats.getTxnThroughput());
        print("Average end-to-end latency: %,.2f ms", stats.getAverageLatency());
        print("Average latency at server: %,.2f ms", stats.getAverageInternalLatency());
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
