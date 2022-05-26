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

package udfbenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Timer;

import org.voltdb.ClientAppBase;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;

import com.google_voltpatches.common.collect.ConcurrentHashMultiset;
import com.google_voltpatches.common.collect.Multiset;

public final class UDFBenchmark extends ClientAppBase {

    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    Timer timer;
    long benchmarkStartTS;

    public UDFBenchmark(UDFBenchmarkConfig config) {
        super(config);
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    @Override
    public void run() throws Exception {
        UDFBenchmarkConfig config = (UDFBenchmarkConfig)m_config;
        m_appRunning = true;

        if (config.table.equalsIgnoreCase("replicated")) {
            // Replicated table R1
            printTaskHeader("Inserting rows into R1...");
            generateData("R1", config.datasize);
            printTaskHeader("Running benchmark on replicated table R1...");
            resetStats();
            for (int i = 0; i < config.datasize; i++) {
                m_client.callProcedure(new NullCallback(), "R1Tester", i);
            }
            m_client.drain();
            m_timer.cancel();

            printResults("udf-replicated");
            m_client.callProcedure("@AdHoc", "TRUNCATE TABLE R1;");

        } else {
            // Partitioned table P1
            printTaskHeader("Inserting rows into P1...");
            generateData("P1", config.datasize);
            printTaskHeader("Running benchmark on partitioned table P1...");
            resetStats();
            for (int i = 0; i < config.datasize; i++) {
                m_client.callProcedure(new NullCallback(), "P1Tester", i);
            }
            m_client.drain();
            m_timer.cancel();

            printResults("udf-partitioned");
            m_client.callProcedure("@AdHoc", "TRUNCATE TABLE P1;");
        }

        // Finish up.
        m_appRunning = false;
        m_client.close();
    }

    private static final String SIMPLE_POLYGON_WTK =
            "PolygonFromText( 'POLYGON((3 3, -3 3, -3 -3, 3 -3, 3 3)," +
            "(1 1, 1 2, 2 1, 1 1),(-1 -1, -1 -2, -2 -1, -1 -1))' )";
    private static final byte[] VARBIN1 = new byte[] {1, 2, 3};
    private static final byte[] VARBIN2 = new byte[] {4, 5};
    private static final TimestampType TIME = new TimestampType("2017-07-19 01:05:06");

    /*
     * Schema:
     * ID      INTEGER NOT NULL PRIMARY KEY,
     * TINY    TINYINT,
     * SMALL   SMALLINT,
     * INT     INTEGER,
     * BIG     BIGINT,
     * NUM     FLOAT,
     * DEC     DECIMAL,
     * VCHAR_INLINE_MAX VARCHAR(63 BYTES),
     * VCHAR            VARCHAR(64 BYTES),
     * TIME    TIMESTAMP,
     * VARBIN1 VARBINARY(100),
     * VARBIN2 VARBINARY(100),
     * POINT1  GEOGRAPHY_POINT,
     * POINT2  GEOGRAPHY_POINT,
     * POLYGON GEOGRAPHY
     */
    private void generateData(String tableName, int count) throws NoConnectionsException, IOException, InterruptedException {
        for (int i = 0; i < count; i++) {
            double idouble = i * 1.1;
            m_client.callProcedure(new NullCallback(), tableName + ".insert",
                i,                                    // ID
                i % Byte.MAX_VALUE,                   // TINY
                i % Short.MAX_VALUE,                  // SMALL
                i,                                    // INT
                i,                                    // BIG
                idouble,                              // NUM
                new BigDecimal(idouble),              // DEC
                String.valueOf(i),                    // VCHAR_INLINE_MAX
                String.valueOf(i),                    // VCHAR
                TIME,                                 // TIME
                VARBIN1,                              // VARBIN1
                VARBIN2,                              // VARBIN2
                new GeographyPointValue(1, 2),
                new GeographyPointValue(3, 4),
                SIMPLE_POLYGON_WTK);
        }
        m_client.drain();
    }

    /**
     * Prints statistics about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults(String statsName) throws Exception {
        long thruput = 0;
        long invocErrs = 0, invocAbrts = 0, invocTimeOuts = 0;
        double avgLatcy = 0.0, k95pLatcy = 0.0, k99pLatcy = 0.0, internalLatcy = 0.0;
        long totalInvoc = 0;
        double duration = 0.0, minLatcy = 0.0, maxLatcy = 0.0;

        // Currently not using this for loop, but it could be added back,
        // if we choose to test against multiple clients
//        for (int i = 0; i < m_config.clientscount; i++) {
        ClientStats stats = m_fullStatsContext.fetchAndResetBaseline().getStats();

        thruput += stats.getTxnThroughput();
        invocErrs += stats.getInvocationErrors();
        invocAbrts += stats.getInvocationAborts();
        invocTimeOuts += stats.getInvocationTimeouts();

        long temp = stats.getInvocationsCompleted() + stats.getInvocationAborts() + stats.getInvocationErrors() + stats.getInvocationTimeouts();
        totalInvoc += temp;

        avgLatcy += stats.getAverageLatency() * (double) temp;
        k95pLatcy += stats.kPercentileLatency(0.95) * (double) temp;
        k99pLatcy += stats.kPercentileLatency(0.99) * (double) temp;
        internalLatcy += stats.getAverageInternalLatency() * (double) temp;
//        }

        avgLatcy = avgLatcy / (double) totalInvoc;
        k95pLatcy = k95pLatcy / (double) totalInvoc;
        k99pLatcy = k99pLatcy / (double) totalInvoc;
        internalLatcy = internalLatcy / (double) totalInvoc;
        duration = stats.getDuration();
        minLatcy = stats.kPercentileLatencyAsDouble(0.0);
        maxLatcy = stats.kPercentileLatencyAsDouble(1.0);


        // Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", thruput);
        System.out.printf("Average latency:               %,9.2f ms\n", avgLatcy);
        System.out.printf("Minimum latency:               %,9.2f ms\n", minLatcy);
        System.out.printf("Maximum latency:               %,9.2f ms\n", maxLatcy);
        System.out.printf("95th percentile latency:       %,9.2f ms\n", k95pLatcy);
        System.out.printf("99th percentile latency:       %,9.2f ms\n", k99pLatcy);

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);
        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", internalLatcy);

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Transaction Results");
        System.out.println(HORIZONTAL_RULE);

        FileWriter fw = null;
        UDFBenchmarkConfig config = (UDFBenchmarkConfig)m_config;

        if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
            fw = new FileWriter(config.statsfile);

            fw.append(String.format("%d,%.6f,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d\n",
                0,
                duration, // in milliseconds
                totalInvoc,
                minLatcy,
                maxLatcy,
                k95pLatcy,
                k99pLatcy,
                internalLatcy,
                0.0,
                0.0,
                invocErrs,
                invocAbrts,
                invocTimeOuts));

            fw.flush();
            fw.close();
        }
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link TheClientConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        UDFBenchmarkConfig config = new UDFBenchmarkConfig();
        config.parse(UDFBenchmark.class.getName(), args);

        UDFBenchmark app = new UDFBenchmark(config);
        app.run();
    }
}
