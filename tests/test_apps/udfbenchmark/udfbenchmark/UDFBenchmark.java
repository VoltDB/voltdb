/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.IOException;
import java.math.BigDecimal;

import org.voltdb.ClientAppBase;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;

public final class UDFBenchmark extends ClientAppBase {

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
