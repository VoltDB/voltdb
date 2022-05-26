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

package org.voltdb;

import java.io.File;
import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class LatencyManualTest {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        try {
            String simpleSchema =
                    "create stream blah partition on column ival (" +
                    "ival bigint default 0 not null, " +
                    "PRIMARY KEY(ival));";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.addStmtProcedure("Insert", "insert into blah values (?);");

            LocalCluster cluster = new LocalCluster("latencycheck.jar",
                    2, 1, 0, BackendTarget.NATIVE_EE_JNI);
            cluster.setHasLocalServer(true);
            boolean success = cluster.compile(builder);
            assert(success);

            cluster.startUp(true);

            final String listener = cluster.getListenerAddresses().get(0);
            final Client client = ClientFactory.createClient();
            client.createConnection(listener);

            long iterations = 10000;
            long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                client.callProcedure("Insert", i);
            }
            long end = System.nanoTime();

            double ms = (end - start) / 1000000.0;

            client.close();
            cluster.shutDown();

            System.out.printf("Avg latency was %.3f ms.\n", ms / iterations);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            File jar = new File("latencycheck.jar");
            jar.delete();
        }
    }

}
