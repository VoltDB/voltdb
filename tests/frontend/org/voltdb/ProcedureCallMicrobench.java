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

package org.voltdb;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

public class ProcedureCallMicrobench {

    static abstract class Runner {
        public abstract void run(Client client) throws Exception;
    };

    public static void main(String[] args) throws Exception {
        int siteCount = 1;

        TPCCProjectBuilder pb = new TPCCProjectBuilder();
        pb.addDefaultSchema();
        pb.addDefaultPartitioning();
        pb.addProcedures(EmptyProcedure.class, MultivariateEmptyProcedure.class);

        pb.compile("procedureCallMicrobench.jar", siteCount, 0);

        ServerThread server = new ServerThread("procedureCallMicrobench.jar",
                BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();

        int[] clientCounts = new int[] {};
        if (args.length >= 1 && !args[0].equals("${clients}")) {
            String[] clientCountString = args[0].split("\\s+");
            clientCounts = new int[clientCountString.length];
            for (int i = 0; i < clientCountString.length; i++) {
                clientCounts[i] = Integer.parseInt(clientCountString[i]);
            }
        }

        for (int clientCount : clientCounts) {
            for (int varmode = 0; varmode < 2; varmode++) {
                final Date date = new Date();
                final String name = varmode == 0 ? "EmptyProcedure"
                        : "MultivariateEmptyProcedure";
                final Runner runner = varmode == 0 ? new Runner() {
                    @Override
                    public void run(Client client) throws Exception {
                        client.callProcedure(name, 0L);
                    }
                } : new Runner() {
                    @Override
                    public void run(Client client) throws Exception {
                        client.callProcedure(name, 0L, 0L, 0L,
                                "String c_first", "String c_middle",
                                "String c_last", "String c_street_1",
                                "String c_street_2", "String d_city",
                                "String d_state", "String d_zip",
                                "String c_phone", date, "String c_credit", 0.0,
                                0.0, 0.0, 0.0, 0L, 0L, "String c_data");
                    }
                };

                // trigger classloading a couple times
                {
                    ClientConfig config = new ClientConfig("program", "none");
                    Client client = ClientFactory.createClient(config);
                    client.createConnection("localhost");
                    for (int i = 0; i < 10000; i++)
                        client.callProcedure("EmptyProcedure", 0L);
                }

                ExecutorService executor = Executors
                        .newFixedThreadPool(clientCount);
                ArrayList<Future<Integer>> futures = new ArrayList<Future<Integer>>(
                        clientCount);
                final CyclicBarrier barrier = new CyclicBarrier(clientCount + 1);
                final long stopTime = System.currentTimeMillis() + 2000;

                for (int i = 0; i < clientCount; i++) {
                    futures.add(executor.submit(new Callable<Integer>() {
                        public Integer call() {
                            try {
                                ClientConfig config = new ClientConfig("program", "none");
                                Client client = ClientFactory.createClient(config);
                                client.createConnection("localhost");

                                int count = 0;
                                barrier.await();

                                for (count = 0; count % 10 != 0
                                        || System.currentTimeMillis() < stopTime; count++) {
                                    runner.run(client);
                                }
                                return count;
                            } catch (Exception ex) {
                                ex.printStackTrace();
                                throw new RuntimeException(ex);
                            }
                        }
                    }));
                }

                barrier.await();
                final long startTime = System.currentTimeMillis();
                int count = 0;
                for (Future<Integer> future : futures) {
                    count += future.get();
                }

                double time = stopTime - startTime;
                System.out.println(name + " with " + clientCount + " clients: "
                        + count + " xacts in " + time + " ms => "
                        + (time / count) + " ms/xact => " + (count / time)
                        * 1000 + "tps");
            }
        }
        System.exit(0);
    }
}
