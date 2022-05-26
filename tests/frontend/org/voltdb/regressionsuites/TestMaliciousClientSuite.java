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

package org.voltdb.regressionsuites;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.voltdb.BackendTarget;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb_testprocs.regressionsuites.malicious.GoSleep;

import junit.framework.Test;

public class TestMaliciousClientSuite extends RegressionSuite {
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestMaliciousClientSuite(String name) {
        super(name);
    }

    @org.junit.Test
    public void testManyClientsComingAndGoing() throws Exception {
        for (int ii = 0; ii < 2000; ii++) {
            ArrayList<SocketChannel> channels = new ArrayList<SocketChannel>();
            for (int zz = 0; zz < 100; zz++) {
                channels.add(getClientChannel(true));
            }
            for (SocketChannel sc : channels) {
                sc.close();
            }
            System.out.printf("Ran through testManyClientsComingAndGoing loop %d times.\n", ii);
            System.out.flush();
        }
    }
//
//    /*
//     * Expect a variety of failure conditions like OOMm out of file descriptors etc.
//     * In reality this test doesn't do a good a job of checking that resources are being freed on the server. The main
//     * goal is to crash the server process one way or another via this behavior.
//     *
//     * This test can only be run to around 500 harassments. After that the server OOMs, but not because it isn't
//     * removing the data associated with old clients correctly. After a certain amount of pressure
//     * it stops detecting that client have disconnected and isn't removing them as fast as they connect.
//     */
//    @org.junit.Test(timeout=600000)
//    public void testManyClientsQueuingAndLeaving() throws Exception {
//        System.gc();
//        System.out.println("Have " + (Runtime.getRuntime().freeMemory() / 1024) + "kb available");
//        final ThreadLocal<ConnectionUtil.ExecutorPair> m_executors = new ThreadLocal<ConnectionUtil.ExecutorPair>() {
//            @Override
//            protected ConnectionUtil.ExecutorPair initialValue() {
//                return new ConnectionUtil.ExecutorPair();
//            }
//        };
//        final ExecutorService executor = Executors.newFixedThreadPool( 4, new ThreadFactory() {
//            private int harasserCount = 0;
//            @Override
//            public Thread newThread(Runnable r) {
//                return new Thread(Thread.currentThread().getThreadGroup(), r, "Harasser " + harasserCount++, 131072);
//            }
//        });
//
//        int numHarassments = 2000;
//        final int numRequests = 4000;
//        ArrayList<Future<Object>> harassments = new ArrayList<Future<Object>>();
//        for (int ii = 0; ii < numHarassments; ii++) {
//            harassments.add(executor.submit(new Callable<Object>() {
//
//                @Override
//                public Object call() throws Exception {
//                    final SocketChannel sc = getClientChannel(true);
//                    final ArrayList<Future<Long>> requests = new ArrayList<Future<Long>>();
//                    final ExecutorService m_executor = m_executors.get().m_writeExecutor;
//                    for (int ii = 0; ii < numRequests; ii++) {
//                        requests.add(ConnectionUtil.sendInvocation( m_executor, sc, "GoSleep", 0, 1, null));
//                    }
//                    for (Future<Long> request : requests) {
//                        request.get();
//                    }
//                    sc.close();
//                    return null;
//                }
//            }));
//        }
//
//        int harassmentsComplete = 0;
//        for (Future<Object> harassment : harassments) {
//            harassmentsComplete++;
//            if (harassmentsComplete % 100 == 0) {
//                System.out.println("Completed " + harassmentsComplete + " harassments with "
//                        + (Runtime.getRuntime().freeMemory() /1024) + " kb free memory");
//            }
//            harassment.get();
//        }
//
//        executor.shutdown();
//        executor.awaitTermination( 1, TimeUnit.DAYS);
//    }
//
//    /**
//     * Test for backpressure generated by the DTXN because there are too many transactions in flight
//     * @throws Exception
//     */
//    public void testDTXNBackPressure() throws Exception {
//        System.gc();
//        System.out.println("Start the test with " + Runtime.getRuntime().freeMemory() + " free");
//        byte junkData[] = new byte[2048];
//        long numRequests = 40000;
//        long sleepTime = 20000;
//        final ArrayDeque<Future<Long>> pendingRequests = new ArrayDeque<Future<Long>>();
//        final ArrayDeque<Future<ClientResponse>> pendingResponses = new ArrayDeque<Future<ClientResponse>>();
//        final ArrayDeque<SocketChannel> connections = new ArrayDeque<SocketChannel>();
//        for (int ii = 0; ii < 4; ii++) {
//            final SocketChannel channel = getClientChannel();
//            connections.add(channel);
//        }
//
//        /**
//         * Queue a request and and the read for the response.
//         * The parameter to GoSleep will cause the first invocation to not return
//         * for a while.
//         * Most of these invocations should never make it into the server due
//         * to DTXN backpressure
//         */
//        for (int ii = 0; ii < numRequests; ii++) {
//            final SocketChannel channel = connections.poll();
//            pendingRequests.offer(ConnectionUtil.sendInvocation(channel, "GoSleep", ii == 0 ? sleepTime : 0, 0, junkData));
//            pendingResponses.offer(ConnectionUtil.readResponse(channel));
//            connections.offer(channel);
//        }
//        System.out.println("Sent " + numRequests + " requests with the first requesting a sleep of " +
//                (sleepTime / 1000) + " seconds");
//
//        /**
//         * Give the TCP stack time to transfer as many invocations as the server will accept
//         */
//        Thread.sleep(10000);
//
//        System.out.println("Slept 10 seconds so the server could transfer invocations");
//
//        /**
//         * Count the number of requests that didn't make it onto the wire due to backpressure
//         */
//        long pendingRequestCount = 0;
//        Future<Long> f = null;
//        while ( (f = pendingRequests.poll()) != null) {
//            if (!f.isDone()) {
//                pendingRequestCount++;
//            } else {
//                f.get();
//            }
//        }
//        pendingRequests.clear();
//
//        System.out.println("Counted " + pendingRequestCount + " requests that didn't make it on the wire");
//
//        /**
//         * The number should be quite large
//         */
//        assertTrue(pendingRequestCount > 30000);
//
//        /**
//         * Now ensure that the backpressure condition can end by waiting for all the responses.
//         */
//        long responseCount = 0;
//        int lastPercentComplete = 0;
//        Future<ClientResponse> response = null;
//        while ( (response = pendingResponses.poll()) != null) {
//            response.get();
//            responseCount++;
//            int percentComplete = (int)Math.floor((responseCount / (double)numRequests) * 100);
//            if (percentComplete > lastPercentComplete) {
//                lastPercentComplete = percentComplete;
//                if (lastPercentComplete % 5 == 0) {
//                    System.out.println(lastPercentComplete + "% complete reading responses with " +  Runtime.getRuntime().freeMemory() + " free");
//                }
//            }
//        }
//
//        System.out.println("Read all the responses for the transactions that couldn't previously make it on the wire");
//
//        assertEquals(responseCount, numRequests);
//
//        /**
//         * Now queue and read another round just to prove it still works
//         */
//        for (final SocketChannel channel : connections) {
//            ConnectionUtil.sendInvocation(channel, "GoSleep", 0).get();
//        }
//
//        for (final SocketChannel channel : connections) {
//            ConnectionUtil.readResponse(channel).get();
//        }
//
//        System.out.println("Was able to queue and read across the other 4 connections");
//    }
//
//    /**
//     * Can't get this to pass reliably
//     */
////    /**
////     * Test for backpressure because a client is not reading his responses. This is difficult because
////     * the server will boot connections that don't read. Only the individual rude client should be blocked.
////     * A polite client should still be able to work.
////     * @throws Exception
////     */
////    public void testIndividualClientBackPressure() throws Exception {
////        System.gc();
////        final ArrayDeque<Future<Long>> pendingRudeRequests = new ArrayDeque<Future<Long>>();
////        final ArrayDeque<Future<Long>> pendingPoliteRequests = new ArrayDeque<Future<Long>>();
////        final ArrayDeque<Future<ClientResponse>> pendingRudeResponses = new ArrayDeque<Future<ClientResponse>>();
////        final ArrayDeque<Future<ClientResponse>> pendingPoliteResponses = new ArrayDeque<Future<ClientResponse>>();
////        final SocketChannel rudeChannel = getClientChannel();
////        /**
////         * Makes it easier to control when data is pulled from the remote side. This value would be
////         * tuned very large otherwise.
////         */
////        rudeChannel.socket().setReceiveBufferSize(16384);
////        System.out.println("Rude channel is called " + rudeChannel.socket().getLocalSocketAddress());
////        final SocketChannel politeChannel = getClientChannel();
////        System.out.println("Polite channel is called " + politeChannel.socket().getLocalSocketAddress());
////        final int numRequests = 15000;
////        final int sleepTime = 0;
////        int rudeReadsSent = 0;
////
////        /**
////         * Send a ton of invocations on the rude channel that will complete immediately and return a relatively large
////         * result table to help fill I/O buffers.
////         */
////        for (int ii = 0; ii < numRequests; ii++) {
////            pendingRudeRequests.add(ConnectionUtil.sendInvocation(rudeChannel, "GoSleep", sleepTime, 1, null));
////            if (ii % 600 == 0) {
////                pendingRudeResponses.add(ConnectionUtil.readResponse(rudeChannel));
////                rudeReadsSent++;
////            }
////        }
////
////        System.out.println("Sent " + numRequests + " requests with the first requesting a sleep of " +
////                (sleepTime / 1000) + " seconds and " + rudeReadsSent + " reads sent to avoid getting booted");
////
////        /**
////         * Give the server time to finish processing the previous requests.
////         */
////        for(int ii = 0; ii < 100; ii++) {
////            Thread.sleep(100);
////            for (int zz = 0; zz < 10; zz++) {
////                pendingRudeResponses.add(ConnectionUtil.readResponse(rudeChannel));
////                rudeReadsSent++;
////            }
////        }
////
////        System.out.println("Slept 10 seconds so the server could transfer invocations and sent " + rudeReadsSent +
////                " reads to avoid getting booted");
////
////        /**
////         * Count the number of requests that didn't make it onto the wire due to backpressure
////         */
////        long pendingRequestCount = 0;
////        for (Future<Long> f : pendingRudeRequests) {
////            if (!f.isDone()) {
////                pendingRequestCount++;
////            } else {
////                f.get();
////            }
////        }
////
////        System.out.println("Counted " + pendingRequestCount + " requests that didn't make it on the wire");
////
////        /**
////         * The number should be quite large
////         */
////        assertTrue(pendingRequestCount > 0);
////
////        System.out.println("Using a  polite channel to send " + numRequests);
////        /**
////         * Now use the polite channel to send requests. These should have no trouble going through the system since
////         * this is also queuing the reads for the responses.
////         */
////        for (int ii = 0; ii < numRequests; ii++) {
////            pendingPoliteRequests.add(ConnectionUtil.sendInvocation(politeChannel, "GoSleep", sleepTime, 0, null));
////            pendingPoliteResponses.add(ConnectionUtil.readResponse(politeChannel));
////            if (ii % 600 == 0) {
////                pendingRudeResponses.add(ConnectionUtil.readResponse(rudeChannel));
////                rudeReadsSent++;
////                Thread.yield();
////            }
////        }
////
////        int numPoliteResponses = 0;
////        int lastPercentPoliteResponses = 0;
////        int rudeReadsSentHere = 0;
////        long startTime = System.currentTimeMillis() - 100;
////        System.out.println("Waiting for all polite requests and responses to make it on the wire");
////        Future<Long> request = null;
////        while ((request = pendingPoliteRequests.poll()) != null) {
////            request.get();
////            pendingPoliteResponses.poll().get();
////            numPoliteResponses++;
////            int percentComplete = (int)Math.floor((numPoliteResponses / (double)numRequests) * 100);
////            if (percentComplete > lastPercentPoliteResponses) {
////                lastPercentPoliteResponses = percentComplete;
////                if (lastPercentPoliteResponses % 10 == 0) {
////                    System.out.println(lastPercentPoliteResponses + "% complete reading polite responses");
////                    System.out.println("Free memory " + Runtime.getRuntime().freeMemory());
////                }
////            }
////
////            final long now = System.currentTimeMillis();
////            if (now - startTime > 100) {
////                //System.out.println("Sending rude reads " + now);
////                startTime = now;
////                for (int zz = 0; zz < 10; zz++) {
////                    pendingRudeResponses.add(ConnectionUtil.readResponse(rudeChannel));
////                    rudeReadsSentHere++;
////                }
////            }
////        }
////
////        rudeReadsSent += rudeReadsSentHere;
////        System.out.println("All polite requests and responses made it onto the wire and had to send " + rudeReadsSentHere +
////                " rude reads to avoid getting booted");
////
////        System.out.println("Queuing reads for all rude requests");
////        /**
////         * Now make sure that if the rude channel becomes polite it can get everything through
////         */
////        for (; rudeReadsSent < numRequests; rudeReadsSent++) {
////            pendingRudeResponses.add(ConnectionUtil.readResponse(rudeChannel));
////        }
////
////        int numRudeRequests = 0;
////        int lastPercentRudeRequests = 0;
////        while ((request = pendingRudeRequests.poll()) != null) {
////            request.get();
////            pendingRudeResponses.poll().get();
////            numRudeRequests++;
////            int percentComplete = (int)Math.floor((numRudeRequests / (double)numRequests) * 100);
////            if (percentComplete > lastPercentRudeRequests) {
////                lastPercentRudeRequests = percentComplete;
////                if (lastPercentRudeRequests % 10 == 0) {
////                    System.out.println(lastPercentRudeRequests + "% complete sending rude requests and receiving rude responses");
////                    System.out.println("Free memory " + Runtime.getRuntime().freeMemory());
////                }
////            }
////        }
////        pendingRudeRequests.clear();
////    }
//
//    /**
//     * Check that the server enforces a limit on the maximum number of connections
//     * @throws Exception
//     */
//    public void testMaxNumConnections() throws Exception {
//        final ExecutorService executor = Executors.newFixedThreadPool( 8, new ThreadFactory() {
//            private int harasserCount = 0;
//            @Override
//            public Thread newThread(Runnable r) {
//                return new Thread(Thread.currentThread().getThreadGroup(), r, "Harasser " + harasserCount++, 131072);
//            }
//        });
//
//        ArrayList<Future<SocketChannel>> attempts = new ArrayList<Future<SocketChannel>>();
//
//        final int connectionsToAttempt = 20000;
//        for (int ii = 0; ii < connectionsToAttempt; ii++) {
//            attempts.add(executor.submit(new Callable<SocketChannel>() {
//                @Override
//                public SocketChannel call() throws Exception {
//                    return getClientChannel(true);
//                }
//            }));
//        }
//
//        for (Future<SocketChannel> attempt : attempts) {
//            try {
//                attempt.get();
//            } catch (Exception e) {
//
//            }
//        }
//
//        int successfulAttempts = 0;
//        for (Future<SocketChannel> attempt : attempts) {
//            try {
//                final SocketChannel sc = attempt.get();
//                successfulAttempts++;
//                sc.close();
//            } catch (Exception e) {
//
//            }
//        }
//
//        executor.shutdown();
//        executor.awaitTermination(1, TimeUnit.DAYS);
//        assertTrue(successfulAttempts < 10000);
//        System.out.println("Had " + successfulAttempts + " successful connection attempts");
//    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMaliciousClientSuite.class);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedure(GoSleep.class);

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        VoltServerConfig config = new LocalCluster("malicious-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: Local Cluster (of processes)
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("malicious-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
