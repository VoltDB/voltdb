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

package org.voltdb.benchmark.twentyindex;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltTable;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

public class ClientBenchmark extends ClientMain {
    public static final AtomicLong globalMainId = new AtomicLong(1);
    public static final Random rand = new java.util.Random(1l);

    public static void main(String args[]) {
        org.voltdb.benchmark.ClientMain.main(ClientBenchmark.class, args, false);
    }

    public ClientBenchmark(String[] args) {
        super(args);
    }

    // Retrieved via reflection by BenchmarkController
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = ProjectBuilderX.class;

    // Retrieved via reflection by BenchmarkController
    //public static final Class<? extends ClientMain> m_loaderClass = anyLoader.class;
    public static final Class<? extends ClientMain> m_loaderClass = null;

    static class AsyncCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            final byte status = clientResponse.getStatus();

            if (status != ClientResponse.SUCCESS) {
                System.err.println("Failed to execute!!!");
                System.err.println(clientResponse.getException());
                System.err.println(clientResponse.getStatusString());
                System.exit(-1);
            } else {
                pClientCallback(clientResponse.getResults());
            }
        }

        protected void pClientCallback(VoltTable[] results) {
        };
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        String countDisplayNames[] = new String[1];
        countDisplayNames[0] = "Inserts";
        return countDisplayNames;
    }

    @Override
    protected String getApplicationName() {
        return "20 Indexes";
    }

    @Override
    protected String getSubApplicationName() {
        return "Client";
    }

    public void doOne() throws IOException {
        // Generate random data

        long currentMainId = globalMainId.get();
        long mainId;
        boolean queued = false;

        // Determine mainId
        int mainIdOdds = rand.nextInt(100);
        if (mainIdOdds < 80) {
            // same mainId
            mainId = currentMainId;
        } else if (mainIdOdds < 82) {
            // user next mainId
            mainId = globalMainId.getAndIncrement();
        } else if (mainIdOdds < 95) {
            // user last mainId
            mainId = Math.max(currentMainId - 1,1);
        } else if (mainIdOdds < 99) {
            // use random mainId 1 to 4 less than current mainId
            mainId = Math.max(currentMainId - rand.nextInt(4) - 1,1);
        } else {
            // use any random mainId (that has currently been used)
            mainId = Math.round(rand.nextDouble() * currentMainId);
        }

        StringBuilder sb;
        sb = new StringBuilder(4);
        for (int zz = 0; zz < 4; zz++) {
            sb.append((char)(rand.nextInt(26) + 97));
        }

        TimestampType ttEventTime = new TimestampType();
        long eventId = rand.nextInt(3000);
        long flag1 = rand.nextInt(2);
        long flag2 = rand.nextInt(2);
        String field1 = sb.toString();
        double field2 = rand.nextDouble() * 10000;
        double field3 = rand.nextDouble() * 100.0;
        double field4 = rand.nextDouble() * 100.0;
        double field5 = rand.nextDouble() * 100.0;
        String field6 = "BROKER 99";
        String field7 = "FIELD7-STRING";
        String field8 = "FIELD8-STRING";
        String field9 = "FIELD9-STRING";
        String field10 = "FIELD10-STRING";
        long field11 = rand.nextInt(5000);
        long field12 = rand.nextInt(20000);
        long field13 = rand.nextInt(30000);
        long field14 = rand.nextInt(40000);
        double field15 = rand.nextDouble() * 15812;
        double field16 = rand.nextDouble() * 15812;
        double field17 = rand.nextDouble() * 15812;
        double field18 = rand.nextDouble() * 15812;
        double field19 = rand.nextDouble() * 15812;
        double field20 = rand.nextDouble() * 15812;

        while (!queued) {
            long callTime = System.currentTimeMillis();

            queued = m_voltClient.callProcedure(new AsyncCallback(), "Insert", mainId, ttEventTime, eventId, flag1, flag2, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11, field12, field13, field14, field15, field16, field17, field18, field19, field20, callTime);

            if (!queued) {
                try {
                    m_voltClient.backpressureBarrier();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        m_counts[0].getAndIncrement();
    }

    @Override
    protected boolean runOnce() throws IOException {
        doOne();

        return true;
    }

    @Override
    protected void runLoop() {
        try {
            while(true) {
                doOne();
            }
        } catch (NoConnectionsException e) {
            return;
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.err);
            throw new RuntimeException(e);
        }
    }

}

