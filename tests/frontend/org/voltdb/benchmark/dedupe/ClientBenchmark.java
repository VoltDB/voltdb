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

package org.voltdb.benchmark.dedupe;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltTable;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;

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
        countDisplayNames[0] = "Insert() Calls";
        return countDisplayNames;
    }

    @Override
    protected String getApplicationName() {
        return "dedupe";
    }

    @Override
    protected String getSubApplicationName() {
        return "Client";
    }


    public void doOne() throws IOException {
        // Generate random data

        int max_playerId = 50000000;
        int max_gameId = 2;

        long playerId = rand.nextInt(max_playerId);
        long gameId = rand.nextInt(max_gameId);
        long socialId = 1l;
        long clientId = 1l;
        long visitTimeMillis = System.currentTimeMillis();

        boolean queued = false;

        while (!queued) {
            //long callTime = System.currentTimeMillis();

            queued = m_voltClient.callProcedure(new AsyncCallback(), "Insert", playerId, gameId, socialId, clientId, visitTimeMillis, visitTimeMillis);

            if (!queued) {
                try {
                    m_voltClient.backpressureBarrier();
                } catch (Exception e) {
                    e.printStackTrace();
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

