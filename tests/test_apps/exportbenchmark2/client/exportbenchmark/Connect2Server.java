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

package exportbenchmark2.client.exportbenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.voltdb.client.Client;

public class Connect2Server {
    final static Client client = null;
    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *ß
     * @param server hostname:port or just hostname (hostname can be ip).
     * @return
     */

    static void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (IOException e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (InterruptedException interrupted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static Client connect(String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB.... Servers: " + servers);

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                    } catch (Throwable ex) {
                        System.out.println("Uncaught exception: " + ex.getMessage() + ex);
                        printJStack();
                        System.exit(-1);
                    }
                }
            }).start();
        }
        // block until all have connected
        connections.await();
        return client;
    }

    static public void printJStack() {

        Map<String, List<String>> deduped = new HashMap<String, List<String>>();

        // collect all the output, but dedup the identical stack traces
        for (Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
            Thread t = e.getKey();
            String header = String.format("\"%s\" %sprio=%d tid=%d %s",
                    t.getName(),
                    t.isDaemon() ? "daemon " : "",
                    t.getPriority(),
                    t.getId(),
                    t.getState().toString());

            String stack = "";
            for (StackTraceElement ste : e.getValue()) {
                stack += "    at " + ste.toString() + "\n";
            }

            if (deduped.containsKey(stack)) {
                deduped.get(stack).add(header);
            }
            else {
                ArrayList<String> headers = new ArrayList<String>();
                headers.add(header);
                deduped.put(stack, headers);
            }
        }

        String logline = "";
        for (Entry<String, List<String>> e : deduped.entrySet()) {
            for (String header : e.getValue()) {
                logline += "\n" + header + "\n";
            }
            logline += e.getKey();
        }
        System.out.println("Full thread dump:\n" + logline);
    }

}
