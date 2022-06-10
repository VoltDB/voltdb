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

package connectcheck;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStatusListenerExt;

/**
 * The point of this program is to attempt to cause the
 * problematic situation of having more than one connection
 * to the same server.
 *
 * There's an easy case where the app makes duplicate
 * connections, and a trickier case where there's a race
 * between the app making a connection, and the topo-aware
 * feature also making a connection.
 *
 * The latter requires manually bouncing servers, and
 * paying close attention to the output from this program.
 * There's no automatic handling: it's a tool for causing
 * a condition, and not a test.
 */
public class ClientProg {

    Config config;
    Client client;
    AtomicInteger conn = new AtomicInteger();
    AtomicInteger fail = new AtomicInteger();

    static class Config extends CLIConfig {
        @Option(desc = "Comma-separated list of Volt servers.")
        String servers = "localhost";
    }

    class Listener extends ClientStatusListenerExt {
        public void connectionCreated(String hostname, int port, AutoConnectionStatus status) {
            if (status == AutoConnectionStatus.SUCCESS)
                print("[up: %s %d]", hostname, port);
        }
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            print("[down: %s %d]", hostname, port);
            if (connectionsLeft == 0)
                reconnect(); // this is bad practice; we're intentionally trying to create a race
        }
    }

    public ClientProg(Config config) {
        this.config = config;
        ClientConfig clientConfig = new ClientConfig("", "", new Listener());
        clientConfig.setTopologyChangeAware(true);
        this.client = ClientFactory.createClient(clientConfig);
    }

    void connect() throws Exception {
        print("Connecting to VoltDB ...");
        print(config.servers);
        parallelConnect().await();
        print("%d connected, %d failed", conn.get(), fail.get());
        if (conn.get() == 0)
            System.exit(2);
    }

    void reconnect() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                print("Reconnecting to VoltDB ...");
                try {
                    for (;;) {
                        parallelConnect().await();
                        print("%d connected, %d failed", conn.get(), fail.get());
                        if (conn.get() > 0)
                            return;
                        Thread.sleep(1000);
                    }
                }
                catch (Exception ex) {
                    print("Abandoning reconnect, %s", ex.getMessage());
                }
            }
        }).start();
    }

    CountDownLatch parallelConnect() {
        conn.set(0); fail.set(0);
        String[] servers = config.servers.split(",");
        CountDownLatch remaining = new CountDownLatch(servers.length);
        for (String s : servers) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        print(" %s", s);
                        client.createConnection(s);
                        conn.incrementAndGet();
                        print("  %s ok", s);
                    }
                    catch (Exception ex) {
                        fail.incrementAndGet();
                        print("  %s failed: %s", s, ex.getMessage());
                    }
                    remaining.countDown();
                }
            }).start();
        }
        return remaining;
    }

    void run() throws Exception {
        connect();

        print("Waiting indefinitely (^C to end) ...");
        long tick = System.currentTimeMillis();
        for (;;) {
            Thread.sleep(1000);
            long now = System.currentTimeMillis();
            if (now - tick >= 60_000) {
                int cnx = client.getConnectedHostList().size();
                print("Waiting, %d cnx", cnx);
                tick = now;
            }
        }
    }

    void print(String fmt, Object... args) {
        String s = String.format(fmt, args);
        System.out.printf("%s  %s%n", Instant.now(), s);
    }

    public static void main(String... args) throws Exception {
        Config config = new Config();
        config.parse(ClientProg.class.getName(), args);
        ClientProg prog = new ClientProg(config);
        prog.run();
    }
}
