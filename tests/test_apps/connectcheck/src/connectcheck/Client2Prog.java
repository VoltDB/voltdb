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

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;

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
public class Client2Prog {

    Config config;
    Client2 client;
    AtomicInteger conn = new AtomicInteger();
    AtomicInteger fail = new AtomicInteger();
    AtomicBoolean reconnectPending = new AtomicBoolean();

    static class Config extends CLIConfig {
        @Option(desc = "Comma-separated list of Volt servers.")
        String servers = "localhost";
    }

    public Client2Prog(Config config) {
        this.config = config;
        Client2Config clientConfig = new Client2Config()
            .connectionUpHandler((h,p) -> print("[up: %s %d]", h, p))
            .connectionDownHandler((h,p) -> { print("[down: %s %d]", h, p); maybeReconnect(); });
        this.client = ClientFactory.createClient(clientConfig);
    }

    void connect() throws Exception {
        print("Connecting to VoltDB ...");
        parallelConnect();
        print("%d connected, %d failed", conn.get(), fail.get());
        if (conn.get() == 0)
            System.exit(2);
    }

    void maybeReconnect() {
        if (!reconnectPending.compareAndSet(false, true))
            return;
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                    int cnx = client.connectedHosts().size();
                    if (cnx == 0) {
                        // manual reconnection is not a good idea with topo-aware on,
                        // but we're trying to make bad things happen
                        print("Reconnecting to VoltDB ...");
                        for (;;) {
                            parallelConnect();
                            print("%d connected, %d failed", conn.get(), fail.get());
                            if (conn.get() > 0)
                                break;
                            Thread.sleep(1000);
                        }
                    }
                }
                catch (Exception ex) {
                    print("Abandoning reconnect, %s", ex.getMessage());
                }
                finally {
                    reconnectPending.set(false);
                }
            }
        }).start();
    }

    void parallelConnect() {
        conn.set(0); fail.set(0);
        String[] servers = config.servers.split(",");
        ArrayList<CompletableFuture<Void>> future = new ArrayList<>();
        for (int i=0; i<servers.length; i++) {
            future.add(client.connectAsync(servers[i]));
        }
        try {
            CompletableFuture<?>[] fa = future.toArray(new CompletableFuture<?>[0]);
            CompletableFuture.allOf(fa).get();
        }
        catch (Exception ex) {
            // ignore
        }
        int d = 0, x = 0;
        for (CompletableFuture<Void> f : future) {
            if (f.isDone()) d++;
            if (f.isCompletedExceptionally()) x++;
        }
        conn.set(d-x); fail.set(x);
    }

    void run() throws Exception {
        connect();

        print("Waiting indefinitely (^C to end) ...");
        long tick = System.currentTimeMillis();
        for (;;) {
            Thread.sleep(1000);
            long now = System.currentTimeMillis();
            if (now - tick >= 60_000) {
                int cnx = client.connectedHosts().size();
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
        config.parse(Client2Prog.class.getName(), args);
        Client2Prog prog = new Client2Prog(config);
        prog.run();
    }
}
