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

package org.voltdb.export;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.exportclient.SocketExporter;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class SocketExportTestServer extends Thread {

    private final ConcurrentMap<Long, AtomicLong> m_seenIds = new ConcurrentHashMap<Long, AtomicLong>();
    private final List<ClientConnectionHandler> m_clients = Collections.synchronizedList(new ArrayList<ClientConnectionHandler>());
    private ServerSocket ssocket;
    private final int m_port;
    private volatile boolean shuttingDown;
    private volatile boolean m_paused;

    public SocketExportTestServer(int port) {
        m_port = port;
        try {
            ssocket = new ServerSocket(port);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void close() throws IOException {
        if (ssocket == null) {
            return;
        }

        try {
            ssocket.close();
        } catch (Exception e) {}
        ssocket = null;
    }

    public void shutdown() throws IOException {
        shuttingDown = true;
        stopClients();
        close();
        m_seenIds.clear();
    }

    public void stopCurrentConnsAndPause() throws IOException {
        m_paused = true;
        stopClients();
        // close the socket and reopen to avoid new connections
        close();
    }

    public void unpause() {
        m_paused = false;
    }

    private void stopClients() throws IOException {
        synchronized(m_clients) {
            for (ClientConnectionHandler s : m_clients) {
                s.stopClient();
            }
            m_clients.clear();
        }
    }

    @Override
    public void run() {
        while (!shuttingDown) {
            if (m_paused) {
                try { Thread.sleep(250); } catch(InterruptedException e) { }
                continue;
            }
            try {
                if (ssocket == null) {
                    ssocket = new ServerSocket(m_port);
                }
                Socket clientSocket = ssocket.accept();
                synchronized(m_clients) {
                    if (!m_paused && !shuttingDown) {
                        ClientConnectionHandler ch = new ClientConnectionHandler(clientSocket);
                        m_clients.add(ch);
                        ch.start();
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception in socket server accept loop: " + ex.getMessage());
            }
        }
    }

    public void verifyExportedTuples(int expsize) {
        verifyExportedTuples(expsize, TimeUnit.MINUTES.toMillis(5));
    }

    public void verifyExportedTuples(int expsize, long waitTimeMs) {
        long end = System.currentTimeMillis() + waitTimeMs;
        boolean passed = false;
        while (true) {
            if (m_seenIds.size() == expsize) {
                passed = true;
                break;
            }
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                break;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
            }
        }
        System.out.println("Seen Id size is: " + m_seenIds.size() + " expected:" + expsize + " Passed: " + passed);
        if (!passed) { // Write more debug info if it failed
            long total = 0;
            for (AtomicLong al : m_seenIds.values()) {
                total += al.longValue();
            }
            System.out.println("Found total from values: " + total);
            System.out.println("keys: " + new TreeSet<Long>(m_seenIds.keySet()));
        }
        assertTrue(passed);
    }


    public class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        private boolean m_closed = false;
        final CSVParser m_parser = new CSVParser();
        public ClientConnectionHandler(Socket clientSocket) {
            m_clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(m_clientSocket.getInputStream()));
                OutputStream out = m_clientSocket.getOutputStream();
                while (!m_closed) {
                    String line = in.readLine();
                    //You should convert your data to params here.
                    if (line == null && m_closed) {
                        break;
                    }
                    if (line == null) {
                        try { Thread.sleep(100); } catch(InterruptedException e) { }
                        continue;
                    }
                    // handle sync_block message
                    if (line.equals(SocketExporter.SYNC_BLOCK_MSG)) {
                        out.write(48); // What we send doesn't matter. Send any byte as ack.
                        out.flush();
                        continue;
                    }
                    String parts[] = m_parser.parseLine(line);
                    if (parts == null) {
                        continue;
                    }
                    Long i = Long.parseLong(parts[0]);
                    if (m_seenIds.putIfAbsent(i, new AtomicLong(1)) != null) {
                        synchronized(m_seenIds) {
                            m_seenIds.get(i).incrementAndGet();
                        }
                    }
                }
                m_clientSocket.close();
            } catch (IOException ioe) {
                System.out.println("ClientConnection handler exited with IOException: " + ioe.getMessage());
            }
        }

        public void stopClient() throws IOException {
            m_closed = true;
            m_clientSocket.close();
        }
    }
}
