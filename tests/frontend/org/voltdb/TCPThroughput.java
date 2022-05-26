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

import java.io.IOException;
import java.nio.*;
import java.nio.channels.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashSet;
import java.util.ArrayList;

/**
 * A class implementing a throughput test across several TCP sockets. This class implements both the client and the server
 *
 */
public class TCPThroughput {

    /**
     * A port is a wrapper around a socket channel. The default behavior of this port when handling work is
     * to read a large quantity of data if it is available and write a large quantity of data if the channel has capacity.
     *
     */
    private static class Port {

        /**
         * Dummy ByteBuffer with no data
         */
        private ByteBuffer m_buffer = ByteBuffer.allocateDirect(expectedPacketSize);

        /**
         * Set to true if a socket channel operation throws an IOException. Causes the socket channel to be closed
         * by the selector thread when this port is reached in the change list.
         */
        public volatile boolean isDead = false;

        public final SocketChannel m_channel;
        public final int m_port;
        final SelectionKey m_selectionKey;

        public Port(SocketChannel channel, SelectionKey key, int port) {
            m_channel = channel;
            m_port = port;
            m_selectionKey = key;
        }

        //protected int m_readyOps = 0;

        public void handleWork() {
            try {
                m_buffer.clear();
                if (m_selectionKey.isReadable()) {
                    bytesReceived.addAndGet(m_channel.read(m_buffer));
                }
                m_buffer.clear();
                if (m_selectionKey.isWritable()) {
                    bytesSent.addAndGet(m_channel.write(m_buffer));
                }
            } catch (IOException e) {
                e.printStackTrace();
                isDead = true;
            } finally {
                addToChangeList(this);
            }
        }
    }

    /*
     * Selected ports is used as the point of synchronization between the selection
     * thread and the watchdog thread. It also protects m_ports which is shared between
     * the two threads.
     */
    private static final HashSet<Port> selectedPorts = new HashSet<Port>();
    private static final ArrayList<Port> m_ports = new ArrayList<Port>();

    /*
     * Various fields containing statistical information and keeping track of the last time a message was received.
     */
    public static boolean messageReceived = false;
    public static long firstMessageReceived = 0;
    public static long lastMessageReceived = 0;
    public static final AtomicLong bytesReceived = new AtomicLong(0);
    public static final AtomicLong bytesSent = new AtomicLong(0);

    /*
     * Structures for the change list
     */
    private static final ArrayList<Port> m_selectorUpdates_1 = new ArrayList<Port>();
    private static final ArrayList<Port> m_selectorUpdates_2 = new ArrayList<Port>();
    private static ArrayList<Port> m_activeUpdateList = new ArrayList<Port>();

    /*
     * Lock used to protect the selector change lists that are shared between the ports
     * and the selector thread.
     */
    private static final Object m_lock = new Object();

    private static final int port = 29600;
    private static final int numPorts = 30;
    static int expectedPacketSize = 600;

    private static ServerSocketChannel servers[];
    private static Selector selector;
    private static ExecutorService executor = Executors.newFixedThreadPool(6);

    private static volatile boolean selectorWoke = false;
    private static volatile boolean shouldContinue = true;

    private static int seconds = 60;
    private static String addressString = "localhost";
    private static InetAddress address;

    /**
     * Called by the selection thread to process any ports added to the changelist. Closes ports that have died due to an IOException
     * and sets the interest ops for ports that have finished handling work.
     */
    protected static void installInterests() {

        // swap the update lists to avoid contention while
        // draining the requested values. also guarantees
        // that the end of the list will be reached if code
        // appends to the update list without bound.
        ArrayList<Port> oldlist;
        synchronized(m_lock) {
            if (m_activeUpdateList == m_selectorUpdates_1) {
                oldlist = m_selectorUpdates_1;
                m_activeUpdateList = m_selectorUpdates_2;
            }
            else {
                oldlist = m_selectorUpdates_2;
                m_activeUpdateList = m_selectorUpdates_1;
            }
        }

        for (Port i : oldlist) {
            try {
                if (i.isDead) {
                    System.out.println("Closing port " + i.m_port);
                    synchronized (selectedPorts) {
                        m_ports.remove(i);
                    }
                    i.m_selectionKey.cancel();
                    try {
                        i.m_channel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    i.m_selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                }
            } catch(CancelledKeyException ex) {
                // continue if a key is canceled.
            }
        }
        oldlist.clear();
    }

    /**
     * Method for a port to add itself to the change list so the selection thread can updates its interest ops.
     * @param port
     */
    private static void addToChangeList(Port port) {
        synchronized (m_lock) {
            m_activeUpdateList.add(port);
        }
    }

    /*
     * A server opens up some server sockets and accepts connections on them. It terminates when no messages have been received for 5 seconds.
     * A client opens up some connections and sends messages for a fixed period of time.
     */
    public static void main(String[] args) {
        boolean runServer = false;
        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                if (parts[0].equals("server")) {
                    runServer = true;
                }
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            } else if (parts[0].equals("address")) {
                addressString = parts[1];
            } else if (parts[0].equals("seconds")) {
                seconds = Integer.parseInt(parts[1]);
            }
        }

        try {
            address = InetAddress.getByName(addressString);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            selector = Selector.open();
        } catch (IOException e1) {
            e1.printStackTrace();
            System.exit(-1);
        }

        if (runServer) {
            servers = new ServerSocketChannel[numPorts];
            for (int ii = 0; ii < numPorts; ii++) {
                try {
                    servers[ii] = ServerSocketChannel.open();
                    servers[ii].configureBlocking(false);
                    servers[ii].socket().bind(new InetSocketAddress(port + ii));
                    SelectionKey serverKey = servers[ii].register(selector,
                            SelectionKey.OP_ACCEPT);
                    serverKey.attach(servers[ii]);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        } else {
            for (int ii = 0; ii < numPorts; ii++) {
                Socket tempsocket = null;
                SocketChannel tempsocketchannel = null;
                try {
                    tempsocketchannel = SocketChannel.open();
                    tempsocket = tempsocketchannel.socket();
                    tempsocket.setTcpNoDelay(false);
                    tempsocket.setReceiveBufferSize(16777216);
                    tempsocket.setSendBufferSize(16777216);
                    tempsocketchannel.configureBlocking(true);
                    tempsocketchannel.connect(new InetSocketAddress( address, port + ii));
                    tempsocketchannel.configureBlocking(false);
                    SelectionKey key = tempsocketchannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    Port p = new Port(tempsocketchannel, key, ii);
                    key.attach(p);
                    m_ports.add(p);
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    System.exit(-1);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        if (!runServer) {
            /*
             * Timer thread that terminates the Selector loop
             */
            new Thread() {
                public void run() {
                    try {
                        Thread.sleep(seconds * 1000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    shouldContinue = false;
                }
            }.start();
        }

        /*
         * Watchdog thread that checks that each port is selected at least once every 60 seconds. Also checks to make sure
         * that at least one port has been selected every 60 seconds.
         */
        new Thread() {
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (true) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (selectedPorts) {
                        for (Port port : m_ports) {
                            if (!selectedPorts.contains(port)) {
                                System.out.println("A port(" + port.m_port + ") interestOps(" + port.m_selectionKey.interestOps() + ")has not been selected for read/write for 60 seconds!");
                            }
                        }
                        if (!selectorWoke) {
                            System.out.println("Selector hasn't returned for 60 seconds!");
                        }
                        selectorWoke = false;
                        selectedPorts.clear();
                    }
                }
            }
        }.start();

        while (shouldContinue) {
            /*
             * Check if no messages have been received for 5 seconds. This is the indication that no more messages are
             * forthcoming and that it is time to print stats and terminate.
             */
            if (messageReceived == true && runServer) {
                long now = System.currentTimeMillis();
                if (now - lastMessageReceived > 5000) {
                    long delta = (lastMessageReceived - firstMessageReceived) / 1000;
                    if (delta == 0) { delta = 1; }
                    System.out
                            .println("TCPThroughputReceiver result:\n\tExpected packet size == " + expectedPacketSize + "\n\tmessagesReceived == "
                                    + bytesReceived.get()
                                    / expectedPacketSize
                                    + "\n\tbytesReceived == "
                                    + bytesReceived.get() +
                                    "\n\tbytesSent == " + bytesSent.get()
                                    + "\nmegabytes/sec == " + ((bytesReceived.get() + bytesSent.get()) / delta / 1024 / 1024)
                                    + "\nmegabits/sec == " + ((bytesReceived.get() + bytesSent.get()) / delta / 1024 / 1024 * 8)
                                    + "\n\nReceived first message "
                                    + firstMessageReceived
                                    + " and last message "
                                    + lastMessageReceived
                                    + " delta is "
                                    + delta);
                    System.exit(0);
                }
            }

            try {
                selector.selectNow();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            selectorWoke = true;
            installInterests();

            for (Iterator<SelectionKey> it = selector.selectedKeys().iterator(); it
                    .hasNext();) {
                SelectionKey key = it.next();
                it.remove();

                if (key.isAcceptable()) {
                    try {
                        ServerSocketChannel server = (ServerSocketChannel)key.attachment();
                        int port = 0;
                        for (int ii = 0; ii < servers.length; ii++) {
                            if (server == servers[ii]) {
                                port = ii;
                            }
                        }
                        SocketChannel client = server.accept();
                        Socket tempsocket = client.socket();
                        tempsocket.setReceiveBufferSize(16777216);
                        tempsocket.setSendBufferSize(16777216);
                        assert client != null;
                        tempsocket.setTcpNoDelay(false);

                        client.configureBlocking(false);
                        SelectionKey clientKey = client.register(selector,
                                SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        Port newPort = new Port(client, clientKey, port);
                        synchronized (selectedPorts) {
                            m_ports.add(newPort);
                        }
                        clientKey.attach(newPort);
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                } else {
                    assert key.isReadable() || key.isWritable();
                    if (!messageReceived && key.isReadable()) {
                        messageReceived = true;
                        System.out.println("First message received");
                        firstMessageReceived = System.currentTimeMillis();
                    }
                    if (key.isReadable()) {
                        lastMessageReceived = System.currentTimeMillis();
                    }
                    final Port port = (Port) key.attachment();
                    synchronized (selectedPorts) {
                        selectedPorts.add(port);
                    }
                    key.interestOps(0);
                    executor.execute(new Runnable() {
                        public void run() {
                            port.handleWork();
                        }
                    });
                }
            }
        }
    }
}
