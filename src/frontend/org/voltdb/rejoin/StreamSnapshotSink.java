/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.rejoin;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;

/**
 * Takes the decompressed snapshot blocks and pass them to EE. Once constructed,
 * caller should initialize it and then poll(). If poll() returns null and
 * isEOF() is true, that means end of stream has reached, no more snapshot
 * blocks will arrive. It's safe to move on.
 *
 * This class is not thread-safe.
 */
public class StreamSnapshotSink implements RejoinSiteProcessor {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final long m_HSId;

    private ServerSocketChannel m_serverSocket = null;
    private SocketChannel m_sock = null;
    private final Semaphore m_initializationLock = new Semaphore(0);
    private boolean m_connectionAccepted = false;
    private final Thread m_acceptThread = new Thread(new Runnable() {
        @Override
        public void run() {
            acceptConnection();
        }
    });

    private StreamSnapshotDataReceiver m_in = null;
    private Thread m_inThread = null;
    private StreamSnapshotAckSender m_ack = null;
    private Thread m_ackThread = null;
    private boolean m_EOF = false;
    // Schema of the table currently streaming
    private byte[] m_schema = null;
    // buffer for a single block
    private ByteBuffer m_buffer = null;
    private long m_bytesReceived = 0;

    public StreamSnapshotSink(long HSId) {
        m_HSId = HSId;
    }

    @Override
    public Pair<List<byte[]>, Integer> initialize() {
        List<byte[]> addresses = new ArrayList<byte[]>();
        try {
            Pair<ServerSocketChannel, Boolean> binded = bindSocket();
            m_serverSocket = binded.getFirst();
            Boolean allInterfaces = binded.getSecond();

            if (allInterfaces) {
                addresses = getLocalAddress();
            } else {
                addresses.add(m_serverSocket.socket().getInetAddress().getAddress());
            }
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("Unable to get local addresses", true, e);
        }

        m_acceptThread.start();

        return Pair.of(addresses, m_serverSocket.socket().getLocalPort());
    }

    @Override
    public boolean isEOF() {
        return m_EOF;
    }

    @Override
    public void close() {
        m_acceptThread.interrupt();
        try {
            m_acceptThread.join();
        } catch (InterruptedException e1) {}

        if (m_in != null) {
            m_in.close();
            // No need to join in thread, once socket is closed, it will exit
        }

        if (m_ack != null) {
            m_ack.close();
            try {
                m_ackThread.join();
            } catch (InterruptedException e) {}
        }

        m_in = null;
        m_ack = null;

        try {
            m_sock.close();
            m_serverSocket.close();
        } catch (IOException e) {
            rejoinLog.error("Failed to close sockets used for rejoin: " +
                    e.getMessage());
        }
    }

    /**
     * Wait for an incoming connection, then set up the thread to receive
     * snapshot stream. If no connection in 5 seconds, crash.
     */
    private void acceptConnection() {
        if (m_serverSocket == null) {
            rejoinLog.error("Unable to accept rejoin connections, not bound to any port");
            return;
        }

        try {
            final long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 5000) {
                try {
                    m_sock = m_serverSocket.accept();
                    if (m_sock != null) {
                        break;
                    }
                } catch (IOException e) {
                    rejoinLog.error("Exception while attempting to accept recovery connection", e);
                    m_serverSocket.close();
                }
                Thread.yield();
            }
            if (m_sock == null) {
                VoltDB.crashLocalVoltDB("Timed out waiting for connection from source partition",
                        false, null);
            }
            m_sock.configureBlocking(true);
            m_sock.socket().setTcpNoDelay(true);
        } catch (ClosedByInterruptException ignore) {
            return;
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("Failed to accept rejoin connection",
                                    true, e);
        } finally {
            try {
                assert(m_sock != null);
                rejoinLog.debug("Closing listening socket, m_sock is " + m_sock);
                m_serverSocket.close();
            } catch (IOException ignore) {}
        }

        rejoinLog.debug("Accepted a stream snapshot connection");
        m_in = new StreamSnapshotDataReceiver(m_sock);
        m_inThread = new Thread(m_in, "Snapshot data receiver");
        m_inThread.setDaemon(true);
        m_ack = new StreamSnapshotAckSender(m_sock, m_HSId);
        m_ackThread = new Thread(m_ack, "Snapshot ack sender");
        m_inThread.start();
        m_ackThread.start();
        m_initializationLock.release();
    }

    /**
     * @throws IOException
     */
    private static Pair<ServerSocketChannel, Boolean> bindSocket() throws IOException {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        InetSocketAddress sockAddr = null;
        boolean allInterfaces = false;
        String internalInterface = VoltDB.instance().getConfig().m_internalInterface;
        if (internalInterface != null && !internalInterface.isEmpty()) {
            rejoinLog.debug("An internal interface was specified (" + internalInterface + ")" +
                    " binding to an ephemeral port to receive recovery connection");
            sockAddr = new InetSocketAddress( internalInterface, 0);
        } else {
            rejoinLog.debug("No internal interface was specified. Binding to " +
                    "all interfaces with an ephemeral port to receive " +
                    "recovery connection");
            allInterfaces = true;
            sockAddr = new InetSocketAddress(0);
        }
        ssc.socket().bind(sockAddr);
        return Pair.of(ssc, allInterfaces);
    }

    private static List<byte[]> getLocalAddress() throws SocketException {
        List<byte[]> addresses = new ArrayList<byte[]>();
        List<byte[]> loopbackInterfaces = new ArrayList<byte[]>();
        List<InetAddress> loopbackAddresses = new ArrayList<InetAddress>();

        /*
         * If no internal interface was specified, bind on everything and then
         * ship over every possible public address. Yikes!
         */
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface intf = interfaces.nextElement();
            if (!intf.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addressesEnum = intf.getInetAddresses();
            while (addressesEnum.hasMoreElements()) {
                InetAddress address = addressesEnum.nextElement();
                if (intf.isLoopback()) {
                    loopbackAddresses.add(address);
                    loopbackInterfaces.add(address.getAddress());
                } else {
                    addresses.add(address.getAddress());
                }
            }
        }
        addresses.addAll(loopbackInterfaces);

        return addresses;
    }

    private ByteBuffer getOutputBuffer(int length) {
        if (m_buffer == null || m_buffer.capacity() < length) {
            m_buffer = ByteBuffer.allocate(length);
        }
        m_buffer.clear();
        return m_buffer;
    }

    /**
     * Assemble the chunk so that it can be used to construct the VoltTable that
     * will be passed to EE.
     *
     * @param buf
     * @return
     */
    private ByteBuffer getNextChunk(ByteBuffer buf) {
        int length = m_schema.length + buf.remaining();
        int rowCount = buf.getInt(buf.limit() - 4);
        buf.limit(buf.limit() - 4);
        // skip partition ID, partition ID CRC, and content CRC
        buf.position(buf.position() + 12);

        ByteBuffer outputBuffer = getOutputBuffer(length);
        outputBuffer.put(m_schema);
        outputBuffer.putInt(rowCount);
        outputBuffer.put(buf);
        outputBuffer.flip();
        return outputBuffer;
    }

    @Override
    public Pair<Integer, ByteBuffer> take() throws InterruptedException {
        if (!m_connectionAccepted) {
            m_initializationLock.acquire();
        }
        m_connectionAccepted = true;

        if (m_in == null || m_ack == null) {
            // terminated already
            return null;
        }

        Pair<Integer, ByteBuffer> result = null;
        while (!m_EOF) {
            BBContainer container = m_in.take();
            result = processMessage(container);
            if (result != null) {
                break;
            }
        }

        return result;
    }

    @Override
    public Pair<Integer, ByteBuffer> poll() {
        if (!m_connectionAccepted && !m_initializationLock.tryAcquire()) {
            return null;
        }
        m_connectionAccepted = true;

        if (m_in == null || m_ack == null) {
            // not initialized yet or terminated already
            return null;
        }

        BBContainer container = m_in.poll();
        return processMessage(container);
    }

    /**
     * Process a message pulled off from the network thread, and discard the
     * container once it's processed.
     *
     * @param container
     * @return The processed message, or null if there's no data block to return
     *         to the site.
     */
    private Pair<Integer, ByteBuffer> processMessage(BBContainer container) {
        if (container == null) {
            return null;
        }

        try {
            ByteBuffer block = container.b;
            byte typeByte = block.get(StreamSnapshotDataTarget.typeOffset);
            StreamSnapshotMessageType type = StreamSnapshotMessageType.values()[typeByte];
            if (type == StreamSnapshotMessageType.END) {
                // End of stream, no need to ack this buffer
                m_EOF = true;
                return null;
            } else if (type == StreamSnapshotMessageType.SCHEMA) {
                block.position(block.position() + 1);
                m_schema = new byte[block.remaining()];
                block.get(m_schema);
                return null;
            }

            // It's normal snapshot data afterwards

            final int blockIndex = block.getInt(StreamSnapshotDataTarget.blockIndexOffset);
            final int tableId = block.getInt(StreamSnapshotDataTarget.tableIdOffset);

            if (m_schema == null) {
                VoltDB.crashLocalVoltDB("No schema for table with ID " + tableId,
                                        false, null);
            }

            // Get the byte buffer ready to be consumed
            block.position(StreamSnapshotDataTarget.contentOffset);
            ByteBuffer nextChunk = getNextChunk(block);
            m_bytesReceived += nextChunk.remaining();

            // Queue ack to this block
            m_ack.ack(blockIndex);

            return Pair.of(tableId, nextChunk);
        } finally {
            container.discard();
        }
    }

    @Override
    public long bytesTransferred() {
        return m_bytesReceived;
    }
}
