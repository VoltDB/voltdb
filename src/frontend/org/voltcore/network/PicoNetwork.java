/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.VoltNetworkPool.IOStatsIntf;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.LatencyWatchdog;
import org.voltcore.utils.Pair;

/**
 * The least amount of code possible to produce a working
 * NIO selector loop. Simpler than VoltNetwork + VoltPort
 * because it doesn't try to tackle backpressure or tracking
 * multiple sockets
 */
public class PicoNetwork implements Runnable, Connection, IOStatsIntf
{
    private static final VoltLogger m_logger = new VoltLogger(VoltNetwork.class.getName());
    protected static final VoltLogger networkLog = new VoltLogger("NETWORK");

    protected final Selector m_selector;
    protected final NetworkDBBPool m_pool = new NetworkDBBPool(64);
    protected final NIOReadStream m_readStream = new NIOReadStream();
    protected PicoNIOWriteStream m_writeStream;
    protected final ConcurrentLinkedQueue<Runnable> m_tasks = new ConcurrentLinkedQueue<>();
    protected volatile boolean m_shouldStop = false;
    protected long m_messagesRead;
    protected int m_interestOps = 0;
    protected final SocketChannel m_sc;
    private final String m_hostDisplayName;
    protected final SelectionKey m_key;
    protected InputHandler m_ih;

    private final Thread m_thread;
    private final String m_remoteHostname;
    private final InetSocketAddress m_remoteSocketAddress;
    private final String m_remoteSocketAddressString;
    private final String m_remoteHostAndAddressAndPort;
    private final String m_threadName;
    private Set<Long> m_verbotenThreads;

    /**
     * Start this VoltNetwork's thread. populate the verbotenThreads set
     * with the id of the thread that is created
     */
    public void start(InputHandler ih, Set<Long> verbotenThreads) {
        m_ih = ih;
        m_verbotenThreads = verbotenThreads;
        startSetup();
        m_thread.start();
    }

    protected void startSetup() {
        m_writeStream = new PicoNIOWriteStream();
    }

    /**
     * Create a pico network thread
     * @param sc  SocketChannel
     * @param hostDisplayName
     */
    public PicoNetwork(SocketChannel sc,
                       String hostDisplayName) {
        m_sc = sc;
        m_hostDisplayName = hostDisplayName;
        InetSocketAddress remoteAddress = (InetSocketAddress)sc.socket().getRemoteSocketAddress();
        m_remoteSocketAddress = remoteAddress;
        m_remoteSocketAddressString = remoteAddress.getAddress().getHostAddress();
        String remoteHostAndAddressAndPort = "/" + m_remoteSocketAddressString + ":" + m_remoteSocketAddress.getPort();
        String remoteHost = ReverseDNSCache.hostnameOrAddress(m_remoteSocketAddress.getAddress());

        String remoteHostname = null;
        if (!remoteHost.equals(m_remoteSocketAddress.getAddress().getHostAddress())) {
            remoteHostname = remoteHost;
            remoteHostAndAddressAndPort = remoteHost + remoteHostAndAddressAndPort;
        }

        m_remoteHostname = remoteHostname;
        m_remoteHostAndAddressAndPort = remoteHostAndAddressAndPort;
        m_threadName = remoteHost;

        m_thread = new Thread(this, "Pico Network - " + m_threadName);
        m_thread.setDaemon(true);
        try {
            sc.configureBlocking(false);
            // Cannot use sc.setOption(StandardSocketOptions.TCP_NODELAY, true)
            // because that is available only in java 7. This class is included in
            // client build with -source 1.6.
            sc.socket().setTcpNoDelay(true);
            m_selector = Selector.open();
            m_interestOps = SelectionKey.OP_READ;
            m_key = m_sc.register(m_selector, m_interestOps);
        } catch (IOException ex) {
            m_logger.fatal(null, ex);
            throw new RuntimeException(ex);
        }
    }

    /** Instruct the network to stop after the current loop */
    public void shutdownAsync() throws InterruptedException {
        m_shouldStop = true;
        if (m_thread != null) {
            m_selector.wakeup();
        }
    }

    //Track how busy the thread is and spin once
    //if there is always work
    protected boolean m_hadWork = false;

    @Override
    public void run() {
        m_verbotenThreads.add(Thread.currentThread().getId());
        try {
            m_ih.starting(this);
            m_ih.started(this);
            while (m_shouldStop == false) {
                LatencyWatchdog.pet();

                //Choose a non-blocking select if things are busy
                if (m_hadWork) {
                    m_selector.selectNow();
                } else {
                    m_selector.select();
                }

                m_hadWork = false;
                Runnable task = null;
                while ((task = m_tasks.poll()) != null) {
                    m_hadWork = true;
                    task.run();
                }
                dispatchReadStream();
                drainWriteStream();
            }
        } catch (CancelledKeyException e) {
            networkLog.warn(
                    "Had a cancelled key exception for "
                            + m_threadName, e);
        } catch (IOException e) {
            final String trimmed = e.getMessage() == null ? "" : e.getMessage().trim();
            if ((e instanceof IOException && (trimmed.equalsIgnoreCase("Connection reset by peer") || trimmed.equalsIgnoreCase("broken pipe"))) ||
                    e instanceof AsynchronousCloseException ||
                    e instanceof ClosedChannelException ||
                    e instanceof ClosedByInterruptException) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug( "PicoNetwork died, probably of natural causes", e);
                }
            } else {
                networkLog.error( "PicoNetwork died due to an unexpected exception", e);
            }
        } catch (Throwable ex) {
            m_logger.error(null, ex);
            m_shouldStop = true;
        } finally {
            m_verbotenThreads.remove(Thread.currentThread().getId());
            try {
                p_shutdown();
            } catch (Throwable t) {
                m_logger.error("Error shutting down Volt Network", t);
            }
        }
    }

    protected void dispatchReadStream() throws IOException {
        if (readyForRead()) {
            if (fillReadStream() > 0) m_hadWork = true;
            ByteBuffer message;

            /*
             * Process all the buffered bytes and retrieve as many messages as possible
             * and pass them off to the input handler.
             */
            try {
                while ((message = m_ih.retrieveNextMessage( m_readStream )) != null) {
                    m_ih.handleMessage( message, this);
                    m_messagesRead++;
                }
            }
            /*
             * An established PicoNetwork connection should only receive valid messages sent by another
             * VoltDB host. Dump the start of the message to determine if this a corrupted message or
             * to identify the message type that the sender might have constructed incorrectly.
             */
            catch (VoltProtocolHandler.BadMessageLength e) {
                networkLog.error(String.format("Bad message length from %s", m_remoteHostAndAddressAndPort), e);
                networkLog.error(VoltProtocolHandler.formatBadLengthDump("Bad message bytes", e));
                throw e;
            }
        }
    }

    private final int fillReadStream() throws IOException {
        if (m_shouldStop)
            return 0;

        final int read = m_readStream.read(m_sc, Integer.MAX_VALUE, m_pool);

        if (read == -1) {
            m_interestOps &= ~SelectionKey.OP_READ;
            m_key.interestOps(m_interestOps);

            if (m_sc.socket().isConnected()) {
                try {
                    m_sc.socket().shutdownInput();
                } catch (SocketException e) {
                    //Safe to ignore to these
                }
            }

            m_shouldStop = true;
            safeStopping();

            /*
             * Allow the write queue to drain if possible
             */
            enableWriteSelection();
        }
        return read;
    }

    protected void drainWriteStream() throws IOException {
        /*
         * Drain the write stream
         */
        if (m_writeStream.serializeQueuedWrites(m_pool) != 0) m_hadWork = true;
        if (m_writeStream.drainTo(m_sc) > 0) m_hadWork = true;
        if (m_writeStream.isEmpty()) {
            disableWriteSelection();

            if (m_shouldStop) {
                m_sc.close();
                unregistered();
            }
        } else {
            enableWriteSelection();
        }
    }

    private boolean m_alreadyStopped = false;
    private void safeStopped() {
        if (!m_alreadyStopped) {
            m_alreadyStopped = true;
            m_ih.stopped(this);
        }
    }

    private boolean m_alreadyStopping = false;
    protected void safeStopping() {
        if (!m_alreadyStopping) {
            m_alreadyStopping = true;
            m_ih.stopping(this);
        }
    }

    /**
     * Called when unregistration is complete and the Connection can no
     * longer be interacted with.
     *
     * Various error paths fall back to unregistering so it can happen multiple times and is really
     * annoying. Suppress it here with a flag
     */
    void unregistered() {
        try {
            if (!m_alreadyStopped) {
                try {
                    safeStopping();
                } finally {
                    try {
                        m_writeStream.shutdown();
                    } finally {
                        m_readStream.shutdown();
                    }
                }
            }
        } finally {
            networkLog.debug("Closing channel " + m_threadName);
            try {
                m_sc.close();
            } catch (IOException e) {
                networkLog.warn(e);
            }
        }
    }

    private void p_shutdown() {
        try {
            safeStopping();
        } finally {
            try {
                safeStopped();
            } finally {
                try {
                    m_readStream.shutdown();
                } finally {
                    try {
                        m_writeStream.shutdown();
                    } finally {
                        try {
                            m_pool.clear();
                        } finally {
                            try {
                                try {
                                    m_selector.close();
                                } finally {
                                    m_sc.close();
                                }
                            } catch (IOException e) {
                                m_logger.error(null, e);
                            }
                        }
                    }
                }
            }
        }
    }

    private Map<Long, Pair<String, long[]>> getIOStatsImpl(boolean interval) {
        final HashMap<Long, Pair<String, long[]>> retval =
                new HashMap<Long, Pair<String, long[]>>();
            final long read = m_readStream.getBytesRead(interval);
            final long writeInfo[] = m_writeStream.getBytesAndMessagesWritten(interval);
            final long messagesRead = m_messagesRead;
            retval.put(
                    m_ih.connectionId(),
                    Pair.of(
                            m_hostDisplayName,
                            new long[]{
                                    read,
                                    messagesRead,
                                    writeInfo[0],
                                    writeInfo[1]}));
            retval.put(
                    -1L,
                    Pair.of(
                            "GLOBAL",
                            new long[] {
                                    read,
                                    messagesRead,
                                    writeInfo[0],
                                    writeInfo[1] }));
            return retval;
    }

    @Override
    public Future<Map<Long, Pair<String, long[]>>> getIOStats(final boolean interval) {
        Callable<Map<Long, Pair<String, long[]>>> task = new Callable<Map<Long, Pair<String, long[]>>>() {
            @Override
            public Map<Long, Pair<String, long[]>> call() throws Exception {
                return getIOStatsImpl(interval);
            }
        };

        FutureTask<Map<Long, Pair<String, long[]>>> ft = new FutureTask<Map<Long, Pair<String, long[]>>>(task);

        m_tasks.offer(ft);
        m_selector.wakeup();

        return ft;
    }

    @Override
    public WriteStream writeStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NIOReadStream readStream() {
        return m_readStream;
    }

    @Override
    public void disableReadSelection() {
        throw new UnsupportedOperationException();
    }

    public void disableWriteSelection() {
        if ((m_interestOps & SelectionKey.OP_WRITE) != 0) {
            m_interestOps &= ~SelectionKey.OP_WRITE;
            m_key.interestOps(m_interestOps);
        }
    }

    public void enableWriteSelection() {
        if ((m_interestOps & SelectionKey.OP_WRITE) == 0) {
            m_interestOps |= SelectionKey.OP_WRITE;
            m_key.interestOps(m_interestOps);
        }
    }

    @Override
    public void enableReadSelection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHostnameAndIPAndPort() {
        return m_remoteHostAndAddressAndPort;
    }

    @Override
    public String getHostnameOrIP() {
        if (m_remoteHostname != null) {
            return m_remoteHostname;
        } else {
            return m_remoteSocketAddressString;
        }
    }

    @Override
    public String getHostnameOrIP(long clientHandle) {
        return getHostnameOrIP();
    }

    @Override
    public int getRemotePort() {
        return m_remoteSocketAddress.getPort();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return m_remoteSocketAddress;
    }

    @Override
    public long connectionId() {
        return m_ih.connectionId();
    }

    @Override
    public long connectionId(long clientHandle) {
        return connectionId();
    }

    @Override
    public void queueTask(Runnable r) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> unregister() {
        throw new UnsupportedOperationException();
    }

    public void enqueue(final DeferredSerialization ds) {
        m_tasks.offer(new Runnable() {
            @Override
            public void run() {
                m_writeStream.enqueue(ds);
            }
        });
        m_selector.wakeup();
    }

    public void enqueue(final ByteBuffer buf) {
        m_tasks.offer(new Runnable() {
            @Override
            public void run() {
                m_writeStream.enqueue(buf);
            }
        });
        m_selector.wakeup();
    }

    public FutureTask<Void> enqueueAndDrain(final ByteBuffer buf) {
        Callable<Void> task = new Callable<Void>() {
            public Void call() throws Exception {
                m_writeStream.enqueue(buf);
                drainWriteStream();
                return null;
            }
        };
        FutureTask<Void> ft = new FutureTask<Void>(task);
        m_tasks.offer(ft);
        m_selector.wakeup();
        return ft;
    }

    boolean readyForRead() {
        return (m_key.readyOps() & SelectionKey.OP_READ) != 0 && (m_interestOps & SelectionKey.OP_READ) != 0;
    }
}
